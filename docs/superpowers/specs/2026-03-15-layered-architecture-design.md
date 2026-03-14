# NovaStor Layered Architecture Specification

## Overview

NovaStor is a unified Kubernetes-native storage system. All data flows through a strict four-layer architecture. Layers MUST NOT be collapsed, bypassed, or combined. Each layer has exactly one responsibility.

```
┌─────────────────────────────────────────────────────────┐
│  Presentation Layer                                      │
│  NVMe-oF bdev  │  NFS server  │  S3 gateway             │
│  Thin protocol translation only — no replication logic   │
└──────────────────────────┬──────────────────────────────┘
                           │ read/write chunks
┌──────────────────────────▼──────────────────────────────┐
│  Chunk Engine                                            │
│  Content-addressed 4MB immutable chunks                  │
│  CRUSH placement + protection scheme                     │
│  Replication fan-out OR Reed-Solomon encoding             │
│  Owner fans out to replicas via gRPC (Rust-to-Rust)      │
└──────────────────────────┬──────────────────────────────┘
                           │ SPDK bdev I/O
┌──────────────────────────▼──────────────────────────────┐
│  Backend Engine                                          │
│  File: file on mounted FS → SPDK AIO bdev                │
│  LVM:  SPDK lvol store on unbound NVMe                   │
│  Raw:  SPDK NVMe bdev on unbound NVMe (no filesystem)    │
│  All produce SPDK bdevs — uniform interface to chunks     │
└─────────────────────────────────────────────────────────┘

  Policy Engine (control plane — NOT in data path)
  Monitors actual vs desired state
  Triggers repairs, rebalancing, rebuilds
```

**Hot data path:** Presentation → Chunk Engine → Backend. Three layers. The Policy Engine is NEVER in the data path.

---

## Layer 1: Backend Engine

### Purpose

Provides raw storage capacity as SPDK bdevs. The backend engine is a resource pool. It knows nothing about chunks, volumes, replication, or erasure coding.

### StoragePool CRD

The StoragePool CRD defines:
- **Backend type**: File, LVM, or Raw
- **Device list**: which devices on which nodes are assigned to this pool
- **One backend type per device**: a device can only belong to one pool
- **Multiple pools per node**: a node can contribute different devices to different pools

Example:
```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: fast-nvme-pool
spec:
  backendType: raw
  devices:
    - node: worker-21
      path: /dev/nvme0n1
    - node: worker-22
      path: /dev/nvme0n1
  protection:
    # Default protection for volumes in this pool (can be overridden per-volume)
    mode: replication
    replicaCount: 3
```

### Backend Types

All three backends produce SPDK bdevs. The chunk engine consumes them identically.

#### File Backend
- **Input**: Directory path on a mounted filesystem (e.g., `/var/lib/novastor/data`)
- **How it works**: Creates files within the directory, wraps each with an SPDK AIO bdev
- **Kernel driver**: Required — the NVMe device stays kernel-mounted with a filesystem
- **Use case**: Simplest to set up. No device unbinding needed. Good for mixed-use nodes.

#### LVM Backend
- **Input**: An unbound NVMe device (e.g., `/dev/nvme0n1` with PCI address `0000:04:00.0`)
- **How it works**: Creates an SPDK lvol store on the NVMe bdev, carves out lvols
- **Kernel driver**: NOT used — device must be unbound from kernel NVMe driver and bound to `vfio-pci` or `uio_pci_generic`
- **Use case**: Thin provisioning, native SPDK snapshots/clones (copy-on-write), efficient space management

#### Raw Backend
- **Input**: An unbound NVMe device — direct block device, NO filesystem
- **How it works**: Creates an SPDK NVMe bdev directly on the device
- **Kernel driver**: NOT used — device must be unbound from kernel NVMe driver and bound to `vfio-pci` or `uio_pci_generic`
- **Use case**: Maximum performance. No filesystem overhead, no lvol overhead. The chunk engine manages all space allocation.

### Device Setup

The Go agent is responsible for device preparation:

| Backend | Device setup |
|---------|-------------|
| File | Nothing — device stays kernel-mounted. Agent ensures the directory exists. |
| LVM | Agent unbinds device from kernel NVMe driver, binds to vfio-pci. Then instructs dataplane to create lvol store. |
| Raw | Agent unbinds device from kernel NVMe driver, binds to vfio-pci. Then instructs dataplane to create NVMe bdev. |

### What the Backend Engine Does NOT Do

- No chunk management
- No content addressing
- No replication
- No erasure coding
- No placement decisions
- No volume-level awareness (it provides bdevs, not volumes)

---

## Layer 2: Chunk Engine

### Purpose

Organizes raw bdev capacity into content-addressed 4MB immutable chunks. Handles placement (CRUSH), replication fan-out, and erasure coding. This is the core of NovaStor.

### Chunk Properties

- **Size**: 4MB (constant, not configurable in v1)
- **ID**: SHA-256 hash of content (content-addressed)
- **Checksum**: CRC-32C, verified on every read
- **Immutability**: Chunks are never modified after creation. Updates create new chunks.
- **Deduplication**: Free — same content produces same chunk ID, stored once

### Volumes

A volume is an ordered mapping of offsets to chunk IDs:

```
Volume "my-volume" (1GB):
  chunk_index 0 → sha256:abc123...   (offset 0–4MB)
  chunk_index 1 → sha256:def456...   (offset 4MB–8MB)
  chunk_index 2 → None               (never written, reads as zeros)
  ...
  chunk_index 255 → sha256:789xyz... (offset 1020MB–1024MB)
```

Gaps (None entries) are unwritten regions that read as zeros. This is inherently thin-provisioned.

### CRUSH Map

The CRUSH map is the single source of truth for:
- **Placement**: which nodes hold which chunks for a volume
- **Protection scheme**: replication (factor=N) or erasure coding (K+M) per volume

CRUSH map lifecycle:
- **Authoritative source**: Metadata service (Raft/BadgerDB)
- **Local cache**: Every chunk engine subscribes to updates and caches locally
- **Hot-path lookups**: Chunk engine uses the local cache — no network round-trip on I/O

### Protection Schemes (per-volume)

Protection is set per volume (not per pool, though the pool provides a default).

#### Replication
- Full chunk copies on N nodes
- Write: owner fans out full chunk to all replica nodes via gRPC
- Ack: after quorum (majority) acknowledge
- Read: from best available replica (local preferred)

#### Erasure Coding (Reed-Solomon)
- Chunk is RS-encoded into K data shards + M parity shards
- Example: 4+2 means 4 data shards + 2 parity shards across 6 nodes
- Write: owner encodes, distributes shards to nodes per CRUSH map via gRPC
- Ack: after sufficient shards are written
- Read: reconstruct from any K shards
- Storage overhead: (K+M)/K (e.g., 1.5x for 4+2 vs 3x for replication factor=3)

### Write Path (Owner Node)

1. Presentation layer sends write request to chunk engine
2. Chunk engine splits data into 4MB chunk-aligned pieces
3. For each chunk:
   a. Compute SHA-256 → chunk ID
   b. Compute CRC-32C checksum
   c. Dedup check: if chunk ID already exists, skip (return existing ID)
   d. Look up CRUSH map for placement + protection scheme
   e. **If replication**: write to local backend bdev, fan out full chunk to replica nodes' chunk engines via gRPC, ack after quorum
   f. **If erasure coding**: RS-encode into shards, distribute shards to nodes via gRPC, ack after sufficient shards written
4. Update volume offset → chunk ID mapping

### Read Path

1. Presentation layer sends read request to chunk engine
2. Chunk engine resolves offset → chunk ID from volume mapping
3. If chunk is local: read from backend bdev, verify CRC-32C
4. If chunk is remote: fetch from best replica via gRPC, verify CRC-32C
5. If erasure-coded: read K shards, reconstruct, verify

### Inter-Node Communication

- **Protocol**: gRPC (Rust-to-Rust)
- **Proto definitions**: `api/proto/` (shared with Go)
- **Operations**: chunk write, chunk shard write, chunk read, chunk shard read, health/heartbeat

### What the Chunk Engine Does NOT Do

- No protocol translation (that's the presentation layer)
- No NVMe-oF target management (presentation layer)
- No policy decisions (policy engine)
- No device management (backend engine / Go agent)

---

## Layer 3: Policy Engine

### Purpose

Control plane component that monitors the cluster and ensures actual state matches desired state. It is NEVER in the hot data path.

### Responsibilities

- **Health monitoring**: tracks which nodes are up, which replicas are healthy, which shards are intact
- **Repair**: when a replica is lost (node down, disk failure), triggers rebuild on a new node
- **Rebalancing**: when nodes are added or removed, redistributes chunks for even capacity usage
- **CRUSH map updates**: when topology changes, computes new CRUSH map and writes to metadata service
- **Degradation detection**: identifies volumes operating below their desired protection level

### Interaction Model

- Reads health and state from metadata service and chunk engines
- Writes updated CRUSH maps to metadata service
- Chunk engines pick up changes via subscription
- Does NOT directly read or write chunk data

### What the Policy Engine Does NOT Do

- No I/O operations of any kind
- No replication fan-out (that's the chunk engine)
- No erasure coding (that's the chunk engine)
- No placement decisions on the hot path (CRUSH map is pre-computed)
- No protocol translation (presentation layer)
- No device management (Go agent)

---

## Layer 4: Presentation Layer

### Purpose

Thin protocol translation. Exposes chunk engine volumes to consumers via standard storage protocols. Does nothing else.

### Presenters

| Presenter | Protocol | Consumers | Translation |
|-----------|----------|-----------|------------|
| NVMe-oF bdev | NVMe-oF/TCP | CSI (block volumes) | NVMe-oF block I/O → chunk engine read/write |
| NFS server | NFS | File gateway (SharedFilesystem) | NFS ops → chunk engine read/write + inode metadata |
| S3 gateway | S3 HTTP API | Object clients | S3 GET/PUT → chunk engine read/write + object metadata |

### NVMe-oF Target Model (ANA)

- **Owner node**: ANA optimized — active NVMe-oF target, handles all I/O
- **Replica nodes**: ANA non-optimized — standby targets, activated on failover only
- **One active target per volume at any time**

### What the Presentation Layer Does NOT Do

- No replication
- No erasure coding
- No placement decisions
- No chunk management or content addressing
- No policy enforcement
- No device management

---

## Dataplane Communication

### Single Protocol: gRPC

The Rust dataplane exposes ONE gRPC server that handles two categories of calls:

#### 1. Local Management (Go agent → Rust dataplane)
- Backend initialization and teardown
- Volume create/delete/resize
- NVMe-oF target create/delete/ANA state
- Device setup commands (create AIO bdev, create lvol store, etc.)
- Health and stats queries
- CRUSH map updates

#### 2. Remote Replication (Rust chunk engine → remote Rust chunk engine)
- Chunk write (full chunk for replication)
- Chunk shard write (encoded shard for erasure coding)
- Chunk read (fetch from remote replica)
- Chunk shard read (fetch shard for reconstruction)
- Health and heartbeat between chunk engines

### What Goes Away

- **JSON-RPC on Unix socket** (`/var/tmp/novastor-spdk.sock`) — replaced by gRPC
- **SPDK native RPC socket** (`/var/tmp/spdk.sock`) — all SPDK operations wrapped in our gRPC service
- Go agent NEVER communicates with SPDK directly

### Proto Definitions

All `.proto` files live in `api/proto/` — single source of truth. Both Go and Rust generate code from the same protos.

---

## Go Agent Responsibilities

### Management and Config ONLY — Never Touches Data

#### Node Setup
- **Hugepage allocation**: configures hugepages for SPDK DMA requirements
- **Device unbinding/binding**: for LVM and Raw backends, unbinds NVMe devices from kernel driver, binds to vfio-pci
- **Waits for Rust dataplane**: ensures dataplane is ready before accepting requests

#### Runtime Management (via gRPC to local Rust dataplane)
- Backend initialization per StoragePool device assignments
- Volume lifecycle (create/delete/resize) on behalf of CSI
- NVMe-oF target management (create/delete/ANA state changes)
- Health reporting to metadata service
- Node registration and heartbeats

#### Kubernetes Coordination
- Watches StoragePool CRDs for device assignments
- Reports node topology to metadata service
- Coordinates with controller/operator for scheduling

#### Quorum / Split-Brain Prevention
- Acts as a quorum member for the local dataplane
- The Rust dataplane must maintain communication with its Go agent to confirm it's still part of the cluster
- If the dataplane loses contact with the agent (or the agent loses contact with the metadata service), the dataplane **fences itself** — stops accepting writes to prevent split-brain
- The agent is the dataplane's link to cluster consensus (metadata service / Raft)

### What the Go Agent Does NOT Do

- No data I/O of any kind
- No chunk operations
- No replication
- No direct SPDK socket communication
- No Malloc bdev creation (unless explicit `--test-mode` flag)

---

## Failover Model

### Detection
- Go agent sends heartbeats to metadata service
- Missed heartbeat beyond timeout → metadata service marks node suspect
- Policy engine detects the failed node

### Fast Failover (ANA State Flip)
- A healthy replica node's NVMe-oF target is promoted to ANA optimized
- The new owner operates in **degraded mode** (fewer replicas than desired)
- No CRUSH map change, no data movement — just ANA state change
- Millisecond-scale failover

### Slow Repair (Policy Engine)
- Policy engine detects degraded volume (actual replicas < desired)
- Policy engine selects a new node from CRUSH map
- Triggers chunk rebuild — new owner's chunk engine copies chunks from surviving replicas via gRPC
- Once rebuild completes, CRUSH map is updated in metadata service
- Chunk engines pick up the update

### Split-Brain Prevention
- Fenced dataplane cannot accept writes if agent loses quorum
- ANA ensures only one optimized target exists at any time
- Metadata service (Raft) is the single arbiter of ownership

---

## Invariants (MUST NEVER be violated)

1. **Layer separation is absolute.** The presentation layer MUST NOT replicate, erasure-code, or make placement decisions. The chunk engine MUST NOT manage devices or translate protocols. The backend engine MUST NOT know about chunks. The policy engine MUST NOT be in the data path.

2. **All data I/O flows through SPDK.** Every backend type produces SPDK bdevs. The chunk engine does all I/O through SPDK bdev operations. No kernel bypass, no raw file I/O outside of SPDK AIO.

3. **Go agent never touches data.** The Go agent is management and configuration only. It communicates with the Rust dataplane via gRPC. It never reads or writes chunk data.

4. **CRUSH map is the single source of truth** for placement and protection scheme. The metadata service is the authoritative store. Chunk engines cache locally.

5. **No Malloc bdevs in production.** Malloc bdevs are only allowed with explicit `--test-mode`. Production deployments use real backends (File, LVM, Raw).

6. **Content addressing is universal.** All backends use the chunk engine. All chunks are 4MB, SHA-256 addressed, CRC-32C checksummed, immutable.

7. **One active NVMe-oF target per volume.** ANA ensures a single optimized target. Replicas are standby only.

8. **Fencing on quorum loss.** If the Rust dataplane loses contact with the Go agent (which links it to cluster consensus), it fences itself and stops accepting writes.

9. **gRPC is the only communication protocol.** Go→Rust, Rust→Rust — all gRPC. No JSON-RPC, no SPDK native RPC from Go.

10. **Proto definitions are shared.** All `.proto` files in `api/proto/`. Go and Rust generate from the same source.
