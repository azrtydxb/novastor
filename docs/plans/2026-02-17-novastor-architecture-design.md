# NovaStor Architecture Design

**Date:** 2026-02-17
**Status:** Approved

## Overview

NovaStor is a unified Kubernetes-native storage system providing block (CSI/NVMe-oF), file (NFS/FUSE), and object (S3-compatible) storage through a shared chunk storage engine.

## Core Principle

Everything is chunks. A block volume is an ordered sequence of 4MB chunks. A file is chunks + inode metadata. An object is chunks + object metadata. One engine, three access layers.

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  CSI Driver  │  │  NFS/FUSE   │  │  S3 Gateway  │
│   (Block)    │  │   (File)    │  │   (Object)   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       └────────┬────────┴────────┬────────┘
         ┌──────▼───────┐  ┌─────▼──────┐
         │  Metadata     │  │  Placement │
         │  Service      │  │  Engine    │
         └──────┬───────┘  └─────┬──────┘
         ┌──────▼────────────────▼──────┐
         │     Chunk Storage Engine      │
         └──────────────┬───────────────┘
              ┌─────────▼─────────┐
              │  Node Agent        │
              │  (DaemonSet)       │
              └───────────────────┘
```

## Components

### 1. Chunk Storage Engine (`internal/chunk/`)

The heart of NovaStor. Handles read, write, delete, replicate, erasure-code, checksum, and scrub operations on 4MB chunks.

- **Chunk size:** 4MB fixed
- **Chunk ID:** Content-addressed (hash of content) for deduplication potential
- **Integrity:** CRC-32C checksum on every chunk, verified on read
- **Immutability:** Chunks are immutable once written; updates create new chunks
- **Access-layer agnostic:** The engine knows nothing about volumes, files, or objects

### 2. Data Protection (`internal/chunk/`)

Both modes available for **all three access layers** (block, file, object). Configured per StoragePool:

**Replication:**
- Synchronous N-way replication (default factor=3)
- Write succeeds when quorum confirms (majority, e.g., 2 of 3)
- Read from any healthy replica (closest/fastest)

**Erasure Coding:**
- Reed-Solomon (default 4+2 — 4 data + 2 parity shards)
- Tolerates 2 node failures with 1.5x storage overhead
- Uses `github.com/klauspost/reedsolomon`

**Recovery (shared):**
- Node heartbeat timeout triggers failure detection
- Under-replicated chunks identified and rebuild scheduled
- Priority-based: fewer surviving copies = higher rebuild priority
- Bandwidth throttling to avoid saturating cluster during rebuild

### 3. Node Agent (`cmd/agent/`, `internal/agent/`)

DaemonSet running on every storage node:

- **Disk discovery & management** — detects raw devices, NVMe drives, manages them directly
- **Chunk server** — gRPC service for chunk read/write/replicate operations
- **Health reporting** — disk health (SMART), capacity, IOPS metrics via CRD status updates

### 4. Metadata Service (`cmd/meta/`, `internal/metadata/`)

Raft-based clustered metadata service (3 or 5 replicas):

- **Volume metadata** — chunk-to-node mapping for each block volume
- **File metadata** — directory tree, inodes, xattrs
- **Object metadata** — bucket listings, object-to-chunk mapping
- **Placement maps** — which nodes store which chunks
- Built on `hashicorp/raft` + BadgerDB (no external dependencies)
- All writes through Raft leader; reads optionally from followers

### 5. Placement Engine (`internal/placement/`)

CRUSH-like algorithm for data placement:

- Determines which nodes store which chunks
- Respects failure domains (rack, zone)
- Handles rebalancing when nodes join/leave
- Phase 1: simple round-robin; CRUSH-like algorithm later

### 6. CSI Driver (`cmd/csi/`, `internal/csi/`)

Kubernetes Container Storage Interface driver:

- **Controller plugin** — CreateVolume, DeleteVolume, CreateSnapshot, ExpandVolume
- **Node plugin** — NodeStageVolume, NodePublishVolume
- Phase 1: gRPC transport to chunk engine
- Phase 2: NVMe-oF/TCP for near-local-disk latency

### 7. File Gateway (`cmd/filer/`, `internal/filer/`)

NFS v4.1 gateway (scalable Deployment):

- Translates file operations → metadata lookups + chunk reads/writes
- File locking and lease management via metadata service
- Supports ReadWriteMany (RWX) access mode
- Optional FUSE client DaemonSet for higher-performance pod-local mounts

### 8. S3 Gateway (`cmd/s3gw/`, `internal/s3/`)

S3-compatible HTTP API (horizontally scalable, stateless):

- Core operations: PutObject, GetObject, ListBucket, DeleteObject, multipart upload
- Translates objects → chunks in the storage engine
- Presigned URLs, bucket policies, basic IAM

### 9. Controller/Operator (`cmd/controller/`, `internal/operator/`)

Kubernetes-native orchestration:

- Watches CRDs, reconciles desired state
- Manages data placement and rebalancing
- Triggers replication repair on node failure
- Handles storage pool expansion/contraction
- Rolling upgrade orchestration

## CRD Design

```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
spec:
  nodeSelector:
    matchLabels:
      storage-tier: nvme
  deviceFilter:
    type: nvme
    minSize: 100Gi
  dataProtection:
    mode: replication          # replication | erasureCoding
    replication:
      factor: 3
      writeQuorum: 2
    erasureCoding:
      dataShards: 4
      parityShards: 2

---
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
spec:
  pool: nvme-fast
  size: 100Gi
  accessMode: ReadWriteOnce

---
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem
spec:
  pool: nvme-fast
  capacity: 1Ti
  accessMode: ReadWriteMany
  export:
    protocol: nfs

---
apiVersion: novastor.io/v1alpha1
kind: ObjectStore
spec:
  pool: nvme-fast
  endpoint:
    service:
      port: 9000
  bucketPolicy:
    maxBuckets: 100
    versioning: enabled
```

## Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Go | Consistent with K8s ecosystem, NovaEdge |
| Block transport (Phase 1) | gRPC | Simple to test and iterate |
| Block transport (Phase 2) | NVMe-oF/TCP | Near-local-disk latency |
| Consensus | hashicorp/raft + BadgerDB | Fast, embedded, no external deps |
| Inter-component RPC | gRPC + protobuf | Standard, efficient, typed |
| Chunk size | 4MB fixed | Good balance for all workload types |
| Erasure coding | klauspost/reedsolomon | Pure Go, high performance |
| Logging | go.uber.org/zap | Structured, fast |
| CLI framework | spf13/cobra | Standard Go CLI toolkit |

## Phased Delivery

### Phase 1 — Foundation (Weeks 1–4)
- Chunk storage engine with replication AND erasure coding
- Disk discovery & management
- Node agent with gRPC chunk server
- Metadata service with Raft consensus
- Placement engine (round-robin)
- CRD types + basic operator skeleton

### Phase 2 — Block Storage (Weeks 5–8)
- NVMe-oF target integration on agent
- CSI driver (controller + node plugins)
- Volume provisioning, attach, mount flow
- StorageClass integration

### Phase 3 — Object Storage (Weeks 9–11)
- S3 gateway with core operations
- Object-to-chunk mapping in metadata service
- Multipart upload support
- Basic bucket management

### Phase 4 — File Storage (Weeks 12–14)
- NFS gateway with file metadata
- Directory tree management
- File locking / leases
- RWX PVC support via CSI

### Phase 5 — Hardening (Weeks 15–18)
- Failure recovery (node down → re-replicate/rebuild)
- Data scrubbing / integrity verification
- Monitoring (Prometheus metrics, Grafana dashboards)
- Performance benchmarking + optimization
- Encryption at rest
- mTLS between components
