# NovaStor Architecture Compliance — Master Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the NovaStor codebase into 100% compliance with the layered architecture spec (`docs/superpowers/specs/2026-03-15-layered-architecture-design.md`).

**Architecture:** Five sub-plans executed in dependency order. Each produces working, testable software. The communication migration (JSON-RPC → gRPC) must come first since all other layers depend on it.

**Tech Stack:** Go 1.25+, Rust (SPDK FFI, tonic gRPC, reed-solomon-simd), Protocol Buffers, Kubernetes CRDs

**Spec:** `docs/superpowers/specs/2026-03-15-layered-architecture-design.md`

---

## Sub-Plans (execute in order)

| # | Sub-Plan | Fixes | Invariants |
|---|----------|-------|------------|
| 1 | [Communication Migration](#sub-plan-1-communication-migration) | JSON-RPC → gRPC (Go→Rust), remove SPDK socket from Go | #3, #9 |
| 2 | [Backend Engine Fix](#sub-plan-2-backend-engine-fix) | File backend → SPDK AIO, Raw backend → direct NVMe bdev, StoragePool CRD | #1, #2 |
| 3 | [Chunk Engine Replication](#sub-plan-3-chunk-engine-replication) | N-replica fan-out, EC shard distribution, per-volume protection | #1, #4, #6 |
| 4 | [Fencing & Quorum](#sub-plan-4-fencing--quorum) | Heartbeat fencing, dataplane self-fence on quorum loss | #8 |
| 5 | [Device Management](#sub-plan-5-device-management) | Device unbinding/binding, hugepage management in Go agent | #2, #3 |

---

## Sub-Plan 1: Communication Migration

**Goal:** Replace ALL JSON-RPC communication (Go→Rust) with gRPC. After this, the JSON-RPC server and Go SPDK client can be removed.

**Why first:** Every other sub-plan needs Go→Rust communication. Doing this first means all subsequent work uses the correct protocol from the start.

### Current State
- Go agent uses `internal/spdk/client.go` (JSON-RPC over Unix socket `/var/tmp/novastor-spdk.sock`)
- ~36 JSON-RPC methods across 7 categories
- Rust dataplane already has a gRPC server on port 9500 (ChunkService only)
- Proto definitions exist in `api/proto/` but only cover chunk and nvme operations

### Task 1.1: Define Dataplane Management Proto

**Files:**
- Create: `api/proto/dataplane/dataplane.proto`
- Create: `api/proto/dataplane/dataplane.pb.go` (generated)

Define a `DataplaneService` proto that covers ALL JSON-RPC methods the Go agent calls:

- [ ] **Step 1: Write the proto file**

```protobuf
syntax = "proto3";
package dataplane;
option go_package = "github.com/azrtydxb/novastor/api/proto/dataplane";

service DataplaneService {
  // Bdev management
  rpc CreateAioBdev(CreateAioBdevRequest) returns (BdevInfo);
  rpc CreateMallocBdev(CreateMallocBdevRequest) returns (BdevInfo);
  rpc CreateLvolStore(CreateLvolStoreRequest) returns (CreateLvolStoreResponse);
  rpc CreateLvol(CreateLvolRequest) returns (BdevInfo);
  rpc DeleteBdev(DeleteBdevRequest) returns (DeleteBdevResponse);
  rpc ListBdevs(ListBdevsRequest) returns (ListBdevsResponse);
  rpc GetBdevInfo(GetBdevInfoRequest) returns (BdevInfo);

  // NVMe-oF target management
  rpc InitTransport(InitTransportRequest) returns (InitTransportResponse);
  rpc CreateNvmfTarget(CreateNvmfTargetRequest) returns (CreateNvmfTargetResponse);
  rpc DeleteNvmfTarget(DeleteNvmfTargetRequest) returns (DeleteNvmfTargetResponse);
  rpc SetAnaState(SetAnaStateRequest) returns (SetAnaStateResponse);
  rpc GetAnaState(GetAnaStateRequest) returns (GetAnaStateResponse);
  rpc ListSubsystems(ListSubsystemsRequest) returns (ListSubsystemsResponse);
  rpc ExportBdev(ExportBdevRequest) returns (ExportBdevResponse);

  // NVMe-oF initiator (for replica connections)
  rpc ConnectInitiator(ConnectInitiatorRequest) returns (ConnectInitiatorResponse);
  rpc DisconnectInitiator(DisconnectInitiatorRequest) returns (DisconnectInitiatorResponse);

  // Replica bdev management
  rpc CreateReplicaBdev(CreateReplicaBdevRequest) returns (CreateReplicaBdevResponse);
  rpc ReplicaStatus(ReplicaStatusRequest) returns (ReplicaStatusResponse);

  // Chunk store management
  rpc InitChunkStore(InitChunkStoreRequest) returns (InitChunkStoreResponse);
  rpc ChunkStoreStats(ChunkStoreStatsRequest) returns (ChunkStoreStatsResponse);

  // Chunk I/O (streaming for data)
  rpc WriteChunk(stream WriteChunkRequest) returns (WriteChunkResponse);
  rpc ReadChunk(ReadChunkRequest) returns (stream ReadChunkResponse);
  rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);
  rpc ChunkExists(ChunkExistsRequest) returns (ChunkExistsResponse);
  rpc ListChunks(ListChunksRequest) returns (ListChunksResponse);
  rpc GarbageCollect(GarbageCollectRequest) returns (GarbageCollectResponse);

  // Backend management
  rpc InitBackend(InitBackendRequest) returns (InitBackendResponse);
  rpc CreateVolume(CreateVolumeRequest) returns (CreateVolumeResponse);
  rpc DeleteVolume(DeleteVolumeRequest) returns (DeleteVolumeResponse);

  // Health
  rpc GetVersion(GetVersionRequest) returns (GetVersionResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}

// Messages follow — one request/response pair per RPC.
// Each message mirrors the JSON-RPC params/result exactly.
// (Full message definitions in the actual proto file)
```

- [ ] **Step 2: Generate Go code**

```bash
make generate-proto
```

- [ ] **Step 3: Commit**

```bash
git add api/proto/dataplane/
git commit -m "[Feature] Define DataplaneService proto for Go-to-Rust gRPC"
```

### Task 1.2: Implement Rust gRPC DataplaneService

**Files:**
- Create: `dataplane/src/transport/dataplane_service.rs`
- Modify: `dataplane/src/transport/server.rs` — add DataplaneService to gRPC server
- Modify: `dataplane/src/transport/mod.rs` — export new module

Each gRPC method delegates to the same backend code that the JSON-RPC methods currently call (the logic in `jsonrpc/methods.rs`). This is a thin adapter layer.

- [ ] **Step 1: Implement DataplaneServiceImpl in Rust**

The service wraps the existing global managers (BdevManager, NvmfManager, ChunkStore registries) and calls the same functions that JSON-RPC methods call.

- [ ] **Step 2: Add DataplaneService to GrpcServer**

Modify `server.rs` to register both ChunkService and DataplaneService:
```rust
tonic::transport::Server::builder()
    .add_service(chunk_server)
    .add_service(dataplane_server)
    .serve_with_incoming(...)
```

- [ ] **Step 3: Generate Rust proto code**

Add `dataplane.proto` to the tonic build script.

- [ ] **Step 4: Test — unit tests for each gRPC method**

- [ ] **Step 5: Commit**

### Task 1.3: Replace Go SPDK Client with gRPC Client

**Files:**
- Create: `internal/dataplane/client.go` — new gRPC client for DataplaneService
- Modify: `cmd/agent/main.go` — use gRPC client instead of SPDK JSON-RPC client
- Modify: `internal/agent/spdk_target_server.go` — use gRPC client
- Modify: `internal/agent/chunk_server.go` — use gRPC client
- Modify: `internal/agent/spdk_gc.go` — use gRPC client
- Modify: `internal/agent/spdk_replica.go` — use gRPC client

- [ ] **Step 1: Create Go gRPC DataplaneService client**

```go
package dataplane

type Client struct {
    conn *grpc.ClientConn
    svc  pb.DataplaneServiceClient
}

func Dial(addr string, opts ...grpc.DialOption) (*Client, error) { ... }
```

Exposes typed methods matching the proto service.

- [ ] **Step 2: Update cmd/agent/main.go**

Remove SPDK socket connection. Connect to local Rust dataplane via gRPC (localhost:9500).

- [ ] **Step 3: Update all agent services to use gRPC client**

Replace every `spdkClient.Call("method", ...)` with `dataplaneClient.Method(ctx, req)`.

- [ ] **Step 4: Test — integration test with mock gRPC server**

- [ ] **Step 5: Commit**

### Task 1.4: Remove JSON-RPC Code

**Files:**
- Delete: `internal/spdk/client.go` (or gut it)
- Modify: `cmd/agent/main.go` — remove `--spdk-socket` flag
- Modify: `deploy/helm/novastor/templates/agent-daemonset.yaml` — remove spdk-socket volume/mount/arg

- [ ] **Step 1: Remove JSON-RPC client from Go**
- [ ] **Step 2: Update Helm chart**
- [ ] **Step 3: Run tests, verify no JSON-RPC references remain in Go**
- [ ] **Step 4: Commit**

> **Note:** Keep the Rust JSON-RPC server for now — it can be removed in a later cleanup once all consumers are migrated. The Go side is the priority.

---

## Sub-Plan 2: Backend Engine Fix

**Goal:** Fix File and Raw backends to use SPDK bdevs. Update StoragePool CRD to declare backend type.

### Task 2.1: Fix File Backend to Use SPDK AIO Bdevs

**Files:**
- Modify: `dataplane/src/backend/file_store.rs`

The File backend currently uses `std::fs::write/read`. It must create a single backing file and wrap it with an SPDK AIO bdev, then do all I/O through the bdev.

- [ ] **Step 1: Refactor FileChunkStore to use BdevManager**

Instead of `std::fs::write()`, the store should:
1. On init: create a single large sparse file, call `bdev_manager().create_aio_bdev()` to get an SPDK bdev
2. Use `BdevChunkStore` on top of the AIO bdev for chunk storage (reuse the bitmap allocator)
3. All read/write goes through `reactor_dispatch::bdev_read_async/bdev_write_async`

- [ ] **Step 2: Update tests**
- [ ] **Step 3: Commit**

### Task 2.2: Fix Raw Backend to Use Direct NVMe Bdevs

**Files:**
- Modify: `dataplane/src/backend/raw_disk.rs`

The Raw backend currently creates file-backed AIO bdevs in `/var/lib/novastor/raw/`. Per spec, it must use an SPDK NVMe bdev on an unbound NVMe device (no filesystem).

- [ ] **Step 1: Refactor RawDiskBackend**

Instead of creating sparse files:
1. Accept a PCI address of an unbound NVMe device
2. Call SPDK's `bdev_nvme_attach_controller()` to create an NVMe bdev directly on the device
3. Use the NVMe bdev for all I/O — no filesystem, no files

- [ ] **Step 2: Update tests**
- [ ] **Step 3: Commit**

### Task 2.3: Update StoragePool CRD

**Files:**
- Modify: `api/v1alpha1/types.go`
- Modify: `config/crd/` (regenerate)

- [ ] **Step 1: Add backend type and device list to StoragePoolSpec**

```go
type StoragePoolSpec struct {
    // BackendType specifies the storage backend: file, lvm, or raw
    // +kubebuilder:validation:Enum=file;lvm;raw
    BackendType string `json:"backendType"`

    // Devices is the list of devices assigned to this pool
    Devices []DeviceAssignment `json:"devices"`

    // Protection is the default protection for volumes in this pool
    DataProtection DataProtectionSpec `json:"dataProtection"`
}

type DeviceAssignment struct {
    // Node is the Kubernetes node name
    Node string `json:"node"`
    // Path is the device path (e.g., /dev/nvme0n1) or directory (for file backend)
    Path string `json:"path"`
}
```

- [ ] **Step 2: Regenerate CRD manifests**

```bash
make generate && make manifests
```

- [ ] **Step 3: Update Go agent to watch StoragePool and configure backends accordingly**
- [ ] **Step 4: Commit**

---

## Sub-Plan 3: Chunk Engine Replication

**Goal:** Implement N-replica fan-out and remote EC shard distribution in the Rust chunk engine.

### Task 3.1: Implement Replication Fan-Out in ChunkEngine

**Files:**
- Modify: `dataplane/src/chunk/engine.rs`

- [ ] **Step 1: Update write path to consult Protection scheme**

The write path must:
1. Look up `VolumeDefinition.protection` for the volume being written
2. For `Protection::Replication { factor }`: call `crush::select(&chunk_id, factor, &topology)` to get N placements
3. Write chunk to local store
4. Fan out full chunk to all replica nodes' ChunkService via gRPC `put_chunk`
5. Wait for quorum (majority) acknowledgment before returning success

```rust
async fn write_replicated(&self, chunk_id: &str, data: &[u8], placements: &[Placement]) -> Result<()> {
    let local_node = &self.node_id;
    let mut futures = Vec::new();

    for (node_id, _backend_id) in placements {
        if node_id == local_node {
            self.local_store.put(chunk_id, data).await?;
        } else {
            let client = self.get_client(node_id).await?;
            futures.push(client.put(chunk_id, data));
        }
    }

    // Wait for quorum (majority)
    let quorum = placements.len() / 2 + 1;
    let results = futures::future::join_all(futures).await;
    let successes = results.iter().filter(|r| r.is_ok()).count() + 1; // +1 for local
    if successes >= quorum {
        Ok(())
    } else {
        Err(DataPlaneError::ReplicationError("quorum not reached"))
    }
}
```

- [ ] **Step 2: Update read path with fallback**

If primary replica fails, try next replica from CRUSH placement.

- [ ] **Step 3: Tests**
- [ ] **Step 4: Commit**

### Task 3.2: Implement Remote EC Shard Distribution

**Files:**
- Modify: `dataplane/src/bdev/erasure.rs`
- Modify: `dataplane/src/chunk/engine.rs`

- [ ] **Step 1: Add distributed shard write to ChunkEngine**

For `Protection::ErasureCoding { data_shards, parity_shards }`:
1. RS-encode chunk into K data + M parity shards
2. Use CRUSH to select K+M nodes for shard placement
3. Distribute each shard to its target node via gRPC
4. Wait for sufficient acknowledgments

- [ ] **Step 2: Add distributed shard read with reconstruction**

Read K shards from available nodes, reconstruct if any missing.

- [ ] **Step 3: Add PutShard/GetShard RPCs to ChunkService proto**

```protobuf
rpc PutShard(stream PutShardRequest) returns (PutShardResponse);
rpc GetShard(GetShardRequest) returns (stream GetShardResponse);
```

- [ ] **Step 4: Tests**
- [ ] **Step 5: Commit**

### Task 3.3: Remove Replication from Presentation Layer

**Files:**
- Modify: `internal/agent/spdk_target_server.go` — remove SetupReplication logic
- Modify: `internal/agent/spdk_replica.go` — remove or simplify
- Modify: `api/proto/nvme/nvme.proto` — remove SetupReplication RPC

The presentation layer must NOT do replication. That's now handled by the chunk engine.

- [ ] **Step 1: Remove replication setup from NVMe-oF target creation**
- [ ] **Step 2: Remove SetupReplication proto**
- [ ] **Step 3: Tests**
- [ ] **Step 4: Commit**

---

## Sub-Plan 4: Fencing & Quorum

**Goal:** Implement heartbeat-based fencing so the Rust dataplane stops accepting writes if it loses contact with the Go agent.

### Task 4.1: Add Heartbeat RPC

**Files:**
- Modify: `api/proto/dataplane/dataplane.proto` — Heartbeat RPC (already defined in Sub-Plan 1)
- Modify: `dataplane/src/transport/dataplane_service.rs` — implement Heartbeat handler
- Modify: `cmd/agent/main.go` — send periodic heartbeats

- [ ] **Step 1: Implement Heartbeat in Rust dataplane**

The dataplane tracks last heartbeat time. If no heartbeat received within timeout (e.g., 5s), the dataplane enters fenced mode.

- [ ] **Step 2: Implement fencing in Rust**

In fenced mode:
- All write operations return error
- Read operations still allowed (best-effort)
- NVMe-oF targets set to ANA inaccessible
- Log loud warnings

- [ ] **Step 3: Implement heartbeat sender in Go agent**

Go agent sends heartbeat every 500ms to local Rust dataplane via gRPC.

- [ ] **Step 4: Tests**
- [ ] **Step 5: Commit**

---

## Sub-Plan 5: Device Management

**Goal:** Go agent handles NVMe device unbinding/binding for LVM and Raw backends, and configures hugepages.

### Task 5.1: Implement Device Unbinding/Binding

**Files:**
- Create: `internal/agent/device/manager.go`
- Create: `internal/agent/device/manager_test.go`

- [ ] **Step 1: Implement device manager**

```go
package device

// Manager handles NVMe device preparation for SPDK.
type Manager struct {
    logger *zap.Logger
}

// UnbindFromKernel unbinds an NVMe device from the kernel driver.
func (m *Manager) UnbindFromKernel(pciAddr string) error {
    // Write to /sys/bus/pci/devices/<addr>/driver/unbind
}

// BindToVFIO binds a PCI device to the vfio-pci driver.
func (m *Manager) BindToVFIO(pciAddr string) error {
    // Write PCI vendor:device ID to /sys/bus/pci/drivers/vfio-pci/new_id
    // Write PCI address to /sys/bus/pci/drivers/vfio-pci/bind
}

// PrepareDevice unbinds from kernel and binds to vfio-pci.
func (m *Manager) PrepareDevice(pciAddr string) error {
    if err := m.UnbindFromKernel(pciAddr); err != nil { return err }
    return m.BindToVFIO(pciAddr)
}
```

- [ ] **Step 2: Tests (mock sysfs)**
- [ ] **Step 3: Commit**

### Task 5.2: Implement Hugepage Management

**Files:**
- Create: `internal/agent/device/hugepages.go`

- [ ] **Step 1: Implement hugepage configuration**

```go
// ConfigureHugepages ensures the required number of 2MB hugepages are allocated.
func ConfigureHugepages(requiredMB int) error {
    required := requiredMB / 2 // 2MB hugepages
    // Write to /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
}
```

- [ ] **Step 2: Wire into agent startup**
- [ ] **Step 3: Tests**
- [ ] **Step 4: Commit**

### Task 5.3: Wire Device Management into StoragePool Reconciler

**Files:**
- Modify: `internal/agent/` or `internal/operator/` — StoragePool watcher

When a StoragePool CRD is created/updated:
1. For `backendType: file` — ensure directory exists, no device prep needed
2. For `backendType: lvm` — unbind device, bind to vfio-pci, instruct dataplane to create lvol store
3. For `backendType: raw` — unbind device, bind to vfio-pci, instruct dataplane to create NVMe bdev

- [ ] **Step 1: Implement StoragePool reconciler**
- [ ] **Step 2: Integration test**
- [ ] **Step 3: Commit**

---

## Execution Order

```
Sub-Plan 1 (Communication) ──→ Sub-Plan 2 (Backends) ──→ Sub-Plan 3 (Replication)
                                                                    │
                              Sub-Plan 4 (Fencing) ←────────────────┘
                                       │
                              Sub-Plan 5 (Device Mgmt)
```

Sub-Plans 4 and 5 can run in parallel after Sub-Plan 1 is complete.

## Verification

After all sub-plans are complete, run the `architecture-compliance` agent to verify 100% compliance with all 10 invariants.
