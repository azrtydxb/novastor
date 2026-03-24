# ChunkEngine Data Path Integration Design

## Goal

Route ALL I/O through the ChunkEngine — the single source of truth for chunk placement. Any frontend on any node can serve any volume by routing sub-block I/O to the correct backend nodes via CRUSH + NDP. Frontend and backend scale independently.

## Architecture

```
Pod → NVMe-oF (kernel multipath, connects to multiple frontends)
         ↓ (ANA selects preferred path)
    Frontend's SPDK bdev → ChunkEngine
         ↓
    CRUSH lookup (volume_id, chunk_index) → backend node list
         ↓
    ┌── backend on this node? → direct bdev I/O (zero network hop)
    └── backend is remote? → NDP to that backend node → bdev I/O
```

### Component Independence

- **Frontend nodes** — run NVMe-oF target (SPDK dataplane). Present volumes to kernel initiators. Don't need local NVMe drives or chunk stores. Route ALL I/O through ChunkEngine → CRUSH → backend nodes.
- **Backend nodes** — run chunk stores on real NVMe drives. Serve sub-block NDP requests from any frontend. Don't need NVMe-oF targets.
- **Combo nodes** — run both. CRUSH may place chunks locally → zero-hop I/O.
- **Application-only nodes** — no frontend, no backend. CSI connects to a frontend on another node via NVMe-oF TCP.

Frontend and backend scale independently: 3 frontends + 20 backends, or 50 frontends + 5 backends.

### ANA Path Preference

Kernel NVMe multipath uses ANA to select preferred paths based on data locality:

| Scenario | ANA State | Kernel Behavior |
|----------|-----------|-----------------|
| Frontend has volume chunks locally (combo node) | **optimized** | Preferred path — kernel routes I/O here |
| Frontend has NO local chunks | **non-optimized** | Failover only — used when optimized paths die |

- Multiple optimized paths → kernel round-robins across them (load balance)
- Mix of optimized + non-optimized → kernel uses only optimized
- All optimized paths die → kernel fails over to non-optimized
- ANA state determined at CreateTarget time by checking CRUSH placement
- On topology change → re-evaluate ANA, SPDK sends ANA Change Notification to initiators

**Example — 3 frontends, 5 backends, rep3:**
- CRUSH places chunks on B1, B2, B3
- F1 is also B1 (combo) → optimized
- F2 is also B2 (combo) → optimized
- F3 has no NVMe → non-optimized (failover only)
- Pod connects to F1, F2, F3. Kernel sends I/O to F1+F2 (round-robin). If F1 dies, F2 only. If both die, F3 serves (all I/O remote).

---

## Section 1: ChunkEngine Sub-Block Interface

The ChunkEngine is extended with sub-block read/write methods. These are the ONLY entry points for data I/O — the SPDK bdev calls these, nothing else.

### New Methods

```rust
pub async fn sub_block_read(
    &self,
    volume_id: &str,
    offset: u64,      // volume-relative byte offset
    length: u64,
) -> Result<Vec<u8>>

pub async fn sub_block_write(
    &self,
    volume_id: &str,
    offset: u64,
    data: &[u8],
) -> Result<()>
```

### Read Flow

1. Compute `chunk_index = offset / CHUNK_SIZE`
2. CRUSH lookup: `crush::select(volume_id, chunk_index, 1, topology)` → primary backend
3. If primary is this node → direct `bdev_read_async` on local chunk store
4. If primary is remote → NDP `Read(volume_hash, offset, length)` to that backend
5. If primary fails → try next replica from CRUSH order

### Write Flow

1. Compute `chunk_index = offset / CHUNK_SIZE`
2. CRUSH lookup: `crush::select(volume_id, chunk_index, replication_factor, topology)` → N backends
3. Fan-out NDP writes to all N backends concurrently:
   - Local backend → direct `bdev_write_async`
   - Remote backends → NDP `Write(volume_hash, offset, data)`
4. Success when write quorum met (majority: 2 of 3 for rep3)
5. Dirty bitmap updated on each backend independently

### Lazy Allocation

When a backend receives a sub-block write for a volume+chunk it hasn't seen, it allocates a 4MB slot in its chunk store, zeros it, then writes the sub-block at the correct offset within the slot. No upfront allocation at volume creation time.

### Background Sync Unchanged

The sync task still reads full 4MB chunks from backend bdevs, computes SHA-256, and persists chunk maps. Content-addressing happens at the chunk level, not per sub-block. This runs on each backend for its local chunks.

---

## Section 2: NDP as Data Path Protocol

gRPC is management only. NDP is the data path protocol between frontends and backends.

### Why NDP, Not gRPC

- Fixed 64-byte binary header — zero serialization overhead
- Zero-copy data transfer
- No HTTP/2 framing, no TLS per-stream overhead
- Already proven at 70K+ IOPS in our benchmarks

### NDP Already Supports Sub-Block I/O

The NDP protocol already has Read/Write operations with headers containing:
- `volume_hash: u64` — identifies the volume
- `offset: u64` — volume-relative byte offset
- `data_length: u32` — bytes to read/write
- Operation type (Read/Write/Flush)

No protocol changes needed. The existing NDP server on backend nodes handles sub-block I/O via `sub_block_read_pub` / `sub_block_write_pub`.

### Connection Pooling

The ChunkEngine maintains a pool of NDP connections to backend nodes:
- Keyed by backend node address
- Created lazily on first I/O to that backend
- Kept alive and reused across requests
- TCP for remote backends, Unix socket for co-located backends
- Uses the existing `NdpConnection` type (generic over `AsyncRead + AsyncWrite`)

### Protocol Boundary

| Protocol | Purpose |
|----------|---------|
| NDP | Sub-block read/write between frontend and backend (hot path) |
| gRPC | Topology updates, volume create/delete, ANA state, chunk store init, metadata |
| NVMe-oF TCP | Kernel initiator ↔ frontend (SPDK handles this natively) |

---

## Section 3: SPDK Bdev Changes

### Current Path (removed)

```rust
bdev_submit_request_cb(READ)
    → sub_block_read(volume_name, offset, length)  // direct local bdev
```

### New Path

```rust
bdev_submit_request_cb(READ)
    → tokio::spawn(chunk_engine.sub_block_read(volume_id, offset, length))
    → ChunkEngine does CRUSH + local/NDP routing
    → send_to_reactor → copy to iov → complete bdev_io
```

### What the Bdev No Longer Does

- No direct `get_backend_bdev_name()` / `get_volume_base_offset()` calls
- No direct `reactor_cache_open()` for backend I/O
- These move into the ChunkEngine's local-backend code path

### What Stays in the Bdev

- NVMe-oF command parsing (offset, length, iovs)
- Dispatching I/O to tokio where ChunkEngine runs
- Completing bdev_io on the reactor after ChunkEngine returns
- WRITE_ZEROES/UNMAP dispatch (routed through ChunkEngine)

### Reactor Fast Path for Local I/O

When CRUSH says "this node is the primary backend", the ChunkEngine calls the existing `bdev_read_async`/`bdev_write_async` on the reactor — same zero-crossing fast path. No performance regression for co-located frontend+backend.

### Readahead

For local backends, readahead prefetches from local bdev as before. For remote backends, readahead is not applicable — NDP has its own pipelining via batched requests.

---

## Section 4: Volume Creation

### New CreateVolume Flow

1. CSI controller receives `CreateVolume(rep3, 10Gi)`
2. **No backend allocation** — lazy per-chunk allocation on first write
3. CSI controller selects frontend nodes for NVMe-oF targets:
   - Prefer combo nodes where CRUSH would place chunks (optimized ANA)
   - Add frontend-only nodes as non-optimized failover paths
4. For each frontend, call `CreateTarget`:
   - Frontend creates NVMe-oF subsystem backed by a ChunkEngine-routed bdev
   - No local chunk store needed
   - ANA state set based on CRUSH affinity

### CreateTarget Changes

- No longer requires a local chunk store
- Creates a novastor bdev backed by ChunkEngine (not local backend bdev)
- ChunkEngine initialized with volume ID and cluster topology
- No `allocate_volume_offset()` on frontend

### DeleteVolume

- CSI controller tells all frontend nodes to delete NVMe-oF targets
- Tells all backend nodes to mark chunks as `pending_delete` (async GC)

---

## Section 5: Topology Changes

### Update Flow

1. Metadata detects topology change (heartbeat timeout, new node, drain)
2. Pushes updated `ClusterMap` with incremented generation to all dataplanes
3. Each frontend's ChunkEngine receives new topology
4. ChunkEngine re-evaluates CRUSH for active volumes:
   - NDP pool adds connections to new backends, closes removed
5. Frontend re-evaluates ANA per volume:
   - Gained CRUSH affinity → promote to optimized
   - Lost CRUSH affinity → demote to non-optimized
   - SPDK sends ANA Change Notification to kernel initiators

### Backend Failure

1. Heartbeat timeout (10s)
2. Metadata marks node offline
3. Topology push to all nodes
4. Frontends stop routing to dead backend
5. CRUSH selects surviving replicas for reads
6. Writes go to remaining backends (degraded: 2 of 3 for rep3)
7. Policy engine triggers rebuild to new backend

### Backend Addition

1. New node registers, added to ClusterMap
2. Topology push → CRUSH includes new node
3. New writes may land on new node (lazy allocation)
4. Policy engine rebalances existing chunks in background

### No Migration on Topology Change

Existing chunks stay where they are. Only new writes use updated CRUSH. Background rebalancing is separate — not in the hot I/O path.

---

## Files Changed

| File | Changes |
|------|---------|
| `dataplane/src/chunk/engine.rs` | New `sub_block_read`, `sub_block_write` methods with CRUSH routing + NDP fan-out |
| `dataplane/src/chunk/ndp_pool.rs` | **New**: NDP connection pool to backend nodes |
| `dataplane/src/bdev/novastor_bdev.rs` | `bdev_submit_request_cb` calls ChunkEngine instead of direct sub_block I/O |
| `dataplane/src/transport/dataplane_service.rs` | `CreateVolume` / `CreateNvmfTarget` no longer requires local chunk store |
| `dataplane/src/metadata/crush.rs` | No changes — already implemented |
| `dataplane/src/metadata/topology.rs` | No changes — already implemented |
| `dataplane/src/transport/ndp_server.rs` | No changes — already handles sub-block I/O |
| `dataplane/ndp/src/connection.rs` | Minor: connection pool support |
| `api/proto/nvme/nvme.proto` | `CreateTargetRequest` no longer requires `bdev_name` |
| `internal/agent/spdk_target_server.go` | `CreateTarget` creates ChunkEngine-backed bdev without local chunk store |
| `internal/csi/controller.go` | Volume placement selects frontend nodes, not backend nodes |
| `deploy/helm/novastor/templates/` | Frontend DaemonSet separated from backend, independent scaling |

## Testing

- Create volume with frontend on node WITHOUT backend → verify I/O works (all remote via NDP)
- Create volume with frontend on combo node → verify local I/O fast path
- Kill a backend with rep3 → verify reads failover to surviving replicas
- Add a new backend → verify new writes land there via CRUSH
- Scale frontends independently of backends → verify ANA states correct
- Benchmark: compare local-only vs remote-via-NDP latency and IOPS
