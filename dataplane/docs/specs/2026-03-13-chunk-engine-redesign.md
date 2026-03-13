# NovaStor Dataplane — Chunk Engine Redesign

**Date**: 2026-03-13
**Status**: Approved
**Scope**: Refactor backend engine, chunk engine, metadata store, and policy engine to implement correct layered architecture.

## Problem

The current dataplane treats backends (raw_disk, lvm, chunk) as independent alternatives. This is wrong. The correct architecture is layered:

- **Backend Engine** (bottom) — resource pool across all nodes
- **Chunk Engine** — content-addressed 4MB chunks stored on backends
- **Policy Engine** — replication, dedup, tiering enforcement
- **Presentation Layer** (top) — NVMe-oF, NFS, S3

Chunks are the universal storage primitive. Every I/O path goes through chunks regardless of backing store type. This enables replication across nodes, deduplication, and tiering.

## Architecture

```
Go management plane (thin)
  CSI driver + K8s API only
  Forwards all requests to Rust dataplane via gRPC

Rust dataplane (owns everything on the data path)
  ├─ Presentation layer (NVMe-oF TCP, NFS 4.1, S3)
  ├─ Policy engine (replication, dedup, tiering, GC, scrub)
  ├─ Chunk engine (volume I/O → chunk dispatch)
  ├─ Metadata store (sharded Raft + CRUSH placement)
  └─ Backend engine (file / LVM / raw chunk I/O)
```

The Go management plane does NOT handle data path operations, metadata lookups, or chunk management. Too slow. All actions are requested to the Rust dataplane via gRPC.

### Consistency Model

- **Write quorum**: Majority of replicas must acknowledge before write returns success (e.g., 2 of 3 for replication_factor=3).
- **Read consistency**: Reads served from local Raft follower replicas (eventual consistency). Strong reads available by routing to shard leader when needed.
- **Write ordering**: Writes to the same volume are serialized by the chunk engine on the coordinating node. Cross-volume writes are independent.

### Threading Model

SPDK owns the reactor thread (core 0) and runs its event loop there. All SPDK bdev I/O must be dispatched to the reactor via `spdk_thread_send_msg`.

**Current state**: The codebase is fully synchronous today. The `reactor_dispatch` module uses `Arc<Completion<T>>` (condvar-based blocking) to bridge sync callers with the SPDK reactor thread. There is no tokio runtime.

**Migration**: This redesign introduces tokio as the async runtime. This is a significant change:
- `main()` will be rewritten to start a tokio runtime alongside the SPDK reactor.
- The JSON-RPC server will be replaced/wrapped with an async version.
- The `reactor_dispatch` bridge will be rewritten from condvar-blocking `Completion` to `tokio::sync::oneshot` channels, so that SPDK I/O can be awaited without blocking tokio worker threads.

The tokio runtime runs on separate cores and handles:
- Raft consensus (openraft is async/tokio-native)
- gRPC server/client (tonic is async/tokio-native)
- Policy engine reconciliation loops
- FileChunkStore I/O (via `spawn_blocking`)

BdevChunkStore bridges the two: its async `put`/`get`/`delete` methods send closures to the SPDK reactor thread via `spdk_thread_send_msg` and receive results via `tokio::sync::oneshot` channels. The `BdevChunkStore` holds a persistent bdev descriptor and I/O channel (opened once at init, closed on drop) — unlike the current `reactor_dispatch` which opens/closes on every I/O call.

## 1. Backend Engine

The backend engine persists and retrieves chunk data. It exposes an async trait (required because `BdevChunkStore` dispatches I/O through SPDK's reactor thread via async channels):

```rust
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Store chunk data. Caller provides the chunk_id (SHA-256 hash).
    /// Data includes a ChunkHeader (CRC-32C, size) prepended by the chunk engine.
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()>;

    /// Retrieve chunk data by ID. Returns data including ChunkHeader.
    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>>;

    /// Delete chunk data by ID.
    async fn delete(&self, chunk_id: &str) -> Result<()>;

    /// Check if a chunk exists locally.
    async fn exists(&self, chunk_id: &str) -> Result<bool>;

    /// Local storage statistics.
    async fn stats(&self) -> Result<ChunkStoreStats>;
}

pub struct ChunkStoreStats {
    pub backend_name: String,  // identifies which backend instance
    pub total_bytes: u64,      // total capacity
    pub used_bytes: u64,       // allocated space (slots * chunk_size for bdev)
    pub data_bytes: u64,       // actual data stored (may differ from used for thin provisioning)
    pub chunk_count: u64,
}
```

`FileChunkStore` wraps its `std::fs` calls with `tokio::task::spawn_blocking` to avoid blocking the async runtime.

Backends do NOT own metadata. They are pure I/O. Hashing, checksums, dedup checks, and location tracking happen in the chunk engine and metadata store above.

### 1.1 FileChunkStore

For users who cannot dedicate a whole disk. Stores chunks as files on an existing filesystem.

- **Layout**: `<base_dir>/chunks/<ab>/<cd>/<abcdef0123456789...>` (2-level hash-prefix subdirectories)
- **I/O**: `std::fs` read/write wrapped in `tokio::task::spawn_blocking`. No SPDK dependency for storage path.
- **Atomicity**: Write to temp file in same directory, then `rename()` (atomic on POSIX). Partial writes never leave corrupt chunk files.
- **Init**: Requires a directory path. Creates subdirectory structure on first use.
- **Capacity**: Determined by available space on the underlying filesystem (`statvfs`).

### 1.2 BdevChunkStore

For raw block devices and LVM logical volumes. Stores chunks at calculated offsets on an SPDK bdev.

- **Offset allocator**: Bitmap allocator tracks which 4MB slots are free/used, aligned to 4MB boundaries. Supports both allocation and deallocation (unlike a bump allocator, freed slots can be reused). Refactored from current `chunk_io.rs`.
- **I/O**: `reactor_dispatch::bdev_write/bdev_read` through SPDK.
- **Raw mode**: Initialized with a raw device bdev name (e.g., uring bdev on `/dev/nvme0n1`). Full device, maximum performance.
- **LVM mode**: Initialized with an lvol bdev name. The lvol was created on top of the physical device by the BdevManager. Provides thin provisioning and device sharing.
- **Same code for both**: The BdevChunkStore doesn't care whether the bdev is raw or an lvol. The BdevManager handles lvol creation separately.
- **Index**: Local mapping of `chunk_id → offset` kept in memory, persisted to a reserved region at the start of the bdev.

#### Superblock and Index Format

```
Offset 0:          Superblock (4KB)
                     - magic: [u8; 8] = b"NVACHUNK"
                     - version: u32
                     - generation: u64 (monotonically increasing, for A/B crash recovery)
                     - chunk_size: u32 (default 4MB)
                     - total_slots: u64
                     - used_slots: u64
                     - bitmap_pages: u32
                     - index_pages: u32
                     - checksum: u32 (CRC-32C of superblock)

Offset 4KB:        Bitmap pages (1 bit per 4MB slot)
                     - For a 1.9TB device: ~58K slots = ~8KB bitmap

After bitmap:      Index pages (chunk_id → slot_number entries)
                     - Each entry: 32 bytes (SHA-256) + 4 bytes (slot_number) = 36 bytes
                     - Sorted by chunk_id for binary search on recovery

After index:       Data region (4MB-aligned slots)
```

Superblock and index use A/B double-buffering for crash safety:

- Two copies of the superblock+bitmap+index region are maintained at fixed offsets (copy A at offset 0, copy B immediately after copy A's reserved region).
- Each superblock contains a monotonically increasing `generation: u64` counter.
- Write sequence: increment generation → write to the **inactive** copy only → flush. The old active copy remains untouched as a valid fallback.
- On recovery: read both copies, verify CRC-32C of each superblock, use the copy with the highest valid generation. If the latest write was interrupted, the previous copy is still intact.
- If both copies are corrupt (extremely unlikely — requires two crashes during two consecutive writes), rebuild the index by scanning all data slots for valid ChunkHeaders — slow but safe.

### Backend comparison

| Backend | Chunk storage | I/O path | Use case |
|---------|--------------|----------|----------|
| File | One file per chunk in a directory | POSIX read/write | Can't dedicate a disk, use existing FS |
| LVM | Offsets within a single lvol on a device | SPDK bdev I/O | Share a device, thin provisioning |
| Raw | Offsets on raw block device | SPDK bdev I/O | Maximum performance, dedicated disk |

## 2. Metadata Store

The metadata store is the cluster brain. It runs in the Rust dataplane on every node. Two subsystems handle different concerns:

### 2.1 CRUSH-like Placement (chunk locations)

Chunk locations are computed, not stored. Given `(chunk_id, replication_factor, cluster_topology)`, every node can independently calculate which nodes and backends should store a given chunk.

- **Algorithm**: Simplified CRUSH based on the Ceph CRUSH paper (Weil et al., 2006). Initial implementation uses **straw2 bucket type only** (weighted random selection proportional to device capacity). No CRUSH rules or multiple bucket types in v1 — a flat hierarchy of `root → node → backend`.
- **Placement function**: `select(chunk_id, count, topology) → Vec<(node_id, backend_id)>`. Uses chunk_id as the PRNG seed. Deterministic: same inputs always produce same output on any node.
- **Failure domains**: Replicas are placed on different nodes (never two replicas on the same node). Within a node, CRUSH selects the backend by weight.
- Every node holds the same cluster map (topology of nodes, backends, weights, capacities).
- Cluster map changes (node join/leave, backend add/remove) go through a single Raft group. These are rare events.
- Zero per-chunk write overhead on the hot path.
- **Future**: Hierarchical bucket types (rack, datacenter) and CRUSH rules can be added when multi-rack deployments are needed.

### 2.2 Sharded Raft (volume/policy metadata)

Volume definitions, chunk-to-volume mappings, and policies are stored in a sharded Raft consensus system.

- **Sharding**: Volume ID space partitioned by first 2 hex characters of volume_id = 256 shards. Each shard is an independent Raft group with its own leader election. All metadata for a volume (definition, chunk map, policies) lives in one shard, determined by volume_id.
- **Distribution**: With 4 nodes, each node leads ~64 shards. Writes spread evenly across the cluster. No single-leader bottleneck.
- **Technology**: `openraft` for Raft consensus, `redb` for local persistence of Raft log and state machine. Each shard gets its own redb database file (`raft_shard_XX.redb`) to avoid lock contention between shards. Implements `openraft::RaftLogStorage` and `openraft::RaftStateMachine` traits with redb tables for log entries, voted-for state, and the applied state machine (volume definitions, chunk maps, policies).
- **Reads**: Served locally from follower replicas (eventual consistency acceptable for most reads). Strong reads route to shard leader.

### What lives where

| Data | Storage mechanism | Rationale |
|------|------------------|-----------|
| Chunk locations | CRUSH (computed from chunk_id) | Hot path, zero write overhead |
| Cluster topology | Single Raft group (all nodes) | Rare changes only |
| Volume definitions | Sharded Raft (shard = volume_id prefix) | Distributed writes |
| Volume chunk maps | Sharded Raft (shard = volume_id prefix) | Co-located with volume |
| Policies | Sharded Raft (shard = volume_id prefix) | Co-located with volume |
| Chunk checksums/size | Prepended as ChunkHeader in backend data | Verified on read, no separate store needed |

### Inter-node communication

Dataplane instances communicate via gRPC (`tonic` + `prost`):

- Raft messages (append entries, vote requests) between shard replicas.
- Chunk I/O forwarding (when CRUSH places a chunk on a remote node).
- Peer discovery via the Go management plane passing peer addresses at startup.

## 3. Chunk Engine

The chunk engine translates volume-level I/O into chunk operations and dispatches them to the correct backends on the correct nodes.

### Chunk Header

Every chunk stored by a backend is prefixed with a fixed-size header:

```rust
pub struct ChunkHeader {
    pub magic: [u8; 4],    // b"NVAC"
    pub version: u8,
    pub flags: u8,         // 0x01 = erasure shard (not content-addressed)
    pub checksum: u32,     // CRC-32C of the data portion (after header)
    pub data_len: u32,     // actual data length (may be < 4MB for partial/last chunk)
    pub _reserved: [u8; 2],
}
// Total: 16 bytes, prepended to chunk data by the chunk engine before calling backend.put()
```

### Responsibilities

1. **Volume I/O translation**: Volume offset → chunk index → chunk ID → CRUSH placement → dispatch to local or remote backend.
2. **Content addressing**: SHA-256 hash of chunk data = chunk ID. Identical data = same ID = automatic deduplication.
3. **Integrity**: CRC-32C computed on write (stored in ChunkHeader), verified on every read.
4. **Chunk splitting**: Incoming writes split into 4MB-aligned chunks.
5. **Read-modify-write**: Partial chunk writes read existing chunk, merge new data, write as new chunk (new hash = new chunk ID).
6. **Remote dispatch**: If CRUSH says a chunk belongs on another node, forward the I/O via gRPC.

### Write path (replicated volumes)

```
Volume write(volume_id, offset, data)
  → split into 4MB-aligned chunks
  → for each chunk:
      → SHA-256(data) → chunk_id
      → prepend ChunkHeader (CRC-32C, data_len, flags=0x00)
      → CRUSH(chunk_id, topology) → target node(s)
      → write to N nodes per replication_factor (parallel)
        → if local: backend.put(chunk_id, header+data)
        → if remote: gRPC to target node's dataplane
      → wait for majority (write quorum) acknowledgments
      → update volume chunk map in sharded Raft (chunk_index → chunk_id)
      → return success to caller
```

### Write path (erasure coded volumes)

Erasure coded volumes do NOT use content-addressed chunk IDs for the shards. The original 4MB chunk is split into K data shards + M parity shards via Reed-Solomon. Each shard has different content, so its SHA-256 hash differs from the original chunk.

```
Volume write(volume_id, offset, data)
  → split into 4MB-aligned chunks
  → for each chunk:
      → SHA-256(data) → original_chunk_id (used in volume chunk map)
      → Reed-Solomon encode → K data shards + M parity shards
      → for each shard i in 0..K+M:
          → shard_id = SHA-256(original_chunk_id || shard_index_i)
          → prepend ChunkHeader (CRC-32C of shard, data_len, flags=0x01)
          → CRUSH(shard_id, topology) → target node for this shard
          → write shard to target node's backend
      → wait for at least K+M-1 shards written (tolerate 1 shard failure; policy engine will reconstruct the missing shard asynchronously)
      → update volume chunk map: chunk_index → {original_chunk_id, ec_params: {K, M}}
      → return success to caller
```

### Write atomicity

Chunk writes and metadata updates are NOT atomic across the system. Failure semantics:

- **Backend write succeeds, Raft update fails**: Orphan chunk on backend. The GC process (policy engine) detects chunks with zero references and deletes them.
- **Partial replica writes**: If fewer than quorum replicas succeed, the write returns an error to the caller. Successfully written replicas become orphans, cleaned up by GC.
- **Raft update succeeds, backend not yet durable**: Not possible — backend writes complete before Raft update is submitted.
- **Read-after-write on different node**: A write completes on node A (Raft update committed). A read on node B may see the old chunk map if node B's Raft follower hasn't replicated the update yet. This is acceptable eventual consistency. For strong read-after-write guarantees (e.g., CSI block volumes), the presenting node should be the same as the coordinating node, or reads should route to the shard leader.

### Read path

```
Volume read(volume_id, offset, length)
  → lookup volume chunk map from sharded Raft (local follower read)
  → for each chunk_index in range:
      → chunk_id from volume chunk map
      → if replicated:
          → CRUSH(chunk_id) → node(s) that have it
          → pick closest/fastest node
          → if local: backend.get(chunk_id), verify CRC-32C from ChunkHeader
          → if remote: gRPC fetch from that node
      → if erasure coded:
          → derive shard_ids from original_chunk_id + shard indices
          → fetch K of K+M shards (prefer local, then closest)
          → verify CRC-32C on each shard
          → Reed-Solomon decode → original chunk data
  → assemble chunks, return requested byte range
```

## 4. Policy Engine

The policy engine is an async background process on every node. It reads desired state (policies) from the Raft store and actual state (computed via CRUSH + health checks) and reconciles the difference.

### Responsibilities

1. **Replication enforcement**: If a volume's policy says `replication_factor=3` and a chunk only exists on 2 nodes (one failed), trigger re-replication to a third node.
2. **Erasure coding enforcement**: If a volume uses Reed-Solomon 4+2 and a shard is missing, reconstruct from remaining shards and place on a new node.
3. **Deduplication**: Content-addressing provides inherent dedup within a volume. Policy can enable cross-volume dedup scanning for chunks written with different alignment.
4. **Tiering**: Move chunks between backend types based on access patterns. E.g., "chunks not accessed in 30 days move from raw SSD backend to file backend on HDD". Updates CRUSH weights/rules.
5. **Garbage collection**: Reference counting via mark-and-sweep. The GC scans all volume chunk maps across all shards to build the set of referenced chunk IDs, then compares against chunks stored on local backends. Chunks not referenced by any volume are deleted. This handles cross-volume dedup correctly — if volumes A and B both reference the same content-addressed chunk_id, the chunk is only deleted when neither volume references it. Mark-and-sweep avoids the need for a separate refcount store.
6. **Scrubbing**: Background integrity verification — read chunks, verify CRC-32C, report corruption, trigger re-replication from healthy copy.

### Execution model

- Reconciliation loop on every node. Each node runs policy for the Raft shards it leads.
- No single bottleneck — policy work distributed the same way as metadata.
- Fast loop for replication enforcement (seconds).
- Slow loop for dedup/tiering (minutes/hours).
- Node failure detection triggers immediate re-replication for affected chunks.

```rust
pub struct PolicyEngine {
    metadata: Arc<MetadataStore>,
    chunk_engine: Arc<ChunkEngine>,
}

pub struct ReconcileReport {
    pub volumes_checked: u64,
    pub chunks_re_replicated: u64,
    pub chunks_gc_deleted: u64,
    pub errors: Vec<String>,
}

pub struct DedupReport {
    pub chunks_scanned: u64,
    pub duplicates_found: u64,
    pub bytes_reclaimed: u64,
}

pub struct ScrubResult {
    pub chunks_scanned: u64,
    pub corrupted_chunks: Vec<String>,  // chunk IDs with CRC mismatch
    pub repairs_triggered: u64,          // re-replications from healthy copy
}

impl PolicyEngine {
    /// Main reconciliation loop — check all volumes in our shards.
    async fn reconcile(&self) -> Result<ReconcileReport>;

    /// Handle node failure — re-replicate affected chunks.
    async fn handle_node_failure(&self, node_id: &str) -> Result<()>;

    /// Run dedup scan for a volume.
    async fn dedup_scan(&self, volume_id: &str) -> Result<DedupReport>;

    /// Run integrity scrub for locally-stored chunks.
    async fn scrub_local(&self) -> Result<ScrubResult>;
}
```

## 5. Codebase Changes

### Files to keep (with modifications)

| Current file | Change |
|---|---|
| `src/bdev/chunk_io.rs` | Refactored into `BdevChunkStore`, stripped of metadata ownership, keeps offset allocator + SPDK bdev I/O |
| `src/bdev/erasure.rs` | Moves into chunk engine — erasure encoding/decoding during chunk dispatch |
| `src/bdev/replica.rs` | Moves into chunk engine — replication logic during chunk dispatch |
| `src/spdk/bdev_manager.rs` | Stays — manages SPDK bdevs (uring, malloc, lvol). Backend engine calls into this |
| `src/spdk/nvmf_manager.rs` | Stays — presentation layer, NVMe-oF target management |
| `src/spdk/env.rs` | Stays — SPDK environment init |
| `src/jsonrpc/` | Updated — RPC methods reflect new layered API |

### New modules

| Module | Purpose |
|---|---|
| `src/backend/traits.rs` | `ChunkStore` trait (put/get/delete/exists/stats) |
| `src/backend/file.rs` | `FileChunkStore` — POSIX file I/O with hash-prefix dirs |
| `src/backend/bdev.rs` | `BdevChunkStore` — refactored from chunk_io.rs, covers raw + LVM |
| `src/chunk/engine.rs` | Chunk engine — volume I/O, content addressing, remote dispatch |
| `src/chunk/volume.rs` | Volume chunk map management |
| `src/metadata/store.rs` | Local metadata store with redb |
| `src/metadata/raft.rs` | Sharded Raft consensus via openraft |
| `src/metadata/crush.rs` | CRUSH-like placement algorithm |
| `src/policy/engine.rs` | Policy reconciliation loop |
| `src/policy/replication.rs` | Replication factor enforcement |
| `src/policy/dedup.rs` | Dedup scanning |
| `src/policy/tiering.rs` | Tier placement rules |
| `src/cluster/transport.rs` | Node-to-node gRPC for remote chunk I/O and Raft messages |

### Files to delete (replaced by new layering)

| File | Replacement |
|---|---|
| `src/backend/chunk.rs` | Volume logic → `chunk/volume.rs`, I/O dispatch → `chunk/engine.rs` |
| `src/backend/raw_disk.rs` | Replaced by `BdevChunkStore` + backend engine |
| `src/backend/lvm.rs` | Replaced by `BdevChunkStore` + backend engine |
| `src/backend/traits.rs` | Old `StorageBackend` trait replaced by `ChunkStore` trait |

### New dependencies

| Crate | Purpose |
|---|---|
| `openraft` | Raft consensus for metadata replication |
| `redb` | Embedded key-value store for Raft log and metadata persistence |
| `tonic` | gRPC server/client for inter-node communication |
| `prost` | Protocol buffer code generation for gRPC |
| `tokio` | Async runtime (new direct dependency — the codebase is currently fully synchronous) |
| `async-trait` | Async trait support for `ChunkStore` |
| `reed-solomon-simd` | Reed-Solomon erasure coding (already in Cargo.toml) |
| `crc32c` | Hardware-accelerated CRC-32C checksums |
| `sha2` | SHA-256 content addressing (already in Cargo.toml) |
| `hex` | Chunk ID hex encoding (already in Cargo.toml) |

### New proto definitions

| Proto file | Purpose |
|---|---|
| `proto/chunk.proto` | `ChunkService` — remote chunk put/get/delete between nodes |
| `proto/raft.proto` | `RaftService` — append entries, vote requests between shard replicas |
| `proto/cluster.proto` | `ClusterService` — peer discovery, topology updates |

## 6. Deferred Features

The following features from the current `StorageBackend` trait are NOT part of this redesign and will be addressed in a future spec:

- **Snapshots**: Volume-level snapshots (copy-on-write via chunk map forking). The chunk-based architecture makes this straightforward — a snapshot is a frozen copy of the volume chunk map. No data copying needed.
- **Clones**: Writable copies of snapshots. Same mechanism — fork the chunk map, new writes create new chunks.
- **Encryption at rest**: Per-chunk encryption before backend storage. Planned for Phase 5 per CLAUDE.md.

These features are architecturally compatible with the chunk engine design and do not require changes to the `ChunkStore` trait (they operate at the volume/chunk-map level, not the backend level).

## 7. Testing Strategy

### Unit tests (no SPDK, no cluster)
- `FileChunkStore`: put/get/delete chunks as files, verify hash-prefix layout, capacity tracking
- `BdevChunkStore`: mock bdev I/O, test offset allocator, dedup behavior
- CRUSH placement: deterministic placement given topology, correct rebalancing on node add/remove
- Chunk engine: volume I/O → chunk splitting, content addressing, read-modify-write
- Policy engine: replication factor enforcement logic, GC reference counting

### Integration tests (single node, SPDK required)
- Full write path: volume write → chunk engine → backend → SPDK bdev → read back and verify
- All three backend types with real I/O
- NVMe-oF presentation of chunk-backed volumes

### Cluster tests (multi-node, K8s)
- Raft shard leader election and failover
- Cross-node chunk replication via gRPC
- Node failure → policy engine re-replication
- CRUSH rebalancing on topology change
