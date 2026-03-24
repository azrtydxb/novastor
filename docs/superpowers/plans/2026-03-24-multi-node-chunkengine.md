# Multi-Node ChunkEngine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the ChunkEngine to operate as a fully distributed storage system with volume-aware topology, stable CRUSH placement, write-back caching, PolicyEngine-driven migration, and distributed chunk maps.

**Architecture:** All I/O flows through ChunkEngine (no bypass). CRUSH is stable (straw2) — topology changes are accepted freely. Writes are cached and flushed on host FLUSH. Migration is driven by PolicyEngine on each node. Chunk maps propagate in real-time via NDP between dataplanes.

**Tech Stack:** Rust (tokio, tonic, SPDK FFI), Go (gRPC, controller-runtime), Protobuf, NDP (custom binary protocol), redb (embedded DB)

**Spec:** `docs/superpowers/specs/2026-03-24-multi-node-chunkengine-design.md`

---

## File Structure

### Proto files (both copies must stay in sync)
- Modify: `api/proto/dataplane/dataplane.proto` — add VolumeInfo, ChunkMapUpdate, SyncChunkMaps RPC
- Modify: `dataplane/proto/dataplane_service.proto` — same additions

### Go files
- Modify: `cmd/agent/main.go` — include volumes in topology push
- Modify: `internal/dataplane/client.go` — update UpdateTopology client, add SyncChunkMaps
- Modify: `api/proto/dataplane/dataplane.pb.go` — regenerated

### Rust dataplane files
- Modify: `dataplane/src/metadata/topology.rs` — add volumes to ClusterMap, bulk set_nodes
- Modify: `dataplane/src/metadata/types.rs` — add placements + generation to ChunkMapEntry
- Modify: `dataplane/src/chunk/engine.rs` — write cache integration, flush, migrate_chunk, chunk map reads
- Modify: `dataplane/src/transport/dataplane_service.rs` — remove topology guard, process volumes, SyncChunkMaps RPC
- Modify: `dataplane/src/transport/ndp_server.rs` — handle ChunkMapSync
- Modify: `dataplane/src/chunk/ndp_pool.rs` — add chunk_map_sync broadcast
- Modify: `dataplane/src/policy/engine.rs` — migration detection + execution
- Modify: `dataplane/src/bdev/novastor_bdev.rs` — FLUSH handler calls ChunkEngine, remove old WriteCache
- Modify: `dataplane/ndp/src/header.rs` — add ChunkMapSync op
- Create: `dataplane/src/chunk/write_cache.rs` — write-back cache

---

## Task 1: Proto — Add VolumeInfo to UpdateTopologyRequest

**Files:**
- Modify: `api/proto/dataplane/dataplane.proto:503-506`
- Modify: `dataplane/proto/dataplane_service.proto:390-393`

- [ ] **Step 1: Add VolumeInfo message and update UpdateTopologyRequest in Go proto**

In `api/proto/dataplane/dataplane.proto`, add before `UpdateTopologyRequest`:

```protobuf
message VolumeInfo {
  string name = 1;
  uint64 size_bytes = 2;
  string protection_type = 3;
  uint32 rep_factor = 4;
  uint32 data_shards = 5;
  uint32 parity_shards = 6;
  string status = 7;
}
```

Add field 3 to `UpdateTopologyRequest`:

```protobuf
message UpdateTopologyRequest {
  uint64 generation = 1;
  repeated TopologyNode nodes = 2;
  repeated VolumeInfo volumes = 3;
}
```

- [ ] **Step 2: Mirror the same changes in Rust proto**

In `dataplane/proto/dataplane_service.proto`, add the same `VolumeInfo` message and field 3 to `UpdateTopologyRequest`.

- [ ] **Step 3: Regenerate Go protobuf code**

Run: `cd /Users/pascal/Development/Nova/novastor && make generate-proto`
Expected: `api/proto/dataplane/dataplane.pb.go` updated with VolumeInfo type

- [ ] **Step 4: Build Rust dataplane to regenerate Rust proto code**

Run: `cd dataplane && docker build -f Dockerfile.build --target rust-builder -t novastor-dataplane:proto-check .`
Expected: Build succeeds (proto compiled by tonic-build in build.rs)

- [ ] **Step 5: Commit**

```bash
git add api/proto/ dataplane/proto/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Proto: add VolumeInfo to UpdateTopologyRequest"
```

---

## Task 2: ClusterMap — Add volumes, fix generation, bulk set_nodes

**Files:**
- Modify: `dataplane/src/metadata/topology.rs`
- Modify: `dataplane/src/metadata/types.rs:49-56`

- [ ] **Step 1: Add placements and generation to ChunkMapEntry**

In `dataplane/src/metadata/types.rs`, update `ChunkMapEntry`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapEntry {
    pub chunk_index: u64,
    pub chunk_id: String,
    pub ec_params: Option<ErasureParams>,
    #[serde(default)]
    pub dirty_bitmap: u64,
    #[serde(default)]
    pub placements: Vec<String>,
    #[serde(default)]
    pub generation: u64,
}
```

- [ ] **Step 2: Add volumes to ClusterMap and add set_nodes method**

In `dataplane/src/metadata/topology.rs`, add to `ClusterMap`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMap {
    generation: u64,
    nodes: Vec<Node>,
    #[serde(default)]
    volumes: Vec<VolumeDefinition>,
}
```

Add import for `VolumeDefinition` from `crate::metadata::types`.

Add methods:

```rust
/// Bulk-set nodes without incrementing generation per node.
/// Used when constructing from proto — generation is set once from proto field.
pub fn set_nodes(&mut self, nodes: Vec<Node>) {
    self.nodes = nodes;
}

pub fn set_volumes(&mut self, volumes: Vec<VolumeDefinition>) {
    self.volumes = volumes;
}

pub fn volumes(&self) -> &[VolumeDefinition] {
    &self.volumes
}
```

- [ ] **Step 3: Fix existing tests that construct ChunkMapEntry without new fields**

Search for `ChunkMapEntry {` across the codebase and add `placements: vec![], generation: 0` to each construction site. The `#[serde(default)]` handles deserialization of old data.

- [ ] **Step 4: Run tests**

Run: `cd dataplane && cargo test --release -- --test-threads=1` (in Docker build container)
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add dataplane/src/metadata/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] ClusterMap: add volumes, placements, generation, bulk set_nodes"
```

---

## Task 3: Dataplane — Process volumes in UpdateTopology, remove guard

**Files:**
- Modify: `dataplane/src/transport/dataplane_service.rs` (update_topology handler)
- Modify: `dataplane/src/chunk/engine.rs` (update_topology method)

- [ ] **Step 1: Update ChunkEngine::update_topology to return old topology and process volumes**

In `dataplane/src/chunk/engine.rs`, change `update_topology`:

```rust
/// Update topology and process volume changes.
/// Returns the old topology (for migration detection) and whether accepted.
pub fn update_topology(&self, new_topology: ClusterMap) -> (bool, Option<Arc<ClusterMap>>) {
    let mut topo = self.topology.write().unwrap();
    let current_gen = topo.generation();
    let new_gen = new_topology.generation();
    if current_gen > 0 && new_gen <= current_gen {
        log::warn!(
            "rejecting stale topology update: current={} proposed={}",
            current_gen, new_gen,
        );
        return (false, None);
    }
    log::info!(
        "topology updated: generation {} -> {}, nodes: {}, volumes: {}",
        current_gen, new_gen,
        new_topology.nodes().len(),
        new_topology.volumes().len(),
    );
    let old = topo.clone();
    *topo = Arc::new(new_topology);
    (true, Some(old))
}
```

- [ ] **Step 2: Update dataplane_service update_topology handler**

In `dataplane/src/transport/dataplane_service.rs`, replace the topology guard and handler:

1. Remove the `if crate::bdev::novastor_bdev::get_backend_bdev_name().is_ok()` guard block entirely
2. Use `set_nodes` instead of per-node `add_node` when building ClusterMap
3. Parse `VolumeInfo` from proto and set on ClusterMap
4. After `engine.update_topology(topology)`, register/unregister volume hashes:

```rust
// Register volume hashes for NDP lookups
for vol in new_topo.volumes() {
    crate::transport::ndp_server::register_volume_hash(&vol.name);
}
// TODO: unregister removed volumes (compare old vs new)
```

- [ ] **Step 3: Build and verify**

Run: Docker build of dataplane
Expected: Compiles cleanly

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/transport/dataplane_service.rs dataplane/src/chunk/engine.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Process volumes in UpdateTopology, remove topology guard"
```

---

## Task 4: Go Agent — Include volumes in topology push

**Files:**
- Modify: `cmd/agent/main.go` (topology push function)
- Modify: `internal/dataplane/client.go` (UpdateTopology client)

- [ ] **Step 1: Update Go agent topology push to include volumes**

In `cmd/agent/main.go`, in the `push()` function (around line 162), after building `protoNodes`, query volumes from the metadata client and include them:

```go
// Fetch active volumes from metadata service
volumes, err := metaClient.ListVolumesMeta(ctx)
if err != nil {
    logging.L.Warn("topology sync: failed to list volumes", zap.Error(err))
    // Non-fatal — push topology without volumes
    volumes = nil
}

var protoVolumes []*pb.VolumeInfo
for _, v := range volumes {
    vi := &pb.VolumeInfo{
        Name:      v.VolumeID,
        SizeBytes: uint64(v.SizeBytes),
        Status:    v.Status,
    }
    if v.ReplicationFactor > 0 {
        vi.ProtectionType = "replication"
        vi.RepFactor = uint32(v.ReplicationFactor)
    }
    // TODO: EC params when available
    protoVolumes = append(protoVolumes, vi)
}
```

Update the `dpClient.UpdateTopology` call to pass volumes.

- [ ] **Step 2: Update UpdateTopology client to accept volumes**

In `internal/dataplane/client.go`, update the `UpdateTopology` function signature:

```go
func (c *Client) UpdateTopology(generation uint64, nodes []*pb.TopologyNode, volumes []*pb.VolumeInfo) (bool, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    resp, err := c.svc.UpdateTopology(ctx, &pb.UpdateTopologyRequest{
        Generation: generation,
        Nodes:      nodes,
        Volumes:    volumes,
    })
```

Update all callers of `UpdateTopology` to pass the new parameter.

- [ ] **Step 3: Build Go**

Run: `cd /Users/pascal/Development/Nova/novastor && make build-all`
Expected: All binaries compile

- [ ] **Step 4: Commit**

```bash
git add cmd/agent/main.go internal/dataplane/client.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Go agent includes volumes in topology push"
```

---

## Task 5: NDP — Add ChunkMapSync operation

**Files:**
- Modify: `dataplane/ndp/src/header.rs:9-21`
- Modify: `dataplane/src/transport/ndp_server.rs`
- Modify: `dataplane/src/chunk/ndp_pool.rs`

- [ ] **Step 1: Add ChunkMapSync to NdpOp enum**

In `dataplane/ndp/src/header.rs`, add to the enum:

```rust
pub enum NdpOp {
    Write = 0x01,
    Read = 0x02,
    WriteResp = 0x03,
    ReadResp = 0x04,
    WriteZeroes = 0x05,
    Unmap = 0x06,
    Replicate = 0x07,
    ReplicateResp = 0x08,
    EcShard = 0x09,
    Ping = 0x0A,
    Pong = 0x0B,
    ChunkMapSync = 0x0C,      // NEW
    ChunkMapSyncResp = 0x0D,  // NEW
}
```

Update `from_u8`, `has_request_data` (ChunkMapSync has data), `has_response_data` (ChunkMapSyncResp does not).

- [ ] **Step 2: Add chunk_map_sync to NdpPool**

In `dataplane/src/chunk/ndp_pool.rs`, add:

```rust
/// Broadcast chunk map updates to all connected peers.
/// Each update is serialized as JSON in the data payload.
pub async fn broadcast_chunk_map_sync(
    &self,
    updates: &[ChunkMapSyncEntry],
) -> Vec<(String, Result<()>)> {
    let data = serde_json::to_vec(updates).unwrap_or_default();
    let conns = self.connections.lock().await;
    let mut results = Vec::new();
    for (addr, conn) in conns.iter() {
        let header = NdpHeader::request(
            NdpOp::ChunkMapSync, 0, 0, 0, data.len() as u32,
        );
        let result = conn.request(header, Some(data.clone())).await
            .map(|_| ())
            .map_err(|e| DataPlaneError::TransportError(
                format!("ChunkMapSync to {}: {}", addr, e)
            ));
        results.push((addr.clone(), result));
    }
    results
}
```

Add the `ChunkMapSyncEntry` struct (or import from types):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapSyncEntry {
    pub volume_id: String,
    pub chunk_index: u64,
    pub dirty_bitmap: u64,
    pub placements: Vec<String>,
    pub generation: u64,
}
```

- [ ] **Step 3: Handle ChunkMapSync in NDP server**

In `dataplane/src/transport/ndp_server.rs`, add to `handle_request` match:

```rust
NdpOp::ChunkMapSync => handle_chunk_map_sync(msg.data).await,
```

Implement:

```rust
async fn handle_chunk_map_sync(data: Option<Vec<u8>>) -> NdpMessage {
    let entries: Vec<ChunkMapSyncEntry> = match data
        .as_deref()
        .and_then(|d| serde_json::from_slice(d).ok())
    {
        Some(e) => e,
        None => {
            return NdpMessage::new(
                NdpHeader::request(NdpOp::ChunkMapSyncResp, 0, 0, 0, 0),
                None,
            );
        }
    };

    // Merge into local chunk maps (highest generation wins)
    if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
        for entry in &entries {
            let existing = store.get_chunk_map(&entry.volume_id, entry.chunk_index)
                .ok().flatten();
            let should_update = match &existing {
                Some(e) => entry.generation > e.generation,
                None => true,
            };
            if should_update {
                let cm = crate::metadata::types::ChunkMapEntry {
                    chunk_index: entry.chunk_index,
                    chunk_id: format!("{}:{}", entry.volume_id, entry.chunk_index),
                    ec_params: None,
                    dirty_bitmap: entry.dirty_bitmap,
                    placements: entry.placements.clone(),
                    generation: entry.generation,
                };
                let _ = store.put_chunk_map(&entry.volume_id, &cm);
            }
        }
    }

    NdpMessage::new(
        NdpHeader::request(NdpOp::ChunkMapSyncResp, 0, 0, 0, 0),
        None,
    )
}
```

- [ ] **Step 4: Build and test**

Run: Docker build of dataplane
Expected: Compiles cleanly

- [ ] **Step 5: Commit**

```bash
git add dataplane/ndp/src/header.rs dataplane/src/transport/ndp_server.rs dataplane/src/chunk/ndp_pool.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] NDP ChunkMapSync operation for distributed chunk maps"
```

---

## Task 6: ChunkEngine — Reads with chunk map fallback

**Files:**
- Modify: `dataplane/src/chunk/engine.rs` (sub_block_read)

- [ ] **Step 1: Update sub_block_read to check chunk map placements first**

In `dataplane/src/chunk/engine.rs`, modify `sub_block_read` to try chunk map recorded placements before CRUSH:

After computing `chunk_index` and before CRUSH selection, check the metadata store for recorded placements:

```rust
// Try chunk map recorded placements first (actual data location)
if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
    if let Ok(Some(cm_entry)) = store.get_chunk_map(volume_id, chunk_index) {
        if !cm_entry.placements.is_empty() {
            let vol_hash = Self::volume_hash(volume_id);
            for placement_node in &cm_entry.placements {
                let result = if placement_node == &self.node_id {
                    #[cfg(feature = "spdk-sys")]
                    { crate::bdev::novastor_bdev::sub_block_read_local(volume_id, offset, length).await }
                    #[cfg(not(feature = "spdk-sys"))]
                    { Err(DataPlaneError::ChunkEngineError("requires spdk-sys".into())) }
                } else {
                    let addr = {
                        let topo = self.topology.read().unwrap();
                        topo.get_node(placement_node)
                            .map(|n| format!("{}:{}", n.address, NDP_PORT))
                    };
                    match addr {
                        Some(a) => self.ndp_pool.sub_block_read(&a, vol_hash, offset, length as u32).await,
                        None => continue,
                    }
                };
                match result {
                    Ok(data) => return Ok(data),
                    Err(_) => continue, // try next placement
                }
            }
            // All recorded placements failed, fall through to CRUSH
        }
    }
}

// Existing CRUSH-based read logic follows...
```

- [ ] **Step 2: Build and test**

Run: Docker build
Expected: Compiles

- [ ] **Step 3: Commit**

```bash
git add dataplane/src/chunk/engine.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] ChunkEngine reads: try chunk map placements before CRUSH fallback"
```

---

## Task 7: ChunkEngine — Record placements and broadcast chunk map on flush

**NOTE:** This task records placements during the CRUSH fan-out path. Once the write-back cache is added (Task 9), writes go to cache first and fan-out happens during FLUSH. The placement recording code written here will be called from the flush path in Task 9, not from the cached write path.

**Files:**
- Modify: `dataplane/src/chunk/engine.rs` (sub_block_write, new flush helper)
- Modify: `dataplane/src/chunk/ndp_pool.rs` (add batched broadcast)

- [ ] **Step 1: Add ChunkMapSync batching to NdpPool**

In `ndp_pool.rs`, add a `ChunkMapBatcher` that accumulates entries and flushes every 100ms or when batch reaches 1000 entries:

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

pub struct ChunkMapBatcher {
    pool: Arc<NdpPool>,
    pending: Arc<Mutex<Vec<ChunkMapSyncEntry>>>,
}

impl ChunkMapBatcher {
    pub fn new(pool: Arc<NdpPool>) -> Self {
        let batcher = Self {
            pool: pool.clone(),
            pending: Arc::new(Mutex::new(Vec::new())),
        };
        // Spawn timer-based flush
        let pending = batcher.pending.clone();
        let pool2 = pool;
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(100));
            loop {
                tick.tick().await;
                let batch: Vec<_> = {
                    let mut p = pending.lock().await;
                    if p.is_empty() { continue; }
                    std::mem::take(&mut *p)
                };
                pool2.broadcast_chunk_map_sync(&batch).await;
            }
        });
        batcher
    }

    pub async fn enqueue(&self, entry: ChunkMapSyncEntry) {
        let mut pending = self.pending.lock().await;
        pending.push(entry);
        if pending.len() >= 1000 {
            let batch = std::mem::take(&mut *pending);
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.broadcast_chunk_map_sync(&batch).await;
            });
        }
    }
}
```

- [ ] **Step 2: Record actual placements after successful write quorum**

In `sub_block_write`, modify the JoinSet result collection to track which nodes succeeded (change `Ok(_target)` to `Ok(target_name)` and collect into a `Vec<String>`).

After the quorum check succeeds, record placements and enqueue broadcast:

```rust
let gen = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as u64;

// Record in local chunk map
if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
    let cm = crate::metadata::types::ChunkMapEntry {
        chunk_index,
        chunk_id: chunk_key.clone(),
        ec_params: None,
        dirty_bitmap: u64::MAX,
        placements: successful_nodes.clone(),
        generation: gen,
    };
    let _ = store.put_chunk_map(volume_id, &cm);
}

// Enqueue broadcast to peers (batched, 100ms or 1000 entries)
if let Some(batcher) = self.chunk_map_batcher.as_ref() {
    let entry = ChunkMapSyncEntry {
        volume_id: volume_id.to_string(),
        chunk_index,
        dirty_bitmap: u64::MAX,
        placements: successful_nodes,
        generation: gen,
    };
    batcher.enqueue(entry).await;
}
```

Add `chunk_map_batcher: Option<ChunkMapBatcher>` to ChunkEngine struct, initialize in constructors.

- [ ] **Step 3: Build and test**

Run: Docker build
Expected: Compiles

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/chunk/engine.rs dataplane/src/chunk/ndp_pool.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Record placements in chunk map, batched NDP broadcast to peers"
```

---

## Task 8: PolicyEngine — Detect and migrate misplaced chunks

**Files:**
- Modify: `dataplane/src/policy/engine.rs`
- Modify: `dataplane/src/chunk/engine.rs` (add migrate_chunk method)
- Modify: `dataplane/ndp/src/header.rs` (add FLAGS_MIGRATION flag)

- [ ] **Step 1: Add migration flag to NDP header**

In `dataplane/ndp/src/header.rs`, add a flags constant:

```rust
/// NDP header flags
pub const FLAG_MIGRATION: u8 = 0x01;  // Write is a migration — check generation before accepting
```

The NDP server's `handle_write` checks this flag: if set, compare the incoming chunk's generation (carried in `chunk_idx` field repurposed as generation high bits + `sb_idx` as low byte, or better: add generation to the data payload header) against the local chunk map generation. Reject if local generation is higher (prevents stale migration from overwriting fresh host writes).

In `handle_write`, add after volume lookup:

```rust
if header.flags & ndp::header::FLAG_MIGRATION != 0 {
    // Check generation before accepting migration write
    if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
        let chunk_idx = header.offset / (4 * 1024 * 1024); // CHUNK_SIZE
        if let Ok(Some(local_cm)) = store.get_chunk_map(&volume_name, chunk_idx) {
            if local_cm.generation > 0 {
                // Local data is newer — reject migration
                return NdpMessage::new(
                    NdpHeader::response(header, NdpOp::WriteResp, 5, 0), // status 5 = stale
                    None,
                );
            }
        }
    }
}
```

- [ ] **Step 2: Add migrate_chunk to ChunkEngine**

In `dataplane/src/chunk/engine.rs`, add:

```rust
/// Migrate a chunk from local backend to the target node via NDP.
/// Uses FLAG_MIGRATION so the target checks generation before accepting.
pub async fn migrate_chunk(
    &self,
    volume_id: &str,
    chunk_index: u64,
    target_node: &str,
) -> Result<()> {
    let chunk_offset = chunk_index * CHUNK_SIZE as u64;
    let vol_hash = Self::volume_hash(volume_id);

    // Read all sub-blocks from local backend
    #[cfg(feature = "spdk-sys")]
    let data = crate::bdev::novastor_bdev::sub_block_read_local(
        volume_id, chunk_offset, CHUNK_SIZE as u64,
    ).await?;

    #[cfg(not(feature = "spdk-sys"))]
    let data = vec![0u8; CHUNK_SIZE];

    let addr = {
        let topo = self.topology.read().unwrap();
        topo.get_node(target_node)
            .map(|n| format!("{}:{}", n.address, NDP_PORT))
            .ok_or_else(|| DataPlaneError::ChunkEngineError(
                format!("migration target {} not in topology", target_node)
            ))?
    };

    // Write with migration flag
    let header = ndp::NdpHeader::request(NdpOp::Write, 0, vol_hash, chunk_offset, data.len() as u32);
    // Set migration flag
    let mut header = header;
    header.flags |= ndp::header::FLAG_MIGRATION;

    let conn = self.ndp_pool.get_or_connect(&addr).await?;
    let resp = conn.request(header, Some(data)).await
        .map_err(|e| DataPlaneError::TransportError(format!("migrate to {}: {}", addr, e)))?;

    if resp.header.status == 5 {
        log::info!("migration {}:{} to {} skipped — target has newer data", volume_id, chunk_index, target_node);
        return Ok(());
    }
    if resp.header.status != 0 {
        return Err(DataPlaneError::TransportError(format!(
            "migration write failed: status={}", resp.header.status
        )));
    }

    log::info!("migrated chunk {}:{} to node {}", volume_id, chunk_index, target_node);
    Ok(())
}
```

Note: `get_or_connect` needs to be made `pub` on NdpPool, or add a `migrate_write` method.

- [ ] **Step 3: Add migration detection to PolicyEngine**

In `dataplane/src/policy/engine.rs`, add:

```rust
/// Detect chunks that should move based on current topology.
/// Returns list of (volume_id, chunk_index, target_node).
/// Skips volumes with status "deleting".
pub fn detect_misplaced_chunks(&self) -> Vec<(String, u64, String)> {
    let topo = self.topology.read().unwrap();
    let mut misplaced = Vec::new();

    if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
        if let Ok(volumes) = store.list_volumes() {
            for vol in &volumes {
                // Skip deleting volumes
                if vol.status == crate::metadata::types::VolumeStatus::Deleting {
                    continue;
                }
                let factor = match &vol.protection {
                    crate::metadata::types::Protection::Replication { factor } => *factor as usize,
                    crate::metadata::types::Protection::ErasureCoding { data_shards, parity_shards } =>
                        (*data_shards + *parity_shards) as usize,
                };
                if let Ok(chunks) = store.list_chunk_map(&vol.id) {
                    for cm in &chunks {
                        if !cm.placements.contains(&self.node_id) {
                            continue;
                        }
                        let chunk_key = format!("{}:{}", vol.id, cm.chunk_index);
                        let placements = crate::metadata::crush::select(&chunk_key, factor, &topo);
                        let dominated = placements.iter().any(|(n, _)| n == &self.node_id);
                        if !dominated {
                            if let Some((target, _)) = placements.first() {
                                misplaced.push((vol.id.clone(), cm.chunk_index, target.clone()));
                            }
                        }
                    }
                }
            }
        }
    }
    misplaced
}
```

- [ ] **Step 4: Add rate-limited migration execution to reconcile loop**

In `PolicyEngine`, add migration execution with `tokio::sync::Semaphore`:

```rust
/// Maximum concurrent migration tasks.
const MAX_CONCURRENT_MIGRATIONS: usize = 16;

// In PolicyEngine struct:
migration_semaphore: Arc<tokio::sync::Semaphore>,

// In reconcile():
let misplaced = self.detect_misplaced_chunks();
if !misplaced.is_empty() {
    log::info!("policy: {} misplaced chunks detected, migrating", misplaced.len());
    CHUNKS_PENDING.set(misplaced.len() as f64);

    for (vol_id, chunk_idx, target) in misplaced {
        let permit = self.migration_semaphore.clone().acquire_owned().await.unwrap();
        let engine = self.chunk_engine.clone();
        let vol = vol_id.clone();
        tokio::spawn(async move {
            match engine.migrate_chunk(&vol, chunk_idx, &target).await {
                Ok(()) => {
                    CHUNKS_MIGRATED.inc();
                    MIGRATION_BYTES.inc_by((4 * 1024 * 1024) as f64);
                }
                Err(e) => {
                    log::warn!("migration failed {}:{} → {}: {}", vol, chunk_idx, target, e);
                    MIGRATION_ERRORS.inc();
                }
            }
            drop(permit);
        });
    }
}
```

- [ ] **Step 5: Add Prometheus metrics**

```rust
use prometheus::{register_counter_vec, register_gauge, CounterVec, Gauge};
use once_cell::sync::Lazy;

static CHUNKS_MIGRATED: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!("novastor_chunks_migrated_total", "Total chunks migrated").unwrap()
});
static CHUNKS_PENDING: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!("novastor_chunks_pending", "Chunks pending migration").unwrap()
});
static MIGRATION_BYTES: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!("novastor_migration_bytes_total", "Total bytes migrated").unwrap()
});
static MIGRATION_ERRORS: Lazy<prometheus::Counter> = Lazy::new(|| {
    prometheus::register_counter!("novastor_migration_errors_total", "Migration errors").unwrap()
});
```

- [ ] **Step 6: Add garbage collection for reclaimable chunks**

In `PolicyEngine`, add a GC method called every 60s (separate from the 30s reconcile):

```rust
/// Garbage collect chunks that have been migrated away.
/// Verifies the target has the data before reclaiming local space.
pub async fn gc_reclaimable_chunks(&self) {
    if let Some(store) = crate::bdev::novastor_bdev::get_metadata_store() {
        // Find chunks where this node is in placements but NOT in CRUSH placement
        // AND the CRUSH-selected target has confirmed the data
        let topo = self.topology.read().unwrap();
        if let Ok(volumes) = store.list_volumes() {
            for vol in &volumes {
                if let Ok(chunks) = store.list_chunk_map(&vol.id) {
                    for cm in &chunks {
                        if !cm.placements.contains(&self.node_id) {
                            continue;
                        }
                        let chunk_key = format!("{}:{}", vol.id, cm.chunk_index);
                        let factor = 1; // from vol.protection
                        let crush_placements = crate::metadata::crush::select(&chunk_key, factor, &topo);
                        let dominated = crush_placements.iter().any(|(n, _)| n == &self.node_id);
                        if dominated { continue; } // still supposed to be here

                        // Check if target has the chunk (via chunk map)
                        let target_has_it = crush_placements.first()
                            .map(|(t, _)| cm.placements.contains(t))
                            .unwrap_or(false);

                        if target_has_it {
                            // Safe to reclaim — remove this node from placements
                            let mut updated = cm.clone();
                            updated.placements.retain(|n| n != &self.node_id);
                            let _ = store.put_chunk_map(&vol.id, &updated);
                            log::info!("GC: reclaimed chunk {}:{} (migrated to {:?})",
                                vol.id, cm.chunk_index, crush_placements);
                        }
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 7: Add unit test for migration detection**

```rust
#[cfg(test)]
mod tests {
    // Test that detect_misplaced_chunks returns chunks where CRUSH no longer
    // selects the local node after a topology change
    #[test]
    fn test_detect_misplaced_after_topology_change() {
        // Build 2-node topology, place a chunk, then change to 3 nodes
        // Verify chunks that moved are detected
    }
}
```

- [ ] **Step 8: Build and test**

Run: Docker build
Expected: Compiles, unit tests pass

- [ ] **Step 9: Commit**

```bash
git add dataplane/src/policy/engine.rs dataplane/src/chunk/engine.rs dataplane/ndp/src/header.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] PolicyEngine migration with generation check, rate limiting, GC, metrics"
```

---

## Task 9: Write-Back Cache

**Files:**
- Create: `dataplane/src/chunk/write_cache.rs`
- Modify: `dataplane/src/chunk/mod.rs` (add module)
- Modify: `dataplane/src/chunk/engine.rs` (integrate cache)
- Modify: `dataplane/src/bdev/novastor_bdev.rs` (FLUSH handler, remove old cache)

- [ ] **Step 1: Create write_cache.rs**

Create `dataplane/src/chunk/write_cache.rs`:

```rust
//! Write-back cache for ChunkEngine sub-block writes.
//!
//! Absorbs writes and returns immediately. Data is flushed to backends
//! on host FLUSH commands or when the cache reaches high-water mark.

use std::collections::HashMap;
use std::time::Instant;

/// Maximum entries per cache instance.
const DEFAULT_CAPACITY: usize = 128;

/// High-water threshold: flush oldest entries when cache is 75% full.
const HIGH_WATER_RATIO: f64 = 0.75;

/// A single cached write entry.
struct CacheEntry {
    volume_id: String,
    offset: u64,
    data: Vec<u8>,
    created: Instant,
}

/// Write-back cache keyed by (volume_id, sub-block-aligned offset).
pub struct WriteCache {
    entries: HashMap<(String, u64), CacheEntry>,
    capacity: usize,
}

impl WriteCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            capacity: DEFAULT_CAPACITY,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
        }
    }

    /// Absorb a write into the cache. Returns true if cached.
    /// Returns false if cache is full (caller should flush first).
    pub fn absorb(&mut self, volume_id: &str, offset: u64, data: &[u8]) -> bool {
        if self.entries.len() >= self.capacity {
            return false;
        }
        let key = (volume_id.to_string(), offset);
        self.entries.insert(key, CacheEntry {
            volume_id: volume_id.to_string(),
            offset,
            data: data.to_vec(),
            created: Instant::now(),
        });
        true
    }

    /// Look up cached data for the given volume + offset.
    /// Returns a clone of the cached data if present.
    pub fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let key = (volume_id.to_string(), offset);
        self.entries.get(&key).map(|e| {
            let end = std::cmp::min(length as usize, e.data.len());
            e.data[..end].to_vec()
        })
    }

    /// Drain all entries for a volume, returning them for flushing.
    pub fn drain_volume(&mut self, volume_id: &str) -> Vec<(u64, Vec<u8>)> {
        let keys: Vec<_> = self.entries.keys()
            .filter(|(vid, _)| vid == volume_id)
            .cloned()
            .collect();
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(entry) = self.entries.remove(&key) {
                result.push((entry.offset, entry.data));
            }
        }
        result.sort_by_key(|(offset, _)| *offset);
        result
    }

    /// Drain oldest entries when above high-water mark.
    /// Returns entries to flush.
    pub fn drain_overflow(&mut self) -> Vec<(String, u64, Vec<u8>)> {
        let threshold = (self.capacity as f64 * HIGH_WATER_RATIO) as usize;
        if self.entries.len() <= threshold {
            return Vec::new();
        }

        // Sort by creation time, drain oldest
        let mut by_age: Vec<_> = self.entries.keys().cloned().collect();
        by_age.sort_by_key(|k| self.entries[k].created);

        let to_drain = self.entries.len() - threshold;
        let mut result = Vec::with_capacity(to_drain);
        for key in by_age.into_iter().take(to_drain) {
            if let Some(entry) = self.entries.remove(&key) {
                result.push((entry.volume_id, entry.offset, entry.data));
            }
        }
        result
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Is the cache empty?
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check if cache needs overflow flush.
    pub fn needs_flush(&self) -> bool {
        self.entries.len() >= (self.capacity as f64 * HIGH_WATER_RATIO) as usize
    }
}

impl Default for WriteCache {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absorb_and_lookup() {
        let mut cache = WriteCache::new();
        let data = vec![42u8; 64 * 1024];
        assert!(cache.absorb("vol-1", 0, &data));
        assert_eq!(cache.len(), 1);

        let result = cache.lookup("vol-1", 0, 64 * 1024);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);

        assert!(cache.lookup("vol-1", 65536, 64 * 1024).is_none());
    }

    #[test]
    fn drain_volume() {
        let mut cache = WriteCache::new();
        cache.absorb("vol-1", 0, &[1; 100]);
        cache.absorb("vol-1", 65536, &[2; 100]);
        cache.absorb("vol-2", 0, &[3; 100]);

        let drained = cache.drain_volume("vol-1");
        assert_eq!(drained.len(), 2);
        assert_eq!(cache.len(), 1); // vol-2 remains
    }

    #[test]
    fn capacity_limit() {
        let mut cache = WriteCache::with_capacity(2);
        assert!(cache.absorb("v", 0, &[1]));
        assert!(cache.absorb("v", 1, &[2]));
        assert!(!cache.absorb("v", 2, &[3])); // full
    }

    #[test]
    fn drain_overflow() {
        let mut cache = WriteCache::with_capacity(4);
        cache.absorb("v", 0, &[1]);
        cache.absorb("v", 1, &[2]);
        cache.absorb("v", 2, &[3]);
        // 3/4 = 75% = threshold, exactly at limit
        assert!(!cache.needs_flush());

        cache.absorb("v", 3, &[4]);
        // 4/4 = 100% > 75%
        assert!(cache.needs_flush());

        let drained = cache.drain_overflow();
        assert_eq!(drained.len(), 1); // drain back to threshold
        assert_eq!(cache.len(), 3);
    }
}
```

- [ ] **Step 2: Add module to chunk/mod.rs**

Add `pub mod write_cache;` to `dataplane/src/chunk/mod.rs`.

- [ ] **Step 3: Integrate cache into ChunkEngine**

In `dataplane/src/chunk/engine.rs`:

1. Add `write_cache: tokio::sync::Mutex<WriteCache>` field to `ChunkEngine`
2. Initialize in all constructors: `write_cache: tokio::sync::Mutex::new(WriteCache::new())`

3. Change `sub_block_write` to absorb into cache:

```rust
pub async fn sub_block_write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<()> {
    let mut cache = self.write_cache.lock().await;

    // If cache is full, flush overflow (oldest entries) first
    if !cache.absorb(volume_id, offset, data) {
        // Drain overflow entries and flush them through CRUSH
        let overflow = cache.drain_overflow();
        drop(cache); // release lock during I/O

        for (vol, off, d) in overflow {
            self.flush_single_write(&vol, off, &d).await?;
        }

        // Retry absorb after making room
        let mut cache = self.write_cache.lock().await;
        if !cache.absorb(volume_id, offset, data) {
            // Still full — flush synchronously through CRUSH
            drop(cache);
            return self.flush_single_write(volume_id, offset, data).await;
        }
    } else {
        // Check if we hit high-water mark — trigger background flush
        if cache.needs_flush() {
            let overflow = cache.drain_overflow();
            drop(cache);
            // Background flush — don't block the write
            let engine = self.clone_for_flush();
            tokio::spawn(async move {
                for (vol, off, d) in overflow {
                    let _ = engine.flush_single_write(&vol, off, &d).await;
                }
            });
        }
    }

    Ok(())
}
```

4. Add `flush_single_write` — the actual CRUSH fan-out for one entry:

```rust
/// Flush a single cached write through CRUSH fan-out.
/// Records placements in chunk map and broadcasts to peers.
async fn flush_single_write(&self, volume_id: &str, offset: u64, data: &[u8]) -> Result<()> {
    // This is the existing CRUSH fan-out logic from the old sub_block_write
    // (compute chunk_key, CRUSH select, JoinSet fan-out, quorum check)
    // After success: record placements in chunk map + broadcast
    // ... (reuse the existing fan-out code, extracted into this method)
}
```

5. In `sub_block_read`: check cache before chunk map and CRUSH:

```rust
// Check write-back cache first (data may not be flushed yet)
{
    let cache = self.write_cache.lock().await;
    if let Some(data) = cache.lookup(volume_id, offset, length) {
        return Ok(data);
    }
}
// Then check chunk map placements, then CRUSH fallback...
```

6. Add `pub async fn flush(&self, volume_id: &str) -> Result<()>`:

```rust
/// Flush all cached writes for a volume through CRUSH fan-out.
/// Called on host FLUSH command. Blocks until all writes are persisted.
pub async fn flush(&self, volume_id: &str) -> Result<()> {
    let entries = {
        let mut cache = self.write_cache.lock().await;
        cache.drain_volume(volume_id)
    };
    if entries.is_empty() {
        return Ok(());
    }
    log::debug!("flushing {} cached writes for volume {}", entries.len(), volume_id);
    for (offset, data) in entries {
        self.flush_single_write(volume_id, offset, &data).await?;
    }
    Ok(())
}
```

- [ ] **Step 4: Update FLUSH handler in bdev**

In `dataplane/src/bdev/novastor_bdev.rs`, update the FLUSH handler (currently a no-op) to call `engine.flush(volume_name)`:

```rust
ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_FLUSH => {
    let bdev_io_addr = bdev_io as usize;
    let handle = get_tokio_handle().expect("tokio handle");
    handle.spawn(async move {
        let status = match get_chunk_engine() {
            Ok(engine) => {
                match engine.flush(&volume_name).await {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_bdev: flush failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                }
            }
            Err(_) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
        };
        reactor_dispatch::send_to_reactor(move || unsafe {
            ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
        });
    });
}
```

- [ ] **Step 5: Remove old WriteCache from novastor_bdev.rs**

First, search for all references to the old cache:

Run: `grep -rn "WriteCache\|WRITE_CACHE_SLOTS\|write_cache\|wb_cache" dataplane/src/bdev/novastor_bdev.rs`

Remove the old `WriteCache` struct, its methods (`new`, `absorb`, `lookup`, `needs_flush`, `collect_dirty`, `mark_clean`), and the `WRITE_CACHE_SLOTS` constant (lines ~866-1000). Also remove any call sites that reference the old cache. These are fully replaced by the ChunkEngine-level cache.

- [ ] **Step 6: Run tests**

Run: `cargo test --release -- --test-threads=1` (in Docker)
Expected: All tests pass including new write_cache tests

- [ ] **Step 7: Commit**

```bash
git add dataplane/src/chunk/write_cache.rs dataplane/src/chunk/mod.rs dataplane/src/chunk/engine.rs dataplane/src/bdev/novastor_bdev.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Write-back cache at ChunkEngine level with FLUSH semantics"
```

---

## Task 10: Go Agent — SyncChunkMaps RPC (periodic full-sync safety net)

**Files:**
- Modify: `api/proto/dataplane/dataplane.proto` — add SyncChunkMaps RPC
- Modify: `dataplane/proto/dataplane_service.proto` — same
- Modify: `dataplane/src/transport/dataplane_service.rs` — implement handler
- Modify: `internal/dataplane/client.go` — add client method
- Modify: `cmd/agent/main.go` — periodic sync goroutine

- [ ] **Step 1: Add SyncChunkMaps RPC to proto**

In both proto files, add:

```protobuf
message ChunkMapUpdate {
  string volume_id = 1;
  uint64 chunk_index = 2;
  uint64 dirty_bitmap = 3;
  repeated string placements = 4;
  uint64 generation = 5;
}

message SyncChunkMapsRequest {
  uint64 since_generation = 1;
}

message SyncChunkMapsResponse {
  repeated ChunkMapUpdate updates = 1;
}
```

Add to the `DataplaneService` service definition:

```protobuf
rpc SyncChunkMaps(SyncChunkMapsRequest) returns (SyncChunkMapsResponse);
```

- [ ] **Step 2: Implement handler in Rust dataplane**

In `dataplane_service.rs`, add the `sync_chunk_maps` handler that reads from the metadata store and returns all chunk map entries with generation > `since_generation`.

- [ ] **Step 3: Add Go client and periodic sync**

In `internal/dataplane/client.go`, add `SyncChunkMaps` client method.
In `cmd/agent/main.go`, add a goroutine that syncs chunk maps every 5 minutes: collects from all dataplanes, redistributes via UpdateTopology.

- [ ] **Step 4: Regenerate proto, build all**

Run: `make generate-proto && make build-all`
Run: Docker build of dataplane
Expected: All compile

- [ ] **Step 5: Commit**

```bash
git add api/proto/ dataplane/proto/ dataplane/src/transport/dataplane_service.rs internal/dataplane/client.go cmd/agent/main.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] SyncChunkMaps RPC for periodic full-sync safety net"
```

---

## Task 11: Build, Deploy, End-to-End Test

**Files:**
- No code changes — integration testing

- [ ] **Step 1: Build all images**

```bash
cd dataplane && docker build -f Dockerfile.build -t novastor-dataplane:multinode .
cd .. && make build-all
docker build -f Dockerfile.agent -t novastor-agent:multinode .
docker build -f Dockerfile.csi -t novastor-csi:multinode .
```

Tag and push to registry `192.168.100.11:30500/novastor/`.

- [ ] **Step 2: Deploy to cluster**

Update DaemonSets to use new images. Verify all pods start cleanly.

- [ ] **Step 3: Test volume creation — all nodes register volume hash**

Create a PVC. Check dataplane logs on ALL nodes — each should show `register_volume_hash` for the new volume.

- [ ] **Step 4: Test mkfs and data integrity**

Create a pod with the PVC. Verify mkfs succeeds, write 100MB test data, verify md5 matches on re-read.

- [ ] **Step 5: Test topology change**

Scale down a worker node (cordon + drain). Verify:
- Reads still succeed (CRUSH fallback to replicas)
- PolicyEngine logs migration activity
- Chunk map updates propagate

- [ ] **Step 6: Test write-back cache + FLUSH**

Run fio with `fsync=1` (forces FLUSH after each write). Verify data integrity.
Run fio without fsync — verify higher throughput (writes cached).

- [ ] **Step 7: Commit any test fixes**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Test] End-to-end validation of multi-node ChunkEngine"
```
