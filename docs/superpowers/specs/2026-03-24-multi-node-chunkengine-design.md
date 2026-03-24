# Multi-Node ChunkEngine, Write-Back Cache & Distributed Chunk Maps

## Goal

Enable the ChunkEngine to operate as a fully distributed storage system where all nodes participate equally, with write-back caching for performance, PolicyEngine-driven data migration on topology changes, and distributed chunk maps for global chunk location awareness.

## Architecture

All I/O flows through the ChunkEngine (no bypass). CRUSH placement is deterministic and stable (straw2). Topology changes are accepted freely — CRUSH stability ensures minimal data movement, the PolicyEngine migrates misplaced chunks in the background, and reads fall back through replicas during the migration window. A write-back cache at the ChunkEngine level absorbs writes and flushes on host FLUSH commands.

## Components

### 1. CRUSH Map with Volume Definitions

**Problem:** Currently, `register_volume_hash` is only called during `bdev::create()` on the creating node. Remote nodes can't serve NDP I/O for volumes they don't know about.

**Solution:** The `UpdateTopologyRequest` proto gains a `repeated VolumeInfo` field. The Go agent includes all active volumes in every topology push (every 30s).

**Proto change:**
```protobuf
message VolumeInfo {
  string name = 1;
  uint64 size_bytes = 2;
  string protection_type = 3;  // "replication" or "erasure_coding"
  uint32 rep_factor = 4;       // for replication
  uint32 data_shards = 5;      // for EC
  uint32 parity_shards = 6;    // for EC
  string status = 7;           // "available", "deleting", etc.
}

message UpdateTopologyRequest {
  uint64 generation = 1;
  repeated TopologyNode nodes = 2;
  repeated VolumeInfo volumes = 3;  // NEW
}
```

**Dataplane behavior on receiving UpdateTopology:**
1. Parse nodes/backends as before (CRUSH topology)
2. Iterate the volume list:
   - Register `volume_hash` for any new volumes not yet known
   - Unregister hashes for volumes no longer in the list
3. Accept the topology update unconditionally (no guard)

**Rust changes:**
- `ClusterMap` gains `volumes: Vec<VolumeDefinition>` field
- `ChunkEngine::update_topology` processes volume changes alongside node changes
- Remove the topology guard from `dataplane_service.rs`

**Go agent changes:**
- `cmd/agent/main.go` topology push includes volume list from metadata service
- The Go agent queries the metadata service for active volumes and includes them in `UpdateTopologyRequest`

### 2. Stable CRUSH with Weighted Moves

**Problem:** The previous topology guard blocked all topology updates when a backend bdev was active, preventing multi-node operation.

**Solution:** Remove the guard. Rely on CRUSH's inherent stability:

- **Node addition:** straw2 guarantees only ~1/N chunks move. New node starts with its real weight. Existing chunks mostly stay put.
- **Node removal:** Chunks that CRUSH now maps to a different node need migration (handled by PolicyEngine, Section 4).
- **Weight change:** Gradual weight changes cause minimal movement.

**Read fallback:** The ChunkEngine already tries each replica in placement order (`sub_block_read` iterates `placements`). If the "correct" node doesn't have the data yet (post-topology-change, pre-migration), it falls through to the next replica which still has it from the old placement. This provides a graceful migration window.

**Write behavior:** New writes go to the current CRUSH placement immediately. The old location becomes stale and is cleaned up by the PolicyEngine.

### 3. Write-Back Cache at ChunkEngine Level

**Problem:** All I/O crosses reactor→tokio→reactor boundaries. A write-back cache can absorb writes and acknowledge immediately, improving latency.

**Design:**

The write-back cache sits inside the ChunkEngine, between the bdev dispatch and the CRUSH fan-out. It operates in tokio.

**Structure:**
- `WriteCache` struct owned by ChunkEngine, keyed by `(volume_id, offset)` at sub-block granularity
- `HashMap<(String, u64), WriteCacheEntry>` of dirty entries
- Each entry holds: `volume_id`, `offset`, `data: Vec<u8>`, `timestamp`
- Configurable capacity, default 128 entries per volume (8MB at 64KB sub-blocks)

**Write path:**
1. `ChunkEngine::sub_block_write` is called
2. Cache absorbs the write, returns `Ok(())` immediately
3. Caller (bdev) completes the NVMe-oF I/O to the host

**Read path:**
1. `ChunkEngine::sub_block_read` checks cache first
2. Cache hit → return cached data (even if not yet flushed)
3. Cache miss → proceed to CRUSH-routed read as normal

**Flush triggers:**
- **FLUSH command from host** — drains all cached writes for that volume through CRUSH fan-out. Completes the FLUSH I/O only after all writes are persisted to backends. This matches NVMe write-back cache semantics.
- **High-water threshold (75%)** — triggers background async flush of oldest entries to prevent cache exhaustion under sustained write load.
- **Volume delete** — flush all + discard.

**Crash semantics:**
- Cached writes are volatile until FLUSH. This matches NVMe write-back cache semantics — the host knows data is at risk until it issues FLUSH.
- Filesystems like ext4 with `barrier=1` (default) issue FLUSH after journal commits.
- The bdev advertises volatile write cache via SPDK bdev properties.

**New files:**
- `dataplane/src/chunk/write_cache.rs` — WriteCache struct and logic

### 4. PolicyEngine-Driven Data Migration

**Problem:** When topology changes cause CRUSH to remap chunks, data needs to move from old locations to new ones.

**Solution:** The PolicyEngine reconciliation loop (already runs every 30s on each node) handles migration.

**Detection of misplaced chunks:**
1. On topology change (generation bump), the PolicyEngine compares old vs new topology
2. For each chunk stored locally (from the local chunk map), recompute CRUSH placement with the new topology
3. If this node is no longer in the placement list for a chunk, that chunk needs to move
4. If this node is still in the placement list, no action needed

**Migration flow:**
1. PolicyEngine identifies chunks that should now be on a different node
2. Reads chunk data from local backend via `sub_block_read_local`
3. Writes to the new CRUSH-selected node via NDP (`ndp_pool.sub_block_write`)
4. After successful copy, the local copy is marked as reclaimable (not deleted immediately — kept as a fallback until confirmed)
5. Updates the distributed chunk map (Section 5) with new placement

**Coordination:**
- Each node only migrates chunks it currently holds that CRUSH says should move elsewhere
- Since each chunk is on exactly one node (rep1) or a defined set (rep3/EC), and each node only looks at its own local chunks, there is no overlap or duplicate work
- Rate limiting: max N concurrent migrations to avoid saturating NDP connections (configurable, default 16)
- Migration is idempotent — writing a chunk that already exists on the target is a no-op

**Node removal:**
- Node goes offline → CRUSH selects replacements from remaining nodes
- Remaining nodes that held replicas detect they need to copy to the new placement
- For rep1 with a dead node: data is lost (expected — rep1 is not production-grade, always rep3 or EC in production)

**New additions to PolicyEngine:**
- `detect_misplaced_chunks(old_topo, new_topo) -> Vec<MigrationTask>`
- `execute_migration(task: MigrationTask) -> Result<()>`
- Migration task queue with concurrency limit
- Metrics: chunks_migrated, chunks_pending, migration_bytes

### 5. Distributed Chunk Maps

**Problem:** Currently, chunk maps are local to each node's redb. Other nodes can't answer "where is chunk X of volume Y" without computing CRUSH — and post-migration, CRUSH's answer may differ from reality.

**Solution:** Every node knows the full chunk map for every volume. Chunk maps record actual placement (where data really is), not computed placement (where CRUSH says it should be).

**ChunkMapEntry updated:**
```rust
pub struct ChunkMapEntry {
    pub chunk_index: u64,
    pub chunk_id: String,
    pub ec_params: Option<ErasureParams>,
    pub dirty_bitmap: u64,
    pub placements: Vec<String>,  // NEW: node IDs where chunk actually lives
}
```

**Distribution mechanism:**
- When a node writes a chunk (via `sub_block_write`), it updates its local chunk map with actual placements (the nodes that acknowledged the write)
- Chunk map updates are included in the topology sync — `UpdateTopologyRequest` gains a `repeated ChunkMapUpdate` field for incremental updates:

```protobuf
message ChunkMapUpdate {
  string volume_id = 1;
  uint64 chunk_index = 2;
  uint64 dirty_bitmap = 3;
  repeated string placements = 4;
  uint64 generation = 5;  // for conflict resolution (highest generation wins)
}

message UpdateTopologyRequest {
  uint64 generation = 1;
  repeated TopologyNode nodes = 2;
  repeated VolumeInfo volumes = 3;
  repeated ChunkMapUpdate chunk_map_updates = 4;  // NEW
}
```

- The Go agent collects chunk map updates from all dataplanes and redistributes them in the next topology push
- On startup, a node loads its local redb chunk map. The Go agent's periodic sync fills in any gaps

**Consistency:**
- Chunk maps are eventually consistent — a node may briefly have stale placement info
- Reads use chunk map placement first; fall back through CRUSH if recorded placement is unreachable
- Writes always go to CRUSH-selected nodes (authoritative), then update the chunk map
- Conflict resolution: highest generation wins when the same chunk has different placements on different nodes

**Persistence:**
- Each node persists the full chunk map in its local redb (existing `put_chunk_map` / `list_chunk_map` API)
- Chunk maps are restored on startup from local redb
- The Go agent's topology push fills in any chunks the node doesn't know about

### 6. Removing the Topology Guard

With all the above in place, the topology guard in `dataplane_service.rs::update_topology` is removed entirely.

**Complete flow:**
1. Go agent pushes `UpdateTopology` with nodes + volumes + chunk map updates every 30s
2. Dataplane accepts unconditionally — updates CRUSH topology, registers/unregisters volume hashes, merges chunk map updates
3. Writes go through write-back cache → CRUSH fan-out on FLUSH
4. Reads check cache → check chunk map placements → fall back through CRUSH replicas
5. PolicyEngine detects misplaced chunks on each 30s reconciliation, migrates via NDP
6. Chunk map updates flow back through the Go agent to all nodes

No guard, no pinning, no special cases.

## Data Flow

### Write (with cache)
```
NVMe-oF host write
  → SPDK bdev_submit_request → tokio spawn
    → ChunkEngine::sub_block_write
      → WriteCache::absorb → Ok(()) immediately
        → bdev I/O complete (host sees write done)
```

### FLUSH
```
NVMe-oF host FLUSH
  → SPDK bdev_submit_request → tokio spawn
    → ChunkEngine::flush(volume_id)
      → for each cached entry:
        → CRUSH select placement nodes
        → fan-out write to all replicas via NDP / local
        → update chunk map with actual placements
      → all writes complete → bdev I/O complete
```

### Read
```
NVMe-oF host read
  → SPDK bdev_submit_request → tokio spawn
    → ChunkEngine::sub_block_read
      → WriteCache::lookup → hit? return cached data
      → miss: check chunk map for recorded placements
        → try recorded placement nodes (NDP / local)
        → fallback: CRUSH-computed placement
      → return data → bdev I/O complete
```

### Topology Update
```
Go agent push (every 30s)
  → UpdateTopology RPC to each dataplane
    → update CRUSH map (nodes, backends, weights)
    → register/unregister volume hashes
    → merge chunk map updates into local redb
    → PolicyEngine detects misplaced chunks
      → background migration via NDP
      → chunk map updates sent back to Go agent
```

## Error Handling

- **NDP write failure during migration:** Retry with exponential backoff, max 3 attempts. If all fail, mark chunk as "migration_pending" and retry next reconciliation cycle.
- **Cache flush failure:** Retry the individual sub-block write. If persistent failure, mark the cache entry as "flush_failed" and report via metrics. The FLUSH command returns an error to the host, which triggers filesystem-level recovery.
- **Volume hash collision:** The `volume_hash` function uses CRC32C forward + backward (64-bit). Collision probability is negligible for practical volume counts (<2^32). If a collision is detected (two volumes mapping to the same hash), log an error and reject the second volume.
- **Split-brain chunk maps:** Two nodes claim different placements for the same chunk. Resolved by generation counter — highest generation wins. If tied, the node in CRUSH position 0 (primary owner) wins.

## Testing

- **Unit tests:** WriteCache absorb/lookup/flush logic, CRUSH stability across topology changes, chunk map merge with conflict resolution
- **Integration tests:** Multi-node write + topology change + read (verify data accessible during and after migration), FLUSH semantics (cached writes not visible until FLUSH on remote node), volume registration via topology push
- **E2E tests:** Create volume → write data → add node → verify data still readable → verify migration completes → remove node → verify replicas rebuilt

## Migration Order

Implementation should proceed in this order (each step builds on the previous):

1. **CRUSH map with volumes** — proto change, Go agent includes volumes, dataplane registers hashes
2. **Remove topology guard** — accept all topology updates
3. **Distributed chunk maps** — proto change, chunk map distribution via topology push, placements field
4. **Stable CRUSH reads with fallback** — reads try chunk map placements, fall back to CRUSH
5. **PolicyEngine migration** — detect misplaced chunks, migrate via NDP
6. **Write-back cache** — cache at ChunkEngine level, FLUSH semantics
