# Sub-Block I/O Design for NovaStor Chunk Engine

**Date:** 2026-03-16
**Status:** Approved
**Author:** Pascal Watteel

## Problem

The chunk engine operates at 4MB granularity. Every sub-chunk write triggers a Read-Modify-Write (RMW): read 4MB, overlay the small write, hash 4MB, write 4MB. This causes 1000x write amplification for 4K random writes and limits sequential write throughput to ~4 MB/s on ARM64.

## Solution

Divide each 4MB chunk into 64 × 64KB sub-blocks. All I/O operates at sub-block granularity. Writes go through to the backend immediately at 64KB (write-through). A periodic background sync assembles the full 4MB chunk, computes SHA-256, and replicates to remote nodes.

## Constants

```
CHUNK_SIZE        = 4,194,304 bytes (4MB)
SUB_BLOCK_SIZE    = 65,536 bytes (64KB)
SUB_BLOCKS_PER_CHUNK = 64
```

## Data Structures

### ChunkSubBlockMeta

Per-chunk metadata stored alongside the chunk map entry:

```rust
struct ChunkSubBlockMeta {
    dirty_bitmap: u64,   // 64 bits, one per sub-block
    chunk_id: String,    // offset-based during writes, SHA-256 after sync
    last_sync: u64,      // timestamp of last full-chunk sync
}
```

No in-memory buffer. The backend bdev is the storage. Sub-block writes go directly to `backend_offset + (sub_block_index × 64KB)`.

Memory overhead per chunk: 8 bytes (bitmap) + chunk ID string. For a 10GB volume (2560 chunks): ~20KB total.

### ChunkMapEntry Changes

Add `dirty_bitmap: u64` field to the existing `ChunkMapEntry` struct. One entry per 4MB chunk — no metadata explosion.

## Write Path

App writes N bytes at volume offset X:

1. Calculate `chunk_index = X / 4MB`, `sub_block_index = (X % 4MB) / 64KB`, `offset_within_sub_block = X % 64KB`
2. Read 64KB sub-block from backend (existing data around the write)
3. Overlay the app's bytes into the 64KB sub-block at the correct offset
4. Write 64KB sub-block to backend at `chunk_base_offset + (sub_block_index × 64KB)`
5. Set dirty bitmap bit for this sub-block
6. Replicate 64KB sub-block to remote nodes via inter-dataplane gRPC (delta replication, majority quorum)
7. Signal completion back to SPDK reactor

**Optimization:** If the write covers the entire 64KB sub-block, skip step 2 (no read needed).

**No SHA-256 hash on writes.** Chunk ID stays offset-based until periodic sync.

### Performance Comparison (4K Random Write)

| Step | Today (4MB RMW) | Sub-block (64KB) |
|------|-----------------|------------------|
| Read | 4MB | 64KB |
| Overlay | 4K into 4MB | 4K into 64KB |
| Hash | SHA-256 of 4MB | None |
| Write | 4MB | 64KB |
| **Total** | **~1000ms** | **~2-5ms** |

## Read Path

App reads N bytes at volume offset X:

1. Calculate `chunk_index = X / 4MB`, `sub_block_index = (X % 4MB) / 64KB`, `offset_within_sub_block = X % 64KB`
2. Read 64KB sub-block from backend at `chunk_base_offset + (sub_block_index × 64KB)`
3. Extract the requested bytes from the 64KB sub-block
4. Return to SPDK reactor

Backend offset is deterministic from the volume offset — no chunk map lookup needed for reads.

**Sequential optimization:** A 1M read spanning 16 contiguous sub-blocks can be issued as a single 1M backend read.

### Performance Comparison (4K Random Read)

| Step | Today | Sub-block |
|------|-------|-----------|
| Lookup | Chunk map (RwLock) | Calculate offset (arithmetic) |
| Read | 4MB from backend | 64KB from backend |
| Extract | 4K from 4MB | 4K from 64KB |
| **Total** | **~440ms** | **~1-5ms** |

## Periodic Sync

Background tokio task per volume. Default interval: 30 seconds.

For each chunk with dirty bitmap != 0:

1. Read all 64 sub-blocks (4MB) from backend in one sequential read
2. Compute SHA-256 on the assembled 4MB
3. If content changed, update chunk map with new content-addressed hash
4. Replicate full 4MB chunk to remote nodes (consistency verification)
5. Clear dirty bitmap, update `last_sync` timestamp
6. Record chunk locations in PolicyEngine for CRUSH tracking

### Properties

- Not in the I/O hot path — reads and writes never wait for sync
- Only place where SHA-256 is computed
- Only place where full 4MB exists in memory (transient, one chunk at a time)
- Only place where content-addressed chunk IDs are created

### Crash Safety

Dirty sub-blocks are already on the backend (write-through). On crash before sync:
- Local data is durable (sub-blocks written to backend)
- Remote nodes have dirty sub-blocks from delta replication
- SHA-256 chunk ID not yet computed — offset-based ID still valid
- On restart, dirty bitmaps are lost (in-memory only) — sync treats all chunks as dirty on first pass

## Replication

### Real-Time Delta Replication (per write)

Owner sends dirty 64KB sub-block to replicas:

```protobuf
message PutSubBlockRequest {
    string volume_id = 1;
    uint64 chunk_index = 2;
    uint32 sub_block_index = 3;  // 0-63
    bytes data = 4;              // 64KB
}
message PutSubBlockResponse {}
```

Replicas write the 64KB at the same backend offset. Majority quorum required before acknowledging the write to the app.

### Periodic Full-Chunk Sync (background)

During sync, the full 4MB chunk is replicated to remote nodes with its SHA-256 hash. Replicas verify their assembled chunk matches. Catches any sub-block drift or missed delta replicas.

### Erasure Coding

EC encoding happens at sync time on the full 4MB chunk. Sub-block delta replication does not apply to EC volumes — RS encoding needs the complete chunk. EC volumes still benefit from 64KB sub-block I/O locally; the EC shard distribution waits for sync.

## Scope of Changes

### Files Modified (Rust dataplane only)

| File | Change |
|------|--------|
| `bdev/novastor_bdev.rs` | Dispatch I/O at 64KB sub-block granularity. Remove `rmw_write`. |
| `chunk/engine.rs` | `write_sub_block()`, `read_sub_block()` methods. Sync background task. |
| `backend/bdev_store.rs` | `put()`/`get()` with offset+length for sub-block I/O. |
| `metadata/types.rs` | `dirty_bitmap: u64` on `ChunkMapEntry`. |
| `transport/dataplane_service.rs` | `PutSubBlock` gRPC handler. |
| `proto/dataplane_service.proto` | `PutSubBlockRequest`/`PutSubBlockResponse` messages. |

### No Changes To

- Go agent, CSI controller, metadata service
- CRUSH, topology, failover controller
- Helm charts, StorageClasses
- Existing gRPC APIs (additive only)
- NVMe-oF target management, ANA multipath

## Expected Performance Impact

Based on baseline benchmark (10GB rep1 volume, ARM64 K3s cluster):

| Test | Before | After (estimate) |
|------|--------|-------------------|
| Seq Write (1M) | 4.0 MB/s | 50-100 MB/s |
| Seq Read (1M) | 9.0 MB/s | 50-100 MB/s |
| Rand Write (4K) | 46 IOPS | 1000-5000 IOPS |
| Rand Read (4K) | 66 IOPS | 500-2000 IOPS |

## Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Flush policy | Write-through at 64KB | No data loss, no flush timers |
| Unwritten sub-block reads | Read from backend | No assumptions about disk content |
| Real-time replication | 64KB sub-block delta | Minimal network per write |
| Full-chunk replication | Periodic sync (30s) | Consistency verification |
| SHA-256 timing | On periodic sync only | Not in hot path |
| In-memory buffer | None (bitmap only) | Backend is the buffer, 20KB memory overhead |
| Chunk size | 4MB unchanged | CRUSH placement, metadata density preserved |
| Sub-block size | 64KB | 64x less amplification, good alignment for NVMe |
