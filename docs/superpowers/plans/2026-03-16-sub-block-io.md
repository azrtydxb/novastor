# Sub-Block I/O Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace 4MB Read-Modify-Write with 64KB sub-block I/O to eliminate write amplification and reduce read latency by 64x.

**Architecture:** The hot I/O path (novastor_bdev submit_request) bypasses ChunkEngine and writes/reads 64KB sub-blocks directly to the backend SPDK bdev. ChunkEngine becomes a background sync engine — assembling full 4MB chunks, computing SHA-256, and replicating to remote nodes periodically. A per-chunk dirty bitmap (u64) tracks which sub-blocks have been written.

**Tech Stack:** Rust, SPDK bdev I/O (reactor_dispatch), tokio (background sync), ring (SHA-256), tonic/gRPC (sub-block replication)

**Spec:** `docs/superpowers/specs/2026-03-16-sub-block-io-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `dataplane/src/bdev/sub_block.rs` | Sub-block constants, bitmap ops, offset math | **Create** |
| `dataplane/src/bdev/novastor_bdev.rs` | Replace rmw_write with sub-block dispatch | **Modify** |
| `dataplane/src/bdev/mod.rs` | Export sub_block module | **Modify** |
| `dataplane/src/chunk/sync.rs` | Background sync task (hash, replicate) | **Create** |
| `dataplane/src/chunk/mod.rs` | Export sync module | **Modify** |
| `dataplane/src/metadata/types.rs` | Add dirty_bitmap to ChunkMapEntry | **Modify** |
| `dataplane/src/transport/dataplane_service.rs` | PutSubBlock handler, spawn sync task | **Modify** |
| `dataplane/proto/dataplane_service.proto` | PutSubBlock RPC message | **Modify** |

---

## Chunk 1: Sub-Block Foundation

### Task 1: Sub-block constants and bitmap operations

**Files:**
- Create: `dataplane/src/bdev/sub_block.rs`
- Modify: `dataplane/src/bdev/mod.rs`

- [ ] **Step 1: Create sub_block.rs with constants and bitmap helpers**

```rust
// dataplane/src/bdev/sub_block.rs

/// 4MB chunk divided into 64 × 64KB sub-blocks.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;
pub const SUB_BLOCK_SIZE: usize = 64 * 1024;
pub const SUB_BLOCKS_PER_CHUNK: usize = CHUNK_SIZE / SUB_BLOCK_SIZE; // 64

/// Calculate which chunk a volume byte offset falls in.
pub fn chunk_index(volume_offset: u64) -> u64 {
    volume_offset / CHUNK_SIZE as u64
}

/// Calculate which sub-block within a chunk a byte offset falls in.
pub fn sub_block_index(volume_offset: u64) -> usize {
    ((volume_offset % CHUNK_SIZE as u64) / SUB_BLOCK_SIZE as u64) as usize
}

/// Calculate the byte offset within a sub-block.
pub fn offset_in_sub_block(volume_offset: u64) -> usize {
    (volume_offset % SUB_BLOCK_SIZE as u64) as usize
}

/// Calculate the backend byte offset for a sub-block.
/// chunk_base is the backend offset where this chunk's data starts.
pub fn backend_sub_block_offset(chunk_base: u64, sb_index: usize) -> u64 {
    chunk_base + (sb_index as u64 * SUB_BLOCK_SIZE as u64)
}

/// Set a bit in the dirty bitmap.
pub fn bitmap_set(bitmap: &mut u64, sb_index: usize) {
    *bitmap |= 1u64 << sb_index;
}

/// Check if a bit is set in the dirty bitmap.
pub fn bitmap_is_set(bitmap: u64, sb_index: usize) -> bool {
    (bitmap >> sb_index) & 1 == 1
}

/// Count dirty sub-blocks.
pub fn bitmap_count(bitmap: u64) -> u32 {
    bitmap.count_ones()
}

/// Check if all 64 sub-blocks are dirty.
pub fn bitmap_is_full(bitmap: u64) -> bool {
    bitmap == u64::MAX
}

/// Iterator over set bit indices.
pub fn bitmap_dirty_indices(bitmap: u64) -> impl Iterator<Item = usize> {
    (0..SUB_BLOCKS_PER_CHUNK).filter(move |i| bitmap_is_set(bitmap, *i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_and_sub_block_math() {
        // Offset 0 -> chunk 0, sub-block 0, offset 0
        assert_eq!(chunk_index(0), 0);
        assert_eq!(sub_block_index(0), 0);
        assert_eq!(offset_in_sub_block(0), 0);

        // Offset 65536 (64KB) -> chunk 0, sub-block 1, offset 0
        assert_eq!(chunk_index(65536), 0);
        assert_eq!(sub_block_index(65536), 1);
        assert_eq!(offset_in_sub_block(65536), 0);

        // Offset 4MB -> chunk 1, sub-block 0
        assert_eq!(chunk_index(4 * 1024 * 1024), 1);
        assert_eq!(sub_block_index(4 * 1024 * 1024), 0);

        // Offset 4MB + 100KB -> chunk 1, sub-block 1, offset 36864
        let off = 4 * 1024 * 1024 + 100 * 1024;
        assert_eq!(chunk_index(off), 1);
        assert_eq!(sub_block_index(off), 1);
        assert_eq!(offset_in_sub_block(off), 36864);
    }

    #[test]
    fn bitmap_operations() {
        let mut bm: u64 = 0;
        assert!(!bitmap_is_set(bm, 0));
        bitmap_set(&mut bm, 0);
        assert!(bitmap_is_set(bm, 0));
        assert_eq!(bitmap_count(bm), 1);

        bitmap_set(&mut bm, 63);
        assert!(bitmap_is_set(bm, 63));
        assert_eq!(bitmap_count(bm), 2);
        assert!(!bitmap_is_full(bm));

        // Fill all bits
        bm = u64::MAX;
        assert!(bitmap_is_full(bm));
        assert_eq!(bitmap_count(bm), 64);
    }

    #[test]
    fn dirty_indices_iterator() {
        let bm: u64 = 0b1010_0001; // bits 0, 5, 7
        let indices: Vec<usize> = bitmap_dirty_indices(bm).collect();
        assert_eq!(indices, vec![0, 5, 7]);
    }

    #[test]
    fn backend_offset_calculation() {
        let chunk_base = 1024 * 1024; // 1MB into the bdev
        assert_eq!(backend_sub_block_offset(chunk_base, 0), chunk_base);
        assert_eq!(backend_sub_block_offset(chunk_base, 1), chunk_base + 65536);
        assert_eq!(backend_sub_block_offset(chunk_base, 63), chunk_base + 63 * 65536);
    }
}
```

- [ ] **Step 2: Add module to bdev/mod.rs**

Add `pub mod sub_block;` to `dataplane/src/bdev/mod.rs`.

- [ ] **Step 3: Run tests**

Run: `cd dataplane && cargo test sub_block -- --test-threads=1`
Expected: All 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/bdev/sub_block.rs dataplane/src/bdev/mod.rs
git commit -m "feat(sub-block): add sub-block constants, bitmap ops, offset math"
```

---

### Task 2: Extend ChunkMapEntry with dirty bitmap

**Files:**
- Modify: `dataplane/src/metadata/types.rs:47-53`

- [ ] **Step 1: Add dirty_bitmap field to ChunkMapEntry**

In `dataplane/src/metadata/types.rs`, change the `ChunkMapEntry` struct:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapEntry {
    pub chunk_index: u64,
    pub chunk_id: String,
    pub ec_params: Option<ErasureParams>,
    /// Bitmap of which 64KB sub-blocks have been written (64 bits = 64 sub-blocks).
    #[serde(default)]
    pub dirty_bitmap: u64,
}
```

The `#[serde(default)]` ensures backward compatibility — existing entries without the field deserialize with `dirty_bitmap = 0`.

- [ ] **Step 2: Fix any compilation errors**

Search for all `ChunkMapEntry {` constructors and add `dirty_bitmap: 0` (or `dirty_bitmap: u64::MAX` for entries created from full-chunk writes).

Run: `cd dataplane && cargo check 2>&1 | grep "missing field"`

Fix each constructor.

- [ ] **Step 3: Run existing tests**

Run: `cd dataplane && cargo test -- --test-threads=1`
Expected: All existing tests pass (no behavior change).

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/metadata/types.rs
git commit -m "feat(sub-block): add dirty_bitmap field to ChunkMapEntry"
```

---

## Chunk 2: Sub-Block Write Path

### Task 3: Replace rmw_write with sub-block write

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs:227-343`

This is the critical change. Replace the `rmw_write` function that reads/writes full 4MB chunks with a `sub_block_write` function that reads/writes 64KB sub-blocks.

- [ ] **Step 1: Write the new sub_block_write function**

Replace `rmw_write` (lines 227-343) with:

```rust
use crate::bdev::sub_block::*;

/// Write data at a volume offset using 64KB sub-block granularity.
/// Write-through: each sub-block is written to the backend immediately.
/// No full-chunk RMW — only the affected 64KB sub-block is read+modified.
async fn sub_block_write(
    volume_name: &str,
    offset: u64,
    data: &[u8],
    engine: &ChunkEngine,
) -> Result<Vec<ChunkMapEntry>> {
    let mut all_entries: Vec<ChunkMapEntry> = Vec::new();
    let mut data_cursor = 0usize;
    let end_offset = offset + data.len() as u64;

    // Process each sub-block that the write touches.
    let mut current_offset = offset;
    while current_offset < end_offset && data_cursor < data.len() {
        let ci = chunk_index(current_offset);
        let sbi = sub_block_index(current_offset);
        let off_in_sb = offset_in_sub_block(current_offset);

        // How many bytes go into this sub-block.
        let remaining_in_sb = SUB_BLOCK_SIZE - off_in_sb;
        let remaining_data = data.len() - data_cursor;
        let write_len = remaining_in_sb.min(remaining_data);

        // Serialize concurrent writes to the same sub-block.
        let lock = chunk_lock(volume_name, ci as usize * SUB_BLOCKS_PER_CHUNK + sbi);
        let _guard = lock.lock().await;

        // Backend offset for this chunk's region.
        let chunk_base = ci * CHUNK_SIZE as u64;
        let sb_backend_offset = backend_sub_block_offset(chunk_base, sbi);

        // If write covers the entire sub-block, skip the read.
        let full_sub_block = if off_in_sb == 0 && write_len >= SUB_BLOCK_SIZE {
            data[data_cursor..data_cursor + SUB_BLOCK_SIZE].to_vec()
        } else {
            // Read existing 64KB sub-block from backend.
            let handle = get_tokio_handle()?;
            let engine_ref = get_chunk_engine()?;
            let existing = crate::spdk::reactor_dispatch::bdev_read_async(
                &engine_ref.local_store_bdev_name(),
                sb_backend_offset,
                SUB_BLOCK_SIZE,
            ).await.unwrap_or_else(|_| vec![0u8; SUB_BLOCK_SIZE]);

            let mut buf = existing;
            if buf.len() < SUB_BLOCK_SIZE {
                buf.resize(SUB_BLOCK_SIZE, 0);
            }
            // Overlay new data.
            buf[off_in_sb..off_in_sb + write_len]
                .copy_from_slice(&data[data_cursor..data_cursor + write_len]);
            buf
        };

        // Write 64KB sub-block to backend (write-through).
        crate::spdk::reactor_dispatch::bdev_write_async(
            &get_chunk_engine()?.local_store_bdev_name(),
            sb_backend_offset,
            &full_sub_block,
        ).await?;

        // Update dirty bitmap in chunk map.
        {
            let mut maps = volume_chunk_maps().write().unwrap();
            if let Some(chunk_map) = maps.get_mut(volume_name) {
                let idx = ci as usize;
                if idx < chunk_map.len() {
                    if let Some(entry) = &mut chunk_map[idx] {
                        sub_block::bitmap_set(&mut entry.dirty_bitmap, sbi);
                    } else {
                        // First write to this chunk — create entry.
                        let mut entry = ChunkMapEntry {
                            chunk_index: ci,
                            chunk_id: format!("{}:{}", volume_name, ci),
                            ec_params: None,
                            dirty_bitmap: 0,
                        };
                        sub_block::bitmap_set(&mut entry.dirty_bitmap, sbi);
                        chunk_map[idx] = Some(entry);
                    }
                }
            }
        }

        // Persist bitmap update to metadata store.
        if let Some(store) = get_metadata_store() {
            let maps = volume_chunk_maps().read().unwrap();
            if let Some(chunk_map) = maps.get(volume_name) {
                if let Some(Some(entry)) = chunk_map.get(ci as usize) {
                    if let Err(e) = store.put_chunk_map(volume_name, entry) {
                        log::error!(
                            "sub-block write-through persistence failed for {}: {}",
                            volume_name, e
                        );
                        return Err(DataPlaneError::MetadataError(format!(
                            "chunk map persistence failed: {}", e
                        )));
                    }
                }
            }
        }

        all_entries.push(ChunkMapEntry {
            chunk_index: ci,
            chunk_id: format!("{}:{}", volume_name, ci),
            ec_params: None,
            dirty_bitmap: 1u64 << sbi,
        });

        data_cursor += write_len;
        current_offset += write_len as u64;
    }

    Ok(all_entries)
}
```

**Note:** This function needs `bdev_read_async` and `bdev_write_async` to support offset+length. See Task 5.

- [ ] **Step 2: Update WRITE handler in submit_request to call sub_block_write**

In the WRITE handler (line ~860), replace:
```rust
let entries = rmw_write(&volume_name, offset, &data, engine).await?;
```
with:
```rust
let entries = sub_block_write(&volume_name, offset, &data, engine).await?;
```

Same for WRITE_ZEROES handler (line ~904).

- [ ] **Step 3: Remove old rmw_write function**

Delete the old `rmw_write` function (lines 227-343) once `sub_block_write` is working.

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/bdev/novastor_bdev.rs
git commit -m "feat(sub-block): replace rmw_write with sub_block_write (64KB granularity)"
```

---

### Task 4: Sub-block read path

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs:684-809`

- [ ] **Step 1: Replace READ handler with sub-block read**

The current READ handler (lines 684-809) reads full chunks via ChunkEngine. Replace with direct 64KB sub-block reads from the backend bdev.

In the READ handler's `io_pool().execute(move || { ... })` block, replace the chunk-map lookup + engine.read() with:

```rust
// Calculate sub-block range for this read.
let mut result = vec![0u8; length as usize];
let mut result_cursor = 0usize;
let mut current_offset = offset;
let end_offset = offset + length;

while current_offset < end_offset {
    let ci = sub_block::chunk_index(current_offset);
    let sbi = sub_block::sub_block_index(current_offset);
    let off_in_sb = sub_block::offset_in_sub_block(current_offset);

    let remaining_in_sb = sub_block::SUB_BLOCK_SIZE - off_in_sb;
    let remaining_read = (end_offset - current_offset) as usize;
    let read_len = remaining_in_sb.min(remaining_read);

    let chunk_base = ci * sub_block::CHUNK_SIZE as u64;
    let sb_backend_offset = sub_block::backend_sub_block_offset(chunk_base, sbi);

    // Read 64KB sub-block from backend.
    let sb_data = crate::spdk::reactor_dispatch::bdev_read_async(
        &bdev_name_for_read,
        sb_backend_offset,
        sub_block::SUB_BLOCK_SIZE,
    ).await.unwrap_or_else(|_| vec![0u8; sub_block::SUB_BLOCK_SIZE]);

    // Extract requested bytes.
    let copy_len = read_len.min(sb_data.len().saturating_sub(off_in_sb));
    result[result_cursor..result_cursor + copy_len]
        .copy_from_slice(&sb_data[off_in_sb..off_in_sb + copy_len]);

    result_cursor += read_len;
    current_offset += read_len as u64;
}
```

The READ handler also needs to switch from `io_pool + block_on` to `handle.spawn` (like the WRITE handler) since reads are now async too.

- [ ] **Step 2: Run tests**

Build: `cd dataplane && cargo check`
Expected: Compiles (actual I/O tests require SPDK, done in Docker build).

- [ ] **Step 3: Commit**

```bash
git add dataplane/src/bdev/novastor_bdev.rs
git commit -m "feat(sub-block): replace full-chunk READ with 64KB sub-block reads"
```

---

### Task 5: Backend bdev_read_async / bdev_write_async with offset+length

**Files:**
- Modify: `dataplane/src/spdk/reactor_dispatch.rs`

The current `bdev_read_async` and `bdev_write_async` may need to support arbitrary offset+length (not just full-slot I/O). Check if they already do — they likely operate at byte offset + buffer length which is what we need.

- [ ] **Step 1: Verify bdev_read_async and bdev_write_async signatures**

Check `reactor_dispatch.rs` for these functions. They should take `(bdev_name, offset, length)` for reads and `(bdev_name, offset, data)` for writes. If they exist with this signature, no changes needed.

If they don't exist or only support full-chunk I/O, add wrappers that call SPDK's `spdk_bdev_read` / `spdk_bdev_write` at arbitrary offsets.

- [ ] **Step 2: Add local_store_bdev_name() accessor to ChunkEngine**

In `chunk/engine.rs`, add a method to get the backend bdev name:

```rust
pub fn local_store_bdev_name(&self) -> String {
    // The bdev name is stored in the local_store (BdevChunkStore).
    // Expose it for direct sub-block I/O.
    self.bdev_name.clone()
}
```

This requires adding a `bdev_name: String` field to `ChunkEngine` and setting it during construction.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(sub-block): expose backend bdev name for direct sub-block I/O"
```

---

## Chunk 3: Delta Replication & Background Sync

### Task 6: PutSubBlock gRPC for delta replication

**Files:**
- Modify: `dataplane/proto/dataplane_service.proto`
- Modify: `dataplane/src/transport/dataplane_service.rs`

- [ ] **Step 1: Add PutSubBlock message and RPC to proto**

In `dataplane/proto/dataplane_service.proto`, add:

```protobuf
// Inter-dataplane sub-block replication (delta sync)
rpc PutSubBlock(PutSubBlockRequest) returns (PutSubBlockResponse);

message PutSubBlockRequest {
    string volume_id = 1;
    uint64 chunk_index = 2;
    uint32 sub_block_index = 3;  // 0-63
    bytes data = 4;              // 64KB
}
message PutSubBlockResponse {}
```

- [ ] **Step 2: Implement PutSubBlock handler**

In `dataplane_service.rs`, add the handler:

```rust
async fn put_sub_block(
    &self,
    request: Request<PutSubBlockRequest>,
) -> Result<Response<PutSubBlockResponse>, Status> {
    let req = request.into_inner();
    if req.data.len() != sub_block::SUB_BLOCK_SIZE {
        return Err(Status::invalid_argument(
            format!("sub-block data must be {} bytes", sub_block::SUB_BLOCK_SIZE)
        ));
    }

    let chunk_base = req.chunk_index * sub_block::CHUNK_SIZE as u64;
    let sb_offset = sub_block::backend_sub_block_offset(chunk_base, req.sub_block_index as usize);

    // Write directly to backend bdev.
    let store = get_any_async_chunk_store()?;
    // Use bdev_write_async to write 64KB at the sub-block offset.
    crate::spdk::reactor_dispatch::bdev_write_async(
        &store.bdev_name(),
        sb_offset,
        &req.data,
    ).await.map_err(|e| Status::internal(format!("sub-block write failed: {e}")))?;

    log::debug!(
        "put_sub_block: vol={} chunk={} sb={} ({}B)",
        req.volume_id, req.chunk_index, req.sub_block_index, req.data.len()
    );

    Ok(Response::new(PutSubBlockResponse {}))
}
```

- [ ] **Step 3: Add sub-block replication to sub_block_write**

After writing the sub-block locally, replicate to remote nodes. In `sub_block_write`, after the backend write:

```rust
// Delta-replicate this sub-block to remote CRUSH nodes.
let topo = engine.topology_snapshot();
let placements = engine.crush_placements_for_chunk(ci, factor);
for (target_node, _) in &placements {
    if target_node != &engine.node_id() {
        // Spawn async gRPC PutSubBlock to remote.
        let addr = engine.node_address(target_node, &topo);
        if let Some(addr) = addr {
            let data_clone = full_sub_block.clone();
            let vol = volume_name.to_string();
            tokio::spawn(async move {
                // Send PutSubBlock RPC
                // (fire-and-forget for delta, sync verifies later)
            });
        }
    }
}
```

Wait for majority quorum before completing.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(sub-block): add PutSubBlock gRPC for delta replication"
```

---

### Task 7: Background sync task

**Files:**
- Create: `dataplane/src/chunk/sync.rs`
- Modify: `dataplane/src/chunk/mod.rs`
- Modify: `dataplane/src/transport/dataplane_service.rs` (spawn sync)

- [ ] **Step 1: Create sync.rs with background sync loop**

```rust
// dataplane/src/chunk/sync.rs

use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::bdev::novastor_bdev::{get_chunk_engine, volume_chunk_maps, get_metadata_store};
use crate::bdev::sub_block::*;
use crate::chunk::engine::ChunkEngine;

/// Background sync interval.
const SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Spawn the background sync task for a volume.
pub fn spawn_sync_task(volume_name: String, bdev_name: String) {
    tokio::spawn(async move {
        let mut interval = time::interval(SYNC_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = sync_dirty_chunks(&volume_name, &bdev_name).await {
                log::warn!("sync failed for volume {}: {}", volume_name, e);
            }
        }
    });
}

/// Sync all dirty chunks for a volume.
/// For each chunk with dirty_bitmap != 0:
/// 1. Read all 64 sub-blocks (4MB) from backend
/// 2. Compute SHA-256
/// 3. Update chunk ID if content changed
/// 4. Replicate full 4MB to remote nodes
/// 5. Clear dirty bitmap
async fn sync_dirty_chunks(
    volume_name: &str,
    bdev_name: &str,
) -> crate::error::Result<()> {
    // Find chunks with dirty bitmaps.
    let dirty_chunks: Vec<(u64, u64)> = {
        let maps = volume_chunk_maps().read().unwrap();
        let Some(chunk_map) = maps.get(volume_name) else { return Ok(()) };
        chunk_map.iter()
            .filter_map(|entry| {
                entry.as_ref()
                    .filter(|e| e.dirty_bitmap != 0)
                    .map(|e| (e.chunk_index, e.dirty_bitmap))
            })
            .collect()
    };

    if dirty_chunks.is_empty() {
        return Ok(());
    }

    log::info!(
        "sync: {} dirty chunks for volume {}",
        dirty_chunks.len(), volume_name
    );

    // Process dirty chunks in parallel via JoinSet.
    let mut join_set = tokio::task::JoinSet::new();
    for (chunk_idx, _bitmap) in dirty_chunks {
        let vol = volume_name.to_string();
        let bdev = bdev_name.to_string();
        join_set.spawn(async move {
            sync_one_chunk(&vol, &bdev, chunk_idx).await
        });
    }

    let mut synced = 0u64;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => synced += 1,
            Ok(Err(e)) => log::warn!("sync chunk failed: {}", e),
            Err(e) => log::warn!("sync task panicked: {}", e),
        }
    }

    log::info!("sync: {}/{} chunks synced for volume {}",
        synced, synced, volume_name);
    Ok(())
}

/// Sync a single dirty chunk:
/// read 4MB from backend, hash, replicate, clear bitmap.
async fn sync_one_chunk(
    volume_name: &str,
    bdev_name: &str,
    chunk_idx: u64,
) -> crate::error::Result<()> {
    let chunk_base = chunk_idx * CHUNK_SIZE as u64;

    // Read all 64 sub-blocks (4MB) from backend.
    let full_chunk = crate::spdk::reactor_dispatch::bdev_read_async(
        bdev_name,
        chunk_base,
        CHUNK_SIZE,
    ).await?;

    // Compute SHA-256 (hardware-accelerated via ring).
    let new_chunk_id = crate::chunk::engine::ChunkEngine::compute_chunk_id(&full_chunk);

    // Update chunk map with content-addressed ID and clear bitmap.
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        if let Some(chunk_map) = maps.get_mut(volume_name) {
            if let Some(Some(entry)) = chunk_map.get_mut(chunk_idx as usize) {
                entry.chunk_id = new_chunk_id.clone();
                entry.dirty_bitmap = 0;
            }
        }
    }

    // Persist updated chunk map entry.
    if let Some(store) = get_metadata_store() {
        let maps = volume_chunk_maps().read().unwrap();
        if let Some(chunk_map) = maps.get(volume_name) {
            if let Some(Some(entry)) = chunk_map.get(chunk_idx as usize) {
                let _ = store.put_chunk_map(volume_name, entry);
            }
        }
    }

    // Replicate full 4MB chunk to remote nodes via ChunkEngine.
    if let Ok(engine) = get_chunk_engine() {
        let prepared = ChunkEngine::prepare_chunk(&full_chunk);
        // Use engine's write_replicated for full-chunk sync.
        // This goes through CRUSH placement + quorum.
        if let Err(e) = engine.write(volume_name, chunk_base, &full_chunk).await {
            log::warn!("sync replication failed for chunk {}: {}", chunk_idx, e);
        }
    }

    log::debug!("sync: chunk {}:{} synced (id={})", volume_name, chunk_idx, &new_chunk_id[..16]);
    Ok(())
}
```

- [ ] **Step 2: Add module export**

Add `pub mod sync;` to `dataplane/src/chunk/mod.rs`.

- [ ] **Step 3: Spawn sync task in init_chunk_store**

In `dataplane/src/transport/dataplane_service.rs`, after the ChunkEngine is created in `init_chunk_store`, spawn the sync task:

```rust
// Spawn background sync task for periodic full-chunk hashing + replication.
crate::chunk::sync::spawn_sync_task(
    req.bdev_name.clone(),
    req.bdev_name.clone(),
);
```

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(sub-block): background sync task — hash + replicate dirty chunks"
```

---

## Chunk 4: Build, Deploy, Benchmark

### Task 8: Build and verify compilation

- [ ] **Step 1: Docker build**

```bash
docker build --no-cache -f dataplane/Dockerfile.build \
  -t 192.168.100.11:30500/novastor/dataplane:sub-block dataplane/
```

Fix any compilation errors.

- [ ] **Step 2: Push and deploy**

```bash
podman push 192.168.100.11:30500/novastor/dataplane:sub-block --tls-verify=false
kubectl -n novastor-system rollout restart daemonset/novastor-dataplane
kubectl -n novastor-system rollout status daemonset/novastor-dataplane --timeout=180s
kubectl -n novastor-system rollout restart daemonset/novastor-agent
kubectl -n novastor-system rollout status daemonset/novastor-agent --timeout=180s
```

- [ ] **Step 3: Create test volume and verify I/O**

```bash
# Create 10GB rep1 volume with fio pod
kubectl apply -f test/benchmark/bench-10g-rep1.yaml
# Wait for Running
# Write + read back + verify MD5
kubectl exec bench-10g-pod -- sh -c 'dd if=/dev/urandom of=/data/test bs=1M count=16 && sync && md5sum /data/test'
kubectl exec bench-10g-pod -- md5sum /data/test
```

- [ ] **Step 4: Run benchmark and compare with baseline**

```bash
./test/benchmark/storage-bench.sh bench-10g-pod --size 256 --label "sub-block v1"
```

Compare with baseline:
```
Baseline:   4.0 MB/s seq write |  9.0 MB/s seq read | 46 IOPS rand write | 66 IOPS rand read
Target:    50+ MB/s seq write  | 50+ MB/s seq read   | 1000+ IOPS rand write | 500+ IOPS rand read
```

- [ ] **Step 5: Commit results**

```bash
git add test/benchmark/results-*.txt
git commit -m "bench: sub-block I/O baseline results"
```

---

### Task 9: Verify sync, replication, and integrity

- [ ] **Step 1: Verify sync produces SHA-256 chunk IDs**

After writing data, wait 30s for sync, then check dataplane logs:
```bash
kubectl -n novastor-system logs <dp-pod> | grep "sync:"
```
Expected: `sync: N dirty chunks synced` with SHA-256 chunk IDs.

- [ ] **Step 2: Verify persistence survives restart**

Delete the dataplane pod, wait for restart, check logs:
```bash
kubectl -n novastor-system delete pod <dp-pod>
sleep 30
kubectl -n novastor-system logs <new-dp-pod> | grep "loaded.*chunk map"
```
Expected: Chunk maps restored from redb.

- [ ] **Step 3: Verify data integrity after restart**

```bash
kubectl exec bench-10g-pod -- md5sum /data/test
```
Expected: Same MD5 as before restart.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(sub-block): verified — sync, persistence, integrity all working"
```
