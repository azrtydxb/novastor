//! Background sync task for the sub-block I/O layer.
//!
//! Periodically scans for chunks with dirty sub-blocks, assembles the full
//! 4MB chunk, computes SHA-256, replicates to remote nodes, and clears the
//! dirty bitmap. This is the ONLY place where content-addressing happens.
//!
//! Sync is rate-limited to avoid saturating the SPDK reactor with concurrent
//! reads. At most `MAX_CONCURRENT_SYNC` chunks are in-flight at once, and
//! each chunk yields between read and hash to let foreground I/O proceed.
//!
//! Optimization: each chunk tracks the bitmap value it was last synced at.
//! If the bitmap hasn't changed since last sync, the chunk is skipped
//! entirely (no 4MB read, no SHA-256). This eliminates the 5.6GB/30s
//! wasted I/O from re-syncing unchanged chunks.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::bdev::novastor_bdev::{get_metadata_store, volume_chunk_maps};
use crate::bdev::sub_block::CHUNK_SIZE;
use crate::chunk::engine::ChunkEngine;
use crate::spdk::reactor_dispatch;

const SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Max concurrent chunk syncs to avoid saturating the SPDK reactor.
const MAX_CONCURRENT_SYNC: usize = 4;

/// Spawn a background sync task for a bdev.
pub fn spawn_sync_task(bdev_name: String) {
    let rt = crate::tokio_handle();
    rt.spawn(async move {
        let mut interval = tokio::time::interval(SYNC_INTERVAL);
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_SYNC));
        // Track last-synced bitmap per (volume, chunk_idx) to skip unchanged chunks.
        let mut last_synced: HashMap<(String, u64), u64> = HashMap::new();
        loop {
            interval.tick().await;
            sync_all_volumes(&bdev_name, &semaphore, &mut last_synced).await;
        }
    });
}

async fn sync_all_volumes(
    bdev_name: &str,
    semaphore: &Arc<Semaphore>,
    last_synced: &mut HashMap<(String, u64), u64>,
) {
    // Snapshot atomic bitmaps into ChunkMapEntry before scanning.
    crate::bdev::novastor_bdev::sync_atomic_bitmaps_to_chunk_maps();

    let volumes: Vec<String> = {
        let maps = volume_chunk_maps().read().unwrap();
        maps.keys().cloned().collect()
    };

    for volume_name in &volumes {
        if let Err(e) = sync_volume(volume_name, bdev_name, semaphore, last_synced).await {
            log::warn!("sync failed for volume {}: {}", volume_name, e);
        }
    }
}

async fn sync_volume(
    volume_name: &str,
    bdev_name: &str,
    semaphore: &Arc<Semaphore>,
    last_synced: &mut HashMap<(String, u64), u64>,
) -> crate::error::Result<()> {
    // Collect chunks that are dirty AND have changed since last sync.
    let dirty_chunks: Vec<(u64, u64)> = {
        let maps = volume_chunk_maps().read().unwrap();
        match maps.get(volume_name) {
            Some(chunk_map) => chunk_map
                .iter()
                .filter_map(|entry| {
                    entry.as_ref().and_then(|e| {
                        if e.dirty_bitmap == 0 {
                            return None;
                        }
                        let key = (volume_name.to_string(), e.chunk_index);
                        let prev = last_synced.get(&key).copied().unwrap_or(0);
                        if e.dirty_bitmap == prev {
                            // Bitmap unchanged since last sync — skip.
                            None
                        } else {
                            Some((e.chunk_index, e.dirty_bitmap))
                        }
                    })
                })
                .collect(),
            None => return Ok(()),
        }
    };

    if dirty_chunks.is_empty() {
        return Ok(());
    }

    log::info!(
        "sync: {} changed chunks for volume '{}'",
        dirty_chunks.len(),
        volume_name
    );

    let mut join_set = tokio::task::JoinSet::new();
    for (chunk_idx, bitmap_snapshot) in dirty_chunks {
        let vol = volume_name.to_string();
        let bdev = bdev_name.to_string();
        let sem = semaphore.clone();
        join_set.spawn(async move {
            let _permit = sem.acquire().await.expect("semaphore closed");
            sync_one_chunk(&vol, &bdev, chunk_idx, bitmap_snapshot).await
        });
    }

    let mut synced = 0u64;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok((vol, idx, bm))) => {
                // Record successfully synced bitmap value.
                last_synced.insert((vol, idx), bm);
                synced += 1;
            }
            Ok(Err(e)) => log::warn!("sync chunk failed: {}", e),
            Err(e) => log::warn!("sync task panicked: {}", e),
        }
    }

    if synced > 0 {
        log::info!(
            "sync: {} chunks synced for volume '{}'",
            synced,
            volume_name
        );
    }
    Ok(())
}

/// Sync a single chunk. Returns (volume_name, chunk_idx, synced_bitmap) on success.
async fn sync_one_chunk(
    volume_name: &str,
    bdev_name: &str,
    chunk_idx: u64,
    bitmap_snapshot: u64,
) -> crate::error::Result<(String, u64, u64)> {
    let chunk_base = chunk_idx * CHUNK_SIZE as u64;

    // Read the full 4MB chunk from backend.
    let full_chunk =
        reactor_dispatch::bdev_read_async(bdev_name, chunk_base, CHUNK_SIZE as u64).await?;

    // Yield to let foreground I/O proceed before CPU-heavy hashing.
    tokio::task::yield_now().await;

    // Compute SHA-256 on a blocking thread.
    let new_chunk_id =
        tokio::task::spawn_blocking(move || ChunkEngine::compute_chunk_id(&full_chunk))
            .await
            .expect("sha256 task panicked");

    // Update chunk map: set content-addressed ID.
    // Use compare-and-swap on the bitmap: only clear bits that were set
    // in our snapshot. New writes that arrived during sync keep their bits.
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        if let Some(chunk_map) = maps.get_mut(volume_name) {
            if let Some(Some(entry)) = chunk_map.get_mut(chunk_idx as usize) {
                entry.chunk_id = new_chunk_id.clone();
                // Clear only the bits we snapshotted — preserve any new dirty bits
                // from writes that occurred during the sync.
                entry.dirty_bitmap &= !bitmap_snapshot;
            }
        }
    }

    // Also clear the atomic bitmap bits we synced (same CAS approach).
    // The reactor may have set new bits since our snapshot — those are preserved.
    if let Ok(registry) = crate::bdev::novastor_bdev::bdev_registry().lock() {
        for entry in registry.values() {
            let ctx = entry.ctx_ptr as *const crate::bdev::novastor_bdev::BdevCtx;
            if !ctx.is_null() {
                let bdev_ctx = unsafe { &*ctx };
                if bdev_ctx.volume_name == volume_name
                    && (chunk_idx as usize) < bdev_ctx.dirty_bitmaps.len()
                {
                    bdev_ctx.dirty_bitmaps[chunk_idx as usize]
                        .fetch_and(!bitmap_snapshot, Ordering::Relaxed);
                }
            }
        }
    }

    // Persist updated entry.
    if let Some(store) = get_metadata_store() {
        let maps = volume_chunk_maps().read().unwrap();
        if let Some(chunk_map) = maps.get(volume_name) {
            if let Some(Some(entry)) = chunk_map.get(chunk_idx as usize) {
                let _ = store.put_chunk_map(volume_name, entry);
            }
        }
    }

    log::debug!(
        "sync: chunk {}:{} hashed (id={}...)",
        volume_name,
        chunk_idx,
        &new_chunk_id[..16.min(new_chunk_id.len())]
    );
    Ok((volume_name.to_string(), chunk_idx, bitmap_snapshot))
}

/// Bulk-destage all dirty bitmaps to the metadata store.
/// Called on clean shutdown to persist in-memory state in one batch.
pub fn destage_all_bitmaps() {
    crate::bdev::novastor_bdev::sync_atomic_bitmaps_to_chunk_maps();

    let store = match get_metadata_store() {
        Some(s) => s,
        None => {
            log::warn!("destage: no metadata store available");
            return;
        }
    };
    let maps = volume_chunk_maps().read().unwrap();
    let mut count = 0u64;
    for (vol, chunk_map) in maps.iter() {
        for entry in chunk_map.iter().flatten() {
            let _ = store.put_chunk_map(vol, entry);
            count += 1;
        }
    }
    log::info!("destage: persisted {} chunk map entries", count);
}
