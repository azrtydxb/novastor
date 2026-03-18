//! Background sync task for the sub-block I/O layer.
//!
//! Periodically scans for chunks with dirty sub-blocks, assembles the full
//! 4MB chunk, computes SHA-256, replicates to remote nodes, and clears the
//! dirty bitmap. This is the ONLY place where content-addressing happens.

use std::time::Duration;

use crate::bdev::novastor_bdev::{get_backend_bdev_name, get_metadata_store, volume_chunk_maps};
use crate::bdev::sub_block::CHUNK_SIZE;
use crate::chunk::engine::ChunkEngine;
use crate::spdk::reactor_dispatch;

const SYNC_INTERVAL: Duration = Duration::from_secs(30);

/// Spawn a background sync task for a bdev.
pub fn spawn_sync_task(bdev_name: String) {
    let rt = crate::tokio_handle();
    rt.spawn(async move {
        let mut interval = tokio::time::interval(SYNC_INTERVAL);
        loop {
            interval.tick().await;
            sync_all_volumes(&bdev_name).await;
        }
    });
}

async fn sync_all_volumes(bdev_name: &str) {
    // Snapshot atomic bitmaps into ChunkMapEntry before scanning for dirty chunks.
    crate::bdev::novastor_bdev::sync_atomic_bitmaps_to_chunk_maps();

    // Get list of volumes with dirty chunks.
    let volumes: Vec<String> = {
        let maps = volume_chunk_maps().read().unwrap();
        maps.keys().cloned().collect()
    };

    for volume_name in &volumes {
        if let Err(e) = sync_volume(volume_name, bdev_name).await {
            log::warn!("sync failed for volume {}: {}", volume_name, e);
        }
    }
}

async fn sync_volume(volume_name: &str, bdev_name: &str) -> crate::error::Result<()> {
    // Collect dirty chunk indices.
    let dirty_chunks: Vec<u64> = {
        let maps = volume_chunk_maps().read().unwrap();
        match maps.get(volume_name) {
            Some(chunk_map) => chunk_map
                .iter()
                .filter_map(|entry| {
                    entry
                        .as_ref()
                        .filter(|e| e.dirty_bitmap != 0)
                        .map(|e| e.chunk_index)
                })
                .collect(),
            None => return Ok(()),
        }
    };

    if dirty_chunks.is_empty() {
        return Ok(());
    }

    log::info!(
        "sync: {} dirty chunks for volume '{}'",
        dirty_chunks.len(),
        volume_name
    );

    // Process dirty chunks in parallel via JoinSet.
    let mut join_set = tokio::task::JoinSet::new();
    for chunk_idx in dirty_chunks {
        let vol = volume_name.to_string();
        let bdev = bdev_name.to_string();
        join_set.spawn(async move { sync_one_chunk(&vol, &bdev, chunk_idx).await });
    }

    let mut synced = 0u64;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => synced += 1,
            Ok(Err(e)) => log::warn!("sync chunk failed: {}", e),
            Err(e) => log::warn!("sync task panicked: {}", e),
        }
    }

    log::info!(
        "sync: {} chunks synced for volume '{}'",
        synced,
        volume_name
    );
    Ok(())
}

#[tracing::instrument(skip_all, fields(volume = %volume_name, chunk_idx))]
async fn sync_one_chunk(
    volume_name: &str,
    bdev_name: &str,
    chunk_idx: u64,
) -> crate::error::Result<()> {
    let chunk_base = chunk_idx * CHUNK_SIZE as u64;

    // Read the full 4MB chunk from backend (all 64 sub-blocks).
    let full_chunk =
        reactor_dispatch::bdev_read_async(bdev_name, chunk_base, CHUNK_SIZE as u64).await?;

    // Compute SHA-256 (hardware-accelerated via ring).
    let new_chunk_id = ChunkEngine::compute_chunk_id(&full_chunk);

    // Update chunk map: set content-addressed ID, clear dirty bitmap.
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        if let Some(chunk_map) = maps.get_mut(volume_name) {
            if let Some(Some(entry)) = chunk_map.get_mut(chunk_idx as usize) {
                entry.chunk_id = new_chunk_id.clone();
                entry.dirty_bitmap = 0;
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
    Ok(())
}

/// Bulk-destage all dirty bitmaps to the metadata store (BlueStore V3 pattern).
/// Called on clean shutdown to persist in-memory state in one batch.
pub fn destage_all_bitmaps() {
    // Flush atomic bitmaps to ChunkMapEntry before persisting.
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
