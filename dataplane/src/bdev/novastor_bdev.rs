//! NovaStor custom SPDK bdev module.
//!
//! Each volume is registered as an SPDK bdev named `novastor_<volume_id>`.
//! When SPDK's NVMe-oF subsystem submits I/O to this bdev, the module
//! routes all reads and writes through the ChunkEngine, which uses CRUSH
//! placement to determine the target backend (local bdev or remote NDP node).
//!
//! All I/O is dispatched via tokio to the ChunkEngine's async sub_block_read
//! and sub_block_write methods. The ChunkEngine handles local vs remote routing
//! transparently. A reactor fast path for local-only chunks can be re-added
//! as a future optimization.

use crate::bdev::sub_block::{self, bitmap_is_set, bitmap_set, CHUNK_SIZE, SUB_BLOCK_SIZE};
use crate::chunk::engine::ChunkEngine;
use crate::chunk::write_cache::AbsorbResult;
use crate::error::{DataPlaneError, Result};
use crate::metadata::crush;
use crate::metadata::store::MetadataStore;
use crate::metadata::types::{ChunkMapEntry, Protection};
use crate::spdk::reactor_dispatch;
use dashmap::DashMap;
use log::{error, info, warn};
use std::collections::HashMap;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use tracing::{info_span, Instrument};

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    improper_ctypes
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

/// Registry of active NovaStor bdevs, keyed by volume name.
static NOVASTOR_BDEVS: OnceLock<Mutex<HashMap<String, NovastorBdevEntry>>> = OnceLock::new();

pub(crate) fn bdev_registry() -> &'static Mutex<HashMap<String, NovastorBdevEntry>> {
    NOVASTOR_BDEVS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The ChunkEngine shared across all NovaStor bdevs.
static CHUNK_ENGINE: OnceLock<Arc<ChunkEngine>> = OnceLock::new();

/// Tokio runtime handle for blocking on async ChunkEngine calls from
/// the synchronous thread pool workers.
static TOKIO_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();

/// Per-volume chunk map: maps volume_name → (chunk_index → ChunkMapEntry).
/// Updated on writes and used for reads. This is the local cache of the
/// volume-to-chunk mapping (authoritative source is the metadata service).
static VOLUME_CHUNK_MAPS: OnceLock<RwLock<HashMap<String, Vec<Option<ChunkMapEntry>>>>> =
    OnceLock::new();

pub fn volume_chunk_maps() -> &'static RwLock<HashMap<String, Vec<Option<ChunkMapEntry>>>> {
    VOLUME_CHUNK_MAPS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Snapshot all atomic bitmaps from BdevCtx into ChunkMapEntry for persistence.
/// Called by the background sync task before persisting dirty chunks.
pub fn sync_atomic_bitmaps_to_chunk_maps() {
    // Read bitmaps from each registered bdev's BdevCtx.
    let registry = bdev_registry().lock().unwrap();
    let mut maps = volume_chunk_maps().write().unwrap();
    for (_, entry) in registry.iter() {
        let ctx = entry.ctx_ptr as *const BdevCtx;
        if ctx.is_null() {
            continue;
        }
        let bdev_ctx = unsafe { &*ctx };
        let vol_name = &bdev_ctx.volume_name;
        if let Some(chunk_map) = maps.get_mut(vol_name.as_str()) {
            if bdev_ctx.dirty_bitmaps.len() > chunk_map.len() {
                chunk_map.resize(bdev_ctx.dirty_bitmaps.len(), None);
            }
            for (i, atomic_bm) in bdev_ctx.dirty_bitmaps.iter().enumerate() {
                let bm = atomic_bm.load(std::sync::atomic::Ordering::Relaxed);
                if bm != 0 {
                    let cm_entry = chunk_map[i].get_or_insert_with(|| ChunkMapEntry {
                        chunk_index: i as u64,
                        chunk_id: format!("{}:{}", vol_name, i),
                        ec_params: None,
                        dirty_bitmap: 0,
                        placements: vec![],
                        generation: 0,
                    });
                    cm_entry.dirty_bitmap = bm;
                }
            }
        }
    }
}

/// Public accessor for volume chunk maps (used by delete_volume cleanup).
pub fn volume_chunk_maps_ref(
) -> Option<&'static RwLock<HashMap<String, Vec<Option<ChunkMapEntry>>>>> {
    VOLUME_CHUNK_MAPS.get()
}

/// Persistent metadata store for chunk maps and volume definitions.
/// Write-through: every update to VOLUME_CHUNK_MAPS is also persisted here.
static METADATA_STORE: OnceLock<Arc<MetadataStore>> = OnceLock::new();

/// Set the metadata store used for persisting chunk maps across restarts.
/// Must be called once during init_chunk_store, before any I/O.
pub fn set_metadata_store(store: Arc<MetadataStore>) {
    if METADATA_STORE.set(store).is_err() {
        info!("novastor_bdev: metadata store already set, ignoring");
    }
}

pub fn get_metadata_store() -> Option<&'static Arc<MetadataStore>> {
    METADATA_STORE.get()
}

/// Public accessor for the metadata store (used by delete_volume cleanup).
pub fn metadata_store_ref() -> Option<&'static Arc<MetadataStore>> {
    METADATA_STORE.get()
}

/// The backend SPDK bdev name for direct sub-block I/O.
static BACKEND_BDEV_NAME: OnceLock<String> = OnceLock::new();

/// Per-volume base offset on the backend bdev. Volumes are laid out
/// sequentially after a reserved region. The reserved region matches
/// BdevChunkStore's data_region_offset (metadata area at start of bdev).
static VOLUME_BASE_OFFSETS: OnceLock<RwLock<HashMap<String, u64>>> = OnceLock::new();

/// Next free offset for allocating new volumes.
static NEXT_VOLUME_OFFSET: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();

fn volume_offsets() -> &'static RwLock<HashMap<String, u64>> {
    VOLUME_BASE_OFFSETS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Reserve space for a volume and return its base offset on the bdev.
pub fn allocate_volume_offset(volume_name: &str, size_bytes: u64) -> u64 {
    let mut offsets = volume_offsets().write().unwrap();
    if let Some(&existing) = offsets.get(volume_name) {
        return existing;
    }
    // Start after 64MB reserved region (metadata, superblock, etc.)
    let next =
        NEXT_VOLUME_OFFSET.get_or_init(|| std::sync::atomic::AtomicU64::new(64 * 1024 * 1024));
    let base = next.fetch_add(
        // Align size up to 4MB chunk boundary
        (size_bytes + CHUNK_SIZE as u64 - 1) & !(CHUNK_SIZE as u64 - 1),
        std::sync::atomic::Ordering::Relaxed,
    );
    offsets.insert(volume_name.to_string(), base);
    info!(
        "novastor_bdev: allocated volume '{}' at bdev offset {} ({}MB)",
        volume_name,
        base,
        base / (1024 * 1024)
    );
    base
}

/// Get the base offset for a volume, lazily allocating if needed.
/// This enables lazy per-chunk allocation: the first sub-block write
/// to a volume on this backend automatically allocates a 4MB chunk slot.
pub fn get_volume_base_offset(volume_name: &str) -> Result<u64> {
    // Fast path: already allocated. Read lock is sub-microsecond (HashMap lookup).
    // Only write locks (allocation) could stall, and those are rare (first I/O per volume).
    {
        let offsets = volume_offsets().read().unwrap();
        if let Some(&offset) = offsets.get(volume_name) {
            return Ok(offset);
        }
    }
    // Lazy allocation: first I/O to this volume on this backend.
    // Allocate a generous default (10GB). The actual used space is
    // tracked by dirty bitmaps at sub-block granularity.
    let default_size = 10 * 1024 * 1024 * 1024u64; // 10GB
    let offset = allocate_volume_offset(volume_name, default_size);
    info!(
        "novastor_bdev: lazy-allocated volume '{}' at offset {}",
        volume_name, offset
    );
    Ok(offset)
}

/// Set the backend bdev name used for direct sub-block I/O.
/// Must be called once during init_chunk_store.
pub fn set_backend_bdev_name(name: &str) {
    if BACKEND_BDEV_NAME.set(name.to_string()).is_err() {
        info!("novastor_bdev: backend bdev name already set, ignoring");
    }
}

pub fn get_backend_bdev_name() -> Result<&'static str> {
    BACKEND_BDEV_NAME
        .get()
        .map(|s| s.as_str())
        .ok_or_else(|| DataPlaneError::BdevError("backend bdev name not set".into()))
}

/// Load all persisted chunk maps from the MetadataStore into VOLUME_CHUNK_MAPS.
///
/// Called once at startup (after set_metadata_store) to restore volume chunk
/// maps that were persisted before the last dataplane restart.
pub fn load_chunk_maps_from_store() {
    let store = match get_metadata_store() {
        Some(s) => s,
        None => {
            warn!("novastor_bdev: load_chunk_maps_from_store called but no metadata store set");
            return;
        }
    };

    // Load all volumes to know their chunk counts.
    let volumes = match store.list_volumes() {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "novastor_bdev: failed to list volumes from metadata store: {}",
                e
            );
            return;
        }
    };

    if volumes.is_empty() {
        info!("novastor_bdev: no persisted volumes found in metadata store");
        return;
    }

    let mut maps = volume_chunk_maps().write().unwrap();
    let mut total_entries = 0usize;

    for vol in &volumes {
        // Load the chunk map entries for this volume.
        let entries = match store.list_chunk_map(&vol.id) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                    "novastor_bdev: failed to load chunk map for volume '{}': {}",
                    vol.id, e
                );
                continue;
            }
        };

        // Create the chunk map vector, sized to the volume's chunk count.
        let num_chunks = vol.chunk_count as usize;
        let mut chunk_map = vec![None; num_chunks];

        for entry in &entries {
            let idx = entry.chunk_index as usize;
            if idx < chunk_map.len() {
                chunk_map[idx] = Some(entry.clone());
            } else {
                warn!(
                    "novastor_bdev: chunk index {} exceeds volume '{}' chunk count {}",
                    idx, vol.id, num_chunks
                );
            }
        }

        total_entries += entries.len();
        maps.insert(vol.id.clone(), chunk_map);
    }

    info!(
        "novastor_bdev: loaded {} chunk map entries across {} volumes from metadata store",
        total_entries,
        volumes.len()
    );
}

/// Set the chunk engine that NovaStor bdevs will use for I/O.
/// Must be called once after the backend and chunk store are initialised.
pub fn set_chunk_engine(engine: Arc<ChunkEngine>, handle: tokio::runtime::Handle) {
    if CHUNK_ENGINE.set(engine).is_err() {
        info!("novastor_bdev: chunk engine already set, ignoring");
    }
    let _ = TOKIO_HANDLE.set(handle);
}

pub fn get_chunk_engine() -> Result<&'static Arc<ChunkEngine>> {
    CHUNK_ENGINE
        .get()
        .ok_or_else(|| DataPlaneError::BdevError("chunk engine not initialised".into()))
}

fn get_tokio_handle() -> Result<&'static tokio::runtime::Handle> {
    TOKIO_HANDLE
        .get()
        .ok_or_else(|| DataPlaneError::BdevError("tokio handle not set".into()))
}

/// Per-sub-block lock to serialize concurrent partial writes on the same sub-block.
/// Key: (volume_name, chunk_index, sub_block_index). The Mutex<()> provides mutual exclusion.
static SUB_BLOCK_LOCKS: OnceLock<DashMap<(String, usize, usize), Arc<tokio::sync::Mutex<()>>>> =
    OnceLock::new();

fn sub_block_lock(volume: &str, chunk_idx: usize, sb_idx: usize) -> Arc<tokio::sync::Mutex<()>> {
    let locks = SUB_BLOCK_LOCKS.get_or_init(DashMap::new);
    locks
        .entry((volume.to_string(), chunk_idx, sb_idx))
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Remove all per-sub-block lock entries for the given volume from SUB_BLOCK_LOCKS.
/// Called during volume destruction to prevent unbounded memory growth.
fn cleanup_volume_locks(volume_name: &str) {
    if let Some(locks) = SUB_BLOCK_LOCKS.get() {
        let before = locks.len();
        locks.retain(|key, _| key.0 != volume_name);
        let removed = before - locks.len();
        if removed > 0 {
            info!(
                "novastor_bdev: cleaned up {} sub-block locks for volume '{}'",
                removed, volume_name
            );
        }
    }
}

/// Sub-block write: writes data in 64KB sub-block granularity directly to the
/// backend bdev, bypassing the ChunkEngine for hot-path I/O.
///
/// Optimisations:
/// - **Batch contiguous**: consecutive full sub-blocks within the same chunk
///   are issued as a single `bdev_write_async` call (e.g. 1MB aligned write →
///   one 1MB bdev write instead of 16 × 64KB writes).
/// - **Skip first-write read**: partial writes to a never-written sub-block
///   start from a zeroed buffer (checked via dirty bitmap) instead of reading
///   64KB of undefined data from the bdev.
/// - Dirty bitmap is updated per sub-block as before.
///
/// No SHA-256 hashing — content-addressing happens during background sync.
/// Volume data is laid out contiguously on the backend bdev: offset maps directly.
/// Direct backend bdev write — bypasses ChunkEngine. Kept for future use
/// when reactor fast path is re-added for local-only chunks.

#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, len = data.len()))]
pub async fn sub_block_write_local(volume_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let backend_bdev = get_backend_bdev_name()?;
    let vol_base = get_volume_base_offset(volume_name)?;
    let total_len = data.len() as u64;
    let end_offset = offset + total_len;
    let mut data_cursor = 0usize;

    // ---------------------------------------------------------------
    // Phase 1: decompose the write into per-sub-block descriptors.
    // ---------------------------------------------------------------
    struct SbDesc {
        chunk_idx: usize,
        sb_idx: usize,
        offset_in_sb: usize,
        write_len: usize,
        data_start: usize, // index into `data`
        full: bool,        // covers entire 64KB sub-block?
    }

    let mut descs: Vec<SbDesc> = Vec::new();
    let mut pos = offset;
    while pos < end_offset {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);
        let offset_in_sb = sub_block::offset_in_sub_block(pos);
        let remaining_in_sb = SUB_BLOCK_SIZE - offset_in_sb;
        let remaining_data = (end_offset - pos) as usize;
        let write_len = remaining_in_sb.min(remaining_data);
        let full = offset_in_sb == 0 && write_len == SUB_BLOCK_SIZE;

        descs.push(SbDesc {
            chunk_idx,
            sb_idx,
            offset_in_sb,
            write_len,
            data_start: data_cursor,
            full,
        });
        data_cursor += write_len;
        pos += write_len as u64;
    }

    // ---------------------------------------------------------------
    // Phase 2: issue I/O — batch contiguous full sub-blocks, RMW partials.
    // ---------------------------------------------------------------
    let mut i = 0;
    while i < descs.len() {
        let d = &descs[i];

        if d.full {
            // Try to extend a contiguous run of full sub-blocks in the same chunk.
            let run_start = i;
            let mut run_end = i + 1;
            while run_end < descs.len() {
                let next = &descs[run_end];
                let prev = &descs[run_end - 1];
                if next.full && next.chunk_idx == d.chunk_idx && next.sb_idx == prev.sb_idx + 1 {
                    run_end += 1;
                } else {
                    break;
                }
            }

            let first = &descs[run_start];
            let last = &descs[run_end - 1];
            let n_sbs = run_end - run_start;
            let batch_bytes = n_sbs * SUB_BLOCK_SIZE;
            let batch_data = &data[first.data_start..first.data_start + batch_bytes];

            let chunk_base = vol_base + first.chunk_idx as u64 * CHUNK_SIZE as u64;
            let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, first.sb_idx);

            reactor_dispatch::bdev_write_async(backend_bdev, bdev_offset, batch_data)
                .instrument(info_span!(
                    "bdev_write_batch",
                    chunk_idx = first.chunk_idx,
                    sb_start = first.sb_idx,
                    sb_end = last.sb_idx,
                    n_sbs
                ))
                .await?;

            // Update bitmaps for all sub-blocks in the batch.
            for desc in &descs[run_start..run_end] {
                update_dirty_bitmap(volume_name, desc.chunk_idx, desc.sb_idx)?;
            }

            i = run_end;
        } else {
            // Partial sub-block write — write directly at exact offset.
            // No read-modify-write: just write the user's bytes at the
            // precise backend position.  The dirty bitmap tracks which
            // sub-blocks have data for the periodic sync/hash cycle.
            const BDEV_BLOCK_SIZE: u64 = 512;

            let chunk_base = vol_base + d.chunk_idx as u64 * CHUNK_SIZE as u64;
            let sb_bdev_start = sub_block::backend_sub_block_offset(chunk_base, d.sb_idx);
            let write_data = &data[d.data_start..d.data_start + d.write_len];

            // SPDK requires block-aligned I/O.  Filesystem writes are
            // always ≥512-byte aligned, so the fast path covers all
            // realistic traffic.
            let off_aligned = (d.offset_in_sb as u64) % BDEV_BLOCK_SIZE == 0;
            let len_aligned = (d.write_len as u64) % BDEV_BLOCK_SIZE == 0;

            if off_aligned && len_aligned {
                // Fast path: direct write, no RMW, no lock.
                reactor_dispatch::bdev_write_async(
                    backend_bdev,
                    sb_bdev_start + d.offset_in_sb as u64,
                    write_data,
                )
                .instrument(info_span!(
                    "bdev_write_direct",
                    chunk_idx = d.chunk_idx,
                    sb_idx = d.sb_idx,
                    offset_in_sb = d.offset_in_sb,
                    write_len = d.write_len
                ))
                .await?;
            } else {
                // Unaligned partial write — RMW at 512-byte granularity.
                let lock = sub_block_lock(volume_name, d.chunk_idx, d.sb_idx);
                let _guard = lock.lock().await;

                let aligned_off = (d.offset_in_sb as u64) & !(BDEV_BLOCK_SIZE - 1);
                let aligned_end = ((d.offset_in_sb + d.write_len) as u64 + BDEV_BLOCK_SIZE - 1)
                    & !(BDEV_BLOCK_SIZE - 1);
                let aligned_len = (aligned_end - aligned_off) as usize;

                let mut buf = reactor_dispatch::bdev_read_async(
                    backend_bdev,
                    sb_bdev_start + aligned_off,
                    aligned_len as u64,
                )
                .instrument(info_span!("bdev_read_rmw_unaligned"))
                .await?;
                buf.resize(aligned_len, 0);

                let buf_off = d.offset_in_sb - aligned_off as usize;
                buf[buf_off..buf_off + d.write_len].copy_from_slice(write_data);

                reactor_dispatch::bdev_write_async(backend_bdev, sb_bdev_start + aligned_off, &buf)
                    .instrument(info_span!("bdev_write_rmw_unaligned"))
                    .await?;
            }

            update_dirty_bitmap(volume_name, d.chunk_idx, d.sb_idx)?;
            i += 1;
        }
    }

    Ok(())
}

/// Zero a byte range on a volume by writing actual zeros through the
/// ChunkEngine write path. We must write real zeros (not just clear the
/// bitmap) because filesystems like ext4 may issue WRITE_ZEROES for ranges
/// that still contain metadata — clearing the bitmap would lose that data.
async fn sub_block_write_zeroes(volume_name: &str, offset: u64, length: u64) -> Result<()> {
    if length == 0 {
        return Ok(());
    }

    let engine = get_chunk_engine()?;

    // Write zeros in SUB_BLOCK_SIZE chunks to avoid huge allocations.
    // Route through ChunkEngine so CRUSH placement is consistent with
    // regular writes and reads.
    let end_offset = offset + length;
    let mut pos = offset;
    let zero_buf = vec![0u8; SUB_BLOCK_SIZE];

    while pos < end_offset {
        let remaining = (end_offset - pos) as usize;
        let write_len = remaining.min(SUB_BLOCK_SIZE);
        engine
            .sub_block_write(volume_name, pos, &zero_buf[..write_len])
            .await?;
        pos += write_len as u64;
    }

    Ok(())
}

/// Find the byte offset of the next data (written) region at or after `offset`.
/// Returns `u64::MAX` if no data exists after `offset`.
fn find_next_data(volume_name: &str, offset: u64) -> u64 {
    let maps = volume_chunk_maps().read().unwrap();
    let chunk_map = match maps.get(volume_name) {
        Some(cm) => cm,
        None => return u64::MAX,
    };

    let mut pos = offset;
    loop {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);

        if chunk_idx >= chunk_map.len() {
            return u64::MAX;
        }

        if let Some(entry) = chunk_map.get(chunk_idx).and_then(|e| e.as_ref()) {
            // Scan from sb_idx to end of this chunk for a dirty bit.
            for i in sb_idx..sub_block::SUB_BLOCKS_PER_CHUNK {
                if bitmap_is_set(entry.dirty_bitmap, i) {
                    let data_offset =
                        chunk_idx as u64 * CHUNK_SIZE as u64 + i as u64 * SUB_BLOCK_SIZE as u64;
                    return data_offset.max(offset);
                }
            }
        }

        // Move to the next chunk.
        pos = (chunk_idx as u64 + 1) * CHUNK_SIZE as u64;
    }
}

/// Find the byte offset of the next hole (unwritten) region at or after `offset`.
/// Returns `u64::MAX` if the entire remaining volume is data.
fn find_next_hole(volume_name: &str, offset: u64) -> u64 {
    let maps = volume_chunk_maps().read().unwrap();
    let chunk_map = match maps.get(volume_name) {
        Some(cm) => cm,
        None => return offset, // No data at all — entire volume is a hole.
    };

    let chunk_idx = sub_block::chunk_index(offset) as usize;
    let sb_idx = sub_block::sub_block_index(offset);

    // If chunk doesn't exist in the map, it's all holes.
    if chunk_idx >= chunk_map.len() {
        return offset;
    }

    if let Some(entry) = chunk_map.get(chunk_idx).and_then(|e| e.as_ref()) {
        // Scan from sb_idx for an unset bit.
        for i in sb_idx..sub_block::SUB_BLOCKS_PER_CHUNK {
            if !bitmap_is_set(entry.dirty_bitmap, i) {
                let hole_offset =
                    chunk_idx as u64 * CHUNK_SIZE as u64 + i as u64 * SUB_BLOCK_SIZE as u64;
                return hole_offset.max(offset);
            }
        }
        // This chunk is fully written. Next chunk starts a new scan.
        let next_chunk = (chunk_idx + 1) as u64 * CHUNK_SIZE as u64;
        return find_next_hole(volume_name, next_chunk);
    }

    // No entry for this chunk — it's a hole.
    offset
}

/// Load the dirty bitmap for a chunk from the BdevCtx atomic array.
/// Returns 0 (all clean) if the chunk index is out of range.
/// SAFETY: `ctx` must be a valid pointer to a BdevCtx. Must be called on
/// the SPDK reactor thread.
unsafe fn load_chunk_bitmap(ctx: *const BdevCtx, chunk_idx: usize) -> u64 {
    if ctx.is_null() {
        return 0;
    }
    let bdev_ctx = &*ctx;
    if chunk_idx < bdev_ctx.dirty_bitmaps.len() {
        bdev_ctx.dirty_bitmaps[chunk_idx].load(std::sync::atomic::Ordering::Relaxed)
    } else {
        0
    }
}

/// Set the dirty bit for a single sub-block. Used by the direct bdev write path.
/// Kept for future use when reactor fast path is re-added.

fn update_dirty_bitmap(volume_name: &str, chunk_idx: usize, sb_idx: usize) -> Result<()> {
    // Update via the bdev registry's BdevCtx if available (flat array, fast).
    if let Ok(registry) = bdev_registry().lock() {
        let bdev_key = format!("novastor_{}", volume_name);
        if let Some(entry) = registry.get(&bdev_key) {
            let ctx = entry.ctx_ptr as *const BdevCtx;
            if !ctx.is_null() {
                let bdev_ctx = unsafe { &*ctx };
                if chunk_idx < bdev_ctx.dirty_bitmaps.len() {
                    bdev_ctx.dirty_bitmaps[chunk_idx]
                        .fetch_or(1u64 << sb_idx, std::sync::atomic::Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
    }
    // Fallback: update chunk map directly (rare — only if bdev not found).
    let mut maps = volume_chunk_maps().write().unwrap();
    if let Some(chunk_map) = maps.get_mut(volume_name) {
        if chunk_idx >= chunk_map.len() {
            chunk_map.resize(chunk_idx + 1, None);
        }
        let entry = chunk_map[chunk_idx].get_or_insert_with(|| ChunkMapEntry {
            chunk_index: chunk_idx as u64,
            chunk_id: format!("{}:{}", volume_name, chunk_idx),
            ec_params: None,
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        });
        bitmap_set(&mut entry.dirty_bitmap, sb_idx);
    }
    Ok(())
}

/// Sub-block read: reads data directly from the backend bdev, bypassing the
/// ChunkEngine for hot-path I/O.
///
/// Optimisations:
/// - **Batch contiguous**: consecutive sub-blocks within the same chunk are
///   read with a single `bdev_read_async` call and the result sliced.
/// - **Read exact size**: when only a portion of a sub-block is needed, reads
///   only the required bytes (aligned to BDEV_BLOCK_SIZE) instead of the full
///   64KB. A 4K read becomes ~4KB bdev I/O, not 64KB.
/// Direct backend bdev read — bypasses ChunkEngine. Kept for future use
/// when reactor fast path is re-added for local-only chunks.

#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, length))]
pub async fn sub_block_read_local(volume_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    /// SPDK bdev block alignment (bytes). Reads must be aligned to this.
    const BDEV_BLOCK_SIZE: u64 = 512;

    let backend_bdev = get_backend_bdev_name()?;
    let vol_base = get_volume_base_offset(volume_name)?;
    let end_offset = offset + length;
    let mut result = Vec::with_capacity(length as usize);

    // ---------------------------------------------------------------
    // Phase 1: decompose into per-sub-block descriptors.
    // ---------------------------------------------------------------
    struct RdDesc {
        chunk_idx: usize,
        sb_idx: usize,
        offset_in_sb: usize,
        read_len: usize,
    }

    let mut descs: Vec<RdDesc> = Vec::new();
    let mut pos = offset;
    while pos < end_offset {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);
        let offset_in_sb = sub_block::offset_in_sub_block(pos);
        let remaining_in_sb = SUB_BLOCK_SIZE - offset_in_sb;
        let remaining_req = (end_offset - pos) as usize;
        let read_len = remaining_in_sb.min(remaining_req);
        descs.push(RdDesc {
            chunk_idx,
            sb_idx,
            offset_in_sb,
            read_len,
        });
        pos += read_len as u64;
    }

    // ---------------------------------------------------------------
    // Phase 2: issue I/O — batch contiguous sub-blocks when possible,
    //          read exact size for single sub-block accesses.
    // ---------------------------------------------------------------
    let mut i = 0;
    while i < descs.len() {
        let d = &descs[i];

        // Check if we can batch contiguous sub-blocks in the same chunk.
        // Batching is only beneficial when consecutive sub-blocks line up.
        let run_start = i;
        let mut run_end = i + 1;
        while run_end < descs.len() {
            let next = &descs[run_end];
            let prev = &descs[run_end - 1];
            if next.chunk_idx == d.chunk_idx && next.sb_idx == prev.sb_idx + 1 {
                run_end += 1;
            } else {
                break;
            }
        }

        let n_sbs = run_end - run_start;

        if n_sbs > 1 {
            // ---- Batched contiguous read across N sub-blocks ----
            let first = &descs[run_start];
            let last = &descs[run_end - 1];

            // The bdev range spans from (first sb start + first offset_in_sb)
            // to (last sb start + last offset_in_sb + last read_len), but for
            // batching we read full sub-block ranges to keep it simple and
            // aligned — the overhead is small when N > 1.
            let chunk_base = vol_base + first.chunk_idx as u64 * CHUNK_SIZE as u64;
            let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, first.sb_idx);
            let total_bdev_bytes = (n_sbs * SUB_BLOCK_SIZE) as u64;

            let batch_buf =
                reactor_dispatch::bdev_read_async(backend_bdev, bdev_offset, total_bdev_bytes)
                    .instrument(info_span!(
                        "bdev_read_batch",
                        chunk_idx = first.chunk_idx,
                        sb_start = first.sb_idx,
                        sb_end = last.sb_idx,
                        n_sbs
                    ))
                    .await?;

            // Extract each descriptor's requested range from the batch buffer.
            for desc in &descs[run_start..run_end] {
                let buf_sb_start = (desc.sb_idx - first.sb_idx) * SUB_BLOCK_SIZE;
                let src_start = buf_sb_start + desc.offset_in_sb;
                let available = batch_buf.len().saturating_sub(src_start);
                let to_copy = desc.read_len.min(available);
                result.extend_from_slice(&batch_buf[src_start..src_start + to_copy]);
                if to_copy < desc.read_len {
                    result.extend(std::iter::repeat(0u8).take(desc.read_len - to_copy));
                }
            }

            i = run_end;
        } else {
            // ---- Single sub-block: read exact size (aligned) ----
            let chunk_base = vol_base + d.chunk_idx as u64 * CHUNK_SIZE as u64;
            let sb_bdev_start = sub_block::backend_sub_block_offset(chunk_base, d.sb_idx);

            // Align offset down and length up to BDEV_BLOCK_SIZE.
            let aligned_off = (d.offset_in_sb as u64) & !(BDEV_BLOCK_SIZE - 1);
            let aligned_end = ((d.offset_in_sb + d.read_len) as u64 + BDEV_BLOCK_SIZE - 1)
                & !(BDEV_BLOCK_SIZE - 1);
            let aligned_len = aligned_end - aligned_off;

            let buf = reactor_dispatch::bdev_read_async(
                backend_bdev,
                sb_bdev_start + aligned_off,
                aligned_len,
            )
            .instrument(info_span!(
                "bdev_read_exact",
                chunk_idx = d.chunk_idx,
                sb_idx = d.sb_idx,
                aligned_off,
                aligned_len
            ))
            .await?;

            // The requested bytes start at (offset_in_sb - aligned_off) within buf.
            let buf_off = d.offset_in_sb - aligned_off as usize;
            let available = buf.len().saturating_sub(buf_off);
            let to_copy = d.read_len.min(available);
            result.extend_from_slice(&buf[buf_off..buf_off + to_copy]);
            if to_copy < d.read_len {
                result.extend(std::iter::repeat(0u8).take(d.read_len - to_copy));
            }

            i += 1;
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Sequential readahead state (reactor-local, no synchronization needed)
// ---------------------------------------------------------------------------

/// Per-volume sequential readahead state.
/// SAFETY: only accessed on the SPDK reactor thread.
struct ReadaheadState {
    last_end_offset: u64,
    sequential_count: u32,
    window_sub_blocks: u32,
    prefetch_offset: u64,
    prefetch_len: u64,
    prefetch_buf: *mut c_void,
    prefetch_buf_size: usize,
    prefetch_ready: bool,
    generation: u64,
}

impl Default for ReadaheadState {
    fn default() -> Self {
        Self {
            last_end_offset: u64::MAX,
            sequential_count: 0,
            window_sub_blocks: 0,
            prefetch_offset: 0,
            prefetch_len: 0,
            prefetch_buf: std::ptr::null_mut(),
            prefetch_buf_size: 0,
            prefetch_ready: false,
            generation: 0,
        }
    }
}

impl ReadaheadState {
    fn reset(&mut self) {
        if self.prefetch_ready && !self.prefetch_buf.is_null() {
            unsafe {
                reactor_dispatch::release_dma_buf_public(self.prefetch_buf, self.prefetch_buf_size);
            }
            self.prefetch_buf = std::ptr::null_mut();
        } else if !self.prefetch_ready && !self.prefetch_buf.is_null() {
            // In-flight I/O: don't free — let completion callback free via generation check.
            self.prefetch_buf = std::ptr::null_mut();
        }
        self.generation += 1;
        self.sequential_count = 0;
        self.window_sub_blocks = 0;
        self.prefetch_ready = false;
        self.prefetch_len = 0;
    }
}

static mut READAHEAD_STATES: Option<HashMap<String, ReadaheadState>> = None;

/// Get or create readahead state for a volume.
/// SAFETY: must be called on the SPDK reactor thread.
unsafe fn readahead_state(volume_name: &str) -> &mut ReadaheadState {
    let states = READAHEAD_STATES.get_or_insert_with(HashMap::new);
    states.entry(volume_name.to_string()).or_default()
}

// ---------------------------------------------------------------------------
// (Old bdev-layer WriteCache removed — write-back caching is now in
// ChunkEngine via chunk::write_cache::WriteCache)
// ---------------------------------------------------------------------------

/// Context for an in-flight prefetch I/O.
struct PrefetchCtx {
    pub(crate) volume_name: String,
    generation: u64,
    buf: *mut c_void,
    buf_size: usize,
    offset: u64,
    len: u64,
}

/// Prefetch completion callback. Runs on the SPDK reactor thread.
unsafe extern "C" fn prefetch_done_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let pctx = Box::from_raw(ctx as *mut PrefetchCtx);
    ffi::spdk_bdev_free_io(child_io);

    let ra = readahead_state(&pctx.volume_name);
    if ra.generation != pctx.generation || !success {
        // Stale or failed: free the buffer.
        reactor_dispatch::release_dma_buf_public(pctx.buf, pctx.buf_size);
        return;
    }

    ra.prefetch_buf = pctx.buf;
    ra.prefetch_buf_size = pctx.buf_size;
    ra.prefetch_offset = pctx.offset;
    ra.prefetch_len = pctx.len;
    ra.prefetch_ready = true;
}

/// Issue a prefetch read for the next `window_sub_blocks` sub-blocks.
/// Only prefetches within the same chunk — does not cross chunk boundaries.
/// SAFETY: must be called on the SPDK reactor thread.
unsafe fn issue_prefetch(
    ra: &mut ReadaheadState,
    cache: &ReactorBdevCache,
    volume_name: &str,
    chunk_base: u64,
    chunk_idx: usize,
    next_offset: u64,
) {
    let pf_chunk_idx = sub_block::chunk_index(next_offset) as usize;
    if pf_chunk_idx != chunk_idx {
        return; // Don't cross chunk boundary.
    }
    let pf_sb_idx = sub_block::sub_block_index(next_offset);
    let remaining = (sub_block::SUB_BLOCKS_PER_CHUNK - pf_sb_idx) as u32;
    let pf_sbs = ra.window_sub_blocks.min(remaining);
    if pf_sbs == 0 {
        return;
    }
    let pf_len = pf_sbs as u64 * sub_block::SUB_BLOCK_SIZE as u64;
    let pf_aligned = (pf_len + cache.block_size - 1) & !(cache.block_size - 1);
    let pf_buf = reactor_dispatch::acquire_dma_buf_public(pf_aligned as usize);
    if pf_buf.is_null() {
        return;
    }
    let pf_bdev_offset = sub_block::backend_sub_block_offset(chunk_base, pf_sb_idx);
    // The DMA read starts at the sub-block boundary, so prefetch_offset
    // must be the volume-relative offset of that boundary — NOT next_offset,
    // which may be mid-sub-block (e.g. 4KB reads within a 64KB sub-block).
    let pf_volume_offset =
        chunk_idx as u64 * CHUNK_SIZE as u64 + pf_sb_idx as u64 * SUB_BLOCK_SIZE as u64;
    let pf_ctx = Box::new(PrefetchCtx {
        volume_name: volume_name.to_string(),
        generation: ra.generation,
        buf: pf_buf,
        buf_size: pf_aligned as usize,
        offset: pf_volume_offset,
        len: pf_len,
    });
    let rc = ffi::spdk_bdev_read(
        cache.desc,
        cache.channel,
        pf_buf,
        pf_bdev_offset,
        pf_aligned,
        Some(prefetch_done_cb),
        Box::into_raw(pf_ctx) as *mut c_void,
    );
    if rc != 0 {
        reactor_dispatch::release_dma_buf_public(pf_buf, pf_aligned as usize);
    }
}

// ---------------------------------------------------------------------------
// Reactor-native backend bdev cache (for zero-crossing I/O)
// ---------------------------------------------------------------------------
// Cached descriptor + channel for the backend bdev, used directly from
// submit_request on the reactor thread. Eliminates all thread crossings
// for the common aligned I/O fast path.

struct ReactorBdevCache {
    desc: *mut ffi::spdk_bdev_desc,
    channel: *mut ffi::spdk_io_channel,
    block_size: u64,
}

// SAFETY: Only accessed from the single SPDK reactor thread.
unsafe impl Send for ReactorBdevCache {}
unsafe impl Sync for ReactorBdevCache {}

static REACTOR_BDEV_CACHE: OnceLock<Mutex<Option<ReactorBdevCache>>> = OnceLock::new();

/// Open the backend bdev for reactor-native I/O. Must be called on reactor.
unsafe fn reactor_cache_open(bdev_name: &str) -> Option<&'static ReactorBdevCache> {
    let cell = REACTOR_BDEV_CACHE.get_or_init(|| Mutex::new(None));
    let mut guard = match cell.try_lock() {
        Ok(g) => g,
        Err(_) => return None,
    };
    if guard.is_none() {
        let name_c = match std::ffi::CString::new(bdev_name) {
            Ok(c) => c,
            Err(_) => return None,
        };
        let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
        let rc = ffi::spdk_bdev_open_ext(
            name_c.as_ptr() as *const c_char,
            true,
            Some(bdev_event_cb_noop),
            std::ptr::null_mut(),
            &mut desc,
        );
        if rc != 0 {
            error!("reactor_cache_open: spdk_bdev_open_ext failed rc={}", rc);
            return None;
        }
        let bdev_ptr = ffi::spdk_bdev_desc_get_bdev(desc);
        let block_size = if bdev_ptr.is_null() {
            512
        } else {
            ffi::spdk_bdev_get_block_size(bdev_ptr)
        } as u64;
        let channel = ffi::spdk_bdev_get_io_channel(desc);
        if channel.is_null() {
            ffi::spdk_bdev_close(desc);
            error!("reactor_cache_open: get_io_channel null");
            return None;
        }
        info!(
            "reactor_cache_open: opened '{}' (block_size={})",
            bdev_name, block_size
        );
        *guard = Some(ReactorBdevCache {
            desc,
            channel,
            block_size,
        });
    }
    // Return a 'static reference — safe because OnceLock+Mutex ensures
    // the value lives for the program's lifetime and we only read it
    // from the reactor thread after initialization.
    guard.as_ref().map(|r| &*(r as *const ReactorBdevCache))
}

unsafe extern "C" fn bdev_event_cb_noop(
    _type_: ffi::spdk_bdev_event_type,
    _bdev: *mut ffi::spdk_bdev,
    _ctx: *mut c_void,
) {
}

/// Context passed to reactor-native write completion callback.
struct ReactorWriteCtx {
    /// The original NVMe-oF bdev_io to complete.
    parent_io: *mut ffi::spdk_bdev_io,
    /// Pointer to BdevCtx (lives as long as the bdev — safe on reactor).
    bdev_ctx: *const BdevCtx,
    /// Chunk index for bitmap update.
    chunk_idx: usize,
    /// Bitmask of sub-blocks to mark dirty (supports multi-sub-block writes).
    dirty_mask: u64,
    /// DMA buffer pointer for deallocation on completion.
    dma_buf: *mut c_void,
    /// DMA buffer size for deallocation on completion.
    dma_len: usize,
}

/// Context passed to reactor-native read completion callback.
struct ReactorReadCtx {
    /// The original NVMe-oF bdev_io to complete.
    parent_io: *mut ffi::spdk_bdev_io,
    /// DMA buffer (must be freed on completion).
    dma_buf: *mut c_void,
    /// Size of dma_buf allocation.
    dma_len: usize,
    /// Total bytes to copy.
    total_len: usize,
    /// Destination iov descriptors: (base_ptr, len) pairs.
    iovs: Vec<(usize, usize)>,
    /// Offset within dma_buf where requested data starts.
    buf_offset: usize,
    /// Dirty bitmap for the chunk (u64::MAX means all dirty, skip zero-fill).
    bitmap: u64,
    /// First sub-block index in this read.
    sb_start: usize,
    /// Offset within first sub-block.
    off_in_first_sb: usize,
}

/// Reactor-native write completion. Runs on reactor thread.
unsafe extern "C" fn reactor_write_done_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let mut rctx = Box::from_raw(ctx as *mut ReactorWriteCtx);
    ffi::spdk_bdev_free_io(child_io);

    // Free DMA buffer if one was allocated for this write.
    if !rctx.dma_buf.is_null() {
        reactor_dispatch::release_dma_buf_public(rctx.dma_buf, rctx.dma_len);
        rctx.dma_buf = std::ptr::null_mut();
    }

    let bdev_ctx = &*rctx.bdev_ctx;
    if success {
        // Update dirty bitmap — direct array index, atomic fetch_or.
        if rctx.chunk_idx < bdev_ctx.dirty_bitmaps.len() {
            bdev_ctx.dirty_bitmaps[rctx.chunk_idx]
                .fetch_or(rctx.dirty_mask, std::sync::atomic::Ordering::Relaxed);
        }
        ffi::spdk_bdev_io_complete(
            rctx.parent_io,
            ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
        );
    } else {
        error!("reactor_write_done_cb: backend write failed");
        ffi::spdk_bdev_io_complete(
            rctx.parent_io,
            ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
        );
    }

    // Return context to pool (avoids malloc on next I/O).
    rctx.parent_io = std::ptr::null_mut();
    if let Ok(mut pool) = bdev_ctx.write_ctx_pool.lock() {
        pool.push(rctx);
    }
}

/// Try reactor-native local write for writes within the same chunk.
///
/// Returns `true` if the write was submitted on the reactor (caller should
/// return immediately). Returns `false` if the reactor path cannot handle
/// this write (caller should fall through to tokio).
///
/// Requirements for reactor-native path:
/// 1. Write fits within a single chunk (sub-blocks are contiguous on backend)
/// 2. CRUSH selects only local placements (rep1 on this node)
/// 3. Write start offset is block-aligned (512 bytes)
/// 4. Backend bdev is available via reactor cache
unsafe fn try_reactor_local_write(
    engine: &ChunkEngine,
    volume_name: &str,
    offset: u64,
    length: u64,
    data: &[u8],
    bdev_io: *mut ffi::spdk_bdev_io,
    ctx: *const BdevCtx,
) -> bool {
    // Only handle writes within the same chunk (sub-blocks are contiguous on backend).
    if length == 0 {
        return false;
    }
    let chunk_start = offset / CHUNK_SIZE as u64;
    let chunk_end = (offset + length - 1) / CHUNK_SIZE as u64;
    if chunk_start != chunk_end {
        // Write spans multiple chunks — fall to tokio.
        return false;
    }

    // CRUSH: check if all placements are local.
    let chunk_idx = sub_block::chunk_index(offset) as usize;
    let chunk_key = format!("{}:{}", volume_name, chunk_idx);
    let prot = engine.protection();
    let factor = match &prot {
        Protection::Replication { factor } => *factor as usize,
        Protection::ErasureCoding {
            data_shards,
            parity_shards,
        } => (*data_shards + *parity_shards) as usize,
    };
    let topo = engine.topology_snapshot();
    let placements = crush::select(&chunk_key, factor, &topo);
    if placements.is_empty() {
        return false;
    }
    if !placements.iter().all(|(n, _)| n == engine.node_id()) {
        // Remote placement required — fall to tokio for NDP fan-out.
        return false;
    }

    // Compute backend offset.
    let vol_base = match get_volume_base_offset(volume_name) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let sb_idx = sub_block::sub_block_index(offset);
    let offset_in_sb = sub_block::offset_in_sub_block(offset);
    let chunk_base = vol_base + chunk_idx as u64 * CHUNK_SIZE as u64;
    let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx);
    let write_start = bdev_offset + offset_in_sb as u64;

    // write_start must be block-aligned (512).
    if write_start & 511 != 0 {
        return false;
    }

    // Open backend bdev via reactor cache.
    let backend_name = match get_backend_bdev_name() {
        Ok(n) => n,
        Err(_) => return false,
    };
    let cache = match reactor_cache_open(backend_name) {
        Some(c) => c,
        None => return false,
    };

    // Align length up to block size.
    let aligned_len = if cache.block_size == 0 {
        length
    } else {
        (length + cache.block_size - 1) & !(cache.block_size - 1)
    };

    // Allocate DMA buffer and copy data.
    let dma_buf = reactor_dispatch::acquire_dma_buf_public(aligned_len as usize);
    if dma_buf.is_null() {
        return false;
    }
    std::ptr::copy_nonoverlapping(data.as_ptr(), dma_buf as *mut u8, length as usize);
    // Zero padding bytes for alignment.
    if aligned_len > length {
        std::ptr::write_bytes(
            (dma_buf as *mut u8).add(length as usize),
            0,
            (aligned_len - length) as usize,
        );
    }

    // Compute dirty mask: all sub-blocks in the write range.
    let sb_last = sub_block::sub_block_index(offset + length - 1);
    let dirty_mask = {
        let mut mask = 0u64;
        for sb in sb_idx..=sb_last {
            mask |= 1u64 << sb;
        }
        mask
    };

    // Try to get a pre-allocated context from the pool.
    let bdev_ctx_ref = &*ctx;
    let mut rctx = if let Ok(mut pool) = bdev_ctx_ref.write_ctx_pool.lock() {
        pool.pop().unwrap_or_else(|| {
            Box::new(ReactorWriteCtx {
                parent_io: std::ptr::null_mut(),
                bdev_ctx: std::ptr::null(),
                chunk_idx: 0,
                dirty_mask: 0,
                dma_buf: std::ptr::null_mut(),
                dma_len: 0,
            })
        })
    } else {
        Box::new(ReactorWriteCtx {
            parent_io: std::ptr::null_mut(),
            bdev_ctx: std::ptr::null(),
            chunk_idx: 0,
            dirty_mask: 0,
            dma_buf: std::ptr::null_mut(),
            dma_len: 0,
        })
    };
    rctx.parent_io = bdev_io;
    rctx.bdev_ctx = ctx;
    rctx.chunk_idx = chunk_idx;
    rctx.dirty_mask = dirty_mask;
    rctx.dma_buf = dma_buf;
    rctx.dma_len = aligned_len as usize;

    let rctx_ptr = Box::into_raw(rctx) as *mut c_void;

    let rc = ffi::spdk_bdev_write(
        cache.desc,
        cache.channel,
        dma_buf,
        write_start,
        aligned_len,
        Some(reactor_write_done_cb),
        rctx_ptr,
    );
    if rc == 0 {
        return true; // Submitted successfully on reactor.
    }

    // Submit failed — clean up.
    warn!(
        "reactor-native write submit failed rc={}, falling to tokio",
        rc
    );
    let mut rctx = Box::from_raw(rctx_ptr as *mut ReactorWriteCtx);
    reactor_dispatch::release_dma_buf_public(rctx.dma_buf, rctx.dma_len);
    rctx.dma_buf = std::ptr::null_mut();
    rctx.parent_io = std::ptr::null_mut();
    // Return to pool.
    if let Ok(mut pool) = bdev_ctx_ref.write_ctx_pool.lock() {
        pool.push(rctx);
    }
    false
}

/// Direct read completion — data is already in the iov buffer (no DMA copy needed).
unsafe extern "C" fn reactor_read_done_direct_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let rctx = Box::from_raw(ctx as *mut ReactorReadCtx);
    ffi::spdk_bdev_free_io(child_io);

    let status = if success {
        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS
    } else {
        error!("reactor_read_done_direct_cb: backend read failed");
        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
    };
    ffi::spdk_bdev_io_complete(rctx.parent_io, status);
    // No DMA buffer to free — data was read directly into iov.
}

/// Reactor-native read completion with DMA scatter. Runs on reactor thread.
unsafe extern "C" fn reactor_read_done_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let rctx = Box::from_raw(ctx as *mut ReactorReadCtx);
    ffi::spdk_bdev_free_io(child_io);

    if success {
        // Scatter from DMA buffer into iov entries.
        let src_base = (rctx.dma_buf as *const u8).add(rctx.buf_offset);
        let mut src_off = 0usize;
        for &(dst_ptr, dst_len) in &rctx.iovs {
            let to_copy = dst_len.min(rctx.total_len.saturating_sub(src_off));
            if to_copy > 0 {
                std::ptr::copy_nonoverlapping(src_base.add(src_off), dst_ptr as *mut u8, to_copy);
            }
            src_off += to_copy;
            if src_off >= rctx.total_len {
                break;
            }
        }

        // Note: bitmap-based zero-fill is disabled (bitmap always u64::MAX)
        // due to race between tokio writes and reactor reads.

        ffi::spdk_bdev_io_complete(
            rctx.parent_io,
            ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
        );
    } else {
        error!("reactor_read_done_cb: backend read failed");
        ffi::spdk_bdev_io_complete(
            rctx.parent_io,
            ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
        );
    }

    // Free DMA buffer.
    reactor_dispatch::release_dma_buf_public(rctx.dma_buf, rctx.dma_len);
}

/// Per-bdev tracking entry.
pub(crate) struct NovastorBdevEntry {
    pub(crate) volume_name: String,
    /// The SPDK bdev pointer, needed for unregister.
    bdev_ptr: usize,
    /// The BdevCtx pointer, used as the io_device key for unregister.
    pub(crate) ctx_ptr: usize,
}

/// Bdev context stored in `bdev->ctxt`. Points back to the volume name
/// and holds the per-volume atomic dirty bitmap (flat array, no HashMap).
pub(crate) struct BdevCtx {
    pub(crate) volume_name: String,
    /// Flat array of AtomicU64, one per chunk. Direct index by chunk_idx —
    /// no DashMap lookup, no hashing, no pointer chasing.
    pub(crate) dirty_bitmaps: Box<[std::sync::atomic::AtomicU64]>,
    /// Pre-allocated pool of write completion contexts to avoid per-I/O
    /// Box::new allocation. The reactor is single-threaded so no contention.
    write_ctx_pool: Mutex<Vec<Box<ReactorWriteCtx>>>,
}

// Safety: BdevCtx is only accessed from SPDK reactor thread or our I/O pool
// threads. The reactor thread creates it, the pool threads read volume_name,
// and the reactor thread frees it on destruct. All access is via raw pointer
// cast from bdev->ctxt.
unsafe impl Send for BdevCtx {}
unsafe impl Sync for BdevCtx {}

// ---------------------------------------------------------------------------
// Public API — called from gRPC DataplaneService handlers
// ---------------------------------------------------------------------------

/// Create a NovaStor bdev wrapping a ChunkBackend volume.
///
/// The volume must already exist in the ChunkBackend. This registers an SPDK
/// bdev named `novastor_<volume_name>` with the given size. The bdev can then
/// be referenced by `nvmf_create_target`.
pub fn create(volume_name: &str, size_bytes: u64) -> Result<String> {
    // Verify the chunk engine is ready.
    let _engine = get_chunk_engine()?;
    let bdev_name = format!("novastor_{}", volume_name);

    // No upfront allocation — ChunkEngine routes I/O to CRUSH-selected
    // backends which lazy-allocate on first write via get_volume_base_offset.

    // Initialise an empty chunk map for this volume.
    let num_chunks = (size_bytes + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        maps.entry(volume_name.to_string())
            .or_insert_with(|| vec![None; num_chunks as usize]);
    }

    // Bitmap is now pre-allocated inside BdevCtx (created below).

    // Persist the volume definition so chunk maps can be restored on restart.
    if let Some(store) = get_metadata_store() {
        use crate::metadata::types::{Protection, VolumeDefinition, VolumeStatus};
        let vol = VolumeDefinition {
            id: volume_name.to_string(),
            name: volume_name.to_string(),
            size_bytes,
            protection: Protection::Replication { factor: 1 },
            status: VolumeStatus::Available,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            chunk_count: num_chunks,
        };
        if let Err(e) = store.put_volume(&vol) {
            warn!(
                "novastor_bdev: failed to persist volume definition '{}': {}",
                volume_name, e
            );
        }
    }

    let block_size: u32 = 512;
    let num_blocks = size_bytes / block_size as u64;

    info!(
        "novastor_bdev: creating bdev '{}' for volume '{}' ({} blocks)",
        bdev_name, volume_name, num_blocks
    );

    // Allocate the bdev context on the heap. SPDK stores it in bdev->ctxt.
    // Pre-allocate the dirty bitmap (one AtomicU64 per chunk) and I/O
    // context pools to eliminate per-I/O allocation on the hot path.
    let chunk_count = (size_bytes + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;
    let dirty_bitmaps: Box<[std::sync::atomic::AtomicU64]> = (0..chunk_count)
        .map(|_| std::sync::atomic::AtomicU64::new(0))
        .collect::<Vec<_>>()
        .into_boxed_slice();

    const CTX_POOL_SIZE: usize = 64;
    let write_pool: Vec<Box<ReactorWriteCtx>> = (0..CTX_POOL_SIZE)
        .map(|_| {
            Box::new(ReactorWriteCtx {
                parent_io: std::ptr::null_mut(),
                bdev_ctx: std::ptr::null(),
                chunk_idx: 0,
                dirty_mask: 0,
                dma_buf: std::ptr::null_mut(),
                dma_len: 0,
            })
        })
        .collect();
    let ctx = Box::new(BdevCtx {
        volume_name: volume_name.to_string(),
        dirty_bitmaps,
        write_ctx_pool: Mutex::new(write_pool),
    });
    let ctx_ptr = Box::into_raw(ctx);
    // Wrap the raw pointer so it can cross the Send boundary to the reactor thread.
    // Safety: the pointer is valid and exclusively owned until the reactor thread uses it.
    let ctx_addr = ctx_ptr as usize;

    let bdev_name_clone = bdev_name.clone();

    // Register on the reactor thread.
    let (bdev_addr, registered_ctx_addr) =
        reactor_dispatch::dispatch_sync(move || -> Result<(usize, usize)> {
            unsafe {
                let ctx_ptr = ctx_addr as *mut BdevCtx;

                // Register this bdev's context as an io_device BEFORE bdev_register.
                // This follows the SPDK bdev_malloc pattern: each bdev instance is
                // its own io_device, and get_io_channel returns spdk_get_io_channel(ctx).
                let io_dev_name =
                    std::ffi::CString::new(format!("novastor_io_{}", bdev_name_clone)).unwrap();
                ffi::spdk_io_device_register(
                    ctx_ptr as *mut c_void,
                    Some(novastor_channel_create_cb),
                    Some(novastor_channel_destroy_cb),
                    0, // ctx_size — no per-channel state needed
                    io_dev_name.as_ptr(),
                );

                // Allocate and zero the bdev struct.
                let bdev =
                    libc::calloc(1, std::mem::size_of::<ffi::spdk_bdev>()) as *mut ffi::spdk_bdev;
                if bdev.is_null() {
                    ffi::spdk_io_device_unregister(ctx_ptr as *mut c_void, None);
                    let _ = Box::from_raw(ctx_ptr); // cleanup
                    return Err(DataPlaneError::BdevError("calloc spdk_bdev failed".into()));
                }

                // Set bdev fields.
                let name_c = std::ffi::CString::new(bdev_name_clone.as_str()).unwrap();
                (*bdev).name = libc::strdup(name_c.as_ptr());
                (*bdev).product_name =
                    libc::strdup(b"NovaStor ChunkBackend\0".as_ptr() as *const c_char);
                (*bdev).blocklen = block_size;
                (*bdev).blockcnt = num_blocks;
                (*bdev).ctxt = ctx_ptr as *mut c_void;
                (*bdev).module = novastor_bdev_module_ptr();
                (*bdev).fn_table = novastor_fn_table();

                let rc = ffi::spdk_bdev_register(bdev);
                if rc != 0 {
                    ffi::spdk_io_device_unregister(ctx_ptr as *mut c_void, None);
                    libc::free((*bdev).name as *mut c_void);
                    libc::free((*bdev).product_name as *mut c_void);
                    libc::free(bdev as *mut c_void);
                    let _ = Box::from_raw(ctx_ptr);
                    return Err(DataPlaneError::BdevError(format!(
                        "spdk_bdev_register failed: rc={rc}"
                    )));
                }

                Ok((bdev as usize, ctx_ptr as usize))
            }
        })?;

    bdev_registry().lock().unwrap().insert(
        volume_name.to_string(),
        NovastorBdevEntry {
            volume_name: volume_name.to_string(),
            bdev_ptr: bdev_addr,
            ctx_ptr: registered_ctx_addr,
        },
    );

    info!(
        "novastor_bdev: registered bdev '{}' (volume='{}', size={}B)",
        format!("novastor_{}", volume_name),
        volume_name,
        size_bytes,
    );

    // Register volume hash for NDP lookups.
    crate::transport::ndp_server::register_volume_hash(volume_name);

    Ok(format!("novastor_{}", volume_name))
}

/// Remove a NovaStor bdev and unregister it from SPDK.
pub fn destroy(volume_name: &str) -> Result<()> {
    let entry = bdev_registry()
        .lock()
        .unwrap()
        .remove(volume_name)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!(
                "novastor bdev for volume '{}' not found",
                volume_name
            ))
        })?;

    let bdev_addr = entry.bdev_ptr;
    let ctx_addr = entry.ctx_ptr;

    info!(
        "novastor_bdev: destroying bdev for volume '{}'",
        volume_name
    );

    use crate::spdk::context::Completion;
    let completion = Arc::new(Completion::<i32>::new());
    let comp = completion.clone();

    reactor_dispatch::send_to_reactor(move || unsafe {
        let bdev = bdev_addr as *mut ffi::spdk_bdev;

        unsafe extern "C" fn unregister_cb(ctx: *mut c_void, rc: i32) {
            let comp = Completion::<i32>::from_ptr(ctx);
            comp.complete(rc);
        }

        ffi::spdk_bdev_unregister(bdev, Some(unregister_cb), comp.as_ptr());
    });

    let rc = completion.wait();
    if rc != 0 {
        return Err(DataPlaneError::BdevError(format!(
            "spdk_bdev_unregister failed: rc={rc}"
        )));
    }

    // Unregister the per-bdev io_device after the bdev itself is gone.
    reactor_dispatch::send_to_reactor(move || unsafe {
        ffi::spdk_io_device_unregister(ctx_addr as *mut c_void, None);
    });

    // Clean up per-sub-block locks to prevent unbounded memory growth.
    cleanup_volume_locks(volume_name);

    // Clean up the volume's chunk map cache.
    if let Some(maps) = VOLUME_CHUNK_MAPS.get() {
        let mut map = maps.write().unwrap();
        if map.remove(volume_name).is_some() {
            info!(
                "novastor_bdev: cleaned up chunk map for volume '{}'",
                volume_name
            );
        }
    }

    info!("novastor_bdev: destroyed bdev for volume '{}'", volume_name);
    Ok(())
}

// ---------------------------------------------------------------------------
// SPDK bdev module and fn_table
// ---------------------------------------------------------------------------

/// The static bdev module descriptor. SPDK requires a non-null module pointer.
static mut NOVASTOR_MODULE: ffi::spdk_bdev_module = unsafe { std::mem::zeroed() };
static NOVASTOR_MODULE_INIT: std::sync::Once = std::sync::Once::new();

/// Get a pointer to the NovaStor bdev module, initialising it on first call.
fn novastor_bdev_module_ptr() -> *mut ffi::spdk_bdev_module {
    NOVASTOR_MODULE_INIT.call_once(|| unsafe {
        NOVASTOR_MODULE.name = b"novastor_chunk\0".as_ptr() as *const c_char;
        NOVASTOR_MODULE.module_init = Some(module_init_cb);
        NOVASTOR_MODULE.module_fini = Some(module_fini_cb);
    });
    unsafe { &mut NOVASTOR_MODULE as *mut ffi::spdk_bdev_module }
}

/// The bdev function table. SPDK calls these for I/O and lifecycle.
/// Initialised at runtime because the struct may have fields that aren't
/// const-initialisable. All unset fields are zero (None/null).
static NOVASTOR_FN_TABLE: OnceLock<ffi::spdk_bdev_fn_table> = OnceLock::new();

fn novastor_fn_table() -> &'static ffi::spdk_bdev_fn_table {
    NOVASTOR_FN_TABLE.get_or_init(|| {
        let mut ft: ffi::spdk_bdev_fn_table = unsafe { std::mem::zeroed() };
        ft.destruct = Some(bdev_destruct_cb);
        ft.submit_request = Some(bdev_submit_request_cb);
        ft.io_type_supported = Some(bdev_io_type_supported_cb);
        ft.get_io_channel = Some(bdev_get_io_channel_cb);
        ft
    })
}

// ---------------------------------------------------------------------------
// SPDK bdev callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn module_init_cb() -> i32 {
    // Per-bdev io_device registration happens in create().
    // Module init is a no-op — it exists because SPDK requires module_init
    // to be non-null, but our module doesn't need global io_device state.
    info!("novastor_bdev: module initialised");
    0
}

unsafe extern "C" fn module_fini_cb() {
    info!("novastor_bdev: module shutdown");
}

/// No-op channel create callback. NovaStor bdevs offload I/O to a thread pool
/// rather than using SPDK per-channel state.
unsafe extern "C" fn novastor_channel_create_cb(_io_device: *mut c_void, _ctx: *mut c_void) -> i32 {
    0
}

/// No-op channel destroy callback.
unsafe extern "C" fn novastor_channel_destroy_cb(_io_device: *mut c_void, _ctx: *mut c_void) {}

unsafe extern "C" fn bdev_destruct_cb(ctx: *mut c_void) -> i32 {
    // Free the BdevCtx.
    if !ctx.is_null() {
        let _ = Box::from_raw(ctx as *mut BdevCtx);
    }
    0
}

unsafe extern "C" fn bdev_io_type_supported_cb(
    _ctx: *mut c_void,
    io_type: ffi::spdk_bdev_io_type,
) -> bool {
    matches!(
        io_type,
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_READ
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_FLUSH
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE_ZEROES
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_UNMAP
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_RESET
            | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_COPY
    )
}

unsafe extern "C" fn bdev_get_io_channel_cb(ctx: *mut c_void) -> *mut ffi::spdk_io_channel {
    // Follow the SPDK bdev_malloc pattern: each bdev's BdevCtx is registered
    // as its own io_device in create(). Return the io_channel for this
    // specific bdev instance.
    ffi::spdk_get_io_channel(ctx)
}

/// The main I/O submission callback. Called on the SPDK reactor thread.
///
/// All reads and writes are dispatched to tokio and routed through the
/// ChunkEngine, which uses CRUSH to select the correct backend (local
/// bdev or remote NDP node). A reactor fast path for local-only chunks
/// can be re-added as a future optimization.
unsafe extern "C" fn bdev_submit_request_cb(
    _channel: *mut ffi::spdk_io_channel,
    bdev_io: *mut ffi::spdk_bdev_io,
) {
    let bdev = (*bdev_io).bdev;
    let ctx = (*bdev).ctxt as *const BdevCtx;
    if ctx.is_null() {
        ffi::spdk_bdev_io_complete(bdev_io, ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
    let volume_name = (*ctx).volume_name.clone();
    let io_type = (*bdev_io).type_ as u32;

    match io_type {
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_READ => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;

            let iovs = bdev_params.iovs;
            let iovcnt = bdev_params.iovcnt as usize;
            if iovs.is_null() || iovcnt == 0 {
                ffi::spdk_bdev_io_complete(
                    bdev_io,
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                );
                return;
            }

            // ---- Reads: try cache lookup on reactor first, fall to tokio on miss ----
            // Fast path: if all sub-blocks are cached, serve from cache
            // directly on the reactor — zero thread crossing.
            if let Ok(engine) = get_chunk_engine() {
                if let Some(cached) = engine.write_cache.lookup(&volume_name, offset, length) {
                    // Cache hit — copy to iovs, complete on reactor.
                    let mut src_off = 0usize;
                    for i in 0..iovcnt {
                        let iov = &*iovs.add(i);
                        let to_copy =
                            std::cmp::min(iov.iov_len, cached.len().saturating_sub(src_off));
                        if to_copy > 0 {
                            std::ptr::copy_nonoverlapping(
                                cached[src_off..].as_ptr(),
                                iov.iov_base as *mut u8,
                                to_copy,
                            );
                        }
                        src_off += to_copy;
                        if src_off >= cached.len() {
                            break;
                        }
                    }
                    ffi::spdk_bdev_io_complete(
                        bdev_io,
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    );
                    return;
                }
            }

            // Cache miss — try reactor-native local read (same-chunk I/O).
            let mut reactor_handled = false;

            // Reactor-native path: same chunk, CRUSH selects local node.
            // Sub-blocks are contiguous on the backend within a chunk,
            // so a multi-sub-block read is a single spdk_bdev_read.
            if length > 0 && offset / CHUNK_SIZE as u64 == (offset + length - 1) / CHUNK_SIZE as u64
            {
                // Fits within a single chunk — try CRUSH on reactor.
                if let Ok(engine) = get_chunk_engine() {
                    let topo = engine.topology_snapshot();
                    let prot = engine.protection();
                    let chunk_idx = sub_block::chunk_index(offset) as usize;
                    let chunk_key = format!("{}:{}", volume_name, chunk_idx);
                    let factor = match &prot {
                        Protection::Replication { factor } => *factor as usize,
                        Protection::ErasureCoding {
                            data_shards,
                            parity_shards,
                        } => (*data_shards + *parity_shards) as usize,
                    };
                    let placements = crush::select(&chunk_key, factor, &topo);
                    // Check if ANY placement is the local node (read from any replica).
                    let is_local = placements.iter().any(|(n, _)| n == engine.node_id());
                    if is_local {
                        if true {
                            // Local node — attempt reactor-native bdev read.
                            if let Ok(backend_name) = get_backend_bdev_name() {
                                if let Some(cache) = reactor_cache_open(backend_name) {
                                    if let Ok(vol_base) = get_volume_base_offset(&volume_name) {
                                        let sb_idx = sub_block::sub_block_index(offset);
                                        let offset_in_sb = sub_block::offset_in_sub_block(offset);
                                        let bdev_offset = sub_block::backend_sub_block_offset(
                                            vol_base + chunk_idx as u64 * CHUNK_SIZE as u64,
                                            sb_idx,
                                        );
                                        let raw_len = (offset_in_sb + length as usize) as u64;
                                        let aligned_len = if cache.block_size == 0 {
                                            raw_len
                                        } else {
                                            (raw_len + cache.block_size - 1)
                                                & !(cache.block_size - 1)
                                        };
                                        let dma_buf = reactor_dispatch::acquire_dma_buf_public(
                                            aligned_len as usize,
                                        );
                                        if !dma_buf.is_null() {
                                            let mut iov_descs: Vec<(usize, usize)> =
                                                Vec::with_capacity(iovcnt);
                                            for i in 0..iovcnt {
                                                let iov = &*iovs.add(i);
                                                iov_descs
                                                    .push((iov.iov_base as usize, iov.iov_len));
                                            }
                                            let rctx = Box::new(ReactorReadCtx {
                                                parent_io: bdev_io,
                                                dma_buf,
                                                dma_len: aligned_len as usize,
                                                total_len: length as usize,
                                                iovs: iov_descs,
                                                buf_offset: offset_in_sb,
                                                bitmap: u64::MAX,
                                                sb_start: sb_idx,
                                                off_in_first_sb: offset_in_sb,
                                            });
                                            let rctx_ptr = Box::into_raw(rctx) as *mut c_void;
                                            let rc = ffi::spdk_bdev_read(
                                                cache.desc,
                                                cache.channel,
                                                dma_buf,
                                                bdev_offset,
                                                aligned_len,
                                                Some(reactor_read_done_cb),
                                                rctx_ptr,
                                            );
                                            if rc == 0 {
                                                reactor_handled = true;
                                            } else {
                                                // Submit failed — recover context and free resources.
                                                let _ =
                                                    Box::from_raw(rctx_ptr as *mut ReactorReadCtx);
                                                reactor_dispatch::release_dma_buf_public(
                                                    dma_buf,
                                                    aligned_len as usize,
                                                );
                                                warn!(
                                                    "reactor-native read submit failed rc={}, falling to tokio",
                                                    rc
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if reactor_handled {
                return;
            }

            // Reactor NDP fallback — try remote read via SPDK sockets (zero crossing).
            #[cfg(feature = "spdk-sys")]
            if !reactor_handled
                && length > 0
                && offset / CHUNK_SIZE as u64 == (offset + length - 1) / CHUNK_SIZE as u64
            {
                if let Ok(engine) = get_chunk_engine() {
                    let chunk_idx = sub_block::chunk_index(offset) as usize;
                    let chunk_key = format!("{}:{}", volume_name, chunk_idx);
                    let prot = engine.protection();
                    let factor = match &prot {
                        Protection::Replication { factor } => *factor as usize,
                        Protection::ErasureCoding {
                            data_shards,
                            parity_shards,
                        } => (*data_shards + *parity_shards) as usize,
                    };
                    let topo = engine.topology_snapshot();
                    let placements = crush::select(&chunk_key, factor, &topo);

                    // Try each placement — first connected reactor NDP peer wins.
                    for (node_id, _) in &placements {
                        if node_id == engine.node_id() {
                            continue; // Skip self — local path already failed above.
                        }
                        // Find the node's address from topology.
                        if let Some(node) = topo.nodes().iter().find(|n| &n.id == node_id) {
                            let peer_addr = format!("{}:4500", node.address);
                            if crate::chunk::reactor_ndp::is_connected(&peer_addr) {
                                let volume_hash = ndp::header::volume_hash(&volume_name);
                                let mut iov_descs: Vec<(usize, usize)> = Vec::with_capacity(iovcnt);
                                for i in 0..iovcnt {
                                    let iov = &*iovs.add(i);
                                    iov_descs.push((iov.iov_base as usize, iov.iov_len));
                                }
                                if crate::chunk::reactor_ndp::send_read(
                                    &peer_addr,
                                    volume_hash,
                                    offset,
                                    length as u32,
                                    bdev_io as *mut std::os::raw::c_void,
                                    iov_descs,
                                    0, // buf_offset
                                ) {
                                    return; // Reactor NDP handles completion.
                                }
                            }
                        }
                    }
                }
            }

            // Tokio fallback — handles anything reactor paths couldn't.
            let bdev_io_addr = bdev_io as usize;
            let mut iov_descs: Vec<(usize, usize)> = Vec::with_capacity(iovcnt);
            for i in 0..iovcnt {
                let iov = &*iovs.add(i);
                iov_descs.push((iov.iov_base as usize, iov.iov_len));
            }
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let engine = get_chunk_engine().expect("chunk engine");
                let result = engine.sub_block_read(&volume_name, offset, length).await;
                match result {
                    Ok(data) => {
                        reactor_dispatch::send_to_reactor(move || unsafe {
                            let mut src_off = 0usize;
                            for &(base, len) in &iov_descs {
                                let to_copy =
                                    std::cmp::min(len, data.len().saturating_sub(src_off));
                                if to_copy > 0 {
                                    std::ptr::copy_nonoverlapping(
                                        data[src_off..].as_ptr(),
                                        base as *mut u8,
                                        to_copy,
                                    );
                                }
                                src_off += to_copy;
                                if src_off >= data.len() {
                                    break;
                                }
                            }
                            ffi::spdk_bdev_io_complete(
                                bdev_io_addr as *mut ffi::spdk_bdev_io,
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                            );
                        });
                    }
                    Err(e) => {
                        error!("novastor_bdev: read failed: {}", e);
                        reactor_dispatch::send_to_reactor(move || unsafe {
                            ffi::spdk_bdev_io_complete(
                                bdev_io_addr as *mut ffi::spdk_bdev_io,
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                            );
                        });
                    }
                };
            });
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;

            let iovs = bdev_params.iovs;
            let iovcnt = bdev_params.iovcnt as usize;
            if iovs.is_null() || iovcnt == 0 {
                ffi::spdk_bdev_io_complete(
                    bdev_io,
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                );
                return;
            }

            // ---- Writes: cache absorb on reactor, flush via tokio ----
            let mut data = vec![0u8; length as usize];
            let mut copied = 0usize;
            for i in 0..iovcnt {
                let iov = &*iovs.add(i);
                let to_copy = std::cmp::min(iov.iov_len, data.len() - copied);
                std::ptr::copy_nonoverlapping(
                    iov.iov_base as *const u8,
                    data[copied..].as_mut_ptr(),
                    to_copy,
                );
                copied += to_copy;
                if copied >= data.len() {
                    break;
                }
            }

            // Try sync cache absorb directly on reactor — zero thread crossing.
            let engine = match get_chunk_engine() {
                Ok(e) => e,
                Err(_) => {
                    ffi::spdk_bdev_io_complete(
                        bdev_io,
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                    );
                    return;
                }
            };

            match engine.write_cache.absorb(&volume_name, offset, &data) {
                AbsorbResult::Cached => {
                    // Cache absorbed — complete immediately on reactor!
                    // No async overflow flush here. Data stays in cache until
                    // host FLUSH command. This prevents the race where async
                    // drain removes entries before persistence, causing reads
                    // to miss both cache and backend.
                    ffi::spdk_bdev_io_complete(
                        bdev_io,
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    );
                }
                AbsorbResult::NotAligned => {
                    // CRITICAL: invalidate stale cached sub-blocks that overlap
                    // with this partial write to prevent stale reads. (#176)
                    engine
                        .write_cache
                        .invalidate_range(&volume_name, offset, length);

                    // Queue the unaligned write for batched flush instead of
                    // writing immediately. This dramatically speeds up mkfs
                    // which does thousands of small writes — they get batched
                    // and flushed together on the next FLUSH command.
                    engine.queue_pending_write(volume_name.clone(), offset, data);
                    ffi::spdk_bdev_io_complete(
                        bdev_io,
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    );
                }
                AbsorbResult::Full => {
                    // Cache full — try reactor-native local write first.
                    if try_reactor_local_write(
                        &engine,
                        &volume_name,
                        offset,
                        length,
                        &data,
                        bdev_io,
                        ctx,
                    ) {
                        return;
                    }

                    // Fall back to tokio for direct write.
                    let bdev_io_addr = bdev_io as usize;
                    let handle = get_tokio_handle().expect("tokio handle");
                    handle.spawn(async move {
                        let engine = get_chunk_engine().expect("chunk engine");
                        let result = engine.flush_single_write(&volume_name, offset, &data).await;
                        let status = match result {
                            Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                            Err(e) => {
                                error!("novastor_bdev: write (cache full) failed: {}", e);
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                            }
                        };
                        reactor_dispatch::send_to_reactor(move || unsafe {
                            ffi::spdk_bdev_io_complete(
                                bdev_io_addr as *mut ffi::spdk_bdev_io,
                                status,
                            );
                        });
                    });
                }
            }
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_FLUSH => {
            // Flush write-back cache + pending unaligned writes through CRUSH fan-out.
            let bdev_io_addr = bdev_io as usize;
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let status = match get_chunk_engine() {
                    Ok(engine) => {
                        // First, flush pending unaligned writes.
                        let pending = engine.drain_all_pending_writes();
                        let mut flush_ok = true;
                        if !pending.is_empty() {
                            // Fan-out all pending writes concurrently.
                            let mut tasks = tokio::task::JoinSet::new();
                            for (vol, offset, data) in pending {
                                let eng = engine.clone();
                                tasks.spawn(async move {
                                    eng.flush_single_write(&vol, offset, &data).await
                                });
                            }
                            while let Some(result) = tasks.join_next().await {
                                if let Ok(Err(e)) = result {
                                    error!("novastor_bdev: pending write flush failed: {}", e);
                                    flush_ok = false;
                                }
                            }
                        }
                        // Then flush the sub-block cache.
                        match engine.flush(&volume_name).await {
                            Ok(()) if flush_ok => {
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS
                            }
                            Ok(()) => {
                                // Cache flush ok but some pending writes failed.
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                            }
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
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE_ZEROES => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;
            let bdev_io_addr = bdev_io as usize;

            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let status = match sub_block_write_zeroes(&volume_name, offset, length).await {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_bdev: write_zeroes failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                };
                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
            });
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_UNMAP => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;
            let bdev_io_addr = bdev_io as usize;

            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let status = match sub_block_write_zeroes(&volume_name, offset, length).await {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_bdev: unmap failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                };
                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
            });
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_RESET => {
            // Reset: gracefully accept — no state to reset.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_COPY => {
            // Copy: read source range, write to destination.
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let dst_offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let num_bytes = bdev_params.num_blocks * (*bdev).blocklen as u64;
            // Source offset is in the copy union member. It shares the
            // same position as iovs in the union, so we access it via
            // the raw struct layout: copy.src_offset_blocks is at the
            // start of the union that also holds iovs/iovcnt.
            let src_offset_blocks_ptr = &bdev_params.iovs as *const _ as *const u64;
            let src_offset = (*src_offset_blocks_ptr) * (*bdev).blocklen as u64;
            let bdev_io_addr = bdev_io as usize;

            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let result = async {
                    let engine = get_chunk_engine()?;
                    let data = engine
                        .sub_block_read(&volume_name, src_offset, num_bytes)
                        .await?;
                    engine
                        .sub_block_write(&volume_name, dst_offset, &data)
                        .await?;
                    Ok::<(), DataPlaneError>(())
                }
                .await;
                let status = match result {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_bdev: copy failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                };
                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
            });
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_SEEK_DATA => {
            // SEEK_DATA: find the next offset (in blocks) that has data.
            // Check the dirty bitmap to find the first sub-block with data
            // at or after the requested offset.
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;

            let result_offset = find_next_data(&volume_name, offset);

            // Set the seek result in blocks. The seek.offset field is the
            // first u64 in the bdev params union (same position as
            // offset_blocks).
            let block_size = (*bdev).blocklen as u64;
            let result_blocks = if result_offset == u64::MAX {
                u64::MAX
            } else {
                result_offset / block_size
            };
            let seek_ptr = &mut (*bdev_io).u.bdev as *mut _ as *mut u64;
            *seek_ptr = result_blocks;

            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_SEEK_HOLE => {
            // SEEK_HOLE: find the next offset (in blocks) that is a hole
            // (unwritten). Check dirty bitmap for the first unset bit.
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;

            let result_offset = find_next_hole(&volume_name, offset);

            let block_size = (*bdev).blocklen as u64;
            let result_blocks = if result_offset == u64::MAX {
                u64::MAX
            } else {
                result_offset / block_size
            };
            let seek_ptr = &mut (*bdev_io).u.bdev as *mut _ as *mut u64;
            *seek_ptr = result_blocks;

            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
        }
        _ => {
            // Unsupported I/O type.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
            );
        }
    }
}

/// Public wrapper for sub_block_write — used by NDP server.
/// All writes go through ChunkEngine for CRUSH-based placement.
pub async fn sub_block_write_pub(volume_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let engine = get_chunk_engine()?;
    engine.sub_block_write(volume_name, offset, data).await
}

/// Public wrapper for sub_block_read — used by NDP server.
/// All reads go through ChunkEngine for CRUSH-based placement.
pub async fn sub_block_read_pub(volume_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let engine = get_chunk_engine()?;
    engine.sub_block_read(volume_name, offset, length).await
}
