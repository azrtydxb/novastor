//! NovaStor custom SPDK bdev module.
//!
//! Each volume is registered as an SPDK bdev named `novastor_<volume_id>`.
//! When SPDK's NVMe-oF subsystem submits I/O to this bdev, the module
//! performs direct reads and writes against the backend SPDK bdev, bypassing
//! the ChunkEngine on the hot I/O path.
//!
//! Volume data is laid out contiguously on the backend bdev. Partial writes
//! go directly to the exact byte offset on the backend — no read-modify-write
//! — so a 4K random write produces a single 4K backend write. Dirty sub-blocks
//! are tracked via a per-chunk bitmap; SHA-256 hashing and content-addressing
//! happen only during periodic background sync (not on the hot path).

use crate::bdev::sub_block::{self, bitmap_is_set, bitmap_set, CHUNK_SIZE, SUB_BLOCK_SIZE};
use crate::chunk::engine::ChunkEngine;
use crate::error::{DataPlaneError, Result};
use crate::metadata::store::MetadataStore;
use crate::metadata::types::ChunkMapEntry;
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

fn bdev_registry() -> &'static Mutex<HashMap<String, NovastorBdevEntry>> {
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

/// Get the base offset for an existing volume.
fn get_volume_base_offset(volume_name: &str) -> Result<u64> {
    let offsets = volume_offsets().read().unwrap();
    offsets.get(volume_name).copied().ok_or_else(|| {
        DataPlaneError::BdevError(format!("no base offset for volume '{}'", volume_name))
    })
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
#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, len = data.len()))]
async fn sub_block_write(volume_name: &str, offset: u64, data: &[u8]) -> Result<()> {
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

/// Helper: set the dirty bit for a single sub-block and ensure the chunk map
/// entry exists. Called after every successful write.
fn update_dirty_bitmap(volume_name: &str, chunk_idx: usize, sb_idx: usize) -> Result<()> {
    let chunk_id = format!("{}:{}", volume_name, chunk_idx);
    let _bm_span = info_span!("bitmap_update", chunk_idx, sb_idx).entered();
    let mut maps = volume_chunk_maps().write().unwrap();
    let chunk_map = maps.get_mut(volume_name).ok_or_else(|| {
        DataPlaneError::BdevError(format!("no chunk map for volume '{}'", volume_name))
    })?;

    // Grow the chunk map if needed.
    if chunk_idx >= chunk_map.len() {
        chunk_map.resize(chunk_idx + 1, None);
    }

    let entry = chunk_map[chunk_idx].get_or_insert_with(|| ChunkMapEntry {
        chunk_index: chunk_idx as u64,
        chunk_id: chunk_id.clone(),
        ec_params: None,
        dirty_bitmap: 0,
    });
    bitmap_set(&mut entry.dirty_bitmap, sb_idx);
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
#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, length))]
async fn sub_block_read(volume_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
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
    let mut guard = cell.lock().unwrap();
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
    /// Volume name for bitmap update.
    volume_name: String,
    /// Chunk index for bitmap update.
    chunk_idx: usize,
    /// Sub-block index for bitmap update.
    sb_idx: usize,
}

/// Context passed to reactor-native read completion callback.
struct ReactorReadCtx {
    /// The original NVMe-oF bdev_io to complete.
    parent_io: *mut ffi::spdk_bdev_io,
    /// DMA buffer (must be freed on completion).
    dma_buf: *mut c_void,
    /// Size of dma_buf allocation.
    dma_len: usize,
    /// Destination iov base pointer.
    dst_base: *mut u8,
    /// Bytes to copy to dst.
    dst_len: usize,
    /// Offset within dma_buf where requested data starts.
    buf_offset: usize,
}

/// Reactor-native write completion. Runs on reactor thread.
unsafe extern "C" fn reactor_write_done_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let rctx = Box::from_raw(ctx as *mut ReactorWriteCtx);
    ffi::spdk_bdev_free_io(child_io);

    if success {
        // Update dirty bitmap (fast — just a bit flip in a HashMap).
        let _ = update_dirty_bitmap(&rctx.volume_name, rctx.chunk_idx, rctx.sb_idx);
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
}

/// Reactor-native read completion. Runs on reactor thread.
unsafe extern "C" fn reactor_read_done_cb(
    child_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut c_void,
) {
    let rctx = Box::from_raw(ctx as *mut ReactorReadCtx);
    ffi::spdk_bdev_free_io(child_io);

    if success {
        // Copy from DMA buffer to the NVMe-oF iov buffer.
        let src = (rctx.dma_buf as *const u8).add(rctx.buf_offset);
        let to_copy = rctx.dst_len;
        std::ptr::copy_nonoverlapping(src, rctx.dst_base, to_copy);
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
struct NovastorBdevEntry {
    volume_name: String,
    /// The SPDK bdev pointer, needed for unregister.
    bdev_ptr: usize,
    /// The BdevCtx pointer, used as the io_device key for unregister.
    ctx_ptr: usize,
}

/// Bdev context stored in `bdev->ctxt`. Points back to the volume name.
struct BdevCtx {
    volume_name: String,
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

    // Allocate a contiguous region on the backend bdev for this volume.
    let _base_offset = allocate_volume_offset(volume_name, size_bytes);

    // Initialise an empty chunk map for this volume.
    let num_chunks = (size_bytes + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64;
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        maps.entry(volume_name.to_string())
            .or_insert_with(|| vec![None; num_chunks as usize]);
    }

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
    let ctx = Box::new(BdevCtx {
        volume_name: volume_name.to_string(),
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
/// For aligned I/O that fits within a single sub-block, we handle it
/// entirely on the reactor thread (zero thread crossings) — like Mayastor.
/// For complex I/O (multi-sub-block, unaligned), we fall back to tokio.
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

            // Check if we can use the reactor-native fast path:
            // single sub-block, 512-byte aligned offset and length.
            let chunk_idx = sub_block::chunk_index(offset) as usize;
            let sb_idx = sub_block::sub_block_index(offset);
            let off_in_sb = sub_block::offset_in_sub_block(offset);
            let end_sb_idx = sub_block::sub_block_index(offset + length - 1);
            let end_chunk_idx = sub_block::chunk_index(offset + length - 1) as usize;
            let aligned = (off_in_sb as u64) % 512 == 0 && length % 512 == 0;

            if aligned && chunk_idx == end_chunk_idx && sb_idx == end_sb_idx && iovcnt == 1 {
                // ---- REACTOR FAST PATH: single sub-block read ----
                if let (Ok(backend), Ok(vol_base)) = (
                    get_backend_bdev_name(),
                    get_volume_base_offset(&volume_name),
                ) {
                    if let Some(cache) = reactor_cache_open(backend) {
                        let chunk_base = vol_base + chunk_idx as u64 * CHUNK_SIZE as u64;
                        let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx)
                            + off_in_sb as u64;

                        // Align length up to block_size.
                        let aligned_len = (length + cache.block_size - 1) & !(cache.block_size - 1);

                        let dma_buf =
                            reactor_dispatch::acquire_dma_buf_public(aligned_len as usize);
                        if dma_buf.is_null() {
                            ffi::spdk_bdev_io_complete(
                                bdev_io,
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                            );
                            return;
                        }

                        let iov = &*iovs;
                        let rctx = Box::new(ReactorReadCtx {
                            parent_io: bdev_io,
                            dma_buf,
                            dma_len: aligned_len as usize,
                            dst_base: iov.iov_base as *mut u8,
                            dst_len: length as usize,
                            buf_offset: 0,
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
                        if rc != 0 {
                            let _ = Box::from_raw(rctx_ptr as *mut ReactorReadCtx);
                            reactor_dispatch::release_dma_buf_public(dma_buf, aligned_len as usize);
                            ffi::spdk_bdev_io_complete(
                                bdev_io,
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                            );
                        }
                        return; // Fast path handled.
                    }
                }
            }

            // ---- TOKIO FALLBACK: multi-sub-block or complex reads ----
            let bdev_io_addr = bdev_io as usize;
            let mut iov_descs: Vec<(usize, usize)> = Vec::with_capacity(iovcnt);
            for i in 0..iovcnt {
                let iov = &*iovs.add(i);
                iov_descs.push((iov.iov_base as usize, iov.iov_len));
            }
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let result = sub_block_read(&volume_name, offset, length).await;
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

            // Check if we can use the reactor-native fast path:
            // single sub-block, 512-byte aligned, single iov.
            let chunk_idx = sub_block::chunk_index(offset) as usize;
            let sb_idx = sub_block::sub_block_index(offset);
            let off_in_sb = sub_block::offset_in_sub_block(offset);
            let end_sb_idx = sub_block::sub_block_index(offset + length - 1);
            let end_chunk_idx = sub_block::chunk_index(offset + length - 1) as usize;
            let aligned = (off_in_sb as u64) % 512 == 0 && length % 512 == 0;

            if aligned && chunk_idx == end_chunk_idx && sb_idx == end_sb_idx && iovcnt == 1 {
                // ---- REACTOR FAST PATH: single sub-block write ----
                if let (Ok(backend), Ok(vol_base)) = (
                    get_backend_bdev_name(),
                    get_volume_base_offset(&volume_name),
                ) {
                    if let Some(cache) = reactor_cache_open(backend) {
                        let chunk_base = vol_base + chunk_idx as u64 * CHUNK_SIZE as u64;
                        let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx)
                            + off_in_sb as u64;

                        // Write the iov data directly to backend.
                        // NVMe-oF target provides DMA-safe buffers in iovs.
                        let iov = &*iovs;
                        let aligned_len = (length + cache.block_size - 1) & !(cache.block_size - 1);

                        // We can use the iov buffer directly if it's
                        // exactly aligned. Otherwise copy to DMA buf.
                        let (write_buf, needs_free) = if length == aligned_len {
                            (iov.iov_base, false)
                        } else {
                            let dma_buf =
                                reactor_dispatch::acquire_dma_buf_public(aligned_len as usize);
                            if dma_buf.is_null() {
                                ffi::spdk_bdev_io_complete(
                                    bdev_io,
                                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                                );
                                return;
                            }
                            std::ptr::write_bytes(dma_buf as *mut u8, 0, aligned_len as usize);
                            std::ptr::copy_nonoverlapping(
                                iov.iov_base as *const u8,
                                dma_buf as *mut u8,
                                length as usize,
                            );
                            (dma_buf, true)
                        };

                        let rctx = Box::new(ReactorWriteCtx {
                            parent_io: bdev_io,
                            volume_name: volume_name.clone(),
                            chunk_idx,
                            sb_idx,
                        });
                        let rctx_ptr = Box::into_raw(rctx) as *mut c_void;

                        let rc = ffi::spdk_bdev_write(
                            cache.desc,
                            cache.channel,
                            write_buf,
                            bdev_offset,
                            aligned_len,
                            Some(reactor_write_done_cb),
                            rctx_ptr,
                        );
                        if rc != 0 {
                            let _ = Box::from_raw(rctx_ptr as *mut ReactorWriteCtx);
                            if needs_free {
                                reactor_dispatch::release_dma_buf_public(
                                    write_buf,
                                    aligned_len as usize,
                                );
                            }
                            ffi::spdk_bdev_io_complete(
                                bdev_io,
                                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                            );
                        }
                        return; // Fast path handled.
                    }
                }
            }

            // ---- TOKIO FALLBACK: multi-sub-block or complex writes ----
            let bdev_io_addr = bdev_io as usize;
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
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let result = sub_block_write(&volume_name, offset, &data).await;
                let status = match result {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_bdev: write failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                };
                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
            });
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_FLUSH => {
            // Flush is a no-op — chunk writes are durable once acknowledged.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
        }
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE_ZEROES
        | ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_UNMAP => {
            // Thin provisioning: unwritten blocks already return zeros on
            // read, so WRITE_ZEROES is a no-op. UNMAP (trim/discard)
            // deallocates blocks — also a no-op since we don't pre-allocate.
            // This makes mkfs.ext4 near-instant instead of writing GBs of
            // zeros through the I/O path.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
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
                    let data = sub_block_read(&volume_name, src_offset, num_bytes).await?;
                    sub_block_write(&volume_name, dst_offset, &data).await?;
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
