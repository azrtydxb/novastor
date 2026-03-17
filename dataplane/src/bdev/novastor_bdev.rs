//! NovaStor custom SPDK bdev module.
//!
//! Each volume is registered as an SPDK bdev named `novastor_<volume_id>`.
//! When SPDK's NVMe-oF subsystem submits I/O to this bdev, the module
//! performs direct 64KB sub-block reads and writes against the backend SPDK
//! bdev, bypassing the ChunkEngine on the hot I/O path.
//!
//! Volume data is laid out contiguously on the backend bdev. The sub-block
//! write path does partial read-modify-write at 64KB granularity (instead
//! of 4MB), and tracks which sub-blocks have been written via a per-chunk
//! dirty bitmap in the chunk map entry. SHA-256 hashing and content-
//! addressing happen only during periodic background sync (not on the hot
//! path).

use crate::bdev::sub_block::{self, bitmap_set, CHUNK_SIZE, SUB_BLOCK_SIZE};
use crate::chunk::engine::ChunkEngine;
use crate::error::{DataPlaneError, Result};
use crate::metadata::store::MetadataStore;
use crate::metadata::types::ChunkMapEntry;
use crate::spdk::reactor_dispatch;
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
/// Set during init_chunk_store — this is the underlying storage bdev that
/// volumes are laid out contiguously on.
static BACKEND_BDEV_NAME: OnceLock<String> = OnceLock::new();

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
static SUB_BLOCK_LOCKS: OnceLock<
    Mutex<HashMap<(String, usize, usize), Arc<tokio::sync::Mutex<()>>>>,
> = OnceLock::new();

fn sub_block_lock(volume: &str, chunk_idx: usize, sb_idx: usize) -> Arc<tokio::sync::Mutex<()>> {
    let locks = SUB_BLOCK_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = locks.lock().unwrap();
    map.entry((volume.to_string(), chunk_idx, sb_idx))
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

/// Remove all per-sub-block lock entries for the given volume from SUB_BLOCK_LOCKS.
/// Called during volume destruction to prevent unbounded memory growth.
fn cleanup_volume_locks(volume_name: &str) {
    if let Some(locks) = SUB_BLOCK_LOCKS.get() {
        let mut map = locks.lock().unwrap();
        let before = map.len();
        map.retain(|key, _| key.0 != volume_name);
        let removed = before - map.len();
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
/// For each sub-block the write touches:
/// - If the write covers the entire 64KB sub-block: write directly (no read).
/// - Otherwise: read the existing 64KB, overlay new bytes, write 64KB back.
/// - Update the dirty bitmap and persist the chunk map entry.
///
/// No SHA-256 hashing — content-addressing happens during background sync.
/// Volume data is laid out contiguously on the backend bdev: offset maps directly.
#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, len = data.len()))]
async fn sub_block_write(volume_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let backend_bdev = get_backend_bdev_name()?;
    let total_len = data.len() as u64;
    let end_offset = offset + total_len;
    let mut data_cursor = 0usize;

    // Walk through each sub-block the write touches.
    let mut pos = offset;
    while pos < end_offset {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);
        let offset_in_sb = sub_block::offset_in_sub_block(pos);

        // How many bytes to write in this sub-block.
        let remaining_in_sb = SUB_BLOCK_SIZE - offset_in_sb;
        let remaining_data = (end_offset - pos) as usize;
        let write_len = remaining_in_sb.min(remaining_data);

        let covers_full_sb = offset_in_sb == 0 && write_len == SUB_BLOCK_SIZE;

        // Backend bdev offset for this sub-block.
        let chunk_base = chunk_idx as u64 * CHUNK_SIZE as u64;
        let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx);

        if covers_full_sb {
            // Full sub-block write — no read needed.
            let sb_data = &data[data_cursor..data_cursor + SUB_BLOCK_SIZE];
            reactor_dispatch::bdev_write_async(backend_bdev, bdev_offset, sb_data)
                .instrument(info_span!("bdev_write_64k", chunk_idx, sb_idx))
                .await?;
        } else {
            // Partial sub-block write — lock, read-modify-write at 64KB granularity.
            let lock = sub_block_lock(volume_name, chunk_idx, sb_idx);
            let _guard = lock.lock().await;

            let mut buf = match reactor_dispatch::bdev_read_async(
                backend_bdev,
                bdev_offset,
                SUB_BLOCK_SIZE as u64,
            )
            .instrument(info_span!("bdev_read_64k", chunk_idx, sb_idx))
            .await
            {
                Ok(existing) => existing,
                Err(_) => {
                    // Sub-block never written — start with zeros.
                    vec![0u8; SUB_BLOCK_SIZE]
                }
            };

            // Ensure buffer is exactly SUB_BLOCK_SIZE.
            buf.resize(SUB_BLOCK_SIZE, 0);

            // Overlay the new data.
            buf[offset_in_sb..offset_in_sb + write_len]
                .copy_from_slice(&data[data_cursor..data_cursor + write_len]);

            reactor_dispatch::bdev_write_async(backend_bdev, bdev_offset, &buf)
                .instrument(info_span!("bdev_write_64k", chunk_idx, sb_idx))
                .await?;
        }

        // Update the chunk map entry's dirty bitmap.
        let chunk_id = format!("{}:{}", volume_name, chunk_idx);
        let entry = {
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
            entry.clone()
        }; // maps guard dropped here, before any .await

        // Write-through: persist the chunk map entry to the metadata store.
        let _md_span = info_span!("metadata_persist", chunk_idx).entered();
        if let Some(store) = get_metadata_store() {
            if let Err(e) = store.put_chunk_map(volume_name, &entry) {
                log::error!(
                    "sub_block_write: persistence failed for {}:{}: {}",
                    volume_name,
                    chunk_idx,
                    e
                );
                return Err(DataPlaneError::MetadataError(format!(
                    "chunk map persistence failed: {}",
                    e
                )));
            }
        }

        data_cursor += write_len;
        pos += write_len as u64;
    }

    Ok(())
}

/// Sub-block read: reads data in 64KB sub-block granularity directly from the
/// backend bdev, bypassing the ChunkEngine for hot-path I/O.
///
/// For each sub-block the read spans, reads 64KB from the backend bdev at the
/// direct volume offset and extracts the requested bytes. Unwritten sub-blocks
/// (dirty bitmap bit not set) return zeros.
#[tracing::instrument(skip_all, fields(volume = %volume_name, offset, length))]
async fn sub_block_read(volume_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let backend_bdev = get_backend_bdev_name()?;
    let end_offset = offset + length;
    let mut result = Vec::with_capacity(length as usize);

    let mut pos = offset;
    while pos < end_offset {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);
        let offset_in_sb = sub_block::offset_in_sub_block(pos);

        // How many bytes to read from this sub-block.
        let remaining_in_sb = SUB_BLOCK_SIZE - offset_in_sb;
        let remaining_req = (end_offset - pos) as usize;
        let read_len = remaining_in_sb.min(remaining_req);

        // Check if this sub-block has been written via the dirty bitmap.
        let is_dirty = {
            let _bm_span = info_span!("bitmap_check", chunk_idx, sb_idx).entered();
            let maps = volume_chunk_maps().read().unwrap();
            maps.get(volume_name)
                .and_then(|chunk_map| {
                    if chunk_idx < chunk_map.len() {
                        chunk_map[chunk_idx].as_ref()
                    } else {
                        None
                    }
                })
                .map(|entry| sub_block::bitmap_is_set(entry.dirty_bitmap, sb_idx))
                .unwrap_or(false)
        }; // maps guard dropped

        if !is_dirty {
            // Sub-block never written — return zeros.
            result.extend(std::iter::repeat(0u8).take(read_len));
        } else {
            // Read the 64KB sub-block from the backend bdev.
            let chunk_base = chunk_idx as u64 * CHUNK_SIZE as u64;
            let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx);
            let sb_data =
                reactor_dispatch::bdev_read_async(backend_bdev, bdev_offset, SUB_BLOCK_SIZE as u64)
                    .instrument(info_span!("bdev_read_64k", chunk_idx, sb_idx))
                    .await?;

            // Extract the requested range from the sub-block.
            let available = sb_data.len().saturating_sub(offset_in_sb);
            let to_copy = read_len.min(available);
            result.extend_from_slice(&sb_data[offset_in_sb..offset_in_sb + to_copy]);
            // Pad with zeros if the sub-block data was shorter than expected.
            if to_copy < read_len {
                result.extend(std::iter::repeat(0u8).take(read_len - to_copy));
            }
        }

        pos += read_len as u64;
    }

    Ok(result)
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
/// We offload the actual ChunkBackend I/O to a thread pool to avoid blocking
/// the reactor. The thread pool worker calls ChunkBackend::read/write (which
/// internally dispatches bdev I/O back to the reactor), then sends the
/// completion back to the reactor via `spdk_thread_send_msg`.
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
    log::debug!(
        "novastor_bdev: submit_request io_type={} volume={}",
        io_type,
        volume_name
    );

    match io_type {
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_READ => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;
            let bdev_io_addr = bdev_io as usize;

            // Get the iov buffer pointer for writing data back.
            let iovs = bdev_params.iovs;
            let iovcnt = bdev_params.iovcnt as usize;
            if iovs.is_null() || iovcnt == 0 {
                ffi::spdk_bdev_io_complete(
                    bdev_io,
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                );
                return;
            }

            // Capture iov metadata for the reactor-side copy.
            let mut iov_descs: Vec<(usize, usize)> = Vec::with_capacity(iovcnt);
            for i in 0..iovcnt {
                let iov = &*iovs.add(i);
                iov_descs.push((iov.iov_base as usize, iov.iov_len));
            }

            // Use tokio async for direct sub-block reads from the backend bdev.
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                log::debug!("novastor_bdev: READ async offset={} len={}", offset, length);

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
            let bdev_io_addr = bdev_io as usize;

            let iovs = bdev_params.iovs;
            let iovcnt = bdev_params.iovcnt as usize;
            if iovs.is_null() || iovcnt == 0 {
                ffi::spdk_bdev_io_complete(
                    bdev_io,
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                );
                return;
            }

            // Copy data from the iov buffer before offloading.
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

            // Spawn write on tokio runtime (non-blocking). Sub-block write
            // goes directly to the backend bdev in 64KB granularity.
            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let result = async {
                    log::debug!(
                        "novastor_bdev: WRITE async start vol={} offset={} len={}",
                        volume_name,
                        offset,
                        data.len()
                    );

                    sub_block_write(&volume_name, offset, &data).await?;

                    log::debug!("novastor_bdev: WRITE async done vol={}", volume_name,);

                    Ok::<(), DataPlaneError>(())
                }
                .await;

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
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_WRITE_ZEROES => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * (*bdev).blocklen as u64;
            let length = bdev_params.num_blocks * (*bdev).blocklen as u64;
            let bdev_io_addr = bdev_io as usize;

            let data = vec![0u8; length as usize];

            let handle = get_tokio_handle().expect("tokio handle");
            handle.spawn(async move {
                let result = async {
                    sub_block_write(&volume_name, offset, &data).await?;
                    Ok::<(), DataPlaneError>(())
                }
                .await;

                let status = match result {
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
        _ => {
            // Unsupported I/O type.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
            );
        }
    }
}
