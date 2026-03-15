//! NovaStor custom SPDK bdev module.
//!
//! This module bridges SPDK's block device layer to the ChunkEngine.
//! Each volume is registered as an SPDK bdev named `novastor_<volume_id>`.
//! When SPDK's NVMe-oF subsystem submits I/O to this bdev, the module
//! offloads the work to a thread pool which calls ChunkEngine::read/write
//! (async, bridged via tokio Handle::block_on), then completes the I/O
//! back on the reactor.
//!
//! The bdev is purely virtual — it has no underlying physical device of its
//! own. The ChunkEngine stores chunks on backend SPDK bdevs via the
//! BdevChunkStore, with CRUSH placement, replication, and EC support.

use crate::backend::chunk_store::CHUNK_SIZE;
use crate::chunk::engine::ChunkEngine;
use crate::error::{DataPlaneError, Result};
use crate::metadata::types::ChunkMapEntry;
use crate::spdk::reactor_dispatch;
use log::{error, info, warn};
use std::collections::HashMap;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

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

fn volume_chunk_maps() -> &'static RwLock<HashMap<String, Vec<Option<ChunkMapEntry>>>> {
    VOLUME_CHUNK_MAPS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Public accessor for volume chunk maps (used by delete_volume cleanup).
pub fn volume_chunk_maps_ref(
) -> Option<&'static RwLock<HashMap<String, Vec<Option<ChunkMapEntry>>>>> {
    VOLUME_CHUNK_MAPS.get()
}

/// Set the chunk engine that NovaStor bdevs will use for I/O.
/// Must be called once after the backend and chunk store are initialised.
pub fn set_chunk_engine(engine: Arc<ChunkEngine>, handle: tokio::runtime::Handle) {
    if CHUNK_ENGINE.set(engine).is_err() {
        info!("novastor_bdev: chunk engine already set, ignoring");
    }
    let _ = TOKIO_HANDLE.set(handle);
}

fn get_chunk_engine() -> Result<&'static Arc<ChunkEngine>> {
    CHUNK_ENGINE
        .get()
        .ok_or_else(|| DataPlaneError::BdevError("chunk engine not initialised".into()))
}

fn get_tokio_handle() -> Result<&'static tokio::runtime::Handle> {
    TOKIO_HANDLE
        .get()
        .ok_or_else(|| DataPlaneError::BdevError("tokio handle not set".into()))
}

/// Thread pool for offloading ChunkBackend I/O from the reactor thread.
static IO_POOL: OnceLock<threadpool::ThreadPool> = OnceLock::new();

fn io_pool() -> &'static threadpool::ThreadPool {
    IO_POOL.get_or_init(|| threadpool::ThreadPool::new(4))
}

/// Per-chunk lock to serialize concurrent RMW operations on the same chunk.
/// Key: (volume_name, chunk_index). The Mutex<()> provides mutual exclusion.
static CHUNK_LOCKS: OnceLock<Mutex<HashMap<(String, usize), Arc<Mutex<()>>>>> = OnceLock::new();

fn chunk_lock(volume: &str, chunk_idx: usize) -> Arc<Mutex<()>> {
    let locks = CHUNK_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = locks.lock().unwrap();
    map.entry((volume.to_string(), chunk_idx))
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Remove all per-chunk lock entries for the given volume from CHUNK_LOCKS.
/// Called during volume destruction to prevent unbounded memory growth.
fn cleanup_volume_locks(volume_name: &str) {
    if let Some(locks) = CHUNK_LOCKS.get() {
        let mut map = locks.lock().unwrap();
        let before = map.len();
        map.retain(|(vol, _)| vol != volume_name);
        let removed = before - map.len();
        if removed > 0 {
            info!(
                "novastor_bdev: cleaned up {} chunk locks for volume '{}'",
                removed, volume_name
            );
        }
    }
}

/// Read-modify-write helper: ensures every write goes through full 4MB chunks.
///
/// For sub-chunk writes, reads the existing chunk, overlays the new data at the
/// correct intra-chunk offset, and writes back the full 4MB chunk. This is
/// essential because the ChunkEngine stores immutable content-addressed chunks
/// and the read path expects full CHUNK_SIZE payloads.
fn rmw_write(
    volume_name: &str,
    offset: u64,
    data: &[u8],
    engine: &ChunkEngine,
    handle: &tokio::runtime::Handle,
) -> Result<Vec<ChunkMapEntry>> {
    let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
    let end_chunk =
        ((offset + data.len() as u64 + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as usize;

    let mut all_entries: Vec<ChunkMapEntry> = Vec::new();
    let mut data_cursor = 0usize;

    for chunk_idx in start_chunk..end_chunk {
        // Serialize concurrent RMW on the same chunk to prevent lost updates.
        let lock = chunk_lock(volume_name, chunk_idx);
        let _guard = lock.lock().unwrap();

        let chunk_start = chunk_idx as u64 * CHUNK_SIZE as u64;
        let chunk_end = chunk_start + CHUNK_SIZE as u64;

        // Determine the byte range within this chunk that the write covers.
        let write_start_in_chunk = if offset > chunk_start {
            (offset - chunk_start) as usize
        } else {
            0
        };
        let write_end_abs = offset + data.len() as u64;
        let write_end_in_chunk = if write_end_abs < chunk_end {
            (write_end_abs - chunk_start) as usize
        } else {
            CHUNK_SIZE
        };
        let write_len = write_end_in_chunk - write_start_in_chunk;

        // If the write covers the entire chunk, skip the read step.
        let full_chunk = if write_start_in_chunk == 0 && write_len >= CHUNK_SIZE {
            data[data_cursor..data_cursor + CHUNK_SIZE].to_vec()
        } else {
            // Read existing chunk data (or zeros if unwritten).
            let mut buf = vec![0u8; CHUNK_SIZE];
            let maps = volume_chunk_maps().read().unwrap();
            if let Some(chunk_map) = maps.get(volume_name) {
                if chunk_idx < chunk_map.len() {
                    if let Some(entry) = &chunk_map[chunk_idx] {
                        let entries = vec![entry.clone()];
                        drop(maps); // release lock before async call
                        if let Ok(existing) =
                            handle.block_on(engine.read(volume_name, chunk_start, &entries))
                        {
                            let copy_len = existing.len().min(CHUNK_SIZE);
                            buf[..copy_len].copy_from_slice(&existing[..copy_len]);
                        }
                    }
                }
            }
            // Overlay the new data.
            buf[write_start_in_chunk..write_end_in_chunk]
                .copy_from_slice(&data[data_cursor..data_cursor + write_len]);
            buf
        };

        data_cursor += write_len;

        // Write the full 4MB chunk through ChunkEngine.
        let entries = handle.block_on(engine.write(volume_name, chunk_start, &full_chunk))?;

        // Update chunk map immediately while still holding the chunk lock.
        let mut maps = volume_chunk_maps().write().unwrap();
        if let Some(chunk_map) = maps.get_mut(volume_name) {
            for entry in &entries {
                let idx = entry.chunk_index as usize;
                if idx < chunk_map.len() {
                    chunk_map[idx] = Some(entry.clone());
                }
            }
        }
        drop(maps);

        all_entries.extend(entries);
    }

    Ok(all_entries)
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
    let num_chunks = (size_bytes + crate::backend::chunk_store::CHUNK_SIZE as u64 - 1)
        / crate::backend::chunk_store::CHUNK_SIZE as u64;
    {
        let mut maps = volume_chunk_maps().write().unwrap();
        maps.entry(volume_name.to_string())
            .or_insert_with(|| vec![None; num_chunks as usize]);
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

    // Clean up per-chunk locks to prevent unbounded memory growth.
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

            io_pool().execute(move || {
                log::debug!(
                    "novastor_bdev: READ io_pool offset={} len={}",
                    offset,
                    length
                );
                let result = (|| -> Result<Vec<u8>> {
                    let engine = get_chunk_engine()?;
                    let handle = get_tokio_handle()?;

                    // Look up the chunk map entries for this offset range.
                    let start_chunk = (offset / CHUNK_SIZE as u64) as usize;
                    let end_chunk =
                        ((offset + length + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as usize;

                    let maps = volume_chunk_maps().read().unwrap();
                    let chunk_map = maps.get(&volume_name).ok_or_else(|| {
                        DataPlaneError::BdevError(format!(
                            "no chunk map for volume '{}'",
                            volume_name
                        ))
                    })?;

                    // Collect entries for the requested range.
                    let mut entries: Vec<ChunkMapEntry> = Vec::new();
                    let mut has_unwritten = false;
                    for idx in start_chunk..end_chunk.min(chunk_map.len()) {
                        match &chunk_map[idx] {
                            Some(entry) => entries.push(entry.clone()),
                            None => {
                                has_unwritten = true;
                                break;
                            }
                        }
                    }

                    // If any chunks in the range have never been written, return zeros.
                    if has_unwritten || entries.is_empty() {
                        return Ok(vec![0u8; length as usize]);
                    }

                    // Read through ChunkEngine (async → sync bridge).
                    let data = handle.block_on(engine.read(&volume_name, offset, &entries))?;

                    // Trim to the exact sub-chunk offset and length requested.
                    // If the stored chunk is smaller than expected (legacy sub-chunk
                    // write), pad with zeros to avoid panicking.
                    let chunk_aligned_start = start_chunk as u64 * CHUNK_SIZE as u64;
                    let sub_offset = (offset - chunk_aligned_start) as usize;
                    if sub_offset >= data.len() {
                        // Offset beyond stored data — return zeros.
                        return Ok(vec![0u8; length as usize]);
                    }
                    let available = data.len() - sub_offset;
                    let to_return = std::cmp::min(available, length as usize);
                    let mut result = data[sub_offset..sub_offset + to_return].to_vec();
                    // Pad with zeros if we couldn't satisfy the full request.
                    if result.len() < length as usize {
                        result.resize(length as usize, 0);
                    }
                    Ok(result)
                })();

                // Copy data into the iov buffers and complete I/O on the
                // reactor thread to guarantee the bdev_io and its iov buffers
                // are still valid (they remain valid until spdk_bdev_io_complete).
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

            io_pool().execute(move || {
                let result = (|| -> Result<()> {
                    let engine = get_chunk_engine()?;
                    let handle = get_tokio_handle()?;

                    log::info!(
                        "novastor_bdev: WRITE io_pool start vol={} offset={} len={}",
                        volume_name,
                        offset,
                        data.len()
                    );

                    let entries = rmw_write(&volume_name, offset, &data, engine, handle)?;

                    log::info!(
                        "novastor_bdev: WRITE io_pool done vol={} entries={}",
                        volume_name,
                        entries.len()
                    );

                    Ok(())
                })();

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

            io_pool().execute(move || {
                let result = (|| -> Result<()> {
                    let engine = get_chunk_engine()?;
                    let handle = get_tokio_handle()?;
                    rmw_write(&volume_name, offset, &data, engine, handle)?;
                    Ok(())
                })();

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
