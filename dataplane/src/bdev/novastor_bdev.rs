//! NovaStor custom SPDK bdev module.
//!
//! This module bridges SPDK's block device layer to the ChunkBackend.
//! Each volume created via the ChunkBackend is registered as an SPDK bdev
//! named `novastor_<volume_id>`. When SPDK's NVMe-oF subsystem submits I/O
//! to this bdev, the module offloads the work to a thread pool which calls
//! ChunkBackend::read/write, then completes the I/O back on the reactor.
//!
//! The bdev is purely virtual — it has no underlying physical device of its
//! own. The ChunkBackend internally stores chunks on real SPDK bdevs via
//! the BdevChunkStore.

use crate::backend::traits::StorageBackend;
use crate::error::{DataPlaneError, Result};
use crate::spdk::reactor_dispatch;
use log::{error, info};
use std::collections::HashMap;
use std::os::raw::{c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock};

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

/// The ChunkBackend shared across all NovaStor bdevs.
static CHUNK_BACKEND: OnceLock<Arc<dyn StorageBackend>> = OnceLock::new();

/// Set the chunk backend that NovaStor bdevs will use for I/O.
/// Must be called once after `backend.init_chunk` initialises the backend.
pub fn set_chunk_backend(backend: Arc<dyn StorageBackend>) {
    if CHUNK_BACKEND.set(backend).is_err() {
        info!("novastor_bdev: chunk backend already set, ignoring");
    }
}

fn get_chunk_backend() -> Result<&'static Arc<dyn StorageBackend>> {
    CHUNK_BACKEND
        .get()
        .ok_or_else(|| DataPlaneError::BdevError("chunk backend not initialised".into()))
}

/// Thread pool for offloading ChunkBackend I/O from the reactor thread.
static IO_POOL: OnceLock<threadpool::ThreadPool> = OnceLock::new();

fn io_pool() -> &'static threadpool::ThreadPool {
    IO_POOL.get_or_init(|| threadpool::ThreadPool::new(4))
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
// Public API — called from JSON-RPC handlers
// ---------------------------------------------------------------------------

/// Create a NovaStor bdev wrapping a ChunkBackend volume.
///
/// The volume must already exist in the ChunkBackend. This registers an SPDK
/// bdev named `novastor_<volume_name>` with the given size. The bdev can then
/// be referenced by `nvmf_create_target`.
pub fn create(volume_name: &str, size_bytes: u64) -> Result<String> {
    let backend = get_chunk_backend()?;
    let bdev_name = format!("novastor_{}", volume_name);

    // Verify the volume exists.
    let info = backend.stat_volume(volume_name)?;
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
        "novastor_bdev: registered bdev '{}' (volume='{}', size={}B, healthy={})",
        format!("novastor_{}", volume_name),
        volume_name,
        info.size_bytes,
        info.healthy
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
            let iov_base = (*iovs).iov_base as usize;
            let iov_len = (*iovs).iov_len;

            io_pool().execute(move || {
                let result = match get_chunk_backend() {
                    Ok(backend) => backend.read(&volume_name, offset, length),
                    Err(e) => Err(e),
                };

                let success = match result {
                    Ok(data) => unsafe {
                        let copy_len = std::cmp::min(data.len(), iov_len);
                        std::ptr::copy_nonoverlapping(data.as_ptr(), iov_base as *mut u8, copy_len);
                        true
                    },
                    Err(e) => {
                        error!("novastor_bdev: read failed: {}", e);
                        false
                    }
                };

                // Complete I/O on the reactor thread.
                let status = if success {
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS
                } else {
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                };
                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
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
                let result = match get_chunk_backend() {
                    Ok(backend) => backend.write(&volume_name, offset, &data),
                    Err(e) => Err(e),
                };

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
        _ => {
            // Unsupported I/O type. SPDK_BDEV_IO_STATUS_FAILED = -1.
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
            );
        }
    }
}
