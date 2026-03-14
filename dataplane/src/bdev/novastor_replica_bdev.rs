//! NovaStor replica bdev SPDK module.
//!
//! Registers a `ReplicaBdev` as an SPDK bdev so it can be used as an NVMe-oF
//! namespace. I/O is offloaded to a thread pool where the `ReplicaBdev`'s
//! quorum write / policy-based read logic executes, then completions are
//! sent back to the reactor via `spdk_thread_send_msg`.
//!
//! This follows the same pattern as `novastor_bdev.rs` (the ChunkBackend
//! SPDK bdev module), but routes I/O through the replica fan-out layer
//! instead of the chunk backend.

use crate::bdev::replica::ReplicaBdev;
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

/// Registry of active replica bdevs, keyed by volume ID.
static REPLICA_BDEV_REGISTRY: OnceLock<Mutex<HashMap<String, ReplicaBdevEntry>>> = OnceLock::new();

fn bdev_registry() -> &'static Mutex<HashMap<String, ReplicaBdevEntry>> {
    REPLICA_BDEV_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Thread pool for offloading replica I/O from the reactor thread.
/// Shared with the chunk bdev module for efficiency.
static IO_POOL: OnceLock<threadpool::ThreadPool> = OnceLock::new();

fn io_pool() -> &'static threadpool::ThreadPool {
    IO_POOL.get_or_init(|| threadpool::ThreadPool::new(4))
}

/// Per-bdev tracking entry.
struct ReplicaBdevEntry {
    volume_id: String,
    /// The SPDK bdev pointer, needed for unregister.
    bdev_ptr: usize,
    /// The BdevCtx pointer, used as the io_device key for unregister.
    ctx_ptr: usize,
    /// The replica bdev Arc, kept alive while SPDK holds a reference.
    _replica: Arc<ReplicaBdev>,
}

/// Bdev context stored in `bdev->ctxt`. Holds the ReplicaBdev for I/O dispatch.
struct ReplicaBdevCtx {
    volume_id: String,
    replica: Arc<ReplicaBdev>,
}

// Safety: ReplicaBdevCtx is only accessed from SPDK reactor thread or our I/O
// pool threads. The reactor thread creates it, the pool threads use the
// Arc<ReplicaBdev>, and the reactor thread frees it on destruct.
unsafe impl Send for ReplicaBdevCtx {}
unsafe impl Sync for ReplicaBdevCtx {}

// ---------------------------------------------------------------------------
// Public API — called from gRPC DataplaneService handlers
// ---------------------------------------------------------------------------

/// Create a replica SPDK bdev wrapping a `ReplicaBdev`.
///
/// The `ReplicaBdev` must already be constructed with `enable_spdk_io()` called.
/// This registers an SPDK bdev named `replicated_<volume_id>` with the given
/// size. The bdev can then be referenced by `nvmf_create_target`.
pub fn create(replica: Arc<ReplicaBdev>, size_bytes: u64) -> Result<String> {
    let volume_id = replica.volume_id.clone();
    // volume_id is already the desired SPDK bdev name (e.g. "replicated_<uuid>").
    // Don't add another prefix.
    let bdev_name = volume_id.clone();
    let block_size: u32 = 512;
    let num_blocks = size_bytes / block_size as u64;

    info!(
        "novastor_replica_bdev: creating bdev '{}' for volume '{}' ({} blocks)",
        bdev_name, volume_id, num_blocks
    );

    let ctx = Box::new(ReplicaBdevCtx {
        volume_id: volume_id.clone(),
        replica: replica.clone(),
    });
    let ctx_ptr = Box::into_raw(ctx);
    let ctx_addr = ctx_ptr as usize;

    let bdev_name_clone = bdev_name.clone();

    let (bdev_addr, registered_ctx_addr) =
        reactor_dispatch::dispatch_sync(move || -> Result<(usize, usize)> {
            unsafe {
                let ctx_ptr = ctx_addr as *mut ReplicaBdevCtx;

                // Register this bdev's context as an io_device.
                let io_dev_name =
                    std::ffi::CString::new(format!("novastor_replica_io_{}", bdev_name_clone))
                        .unwrap();
                ffi::spdk_io_device_register(
                    ctx_ptr as *mut c_void,
                    Some(replica_channel_create_cb),
                    Some(replica_channel_destroy_cb),
                    0,
                    io_dev_name.as_ptr(),
                );

                // Allocate and zero the bdev struct.
                let bdev =
                    libc::calloc(1, std::mem::size_of::<ffi::spdk_bdev>()) as *mut ffi::spdk_bdev;
                if bdev.is_null() {
                    ffi::spdk_io_device_unregister(ctx_ptr as *mut c_void, None);
                    let _ = Box::from_raw(ctx_ptr);
                    return Err(DataPlaneError::BdevError("calloc spdk_bdev failed".into()));
                }

                // Set bdev fields.
                let name_c = std::ffi::CString::new(bdev_name_clone.as_str()).unwrap();
                (*bdev).name = libc::strdup(name_c.as_ptr());
                (*bdev).product_name =
                    libc::strdup(b"NovaStor ReplicaBdev\0".as_ptr() as *const c_char);
                (*bdev).blocklen = block_size;
                (*bdev).blockcnt = num_blocks;
                (*bdev).ctxt = ctx_ptr as *mut c_void;
                (*bdev).module = replica_bdev_module_ptr();
                (*bdev).fn_table = replica_fn_table();

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
        volume_id.clone(),
        ReplicaBdevEntry {
            volume_id: volume_id.clone(),
            bdev_ptr: bdev_addr,
            ctx_ptr: registered_ctx_addr,
            _replica: replica,
        },
    );

    info!(
        "novastor_replica_bdev: registered bdev '{}' (volume='{}', size={}B)",
        bdev_name, volume_id, size_bytes
    );

    Ok(bdev_name)
}

/// Remove a replica SPDK bdev and unregister it from SPDK.
pub fn destroy(volume_id: &str) -> Result<()> {
    let entry = bdev_registry()
        .lock()
        .unwrap()
        .remove(volume_id)
        .ok_or_else(|| {
            DataPlaneError::BdevError(format!("replica bdev for volume '{}' not found", volume_id))
        })?;

    let bdev_addr = entry.bdev_ptr;
    let ctx_addr = entry.ctx_ptr;

    info!(
        "novastor_replica_bdev: destroying bdev for volume '{}'",
        volume_id
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

    info!(
        "novastor_replica_bdev: destroyed bdev for volume '{}'",
        volume_id
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// SPDK bdev module and fn_table
// ---------------------------------------------------------------------------

/// The static bdev module descriptor for replica bdevs.
static mut REPLICA_MODULE: ffi::spdk_bdev_module = unsafe { std::mem::zeroed() };
static REPLICA_MODULE_INIT: std::sync::Once = std::sync::Once::new();

fn replica_bdev_module_ptr() -> *mut ffi::spdk_bdev_module {
    REPLICA_MODULE_INIT.call_once(|| unsafe {
        REPLICA_MODULE.name = b"novastor_replica\0".as_ptr() as *const c_char;
        REPLICA_MODULE.module_init = Some(module_init_cb);
        REPLICA_MODULE.module_fini = Some(module_fini_cb);
    });
    unsafe { &mut REPLICA_MODULE as *mut ffi::spdk_bdev_module }
}

/// The bdev function table for replica bdevs.
static REPLICA_FN_TABLE: OnceLock<ffi::spdk_bdev_fn_table> = OnceLock::new();

fn replica_fn_table() -> &'static ffi::spdk_bdev_fn_table {
    REPLICA_FN_TABLE.get_or_init(|| {
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
    info!("novastor_replica_bdev: module initialised");
    0
}

unsafe extern "C" fn module_fini_cb() {
    info!("novastor_replica_bdev: module shutdown");
}

/// No-op channel create callback.
unsafe extern "C" fn replica_channel_create_cb(_io_device: *mut c_void, _ctx: *mut c_void) -> i32 {
    0
}

/// No-op channel destroy callback.
unsafe extern "C" fn replica_channel_destroy_cb(_io_device: *mut c_void, _ctx: *mut c_void) {}

unsafe extern "C" fn bdev_destruct_cb(ctx: *mut c_void) -> i32 {
    if !ctx.is_null() {
        let _ = Box::from_raw(ctx as *mut ReplicaBdevCtx);
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
    ffi::spdk_get_io_channel(ctx)
}

/// The main I/O submission callback. Called on the SPDK reactor thread.
///
/// Offloads I/O to the thread pool where `ReplicaBdev::submit_write` /
/// `submit_read` performs the quorum-based fan-out across replica targets.
unsafe extern "C" fn bdev_submit_request_cb(
    _channel: *mut ffi::spdk_io_channel,
    bdev_io: *mut ffi::spdk_bdev_io,
) {
    let bdev = (*bdev_io).bdev;
    let ctx = (*bdev).ctxt as *const ReplicaBdevCtx;
    if ctx.is_null() {
        ffi::spdk_bdev_io_complete(bdev_io, ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
    let replica = (*ctx).replica.clone();

    let io_type = (*bdev_io).type_ as u32;
    let block_size = (*bdev).blocklen as u64;

    match io_type {
        ffi::spdk_bdev_io_type_SPDK_BDEV_IO_TYPE_READ => {
            let bdev_params = (*bdev_io).u.bdev.as_ref();
            let offset = bdev_params.offset_blocks * block_size;
            let length = bdev_params.num_blocks * block_size;
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

            let mut iov_descs: Vec<(usize, usize)> = Vec::with_capacity(iovcnt);
            for i in 0..iovcnt {
                let iov = &*iovs.add(i);
                iov_descs.push((iov.iov_base as usize, iov.iov_len));
            }

            io_pool().execute(move || {
                let result = replica.submit_read(offset, length);

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
                        error!("novastor_replica_bdev: read failed: {}", e);
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
            let offset = bdev_params.offset_blocks * block_size;
            let length = bdev_params.num_blocks * block_size;
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
                let result = replica.submit_write(offset, &data);

                let status = match result {
                    Ok(()) => ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
                    Err(e) => {
                        error!("novastor_replica_bdev: write failed: {}", e);
                        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED
                    }
                };

                reactor_dispatch::send_to_reactor(move || unsafe {
                    ffi::spdk_bdev_io_complete(bdev_io_addr as *mut ffi::spdk_bdev_io, status);
                });
            });
        }
        _ => {
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
            );
        }
    }
}
