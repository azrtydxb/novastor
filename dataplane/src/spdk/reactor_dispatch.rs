//! Reactor dispatch — run SPDK operations on the reactor thread.
//!
//! All SPDK bdev and lvol operations must execute on an SPDK thread. The
//! gRPC service runs on tokio threads, so we dispatch work to the reactor
//! via `spdk_thread_send_msg()` and block until results arrive.
//!
//! - Sync ops (create_malloc, query_bdev): `dispatch_sync` sends a closure,
//!   waits for it to return.
//! - Async I/O (bdev read/write): `send_to_reactor` submits I/O, the SPDK
//!   callback signals a `Completion`, and the caller blocks on it.

use crate::error::{DataPlaneError, Result};
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::{Arc, Mutex, OnceLock};

use super::context::Completion;

// ---------------------------------------------------------------------------
// Pre-allocated DMA buffer pool
// ---------------------------------------------------------------------------
// Eliminates per-I/O spdk_dma_malloc/free overhead. Each buffer is 64KB
// (SUB_BLOCK_SIZE) aligned to 4096. Pool supports up to 256 concurrent I/Os.
// All pool operations run on the SPDK reactor thread (no contention).

const DMA_POOL_SIZE: usize = 256;
const DMA_BUF_SIZE: usize = 64 * 1024; // 64KB

struct DmaPool {
    buffers: Vec<*mut u8>,
}

// SAFETY: Only accessed from the SPDK reactor thread via send_to_reactor.
unsafe impl Send for DmaPool {}

static DMA_POOL: OnceLock<Mutex<DmaPool>> = OnceLock::new();

/// Initialize the DMA buffer pool. Must be called on the SPDK reactor thread
/// after subsystems are initialized (e.g. from spdk_startup_cb).
pub fn init_dma_pool() {
    let mut buffers = Vec::with_capacity(DMA_POOL_SIZE);
    for _ in 0..DMA_POOL_SIZE {
        let buf =
            unsafe { ffi::spdk_dma_malloc(DMA_BUF_SIZE, 0x1000, std::ptr::null_mut()) as *mut u8 };
        if buf.is_null() {
            log::warn!(
                "DMA pool: could only pre-allocate {} of {} buffers",
                buffers.len(),
                DMA_POOL_SIZE
            );
            break;
        }
        buffers.push(buf);
    }
    log::info!(
        "DMA buffer pool initialized: {} x {}KB buffers",
        buffers.len(),
        DMA_BUF_SIZE / 1024
    );
    DMA_POOL.set(Mutex::new(DmaPool { buffers })).ok();
}

/// Acquire a DMA buffer. For sizes <= 64KB, tries the pool first.
/// Falls back to spdk_dma_malloc for larger buffers or when pool is empty.
fn acquire_dma_buf(size: usize) -> *mut std::os::raw::c_void {
    if size <= DMA_BUF_SIZE {
        if let Some(pool) = DMA_POOL.get() {
            if let Ok(mut p) = pool.lock() {
                if let Some(buf) = p.buffers.pop() {
                    return buf as *mut std::os::raw::c_void;
                }
            }
        }
    }
    // Fall back to malloc for large buffers or when pool is empty.
    unsafe { ffi::spdk_dma_malloc(size, 0x1000, std::ptr::null_mut()) }
}

/// Public wrapper for acquire_dma_buf (used by reactor-native I/O path).
pub fn acquire_dma_buf_public(size: usize) -> *mut std::os::raw::c_void {
    acquire_dma_buf(size)
}

/// Public wrapper for release_dma_buf (used by reactor-native I/O path).
pub fn release_dma_buf_public(buf: *mut std::os::raw::c_void, size: usize) {
    release_dma_buf(buf, size);
}

/// Release a DMA buffer back to the pool. For buffers that came from the pool
/// (size <= 64KB), returns them if the pool isn't full. Otherwise frees via
/// spdk_dma_free.
fn release_dma_buf(buf: *mut std::os::raw::c_void, size: usize) {
    if size <= DMA_BUF_SIZE {
        if let Some(pool) = DMA_POOL.get() {
            if let Ok(mut p) = pool.lock() {
                if p.buffers.len() < DMA_POOL_SIZE {
                    p.buffers.push(buf as *mut u8);
                    return;
                }
            }
        }
    }
    unsafe {
        ffi::spdk_dma_free(buf);
    }
}

// ---------------------------------------------------------------------------
// Cached bdev descriptor + I/O channel
// ---------------------------------------------------------------------------
// Bdev descriptor cache (shared across reactor cores) and per-core I/O
// channel cache. SPDK bdev descriptors are thread-safe, but I/O channels
// are per-thread — each reactor core must have its own channel.

struct CachedBdevDesc {
    desc: *mut ffi::spdk_bdev_desc,
    block_size: u64,
}

// SAFETY: spdk_bdev_desc is thread-safe (can be shared across reactor cores).
unsafe impl Send for CachedBdevDesc {}
unsafe impl Sync for CachedBdevDesc {}

/// Global cache of bdev descriptors (shared across all reactor cores).
static BDEV_DESC_CACHE: OnceLock<Mutex<HashMap<String, CachedBdevDesc>>> = OnceLock::new();

fn bdev_desc_cache() -> &'static Mutex<HashMap<String, CachedBdevDesc>> {
    BDEV_DESC_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Per-core I/O channel cache. Each reactor core gets its own channel.
/// thread_local ensures no cross-core channel sharing.
thread_local! {
    static CHANNEL_CACHE: std::cell::RefCell<HashMap<String, *mut ffi::spdk_io_channel>>
        = std::cell::RefCell::new(HashMap::new());
}

/// Open a bdev (or return cached handles). MUST be called on a reactor thread.
/// Returns (desc, channel, block_size) or an error string.
/// The descriptor is shared across cores; the channel is per-core.
unsafe fn get_or_open_bdev(
    name: &str,
    write: bool,
) -> std::result::Result<(*mut ffi::spdk_bdev_desc, *mut ffi::spdk_io_channel, u64), String> {
    // Get or open the shared descriptor.
    let (desc, block_size) = {
        let mut cache = bdev_desc_cache().lock().unwrap();
        if let Some(entry) = cache.get(name) {
            (entry.desc, entry.block_size)
        } else {
            let name_c = std::ffi::CString::new(name).map_err(|e| format!("invalid name: {e}"))?;
            let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
            let rc = ffi::spdk_bdev_open_ext(
                name_c.as_ptr() as *const c_char,
                write,
                Some(bdev_event_cb),
                std::ptr::null_mut(),
                &mut desc,
            );
            if rc != 0 {
                return Err(format!("spdk_bdev_open_ext('{}') failed: rc={rc}", name));
            }
            let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
            let block_size = if bdev.is_null() {
                512
            } else {
                ffi::spdk_bdev_get_block_size(bdev)
            } as u64;

            log::info!(
                "bdev desc cache: opened '{}' (block_size={}, write={})",
                name,
                block_size,
                write
            );
            cache.insert(name.to_string(), CachedBdevDesc { desc, block_size });
            (desc, block_size)
        }
    };

    // Get or create the per-core I/O channel.
    let channel = CHANNEL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(&ch) = cache.get(name) {
            return Ok(ch);
        }
        let ch = ffi::spdk_bdev_get_io_channel(desc);
        if ch.is_null() {
            return Err("spdk_bdev_get_io_channel null".to_string());
        }
        log::info!(
            "bdev channel cache: created per-core channel for '{}' on core {:?}",
            name,
            std::thread::current().id()
        );
        cache.insert(name.to_string(), ch);
        Ok(ch)
    })?;

    Ok((desc, channel, block_size))
}

/// Close all cached bdev descriptors and channels. Call on shutdown.
pub fn close_all_cached_bdevs() {
    send_to_reactor(|| unsafe {
        // Close per-core channels
        CHANNEL_CACHE.with(|cache| {
            let mut cache = cache.borrow_mut();
            for (name, ch) in cache.drain() {
                log::info!("bdev channel cache: closing '{}'", name);
                ffi::spdk_put_io_channel(ch);
            }
        });
        // Close shared descriptors
        let mut cache = bdev_desc_cache().lock().unwrap();
        for (name, entry) in cache.drain() {
            log::info!("bdev desc cache: closing '{}'", name);
            ffi::spdk_bdev_close(entry.desc);
        }
    });
}

// ---------------------------------------------------------------------------
// Lvol store pointer registry
// ---------------------------------------------------------------------------
// The spdk_lvol_store* pointer is received in lvs_init_cb and must be passed
// to spdk_lvol_create.  Both bdev_manager and the lvm backend need access,
// so we keep a global registry keyed by store name.

/// A Send-safe wrapper for an SPDK pointer (only used on the reactor thread).
#[derive(Clone, Copy)]
pub struct SendPtr(usize);

// SAFETY: The pointer is only dereferenced on the SPDK reactor thread.
unsafe impl Send for SendPtr {}

impl SendPtr {
    pub fn new(ptr: *mut std::os::raw::c_void) -> Self {
        Self(ptr as usize)
    }

    pub fn as_ptr(self) -> *mut std::os::raw::c_void {
        self.0 as *mut std::os::raw::c_void
    }
}

static LVS_REGISTRY: std::sync::LazyLock<Mutex<HashMap<String, usize>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Register an lvol store pointer (call from lvs_init_cb on success).
pub fn register_lvol_store(name: &str, ptr: *mut std::os::raw::c_void) {
    LVS_REGISTRY
        .lock()
        .unwrap()
        .insert(name.to_string(), ptr as usize);
}

/// Retrieve a previously registered lvol store pointer as a Send-safe wrapper.
pub fn get_lvol_store_ptr(name: &str) -> Option<SendPtr> {
    LVS_REGISTRY
        .lock()
        .unwrap()
        .get(name)
        .map(|&addr| SendPtr(addr))
}

/// Remove an lvol store pointer from the registry.
pub fn unregister_lvol_store(name: &str) {
    LVS_REGISTRY.lock().unwrap().remove(name);
}

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

// ---------------------------------------------------------------------------
// Core dispatch primitives
// ---------------------------------------------------------------------------

/// Send a closure to the SPDK reactor thread (fire-and-forget).
pub fn send_to_reactor<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    struct Ctx<F>(Option<F>);

    let ctx = Box::new(Ctx(Some(f)));
    let ctx_ptr = Box::into_raw(ctx) as *mut std::os::raw::c_void;

    unsafe extern "C" fn trampoline<F: FnOnce() + Send + 'static>(arg: *mut std::os::raw::c_void) {
        let mut ctx = Box::from_raw(arg as *mut Ctx<F>);
        if let Some(f) = ctx.0.take() {
            f();
        }
    }

    unsafe {
        let thread = ffi::spdk_thread_get_app_thread();
        assert!(!thread.is_null(), "SPDK app thread not available");
        let rc = ffi::spdk_thread_send_msg(thread, Some(trampoline::<F>), ctx_ptr);
        assert!(rc == 0, "spdk_thread_send_msg failed: rc={}", rc);
    }
}

/// Dispatch a synchronous operation to the reactor and wait for the result.
/// The closure runs on the reactor, completes, and the result is returned.
pub fn dispatch_sync<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // If already on SPDK thread, run directly.
    let on_spdk = unsafe { !ffi::spdk_get_thread().is_null() };
    if on_spdk {
        return f();
    }

    let completion = Arc::new(Completion::<R>::new());
    let comp = completion.clone();
    send_to_reactor(move || {
        let result = f();
        comp.complete(result);
    });
    completion.wait()
}

// ---------------------------------------------------------------------------
// Bdev event callback (required non-null in SPDK v24.09)
// ---------------------------------------------------------------------------

unsafe extern "C" fn bdev_event_cb(
    _type_: ffi::spdk_bdev_event_type,
    _bdev: *mut ffi::spdk_bdev,
    _event_ctx: *mut std::os::raw::c_void,
) {
    // No-op — we handle bdev lifecycle via our own tracking.
}

// ---------------------------------------------------------------------------
// Malloc bdev helpers
// ---------------------------------------------------------------------------

/// Create a malloc bdev on the reactor thread.
pub fn create_malloc_bdev(name: &str, num_blocks: u64, block_size: u32) -> Result<()> {
    let name = name.to_string();
    dispatch_sync(move || {
        let name_c = std::ffi::CString::new(name.as_str()).unwrap();
        unsafe {
            let mut opts: ffi::malloc_bdev_opts = std::mem::zeroed();
            opts.name = name_c.as_ptr() as *mut c_char;
            opts.num_blocks = num_blocks;
            opts.block_size = block_size;
            let mut bdev_ptr: *mut ffi::spdk_bdev = std::ptr::null_mut();
            let rc = ffi::create_malloc_disk(&mut bdev_ptr, &opts);
            if rc != 0 {
                Err(DataPlaneError::BdevError(format!(
                    "create_malloc_disk failed: rc={rc}"
                )))
            } else {
                Ok(())
            }
        }
    })
}

/// Delete a malloc bdev on the reactor thread.
pub fn delete_malloc_bdev(name: &str) -> Result<()> {
    let name = name.to_string();
    let completion = Arc::new(Completion::<i32>::new());
    let comp = completion.clone();

    send_to_reactor(move || {
        let name_c = std::ffi::CString::new(name.as_str()).unwrap();

        unsafe extern "C" fn delete_done(cb_arg: *mut std::os::raw::c_void, rc: i32) {
            let comp = Arc::from_raw(cb_arg as *const Completion<i32>);
            comp.complete(rc);
        }

        unsafe {
            let bdev = ffi::spdk_bdev_get_by_name(name_c.as_ptr() as *const c_char);
            let comp_ptr = Arc::into_raw(comp) as *mut std::os::raw::c_void;
            if bdev.is_null() {
                // bdev not found — treat as success (already deleted).
                let c = Arc::from_raw(comp_ptr as *const Completion<i32>);
                c.complete(0);
                return;
            }
            ffi::delete_malloc_disk(bdev, Some(delete_done), comp_ptr);
        }
    });

    let rc = completion.wait();
    if rc != 0 {
        Err(DataPlaneError::BdevError(format!(
            "delete_malloc_disk failed: rc={rc}"
        )))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Bdev I/O helpers
// ---------------------------------------------------------------------------

/// Context passed through the SPDK I/O callback for read operations.
struct ReadIoCtx {
    desc: *mut ffi::spdk_bdev_desc,
    channel: *mut ffi::spdk_io_channel,
    buf: *mut std::os::raw::c_void,
    buf_len: usize,
    aligned_len: usize,
    completion: Arc<Completion<Result<Vec<u8>>>>,
}

// SAFETY: ReadIoCtx contains raw pointers that are only used on the reactor thread.
unsafe impl Send for ReadIoCtx {}

/// Context passed through the SPDK I/O callback for write operations.
struct WriteIoCtx {
    desc: *mut ffi::spdk_bdev_desc,
    channel: *mut ffi::spdk_io_channel,
    buf: *mut std::os::raw::c_void,
    aligned_len: usize,
    completion: Arc<Completion<Result<()>>>,
}

unsafe impl Send for WriteIoCtx {}

// ---------------------------------------------------------------------------
// Async I/O contexts (for tokio oneshot completion)
// ---------------------------------------------------------------------------

struct AsyncReadIoCtx {
    buf: *mut std::os::raw::c_void,
    buf_len: usize,
    aligned_len: usize,
    sender_ptr: *mut std::os::raw::c_void,
}

unsafe impl Send for AsyncReadIoCtx {}

struct AsyncWriteIoCtx {
    buf: *mut std::os::raw::c_void,
    aligned_len: usize,
    sender_ptr: *mut std::os::raw::c_void,
}

unsafe impl Send for AsyncWriteIoCtx {}

unsafe extern "C" fn async_read_io_done_cb(
    bdev_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut std::os::raw::c_void,
) {
    let io_ctx = Box::from_raw(ctx as *mut AsyncReadIoCtx);

    let result = if success {
        let mut data = vec![0u8; io_ctx.buf_len];
        std::ptr::copy_nonoverlapping(io_ctx.buf as *const u8, data.as_mut_ptr(), io_ctx.buf_len);
        Ok(data)
    } else {
        Err(DataPlaneError::BdevError("bdev read I/O failed".into()))
    };

    ffi::spdk_bdev_free_io(bdev_io);
    release_dma_buf(io_ctx.buf, io_ctx.aligned_len);
    // desc + channel are cached — do NOT close/put them.

    let mut sender: AsyncCompletionSender<Result<Vec<u8>>> =
        AsyncCompletionSender::from_ptr(io_ctx.sender_ptr);
    sender.complete(result);
}

unsafe extern "C" fn async_write_io_done_cb(
    bdev_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut std::os::raw::c_void,
) {
    let io_ctx = Box::from_raw(ctx as *mut AsyncWriteIoCtx);

    let result = if success {
        Ok(())
    } else {
        Err(DataPlaneError::BdevError("bdev write I/O failed".into()))
    };

    ffi::spdk_bdev_free_io(bdev_io);
    release_dma_buf(io_ctx.buf, io_ctx.aligned_len);
    // desc + channel are cached — do NOT close/put them.

    let mut sender: AsyncCompletionSender<Result<()>> =
        AsyncCompletionSender::from_ptr(io_ctx.sender_ptr);
    sender.complete(result);
}

/// Round up `val` to the next multiple of `align`.
fn align_up(val: u64, align: u64) -> u64 {
    if align == 0 {
        return val;
    }
    (val + align - 1) / align * align
}

/// Read `length` bytes from a bdev at `offset`. Dispatches to the reactor
/// thread and blocks until the async I/O completes.
///
/// The actual I/O size is rounded up to the bdev's block size.
/// The returned Vec is truncated to the requested `length`.
pub fn bdev_read(bdev_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let name = bdev_name.to_string();
    let requested_len = length as usize;
    let completion = Arc::new(Completion::<Result<Vec<u8>>>::new());
    let comp = completion.clone();

    send_to_reactor(move || {
        unsafe {
            let name_c = match std::ffi::CString::new(name.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    comp.complete(Err(DataPlaneError::BdevError(format!("invalid name: {e}"))));
                    return;
                }
            };

            let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
            let rc = ffi::spdk_bdev_open_ext(
                name_c.as_ptr() as *const c_char,
                false,
                Some(bdev_event_cb),
                std::ptr::null_mut(),
                &mut desc,
            );
            if rc != 0 {
                comp.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_open_ext('{}') failed: rc={rc}",
                    name
                ))));
                return;
            }

            // Get block size for alignment.
            let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
            let block_size = if bdev.is_null() {
                512
            } else {
                ffi::spdk_bdev_get_block_size(bdev)
            } as u64;
            let aligned_len = align_up(length, block_size);

            let channel = ffi::spdk_bdev_get_io_channel(desc);
            if channel.is_null() {
                ffi::spdk_bdev_close(desc);
                comp.complete(Err(DataPlaneError::BdevError(
                    "spdk_bdev_get_io_channel null".into(),
                )));
                return;
            }

            let buf = acquire_dma_buf(aligned_len as usize);
            if buf.is_null() {
                ffi::spdk_put_io_channel(channel);
                ffi::spdk_bdev_close(desc);
                comp.complete(Err(DataPlaneError::BdevError(
                    "DMA buffer acquisition failed".into(),
                )));
                return;
            }

            let io_ctx = Box::new(ReadIoCtx {
                desc,
                channel,
                buf,
                buf_len: requested_len,
                aligned_len: aligned_len as usize,
                completion: comp,
            });
            let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

            let rc = ffi::spdk_bdev_read(
                desc,
                channel,
                buf,
                offset,
                aligned_len,
                Some(read_io_done_cb),
                io_ctx_ptr,
            );
            if rc != 0 {
                let ctx = Box::from_raw(io_ctx_ptr as *mut ReadIoCtx);
                release_dma_buf(ctx.buf, ctx.aligned_len);
                ffi::spdk_put_io_channel(ctx.channel);
                ffi::spdk_bdev_close(ctx.desc);
                ctx.completion
                    .complete(Err(DataPlaneError::BdevError(format!(
                        "spdk_bdev_read submit failed: rc={rc}"
                    ))));
            }
        }
    });

    completion.wait()
}

/// Write `data` to a bdev at `offset`. Dispatches to the reactor thread
/// and blocks until the async I/O completes.
///
/// The actual I/O size is rounded up to the bdev's block size.
/// Padding bytes are zeros.
pub fn bdev_write(bdev_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let name = bdev_name.to_string();
    let data = data.to_vec();
    let completion = Arc::new(Completion::<Result<()>>::new());
    let comp = completion.clone();

    send_to_reactor(move || {
        unsafe {
            let name_c = match std::ffi::CString::new(name.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    comp.complete(Err(DataPlaneError::BdevError(format!("invalid name: {e}"))));
                    return;
                }
            };

            let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
            let rc = ffi::spdk_bdev_open_ext(
                name_c.as_ptr() as *const c_char,
                true,
                Some(bdev_event_cb),
                std::ptr::null_mut(),
                &mut desc,
            );
            if rc != 0 {
                comp.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_open_ext('{}') failed: rc={rc}",
                    name
                ))));
                return;
            }

            // Get block size for alignment.
            let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
            let block_size = if bdev.is_null() {
                512
            } else {
                ffi::spdk_bdev_get_block_size(bdev)
            } as u64;
            let aligned_len = align_up(data.len() as u64, block_size);

            let channel = ffi::spdk_bdev_get_io_channel(desc);
            if channel.is_null() {
                ffi::spdk_bdev_close(desc);
                comp.complete(Err(DataPlaneError::BdevError(
                    "spdk_bdev_get_io_channel null".into(),
                )));
                return;
            }

            let buf = acquire_dma_buf(aligned_len as usize);
            if buf.is_null() {
                ffi::spdk_put_io_channel(channel);
                ffi::spdk_bdev_close(desc);
                comp.complete(Err(DataPlaneError::BdevError(
                    "DMA buffer acquisition failed".into(),
                )));
                return;
            }
            // Zero the entire buffer, then copy data into it.
            std::ptr::write_bytes(buf as *mut u8, 0, aligned_len as usize);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf as *mut u8, data.len());

            let io_ctx = Box::new(WriteIoCtx {
                desc,
                channel,
                buf,
                aligned_len: aligned_len as usize,
                completion: comp,
            });
            let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

            let rc = ffi::spdk_bdev_write(
                desc,
                channel,
                buf,
                offset,
                aligned_len,
                Some(write_io_done_cb),
                io_ctx_ptr,
            );
            if rc != 0 {
                let ctx = Box::from_raw(io_ctx_ptr as *mut WriteIoCtx);
                release_dma_buf(ctx.buf, ctx.aligned_len);
                ffi::spdk_put_io_channel(ctx.channel);
                ffi::spdk_bdev_close(ctx.desc);
                ctx.completion
                    .complete(Err(DataPlaneError::BdevError(format!(
                        "spdk_bdev_write submit failed: rc={rc}"
                    ))));
            }
        }
    });

    completion.wait()
}

/// Query bdev size on the reactor thread.
pub fn query_bdev(name: &str) -> Result<(u64, u32)> {
    let name = name.to_string();
    dispatch_sync(move || {
        let name_c = std::ffi::CString::new(name.as_str()).unwrap();
        unsafe {
            let bdev = ffi::spdk_bdev_get_by_name(name_c.as_ptr() as *const c_char);
            if bdev.is_null() {
                Err(DataPlaneError::BdevError(format!(
                    "bdev '{}' not found",
                    name
                )))
            } else {
                let num_blocks = ffi::spdk_bdev_get_num_blocks(bdev);
                let block_size = ffi::spdk_bdev_get_block_size(bdev);
                Ok((num_blocks, block_size))
            }
        }
    })
}

// ---------------------------------------------------------------------------
// SPDK I/O completion callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn read_io_done_cb(
    bdev_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut std::os::raw::c_void,
) {
    let io_ctx = Box::from_raw(ctx as *mut ReadIoCtx);

    let result = if success {
        let mut data = vec![0u8; io_ctx.buf_len];
        std::ptr::copy_nonoverlapping(io_ctx.buf as *const u8, data.as_mut_ptr(), io_ctx.buf_len);
        Ok(data)
    } else {
        Err(DataPlaneError::BdevError("bdev read I/O failed".into()))
    };

    ffi::spdk_bdev_free_io(bdev_io);
    release_dma_buf(io_ctx.buf, io_ctx.aligned_len);
    ffi::spdk_put_io_channel(io_ctx.channel);
    ffi::spdk_bdev_close(io_ctx.desc);

    io_ctx.completion.complete(result);
}

unsafe extern "C" fn write_io_done_cb(
    bdev_io: *mut ffi::spdk_bdev_io,
    success: bool,
    ctx: *mut std::os::raw::c_void,
) {
    let io_ctx = Box::from_raw(ctx as *mut WriteIoCtx);

    let result = if success {
        Ok(())
    } else {
        Err(DataPlaneError::BdevError("bdev write I/O failed".into()))
    };

    ffi::spdk_bdev_free_io(bdev_io);
    release_dma_buf(io_ctx.buf, io_ctx.aligned_len);
    ffi::spdk_put_io_channel(io_ctx.channel);
    ffi::spdk_bdev_close(io_ctx.desc);

    io_ctx.completion.complete(result);
}

// ---------------------------------------------------------------------------
// Async dispatch and bdev I/O (using tokio oneshot via AsyncCompletion)
// ---------------------------------------------------------------------------

use super::context::{AsyncCompletion, AsyncCompletionSender};

/// Dispatch a closure to the SPDK reactor and return a future for the result.
/// This is the async equivalent of [`dispatch_sync`].
pub async fn dispatch_async<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    // If already on SPDK thread, run directly.
    let on_spdk = unsafe { !ffi::spdk_get_thread().is_null() };
    if on_spdk {
        return f();
    }

    let (completion, mut sender) = AsyncCompletion::<R>::new();

    send_to_reactor(move || {
        let result = f();
        sender.complete(result);
    });

    completion.wait().await
}

/// Async version of [`bdev_read`]. Returns a future instead of blocking.
pub async fn bdev_read_async(bdev_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let name = bdev_name.to_string();
    let requested_len = length as usize;
    let (completion, sender) = AsyncCompletion::<Result<Vec<u8>>>::new();
    let sender_addr = sender.into_ptr() as usize;

    send_to_reactor(move || unsafe {
        let sender_ptr = sender_addr as *mut std::os::raw::c_void;

        // Open with write=true so the cached handle works for both reads and writes.
        let (desc, channel, block_size) = match get_or_open_bdev(&name, true) {
            Ok(v) => v,
            Err(e) => {
                let mut s: AsyncCompletionSender<Result<Vec<u8>>> =
                    AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(e)));
                return;
            }
        };

        let aligned_len = align_up(length, block_size);

        let buf = acquire_dma_buf(aligned_len as usize);
        if buf.is_null() {
            let mut s: AsyncCompletionSender<Result<Vec<u8>>> =
                AsyncCompletionSender::from_ptr(sender_ptr);
            s.complete(Err(DataPlaneError::BdevError(
                "DMA buffer acquisition failed".into(),
            )));
            return;
        }

        let io_ctx = Box::new(AsyncReadIoCtx {
            buf,
            buf_len: requested_len,
            aligned_len: aligned_len as usize,
            sender_ptr,
        });
        let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

        let rc = ffi::spdk_bdev_read(
            desc,
            channel,
            buf,
            offset,
            aligned_len,
            Some(async_read_io_done_cb),
            io_ctx_ptr,
        );
        if rc != 0 {
            let ctx = Box::from_raw(io_ctx_ptr as *mut AsyncReadIoCtx);
            release_dma_buf(ctx.buf, ctx.aligned_len);
            let mut s: AsyncCompletionSender<Result<Vec<u8>>> =
                AsyncCompletionSender::from_ptr(ctx.sender_ptr);
            s.complete(Err(DataPlaneError::BdevError(format!(
                "spdk_bdev_read submit failed: rc={rc}"
            ))));
        }
    });

    completion.wait().await
}

/// Async version of [`bdev_write`]. Returns a future instead of blocking.
pub async fn bdev_write_async(bdev_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let name = bdev_name.to_string();
    let data = data.to_vec();
    let (completion, sender) = AsyncCompletion::<Result<()>>::new();
    let sender_addr = sender.into_ptr() as usize;

    send_to_reactor(move || unsafe {
        let sender_ptr = sender_addr as *mut std::os::raw::c_void;

        let (desc, channel, block_size) = match get_or_open_bdev(&name, true) {
            Ok(v) => v,
            Err(e) => {
                let mut s: AsyncCompletionSender<Result<()>> =
                    AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(e)));
                return;
            }
        };

        let aligned_len = align_up(data.len() as u64, block_size);

        let buf = acquire_dma_buf(aligned_len as usize);
        if buf.is_null() {
            let mut s: AsyncCompletionSender<Result<()>> =
                AsyncCompletionSender::from_ptr(sender_ptr);
            s.complete(Err(DataPlaneError::BdevError(
                "DMA buffer acquisition failed".into(),
            )));
            return;
        }
        std::ptr::write_bytes(buf as *mut u8, 0, aligned_len as usize);
        std::ptr::copy_nonoverlapping(data.as_ptr(), buf as *mut u8, data.len());

        let io_ctx = Box::new(AsyncWriteIoCtx {
            buf,
            aligned_len: aligned_len as usize,
            sender_ptr,
        });
        let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

        let rc = ffi::spdk_bdev_write(
            desc,
            channel,
            buf,
            offset,
            aligned_len,
            Some(async_write_io_done_cb),
            io_ctx_ptr,
        );
        if rc != 0 {
            let ctx = Box::from_raw(io_ctx_ptr as *mut AsyncWriteIoCtx);
            release_dma_buf(ctx.buf, ctx.aligned_len);
            let mut s: AsyncCompletionSender<Result<()>> =
                AsyncCompletionSender::from_ptr(ctx.sender_ptr);
            s.complete(Err(DataPlaneError::BdevError(format!(
                "spdk_bdev_write submit failed: rc={rc}"
            ))));
        }
    });

    completion.wait().await
}
