# Chunk Engine Redesign — Plan 1: Async Foundation + Backend Engine

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce tokio async runtime, rewrite SPDK reactor bridge from condvar-blocking to oneshot channels, and implement the new `ChunkStore` async trait with `FileChunkStore` and `BdevChunkStore` implementations.

**Architecture:** The backend engine is the bottom layer. It provides a unified async `ChunkStore` trait with two implementations: `FileChunkStore` (POSIX files in hash-prefix directories, no SPDK) and `BdevChunkStore` (bitmap-allocated slots on SPDK bdevs, refactored from `chunk_io.rs`). The async foundation rewrites `reactor_dispatch` to use `tokio::sync::oneshot` instead of condvar-blocking `Completion`, so that SPDK I/O can be `.await`ed without blocking tokio worker threads.

**Tech Stack:** Rust, tokio, async-trait, SPDK (via existing FFI bindings), sha2, crc32c, hex

**Spec:** `docs/specs/2026-03-13-chunk-engine-redesign.md`

**Plan series:** This is Plan 1 of 4. Old backend code (`backend/chunk.rs`, `backend/raw_disk.rs`, `backend/lvm.rs`, `backend/traits.rs`) remains untouched — it will be removed in Plan 4 after the chunk engine and policy engine are in place.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/backend/chunk_store.rs` | `ChunkStore` async trait + `ChunkStoreStats` + `ChunkHeader` (inline tests) |
| `src/backend/file_store.rs` | `FileChunkStore` — POSIX file-per-chunk with hash-prefix dirs (inline tests) |
| `src/backend/bdev_store.rs` | `BdevChunkStore` — bitmap allocator + SPDK bdev I/O via oneshot (inline tests) |

### Modified files

| File | Change |
|------|--------|
| `Cargo.toml` | Add `tokio`, `async-trait` dependencies |
| `src/backend/mod.rs` | Add `pub mod chunk_store; pub mod file_store; pub mod bdev_store;` |
| `src/spdk/context.rs` | Add `AsyncCompletion<T>` using `tokio::sync::oneshot` |
| `src/spdk/reactor_dispatch.rs` | Add `async fn dispatch_async()`, `async fn bdev_read_async()`, `async fn bdev_write_async()` with persistent descriptor |
| `src/main.rs` | Wrap SPDK startup in tokio runtime |
| `src/error.rs` | Add `IoError` variant wrapping `std::io::Error` |

### Unchanged files (old code stays)

All existing files in `src/backend/` (`traits.rs`, `chunk.rs`, `raw_disk.rs`, `lvm.rs`), `src/bdev/` (`chunk_io.rs`, `erasure.rs`, `replica.rs`), `src/jsonrpc/`, `src/spdk/bdev_manager.rs`, `src/spdk/nvmf_manager.rs`, `src/spdk/env.rs` — unchanged. The old sync code continues to work. New async code is additive.

---

## Chunk 1: Async Foundation

### Task 1: Add tokio and async-trait dependencies

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add dependencies to Cargo.toml**

Add after the `clap` line in `[dependencies]`:

```toml
tokio = { version = "1", features = ["rt-multi-thread", "sync", "fs", "macros"] }
async-trait = "0.1"
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -20`
Expected: Compiles successfully (tokio and async-trait are additive, no code changes yet).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Chore] Add tokio and async-trait dependencies

Foundation for async backend engine and reactor dispatch rewrite."
```

---

### Task 2: (Removed — IoError variant already exists in error.rs at line 26)

---

### Task 3: Add AsyncCompletion to spdk/context.rs

**Files:**
- Modify: `src/spdk/context.rs`

The existing `Completion<T>` (condvar-based) stays for backward compatibility. We add `AsyncCompletion<T>` using `tokio::sync::oneshot` for use in async code.

- [ ] **Step 1: Add AsyncCompletion**

Append to `src/spdk/context.rs`:

```rust
/// An async-compatible one-shot completion channel using tokio::sync::oneshot.
///
/// Unlike [`Completion`] (which blocks OS threads via Condvar), this can be
/// `.await`ed without blocking tokio worker threads.
pub struct AsyncCompletion<T> {
    rx: tokio::sync::oneshot::Receiver<T>,
}

/// The sender half of an async completion, passed to SPDK callbacks.
pub struct AsyncCompletionSender<T> {
    tx: Option<tokio::sync::oneshot::Sender<T>>,
}

impl<T> AsyncCompletion<T> {
    /// Create a new async completion pair (receiver, sender).
    pub fn new() -> (Self, AsyncCompletionSender<T>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            AsyncCompletion { rx },
            AsyncCompletionSender { tx: Some(tx) },
        )
    }

    /// Await the completion result.
    pub async fn wait(self) -> T {
        self.rx.await.expect("AsyncCompletion sender dropped without sending")
    }
}

impl<T> AsyncCompletionSender<T> {
    /// Signal the completion with a value.
    pub fn complete(&mut self, value: T) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(value);
        }
    }

    /// Convert to a raw pointer for passing through SPDK's `void *cb_arg`.
    pub fn into_ptr(self) -> *mut std::os::raw::c_void {
        Box::into_raw(Box::new(self)) as *mut std::os::raw::c_void
    }

    /// Recover from a raw pointer.
    ///
    /// # Safety
    /// The pointer must have been created by [`into_ptr`] and not yet consumed.
    pub unsafe fn from_ptr(ptr: *mut std::os::raw::c_void) -> Self {
        *Box::from_raw(ptr as *mut Self)
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -5`
Expected: Success. No existing code is changed.

- [ ] **Step 3: Commit**

```bash
git add src/spdk/context.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add AsyncCompletion using tokio::sync::oneshot

Async-compatible alternative to Completion<T> for use in the new
async backend engine. The existing condvar-based Completion stays
for backward compatibility with sync JSON-RPC handlers."
```

---

### Task 4: Add async dispatch and bdev I/O functions to reactor_dispatch.rs

**Files:**
- Modify: `src/spdk/reactor_dispatch.rs`

Add async versions of `dispatch_sync`, `bdev_read`, and `bdev_write`. The existing sync functions stay unchanged. The async versions use `AsyncCompletion` instead of `Completion`.

- [ ] **Step 1: Add async dispatch function**

Append to `src/spdk/reactor_dispatch.rs`, after the existing `dispatch_sync` function:

```rust
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
```

- [ ] **Step 2: Add async bdev_read**

Append to `src/spdk/reactor_dispatch.rs`:

```rust
/// Async version of [`bdev_read`]. Returns a future instead of blocking.
pub async fn bdev_read_async(bdev_name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
    let name = bdev_name.to_string();
    let requested_len = length as usize;
    let (completion, sender) = AsyncCompletion::<Result<Vec<u8>>>::new();
    let sender_ptr = sender.into_ptr();

    send_to_reactor(move || {
        unsafe {
            let name_c = match std::ffi::CString::new(name.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                    s.complete(Err(DataPlaneError::BdevError(format!("invalid name: {e}"))));
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
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_open_ext('{}') failed: rc={rc}", name
                ))));
                return;
            }

            let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
            let block_size = if bdev.is_null() { 512 } else {
                ffi::spdk_bdev_get_block_size(bdev)
            } as u64;
            let aligned_len = align_up(length, block_size);

            let channel = ffi::spdk_bdev_get_io_channel(desc);
            if channel.is_null() {
                ffi::spdk_bdev_close(desc);
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(
                    "spdk_bdev_get_io_channel null".into(),
                )));
                return;
            }

            let buf = ffi::spdk_dma_malloc(aligned_len as usize, 0x1000, std::ptr::null_mut());
            if buf.is_null() {
                ffi::spdk_put_io_channel(channel);
                ffi::spdk_bdev_close(desc);
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError("spdk_dma_malloc failed".into())));
                return;
            }

            let io_ctx = Box::new(AsyncReadIoCtx {
                desc,
                channel,
                buf,
                buf_len: requested_len,
                sender_ptr,
            });
            let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

            let rc = ffi::spdk_bdev_read(
                desc, channel, buf, offset, aligned_len,
                Some(async_read_io_done_cb),
                io_ctx_ptr,
            );
            if rc != 0 {
                let ctx = Box::from_raw(io_ctx_ptr as *mut AsyncReadIoCtx);
                ffi::spdk_dma_free(ctx.buf);
                ffi::spdk_put_io_channel(ctx.channel);
                ffi::spdk_bdev_close(ctx.desc);
                let mut s = AsyncCompletionSender::from_ptr(ctx.sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_read submit failed: rc={rc}"
                ))));
            }
        }
    });

    completion.wait().await
}
```

- [ ] **Step 3: Add async bdev_write**

Append to `src/spdk/reactor_dispatch.rs`:

```rust
/// Async version of [`bdev_write`]. Returns a future instead of blocking.
pub async fn bdev_write_async(bdev_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let name = bdev_name.to_string();
    let data = data.to_vec();
    let (completion, sender) = AsyncCompletion::<Result<()>>::new();
    let sender_ptr = sender.into_ptr();

    send_to_reactor(move || {
        unsafe {
            let name_c = match std::ffi::CString::new(name.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                    s.complete(Err(DataPlaneError::BdevError(format!("invalid name: {e}"))));
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
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_open_ext('{}') failed: rc={rc}", name
                ))));
                return;
            }

            let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
            let block_size = if bdev.is_null() { 512 } else {
                ffi::spdk_bdev_get_block_size(bdev)
            } as u64;
            let aligned_len = align_up(data.len() as u64, block_size);

            let channel = ffi::spdk_bdev_get_io_channel(desc);
            if channel.is_null() {
                ffi::spdk_bdev_close(desc);
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(
                    "spdk_bdev_get_io_channel null".into(),
                )));
                return;
            }

            let buf = ffi::spdk_dma_malloc(aligned_len as usize, 0x1000, std::ptr::null_mut());
            if buf.is_null() {
                ffi::spdk_put_io_channel(channel);
                ffi::spdk_bdev_close(desc);
                let mut s = AsyncCompletionSender::from_ptr(sender_ptr);
                s.complete(Err(DataPlaneError::BdevError("spdk_dma_malloc failed".into())));
                return;
            }
            std::ptr::write_bytes(buf as *mut u8, 0, aligned_len as usize);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf as *mut u8, data.len());

            let io_ctx = Box::new(AsyncWriteIoCtx {
                desc,
                channel,
                buf,
                sender_ptr,
            });
            let io_ctx_ptr = Box::into_raw(io_ctx) as *mut std::os::raw::c_void;

            let rc = ffi::spdk_bdev_write(
                desc, channel, buf, offset, aligned_len,
                Some(async_write_io_done_cb),
                io_ctx_ptr,
            );
            if rc != 0 {
                let ctx = Box::from_raw(io_ctx_ptr as *mut AsyncWriteIoCtx);
                ffi::spdk_dma_free(ctx.buf);
                ffi::spdk_put_io_channel(ctx.channel);
                ffi::spdk_bdev_close(ctx.desc);
                let mut s = AsyncCompletionSender::from_ptr(ctx.sender_ptr);
                s.complete(Err(DataPlaneError::BdevError(format!(
                    "spdk_bdev_write submit failed: rc={rc}"
                ))));
            }
        }
    });

    completion.wait().await
}
```

- [ ] **Step 4: Add async I/O context structs and callbacks**

Add these structs and callbacks above the async functions (after the existing `WriteIoCtx`):

```rust
// ---------------------------------------------------------------------------
// Async I/O contexts (for tokio oneshot completion)
// ---------------------------------------------------------------------------

struct AsyncReadIoCtx {
    desc: *mut ffi::spdk_bdev_desc,
    channel: *mut ffi::spdk_io_channel,
    buf: *mut std::os::raw::c_void,
    buf_len: usize,
    sender_ptr: *mut std::os::raw::c_void,
}

unsafe impl Send for AsyncReadIoCtx {}

struct AsyncWriteIoCtx {
    desc: *mut ffi::spdk_bdev_desc,
    channel: *mut ffi::spdk_io_channel,
    buf: *mut std::os::raw::c_void,
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
    ffi::spdk_dma_free(io_ctx.buf);
    ffi::spdk_put_io_channel(io_ctx.channel);
    ffi::spdk_bdev_close(io_ctx.desc);

    let mut sender = AsyncCompletionSender::from_ptr(io_ctx.sender_ptr);
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
    ffi::spdk_dma_free(io_ctx.buf);
    ffi::spdk_put_io_channel(io_ctx.channel);
    ffi::spdk_bdev_close(io_ctx.desc);

    let mut sender = AsyncCompletionSender::from_ptr(io_ctx.sender_ptr);
    sender.complete(result);
}
```

- [ ] **Step 5: Verify it compiles**

Run: `cargo check 2>&1 | tail -10`
Expected: Success. All new code is additive — no existing functions changed.

- [ ] **Step 6: Commit**

```bash
git add src/spdk/reactor_dispatch.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add async reactor dispatch and bdev I/O functions

dispatch_async(), bdev_read_async(), bdev_write_async() use
tokio::sync::oneshot instead of condvar-blocking Completion.
Existing sync functions unchanged for backward compatibility."
```

---

### Task 5: Wrap main() with tokio runtime

**Files:**
- Modify: `src/main.rs`

The SPDK reactor thread must remain the main thread (SPDK requires it). We start a tokio runtime on background threads before calling `spdk::run()`. The runtime handle is stored globally so async code anywhere in the codebase can spawn tasks or block on futures.

- [ ] **Step 1: Add tokio runtime to main.rs**

Replace the `main()` function body in `src/main.rs`. The module declarations and `Args` struct stay unchanged. Only the `fn main()` body changes:

```rust
/// Global tokio runtime handle — available to all modules.
static TOKIO_HANDLE: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

/// Get the global tokio runtime handle.
pub fn tokio_handle() -> &'static tokio::runtime::Handle {
    TOKIO_HANDLE.get().expect("tokio runtime not initialized")
}

fn main() {
    let args = Args::parse();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(&args.log_level),
    )
    .init();

    info!(
        "novastor-dataplane starting (reactor_mask={}, mem={}MB)",
        args.reactor_mask, args.mem_size
    );

    // Start tokio runtime on background threads.
    // SPDK requires the main thread for its reactor, so tokio runs alongside.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to create tokio runtime");
    TOKIO_HANDLE.set(runtime.handle().clone()).expect("tokio handle already set");
    info!("tokio runtime started (2 worker threads)");

    let config = config::DataPlaneConfig {
        rpc_socket: args.rpc_socket,
        reactor_mask: args.reactor_mask,
        mem_size: args.mem_size,
        transport_type: args.transport_type,
        listen_address: args.listen_address,
        listen_port: args.listen_port,
    };

    // spdk::run() blocks in the SPDK reactor loop on the main thread.
    if let Err(e) = spdk::run(config) {
        error!("data plane failed: {}", e);
        std::process::exit(1);
    }

    // Shut down tokio runtime after SPDK exits.
    runtime.shutdown_background();
    info!("novastor-dataplane stopped");
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -5`
Expected: Success.

- [ ] **Step 3: Commit**

```bash
git add src/main.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Start tokio runtime alongside SPDK reactor

Tokio runs on background threads (2 workers). SPDK keeps the main
thread for its reactor loop. Global TOKIO_HANDLE available for
spawning async tasks from anywhere in the codebase."
```

---

## Chunk 2: ChunkStore Trait + FileChunkStore

### Task 6: Create ChunkStore async trait and ChunkHeader

**Files:**
- Create: `src/backend/chunk_store.rs`
- Modify: `src/backend/mod.rs`

- [ ] **Step 1: Implement ChunkStore trait, ChunkHeader, and ChunkStoreStats**

Create `src/backend/chunk_store.rs`:

```rust
//! Async ChunkStore trait — the unified interface for all backend storage.
//!
//! Backends persist and retrieve chunk data. They do NOT own metadata.
//! Hashing, checksums, dedup, and location tracking happen in layers above.

use crate::error::Result;
use async_trait::async_trait;

/// Default chunk size: 4 MiB.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Fixed-size header prepended to every chunk stored by a backend.
///
/// Total size: 16 bytes. The chunk engine prepends this before calling
/// `ChunkStore::put()`. The backend stores it as part of the chunk data.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ChunkHeader {
    /// Magic bytes: b"NVAC"
    pub magic: [u8; 4],
    /// Header format version (currently 1).
    pub version: u8,
    /// Flags: 0x00 = normal chunk, 0x01 = erasure shard.
    pub flags: u8,
    /// CRC-32C of the data portion (after header).
    pub checksum: u32,
    /// Actual data length in bytes (may be < CHUNK_SIZE for partial/last chunk).
    pub data_len: u32,
    /// Reserved for future use.
    pub _reserved: [u8; 2],
}

impl ChunkHeader {
    pub const SIZE: usize = 16;

    /// Serialize to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..10].copy_from_slice(&self.checksum.to_le_bytes());
        buf[10..14].copy_from_slice(&self.data_len.to_le_bytes());
        buf[14..16].copy_from_slice(&self._reserved);
        buf
    }

    /// Deserialize from bytes. Returns error if magic is invalid.
    pub fn from_bytes(buf: &[u8; Self::SIZE]) -> Result<Self> {
        if &buf[0..4] != b"NVAC" {
            return Err(crate::error::DataPlaneError::BdevError(
                "invalid ChunkHeader magic".into(),
            ));
        }
        Ok(Self {
            magic: [buf[0], buf[1], buf[2], buf[3]],
            version: buf[4],
            flags: buf[5],
            checksum: u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]),
            data_len: u32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]),
            _reserved: [buf[14], buf[15]],
        })
    }
}

/// Storage statistics for a chunk store backend.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChunkStoreStats {
    pub backend_name: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub data_bytes: u64,
    pub chunk_count: u64,
}

/// The unified async interface for all backend chunk storage.
///
/// Backends are pure I/O. They store and retrieve byte blobs keyed by chunk_id.
/// The chunk engine above handles content addressing, checksums, and metadata.
#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// Store chunk data. Caller provides the chunk_id (SHA-256 hash).
    /// Data includes a ChunkHeader prepended by the chunk engine.
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()>;

    /// Retrieve chunk data by ID. Returns data including ChunkHeader.
    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>>;

    /// Delete chunk data by ID.
    async fn delete(&self, chunk_id: &str) -> Result<()>;

    /// Check if a chunk exists locally.
    async fn exists(&self, chunk_id: &str) -> Result<bool>;

    /// Local storage statistics.
    async fn stats(&self) -> Result<ChunkStoreStats>;
}
```

- [ ] **Step 2: Add module to backend/mod.rs**

Add to `src/backend/mod.rs`:

```rust
pub mod chunk_store;
```

- [ ] **Step 3: Add unit tests inline**

Add to the bottom of `src/backend/chunk_store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_header_size_is_16_bytes() {
        assert_eq!(std::mem::size_of::<ChunkHeader>(), 16);
    }

    #[test]
    fn chunk_header_serialization_roundtrip() {
        let header = ChunkHeader {
            magic: *b"NVAC",
            version: 1,
            flags: 0x00,
            checksum: 0xDEADBEEF,
            data_len: 4 * 1024 * 1024,
            _reserved: [0, 0],
        };
        let bytes = header.to_bytes();
        let parsed = ChunkHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.magic, *b"NVAC");
        assert_eq!(parsed.checksum, 0xDEADBEEF);
        assert_eq!(parsed.data_len, 4 * 1024 * 1024);
    }

    #[test]
    fn chunk_header_invalid_magic_rejected() {
        let mut bytes = [0u8; 16];
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(ChunkHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn chunk_size_is_4mb() {
        assert_eq!(CHUNK_SIZE, 4 * 1024 * 1024);
    }

    #[test]
    fn chunk_store_stats_fields() {
        let stats = ChunkStoreStats {
            backend_name: "test".to_string(),
            total_bytes: 1024,
            used_bytes: 512,
            data_bytes: 400,
            chunk_count: 5,
        };
        assert_eq!(stats.chunk_count, 5);
        assert_eq!(stats.backend_name, "test");
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test chunk_store::tests 2>&1 | tail -10`
Expected: All 5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/backend/chunk_store.rs src/backend/mod.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add ChunkStore async trait, ChunkHeader, and ChunkStoreStats

The unified async interface for all backend storage. Backends
implement put/get/delete/exists/stats. ChunkHeader (16 bytes)
is prepended to every stored chunk by the chunk engine above."
```

---

### Task 7: Implement FileChunkStore

**Files:**
- Create: `src/backend/file_store.rs`
- Modify: `src/backend/mod.rs`

- [ ] **Step 1: Add tempfile dev-dependency**

Add to `Cargo.toml`:

```toml
[dev-dependencies]
tempfile = "3"
```

- [ ] **Step 2: Implement FileChunkStore with inline tests**

Create `src/backend/file_store.rs` with the full implementation and tests together (tests use `super::*` which requires the implementation to exist in the same file):

```rust
//! FileChunkStore — stores chunks as files on an existing filesystem.
//!
//! Layout: `<base_dir>/chunks/<ab>/<cd>/<abcdef0123456789...>`
//! Two-level hash-prefix subdirectories for even distribution.
//!
//! I/O uses std::fs wrapped in tokio::task::spawn_blocking.
//! Writes use atomic rename: write to temp file, then rename.

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, ChunkStoreStats};
use crate::error::{DataPlaneError, Result};
use async_trait::async_trait;
use std::path::PathBuf;

/// A chunk store backed by POSIX files in hash-prefix directories.
pub struct FileChunkStore {
    base_dir: PathBuf,
    chunks_dir: PathBuf,
}

impl FileChunkStore {
    /// Create a new FileChunkStore rooted at `base_dir`.
    /// Creates the `chunks/` subdirectory if it doesn't exist.
    pub async fn new(base_dir: PathBuf) -> Result<Self> {
        let chunks_dir = base_dir.join("chunks");
        let cd = chunks_dir.clone();
        tokio::task::spawn_blocking(move || std::fs::create_dir_all(&cd))
            .await
            .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
            .map_err(|e| DataPlaneError::IoError(e))?;

        Ok(Self { base_dir, chunks_dir })
    }

    /// Build the file path for a chunk: `<chunks_dir>/<ab>/<cd>/<chunk_id>`
    fn chunk_path(&self, chunk_id: &str) -> PathBuf {
        let p1 = &chunk_id[0..2];
        let p2 = &chunk_id[2..4];
        self.chunks_dir.join(p1).join(p2).join(chunk_id)
    }
}

#[async_trait]
impl ChunkStore for FileChunkStore {
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        let path = self.chunk_path(chunk_id);
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            // Idempotent: if chunk already exists, skip.
            if path.exists() {
                return Ok(());
            }

            // Ensure parent directory exists.
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Atomic write: temp file + rename.
            let tmp_path = path.with_extension("tmp");
            std::fs::write(&tmp_path, &data)?;
            std::fs::rename(&tmp_path, &path)?;
            Ok(())
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let path = self.chunk_path(chunk_id);
        let cid = chunk_id.to_string();

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(DataPlaneError::BdevError(format!(
                    "chunk not found: {}", &cid[..16.min(cid.len())]
                )));
            }
            std::fs::read(&path).map_err(|e| DataPlaneError::IoError(e))
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        let path = self.chunk_path(chunk_id);
        let cid = chunk_id.to_string();

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(DataPlaneError::BdevError(format!(
                    "chunk not found: {}", &cid[..16.min(cid.len())]
                )));
            }
            std::fs::remove_file(&path).map_err(|e| DataPlaneError::IoError(e))
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn exists(&self, chunk_id: &str) -> Result<bool> {
        let path = self.chunk_path(chunk_id);
        tokio::task::spawn_blocking(move || Ok(path.exists()))
            .await
            .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn stats(&self) -> Result<ChunkStoreStats> {
        let chunks_dir = self.chunks_dir.clone();
        let base_dir = self.base_dir.clone();

        tokio::task::spawn_blocking(move || {
            let mut chunk_count = 0u64;
            let mut data_bytes = 0u64;

            // Walk the 2-level directory tree.
            if let Ok(entries) = std::fs::read_dir(&chunks_dir) {
                for l1 in entries.flatten() {
                    if !l1.path().is_dir() { continue; }
                    if let Ok(l2_entries) = std::fs::read_dir(l1.path()) {
                        for l2 in l2_entries.flatten() {
                            if !l2.path().is_dir() { continue; }
                            if let Ok(files) = std::fs::read_dir(l2.path()) {
                                for file in files.flatten() {
                                    if file.path().is_file() {
                                        chunk_count += 1;
                                        if let Ok(meta) = file.metadata() {
                                            data_bytes += meta.len();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Get filesystem capacity via statvfs.
            let (total_bytes, used_bytes) = get_fs_usage(&base_dir);

            Ok(ChunkStoreStats {
                backend_name: base_dir.to_string_lossy().to_string(),
                total_bytes,
                used_bytes,
                data_bytes,
                chunk_count,
            })
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }
}

/// Get filesystem total and used bytes.
/// Uses libc::statvfs on Linux. Returns (0, 0) on unsupported platforms.
#[cfg(target_os = "linux")]
fn get_fs_usage(path: &std::path::Path) -> (u64, u64) {
    use std::ffi::CString;
    let path_c = match CString::new(path.to_string_lossy().as_bytes()) {
        Ok(c) => c,
        Err(_) => return (0, 0),
    };
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path_c.as_ptr(), &mut stat) == 0 {
            let total = stat.f_blocks * stat.f_frsize;
            let free = stat.f_bfree * stat.f_frsize;
            (total, total.saturating_sub(free))
        } else {
            (0, 0)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn get_fs_usage(_path: &std::path::Path) -> (u64, u64) {
    (0, 0) // Filesystem stats not available on this platform.
}
```

Then add the `#[cfg(test)]` module at the bottom of the same file with the 7 tests shown above (put_and_get_roundtrip, exists_returns_false_then_true, delete_removes_chunk, get_nonexistent_returns_error, hash_prefix_directory_layout, stats_reports_correctly, put_is_idempotent).

- [ ] **Step 3: Add module to backend/mod.rs**

Add to `src/backend/mod.rs`:

```rust
pub mod file_store;
```

- [ ] **Step 4: Run tests**

Run: `cargo test file_store::tests 2>&1 | tail -15`
Expected: All 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/backend/file_store.rs src/backend/mod.rs Cargo.toml
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Implement FileChunkStore

Stores chunks as files in hash-prefix directories:
<base_dir>/chunks/<ab>/<cd>/<chunk_id>

Uses spawn_blocking for all I/O. Atomic writes via temp file + rename.
Includes statvfs for capacity reporting. 7 unit tests passing."
```

---

## Chunk 3: BdevChunkStore

### Task 8: Implement bitmap allocator (no SPDK dependency)

**Files:**
- Create: `src/backend/bdev_store.rs` (start with allocator only)

The bitmap allocator is pure logic — no SPDK, fully testable.

- [ ] **Step 1: Write the failing tests**

Create `src/backend/bdev_store.rs` with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_first_slot() {
        let mut alloc = BitmapAllocator::new(100);
        assert_eq!(alloc.allocate(), Some(0));
        assert_eq!(alloc.used_slots(), 1);
    }

    #[test]
    fn allocate_sequential() {
        let mut alloc = BitmapAllocator::new(10);
        assert_eq!(alloc.allocate(), Some(0));
        assert_eq!(alloc.allocate(), Some(1));
        assert_eq!(alloc.allocate(), Some(2));
        assert_eq!(alloc.used_slots(), 3);
    }

    #[test]
    fn free_and_reuse() {
        let mut alloc = BitmapAllocator::new(10);
        let s0 = alloc.allocate().unwrap();
        let _s1 = alloc.allocate().unwrap();
        alloc.free(s0);
        // Next allocate should reuse freed slot.
        let reused = alloc.allocate().unwrap();
        assert_eq!(reused, s0);
    }

    #[test]
    fn allocate_exhaustion() {
        let mut alloc = BitmapAllocator::new(3);
        assert!(alloc.allocate().is_some());
        assert!(alloc.allocate().is_some());
        assert!(alloc.allocate().is_some());
        assert!(alloc.allocate().is_none()); // Full.
    }

    #[test]
    fn free_invalid_slot_is_noop() {
        let mut alloc = BitmapAllocator::new(10);
        alloc.free(999); // Out of range — should not panic.
    }

    #[test]
    fn is_allocated_check() {
        let mut alloc = BitmapAllocator::new(10);
        assert!(!alloc.is_allocated(0));
        alloc.allocate();
        assert!(alloc.is_allocated(0));
        assert!(!alloc.is_allocated(1));
    }

    #[test]
    fn total_and_free_slots() {
        let mut alloc = BitmapAllocator::new(100);
        assert_eq!(alloc.total_slots(), 100);
        assert_eq!(alloc.free_slots(), 100);
        alloc.allocate();
        assert_eq!(alloc.free_slots(), 99);
    }

    #[test]
    fn superblock_serialization_roundtrip() {
        let sb = Superblock {
            magic: *b"NVACHUNK",
            version: 1,
            generation: 42,
            chunk_size: CHUNK_SIZE as u32,
            total_slots: 1000,
            used_slots: 50,
            bitmap_bytes: 125,
            index_entries: 50,
            checksum: 0, // Computed in to_bytes().
        };
        let bytes = sb.to_bytes();
        assert_eq!(bytes.len(), Superblock::SIZE); // 4096 bytes.

        let parsed = Superblock::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.magic, *b"NVACHUNK");
        assert_eq!(parsed.generation, 42);
        assert_eq!(parsed.total_slots, 1000);
    }

    #[test]
    fn superblock_invalid_magic_rejected() {
        let mut bytes = vec![0u8; Superblock::SIZE];
        bytes[0..8].copy_from_slice(b"BADMAGIC");
        assert!(Superblock::from_bytes(&bytes).is_err());
    }
}
```

- [ ] **Step 2: Run to verify tests fail**

Run: `cargo test backend::bdev_store::tests 2>&1 | tail -5`
Expected: FAIL — types not found.

- [ ] **Step 3: Implement BitmapAllocator and Superblock**

Add above the `#[cfg(test)]` block in `src/backend/bdev_store.rs`:

```rust
//! BdevChunkStore — chunk storage on SPDK bdevs (raw device or lvol).
//!
//! Uses a bitmap allocator for 4MB-aligned slot management and stores
//! chunk_id → slot mappings in an on-disk index. Supports both raw block
//! devices and LVM logical volumes — the BdevChunkStore doesn't care
//! which type of bdev it operates on.

use crate::backend::chunk_store::{ChunkStore, ChunkStoreStats, CHUNK_SIZE};
use crate::error::{DataPlaneError, Result};
use std::collections::HashMap;

/// Bitmap allocator for 4MB-aligned slots on a bdev.
///
/// Tracks which slots are free/used. Supports allocation and deallocation
/// (freed slots can be reused, unlike a bump allocator).
pub struct BitmapAllocator {
    /// One bit per slot: 1 = used, 0 = free.
    bitmap: Vec<u8>,
    total_slots: u64,
    used_slots: u64,
    /// Hint: start searching from this slot for next allocation.
    next_search: u64,
}

impl BitmapAllocator {
    /// Create a new allocator for `total_slots` slots.
    pub fn new(total_slots: u64) -> Self {
        let bitmap_bytes = ((total_slots + 7) / 8) as usize;
        Self {
            bitmap: vec![0u8; bitmap_bytes],
            total_slots,
            used_slots: 0,
            next_search: 0,
        }
    }

    /// Allocate a free slot. Returns the slot number, or None if full.
    pub fn allocate(&mut self) -> Option<u64> {
        let start = self.next_search;
        for i in 0..self.total_slots {
            let slot = (start + i) % self.total_slots;
            if !self.is_allocated(slot) {
                self.set_allocated(slot, true);
                self.used_slots += 1;
                self.next_search = (slot + 1) % self.total_slots;
                return Some(slot);
            }
        }
        None
    }

    /// Free a previously allocated slot.
    pub fn free(&mut self, slot: u64) {
        if slot >= self.total_slots { return; }
        if self.is_allocated(slot) {
            self.set_allocated(slot, false);
            self.used_slots -= 1;
            // Hint: start future searches from this freed slot.
            if slot < self.next_search {
                self.next_search = slot;
            }
        }
    }

    /// Check if a slot is allocated.
    pub fn is_allocated(&self, slot: u64) -> bool {
        if slot >= self.total_slots { return false; }
        let byte_idx = (slot / 8) as usize;
        let bit_idx = (slot % 8) as u8;
        (self.bitmap[byte_idx] >> bit_idx) & 1 == 1
    }

    pub fn total_slots(&self) -> u64 { self.total_slots }
    pub fn used_slots(&self) -> u64 { self.used_slots }
    pub fn free_slots(&self) -> u64 { self.total_slots - self.used_slots }

    /// Get the raw bitmap bytes (for persistence).
    pub fn bitmap_bytes(&self) -> &[u8] { &self.bitmap }

    /// Restore bitmap from persisted bytes.
    pub fn restore_from_bytes(&mut self, bytes: &[u8]) {
        let len = bytes.len().min(self.bitmap.len());
        self.bitmap[..len].copy_from_slice(&bytes[..len]);
        // Recount used slots.
        self.used_slots = 0;
        for slot in 0..self.total_slots {
            if self.is_allocated(slot) {
                self.used_slots += 1;
            }
        }
        self.next_search = 0;
    }

    fn set_allocated(&mut self, slot: u64, allocated: bool) {
        let byte_idx = (slot / 8) as usize;
        let bit_idx = (slot % 8) as u8;
        if allocated {
            self.bitmap[byte_idx] |= 1 << bit_idx;
        } else {
            self.bitmap[byte_idx] &= !(1 << bit_idx);
        }
    }
}

/// On-disk superblock for BdevChunkStore.
///
/// Stored at offset 0 (copy A) and after the first reserved region (copy B).
/// A/B double-buffering with a generation counter for crash recovery.
#[derive(Debug, Clone)]
pub struct Superblock {
    pub magic: [u8; 8],       // b"NVACHUNK"
    pub version: u32,
    pub generation: u64,       // Monotonically increasing for A/B recovery.
    pub chunk_size: u32,
    pub total_slots: u64,
    pub used_slots: u64,
    pub bitmap_bytes: u32,     // Size of bitmap region in bytes.
    pub index_entries: u32,    // Number of chunk_id→slot entries.
    pub checksum: u32,         // CRC-32C of all preceding fields.
}

impl Superblock {
    /// Size of serialized superblock: 4KB (4096 bytes), matching the spec.
    /// The first 52 bytes contain fields, followed by zero padding to 4KB.
    pub const SIZE: usize = 4096;

    /// Byte offset where fields end. CRC covers buf[0..FIELDS_END].
    /// Checksum is stored at FIELDS_END..FIELDS_END+4.
    const FIELDS_END: usize = 48;

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; Self::SIZE];
        let mut off = 0;

        buf[off..off + 8].copy_from_slice(&self.magic); off += 8;
        buf[off..off + 4].copy_from_slice(&self.version.to_le_bytes()); off += 4;
        buf[off..off + 8].copy_from_slice(&self.generation.to_le_bytes()); off += 8;
        buf[off..off + 4].copy_from_slice(&self.chunk_size.to_le_bytes()); off += 4;
        buf[off..off + 8].copy_from_slice(&self.total_slots.to_le_bytes()); off += 8;
        buf[off..off + 8].copy_from_slice(&self.used_slots.to_le_bytes()); off += 8;
        buf[off..off + 4].copy_from_slice(&self.bitmap_bytes.to_le_bytes()); off += 4;
        buf[off..off + 4].copy_from_slice(&self.index_entries.to_le_bytes()); off += 4;
        // Checksum covers bytes 0..48 (the field data only, not the checksum itself).
        let crc = crc32c::crc32c(&buf[..Self::FIELDS_END]);
        buf[Self::FIELDS_END..Self::FIELDS_END + 4].copy_from_slice(&crc.to_le_bytes());
        // Remaining bytes 52..4096 are zero padding.

        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::SIZE {
            return Err(DataPlaneError::BdevError("superblock too short".into()));
        }
        if &buf[0..8] != b"NVACHUNK" {
            return Err(DataPlaneError::BdevError("invalid Superblock magic".into()));
        }

        let mut off = 0;
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[off..off + 8]); off += 8;
        let version = u32::from_le_bytes([buf[off], buf[off+1], buf[off+2], buf[off+3]]); off += 4;
        let generation = u64::from_le_bytes(buf[off..off+8].try_into().unwrap()); off += 8;
        let chunk_size = u32::from_le_bytes([buf[off], buf[off+1], buf[off+2], buf[off+3]]); off += 4;
        let total_slots = u64::from_le_bytes(buf[off..off+8].try_into().unwrap()); off += 8;
        let used_slots = u64::from_le_bytes(buf[off..off+8].try_into().unwrap()); off += 8;
        let bitmap_bytes = u32::from_le_bytes([buf[off], buf[off+1], buf[off+2], buf[off+3]]); off += 4;
        let index_entries = u32::from_le_bytes([buf[off], buf[off+1], buf[off+2], buf[off+3]]);
        // off is now 48 == FIELDS_END. Checksum stored at bytes 48..52.
        let checksum = u32::from_le_bytes([
            buf[Self::FIELDS_END], buf[Self::FIELDS_END+1],
            buf[Self::FIELDS_END+2], buf[Self::FIELDS_END+3],
        ]);

        // Verify checksum covers bytes 0..48 (fields only, not the checksum itself).
        let expected_crc = crc32c::crc32c(&buf[..Self::FIELDS_END]);
        if checksum != expected_crc {
            return Err(DataPlaneError::BdevError(format!(
                "superblock checksum mismatch: stored={checksum:#x}, computed={expected_crc:#x}"
            )));
        }

        Ok(Self { magic, version, generation, chunk_size, total_slots, used_slots, bitmap_bytes, index_entries, checksum })
    }
}

/// On-disk index entry: chunk_id → slot number.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub chunk_id: String,  // 64-char hex SHA-256.
    pub slot: u64,
}

impl IndexEntry {
    /// Serialized size: 32 bytes (SHA-256 raw) + 8 bytes (slot u64) = 40 bytes.
    pub const SIZE: usize = 40;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        let hash_bytes = hex::decode(&self.chunk_id).unwrap_or_else(|_| vec![0u8; 32]);
        buf[0..32].copy_from_slice(&hash_bytes[..32]);
        buf[32..40].copy_from_slice(&self.slot.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; Self::SIZE]) -> Self {
        let chunk_id = hex::encode(&buf[0..32]);
        let slot = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        Self { chunk_id, slot }
    }
}
```

- [ ] **Step 4: Add module to backend/mod.rs**

Add to `src/backend/mod.rs`:

```rust
pub mod bdev_store;
```

- [ ] **Step 5: Run tests**

Run: `cargo test backend::bdev_store::tests 2>&1 | tail -15`
Expected: All 9 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/backend/bdev_store.rs src/backend/mod.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add BitmapAllocator, Superblock, and IndexEntry for BdevChunkStore

Pure-logic data structures with no SPDK dependency:
- BitmapAllocator: allocate/free 4MB slots, reuse freed slots
- Superblock: on-disk header with A/B generation counter
- IndexEntry: chunk_id-to-slot mapping for on-disk persistence
9 unit tests passing."
```

---

### Task 9: Implement BdevChunkStore with async ChunkStore trait

**Files:**
- Modify: `src/backend/bdev_store.rs`

The `BdevChunkStore` holds a persistent in-memory index (`HashMap<String, u32>` for chunk_id → slot) and a `BitmapAllocator`. I/O goes through the async reactor dispatch. On-disk persistence (superblock/index to bdev) is handled in a future task when SPDK integration tests confirm it works.

- [ ] **Step 1: Write tests for BdevChunkStore**

Add to the `tests` module in `src/backend/bdev_store.rs`:

```rust
    // BdevChunkStore tests use a mock I/O backend since we can't
    // run SPDK in unit tests. The ChunkStore trait implementation
    // is tested via FileChunkStore for full I/O integration.
    // Here we test the index and allocator logic.

    #[test]
    fn index_entry_roundtrip() {
        let entry = IndexEntry {
            chunk_id: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string(),
            slot: 42,
        };
        let bytes = entry.to_bytes();
        let parsed = IndexEntry::from_bytes(&bytes);
        assert_eq!(parsed.chunk_id, entry.chunk_id);
        assert_eq!(parsed.slot, 42);
    }

    #[test]
    fn bitmap_restore_from_bytes() {
        let mut alloc = BitmapAllocator::new(16);
        alloc.allocate(); // slot 0
        alloc.allocate(); // slot 1
        alloc.allocate(); // slot 2

        let bytes = alloc.bitmap_bytes().to_vec();

        let mut alloc2 = BitmapAllocator::new(16);
        alloc2.restore_from_bytes(&bytes);
        assert_eq!(alloc2.used_slots(), 3);
        assert!(alloc2.is_allocated(0));
        assert!(alloc2.is_allocated(1));
        assert!(alloc2.is_allocated(2));
        assert!(!alloc2.is_allocated(3));
    }
```

- [ ] **Step 2: Run tests**

Run: `cargo test backend::bdev_store::tests 2>&1 | tail -15`
Expected: All 11 tests pass (9 previous + 2 new).

- [ ] **Step 3: Commit**

```bash
git add src/backend/bdev_store.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Test] Add IndexEntry roundtrip and bitmap restore tests

Verifies IndexEntry serialization and BitmapAllocator state
restoration from persisted bytes. 11 tests total."
```

---

### Task 10: Add BdevChunkStore struct with in-memory index

**Files:**
- Modify: `src/backend/bdev_store.rs`

This adds the `BdevChunkStore` struct that holds the bdev name, bitmap allocator, and chunk_id→slot index. The `ChunkStore` trait implementation dispatches I/O through the async reactor. This task only adds the struct and its non-trait methods.

- [ ] **Step 1: Add BdevChunkStore struct**

Add after the `IndexEntry` implementation in `src/backend/bdev_store.rs`:

```rust
use async_trait::async_trait;
use std::sync::RwLock;
use crate::spdk::reactor_dispatch;

/// Round up `val` to the next multiple of `align`.
fn align_up(val: u64, align: u64) -> u64 {
    if align == 0 { return val; }
    (val + align - 1) / align * align
}

/// A chunk store backed by an SPDK bdev (raw device or lvol).
///
/// Uses a bitmap allocator for 4MB slot management. The bdev descriptor
/// is opened per-I/O currently (matching existing reactor_dispatch behavior).
/// A future optimization will hold a persistent descriptor.
///
/// The index (chunk_id → slot) and bitmap are kept in memory. On-disk
/// persistence via the Superblock is implemented separately.
pub struct BdevChunkStore {
    bdev_name: String,
    capacity_bytes: u64,
    allocator: std::sync::Mutex<BitmapAllocator>,
    /// chunk_id → slot number.
    index: RwLock<HashMap<String, u64>>,
    /// Byte offset where the data region starts (after superblock + bitmap + index).
    data_region_offset: u64,
}

impl BdevChunkStore {
    /// Create a new BdevChunkStore on the named bdev.
    ///
    /// `capacity_bytes` is the total bdev size. The first portion is reserved
    /// for A/B superblock copies, bitmap, and index. The rest is the data region.
    pub fn new(bdev_name: &str, capacity_bytes: u64) -> Self {
        // Calculate reserved region dynamically based on device size.
        // A/B double-buffering: TWO copies of (superblock + bitmap + index).
        //
        // We need total_slots to size bitmap+index, but total_slots depends on
        // the reserved region. Solve iteratively: estimate slots from full capacity,
        // compute reserved, then recalculate slots from remaining space.
        let estimated_slots = capacity_bytes / CHUNK_SIZE as u64;
        let bitmap_size = (estimated_slots + 7) / 8;
        let index_size = estimated_slots * IndexEntry::SIZE as u64;
        let per_copy = Superblock::SIZE as u64 + bitmap_size + index_size;
        // Align total reserved (2 copies) up to 4MB boundary for clean data region start.
        let reserved = align_up(per_copy * 2, CHUNK_SIZE as u64);

        let data_capacity = capacity_bytes.saturating_sub(reserved);
        let total_slots = data_capacity / CHUNK_SIZE as u64;

        Self {
            bdev_name: bdev_name.to_string(),
            capacity_bytes,
            allocator: std::sync::Mutex::new(BitmapAllocator::new(total_slots)),
            index: RwLock::new(HashMap::new()),
            data_region_offset: reserved,
        }
    }


    /// Convert a slot number to a byte offset on the bdev.
    fn slot_to_offset(&self, slot: u64) -> u64 {
        self.data_region_offset + slot * CHUNK_SIZE as u64
    }

    /// Get the bdev name.
    pub fn bdev_name(&self) -> &str {
        &self.bdev_name
    }
}

#[async_trait]
impl ChunkStore for BdevChunkStore {
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        // Deduplication: if chunk already exists, skip.
        if self.index.read().unwrap().contains_key(chunk_id) {
            return Ok(());
        }

        // Allocate a slot.
        let slot = self.allocator.lock().unwrap().allocate()
            .ok_or_else(|| DataPlaneError::BdevError(format!(
                "bdev {} out of space", self.bdev_name
            )))?;

        let offset = self.slot_to_offset(slot);

        // Write data to the bdev via async reactor dispatch.
        if let Err(e) = reactor_dispatch::bdev_write_async(&self.bdev_name, offset, data).await {
            // Roll back allocation on failure.
            self.allocator.lock().unwrap().free(slot);
            return Err(e);
        }

        // Record in index.
        self.index.write().unwrap().insert(chunk_id.to_string(), slot);
        Ok(())
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let slot = {
            let index = self.index.read().unwrap();
            *index.get(chunk_id).ok_or_else(|| {
                DataPlaneError::BdevError(format!(
                    "chunk not found: {}", &chunk_id[..16.min(chunk_id.len())]
                ))
            })?
        };

        let offset = self.slot_to_offset(slot);
        reactor_dispatch::bdev_read_async(&self.bdev_name, offset, CHUNK_SIZE as u64).await
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        let slot = {
            let mut index = self.index.write().unwrap();
            index.remove(chunk_id).ok_or_else(|| {
                DataPlaneError::BdevError(format!(
                    "chunk not found: {}", &chunk_id[..16.min(chunk_id.len())]
                ))
            })?
        };

        self.allocator.lock().unwrap().free(slot);
        Ok(())
    }

    async fn exists(&self, chunk_id: &str) -> Result<bool> {
        Ok(self.index.read().unwrap().contains_key(chunk_id))
    }

    async fn stats(&self) -> Result<ChunkStoreStats> {
        let alloc = self.allocator.lock().unwrap();
        let index = self.index.read().unwrap();
        Ok(ChunkStoreStats {
            backend_name: self.bdev_name.clone(),
            total_bytes: self.capacity_bytes,
            used_bytes: alloc.used_slots() * CHUNK_SIZE as u64,
            data_bytes: index.len() as u64 * CHUNK_SIZE as u64,
            chunk_count: index.len() as u64,
        })
    }
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -10`
Expected: Success. The `bdev_write_async`/`bdev_read_async` functions were added in Task 4.

- [ ] **Step 3: Commit**

```bash
git add src/backend/bdev_store.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add BdevChunkStore with ChunkStore trait implementation

In-memory chunk_id→slot index backed by BitmapAllocator.
I/O dispatched through async reactor_dispatch to SPDK bdevs.
Deduplication via index check before write. Allocation rollback
on write failure. Requires SPDK for integration testing."
```

---

### Task 11: Run full test suite and verify no regressions

- [ ] **Step 1: Run all tests**

Run: `cargo test 2>&1 | tail -30`
Expected: All existing tests pass plus all new tests. No regressions.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -- -D warnings 2>&1 | tail -20`
Expected: No warnings. Fix any that appear.

- [ ] **Step 3: Commit any clippy fixes if needed**

```bash
git add -u
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Fix] Address clippy warnings in new backend modules"
```

---

## Summary

After completing this plan, the codebase will have:

1. **Tokio runtime** running alongside the SPDK reactor (2 worker threads).
2. **Async reactor dispatch** (`dispatch_async`, `bdev_read_async`, `bdev_write_async`) using `tokio::sync::oneshot`.
3. **`ChunkStore` async trait** with `ChunkHeader` (16 bytes) and `ChunkStoreStats`.
4. **`FileChunkStore`** — POSIX file-per-chunk with 2-level hash prefix dirs, atomic rename writes, `statvfs` capacity reporting.
5. **`BdevChunkStore`** — bitmap allocator, superblock/index data structures, async bdev I/O, dedup via index check.

All old code remains untouched and functional. The new code is additive.

**Next plan (Plan 2):** Metadata store — CRUSH-like placement algorithm and sharded Raft consensus with openraft + redb.
