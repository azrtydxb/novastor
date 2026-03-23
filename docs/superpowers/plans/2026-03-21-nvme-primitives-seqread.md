# NVMe Primitives & Sequential Read Performance Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement real WRITE_ZEROES/UNMAP with bitmap management, advertise NVMe capabilities, extend the reactor fast path for multi-sub-block reads, and add sequential readahead.

**Architecture:** Four layered changes: (1) bitmap-clearing WRITE_ZEROES/UNMAP in the bdev layer, (2) NVMe-oF TCP dispatch + Identify advertisement, (3) multi-sub-block reactor fast path with dirty bitmap checks, (4) sequential readahead with prefetch on the reactor thread.

**Tech Stack:** Rust, SPDK FFI, tokio, NVMe-oF TCP protocol

**Spec:** `docs/superpowers/specs/2026-03-21-nvme-primitives-seqread-design.md`

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `dataplane/src/bdev/sub_block.rs` | Add `bitmap_clear` helper | 1 |
| `dataplane/src/bdev/novastor_bdev.rs` | `sub_block_write_zeroes`, WRITE_ZEROES/UNMAP handlers, read bitmap check, multi-SB fast path, readahead | 1, 3, 4, 5 |
| `dataplane/nvmeof-tcp/src/pdu.rs` | DSM range descriptor parser | 2 |
| `dataplane/nvmeof-tcp/src/target.rs` | Backend trait extension, WRITE_ZEROES/DSM dispatch, Identify ONCS/NSFEAT/DLFEAT, MDTS increase | 2 |

**Deferred:** `frontend/src/ndp_backend.rs` — NDP backend `write_zeroes`/`unmap` forwarding. Not needed for SPDK reactor mode (current deployment). Default no-op implementations on the trait are sufficient.

---

## Chunk 1: WRITE_ZEROES & UNMAP Implementation

### Task 1: Bitmap clear helper + sub_block_write_zeroes

**Files:**
- Modify: `dataplane/src/bdev/sub_block.rs:37-59`
- Modify: `dataplane/src/bdev/novastor_bdev.rs:1548-1559`

- [ ] **Step 1: Add `bitmap_clear` to sub_block.rs**

Add after `bitmap_set` (line 39):

```rust
/// Clear a single sub-block bit in the dirty bitmap.
pub fn bitmap_clear(bitmap: &mut u64, sb_index: usize) {
    *bitmap &= !(1u64 << sb_index);
}
```

- [ ] **Step 2: Add `sub_block_write_zeroes` function to novastor_bdev.rs**

Add after `sub_block_write` (find by searching for `async fn sub_block_write`). This function handles the WRITE_ZEROES logic from the spec:

```rust
/// Zero a byte range on a volume. Full sub-blocks clear the dirty bitmap
/// (reads return zeros without backend I/O). Partial sub-blocks write actual
/// zeros via the normal write path.
async fn sub_block_write_zeroes(volume_name: &str, offset: u64, length: u64) -> crate::error::Result<()> {
    if length == 0 {
        return Ok(());
    }

    let end_offset = offset + length;
    let mut pos = offset;

    while pos < end_offset {
        let chunk_idx = sub_block::chunk_index(pos) as usize;
        let sb_idx = sub_block::sub_block_index(pos);
        let off_in_sb = sub_block::offset_in_sub_block(pos);
        let remaining_in_sb = sub_block::SUB_BLOCK_SIZE - off_in_sb;
        let remaining_req = (end_offset - pos) as usize;
        let zero_len = remaining_in_sb.min(remaining_req);

        let full_sub_block = off_in_sb == 0 && zero_len == sub_block::SUB_BLOCK_SIZE;

        if full_sub_block {
            // Full sub-block: clear dirty bitmap bit.
            // Acquire sub-block lock to serialize with concurrent writes.
            let lock = sub_block_lock(volume_name, chunk_idx, sb_idx);
            let _guard = lock.lock().await;

            // Clear the atomic bitmap on the BdevCtx.
            if let Some(ctx) = get_bdev_ctx(volume_name) {
                if chunk_idx < ctx.dirty_bitmaps.len() {
                    ctx.dirty_bitmaps[chunk_idx]
                        .fetch_and(!(1u64 << sb_idx), std::sync::atomic::Ordering::Relaxed);
                }
            }

            // Also clear in ChunkMapEntry if it exists.
            {
                let mut maps = volume_chunk_maps().write().unwrap();
                if let Some(chunk_map) = maps.get_mut(volume_name) {
                    if let Some(Some(entry)) = chunk_map.get_mut(chunk_idx) {
                        sub_block::bitmap_clear(&mut entry.dirty_bitmap, sb_idx);
                        // If entire chunk is now clean, clear the chunk_id.
                        if entry.dirty_bitmap == 0 {
                            entry.chunk_id = String::new();
                        }
                    }
                }
            }
        } else {
            // Partial sub-block: write actual zeros via normal write path.
            let zeros = vec![0u8; zero_len];
            sub_block_write(volume_name, pos, &zeros).await?;
        }

        pos += zero_len as u64;
    }

    Ok(())
}
```

- [ ] **Step 3: Update WRITE_ZEROES and UNMAP handlers in bdev_submit_request_cb**

Replace the no-op handler at line ~1548-1559 with:

```rust
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
    // UNMAP is identical to WRITE_ZEROES at our abstraction level.
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
```

- [ ] **Step 4: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

Note: Full SPDK build requires Docker (`podman build -f deploy/docker/Dockerfile.dataplane .`). Stub-mode cargo check validates syntax and non-SPDK code paths.

- [ ] **Step 5: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" add dataplane/src/bdev/sub_block.rs dataplane/src/bdev/novastor_bdev.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Implement real WRITE_ZEROES and UNMAP with bitmap management

Full sub-blocks clear the dirty bitmap so reads return zeros.
Partial sub-blocks write actual zeros via the normal write path.
Sub-block lock serializes with concurrent writes."
```

---

## Chunk 2: NVMe-oF TCP Target Primitive Dispatch

### Task 2: Backend trait + NVMe-oF dispatch + Identify advertisement

**Files:**
- Modify: `dataplane/nvmeof-tcp/src/pdu.rs:344-349`
- Modify: `dataplane/nvmeof-tcp/src/target.rs:18-26,257-302,853-904`

- [ ] **Step 1: Add DSM range descriptor parser to pdu.rs**

Add after the opcode module (line ~355):

```rust
/// NVMe Dataset Management range descriptor (16 bytes, little-endian).
/// Used by DATASET_MGMT (TRIM/deallocate) command.
pub struct DsmRange {
    pub context_attrs: u32,
    pub length_blocks: u32,
    pub starting_lba: u64,
}

impl DsmRange {
    /// Parse a DSM range descriptor from a 16-byte little-endian buffer.
    pub fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < 16 {
            return None;
        }
        Some(Self {
            context_attrs: u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]),
            length_blocks: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            starting_lba: u64::from_le_bytes([buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]]),
        })
    }

    /// Parse an array of DSM range descriptors.
    pub fn parse_ranges(data: &[u8], count: usize) -> Vec<Self> {
        (0..count)
            .filter_map(|i| {
                let offset = i * 16;
                if offset + 16 <= data.len() {
                    Self::from_bytes(&data[offset..offset + 16])
                } else {
                    None
                }
            })
            .collect()
    }
}
```

- [ ] **Step 2: Extend NvmeOfBackend trait**

At `target.rs` line 18-26, add `write_zeroes` and `unmap` methods with default no-op implementations:

```rust
#[async_trait]
pub trait NvmeOfBackend: Send + Sync + 'static {
    fn size(&self) -> u64;
    fn block_size(&self) -> u32 { 512 }
    async fn read(&self, offset: u64, length: u32) -> Result<Vec<u8>>;
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;
    async fn flush(&self) -> Result<()> { Ok(()) }
    async fn write_zeroes(&self, offset: u64, length: u32) -> Result<()> { Ok(()) }
    async fn unmap(&self, ranges: &[(u64, u32)]) -> Result<()> { Ok(()) }
}
```

- [ ] **Step 3: Add WRITE_ZEROES and DATASET_MGMT dispatch**

In the I/O command match block (lines 257-302), add after the FLUSH handler (line ~298):

```rust
opcode::WRITE_ZEROES => {
    let bs = subsys.backend.block_size() as u64;
    let lba = ((cmd.cdw11 as u64) << 32) | cmd.cdw10 as u64;
    let nlb = (cmd.cdw12 & 0xFFFF) as u32 + 1;
    let resp = match subsys.backend.write_zeroes(lba * bs, nlb * bs as u32).await {
        Ok(()) => build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id)),
        Err(e) => {
            warn!("NVMe-oF: write_zeroes error: {}", e);
            build_capsule_resp(&NvmeCqe::error(cmd.cid, sq_id, status::SCT_GENERIC, status::DATA_XFER_ERROR))
        }
    };
    let _ = tx.send(resp.to_vec()).await;
}
opcode::DATASET_MGMT => {
    let nr = (cmd.cdw10 & 0xFF) as usize + 1; // Number of ranges (0-based)
    let ad = (cmd.cdw11 >> 2) & 1; // Attribute — Deallocate
    if ad == 1 {
        if let Some(data) = write_data {
            let bs = subsys.backend.block_size() as u64;
            let dsm_ranges = crate::pdu::DsmRange::parse_ranges(&data, nr);
            let ranges: Vec<(u64, u32)> = dsm_ranges.iter()
                .filter(|r| r.length_blocks > 0)
                .map(|r| (r.starting_lba * bs, (r.length_blocks as u64 * bs) as u32))
                .collect();
            if !ranges.is_empty() {
                if let Err(e) = subsys.backend.unmap(&ranges).await {
                    warn!("NVMe-oF: unmap error: {}", e);
                }
            }
        }
    }
    let _ = tx.send(build_capsule_resp(&NvmeCqe::success(cmd.cid, sq_id)).to_vec()).await;
}
```

- [ ] **Step 4: Remove old WRITE_ZEROES/DATASET_MGMT stub**

Remove or update the stub at line ~753 (in the legacy `handle_io_cmd` function) to also call the backend methods instead of returning success directly.

- [ ] **Step 5: Set ONCS in build_identify_controller**

In `build_identify_controller` (line ~853), add after existing fields:

```rust
// ONCS — Optional NVMe Command Support (offset 520, 2 bytes LE)
// Bit 2: Dataset Management, Bit 3: Write Zeroes
let oncs: u16 = (1 << 2) | (1 << 3); // 0x000C
buf[520..522].copy_from_slice(&oncs.to_le_bytes());
```

- [ ] **Step 5b: Increase MDTS for larger reads**

In `build_identify_controller`, MDTS is currently set to 5 (128KB = 2^5 * 4KB). Increase to 8 (1MB = 2^8 * 4KB) to allow the kernel to issue larger I/O that benefits from the multi-sub-block fast path:

```rust
// MDTS — Maximum Data Transfer Size (offset 77, 1 byte)
// Value N means 2^N * minimum page size (4KB). 8 = 1MB.
buf[77] = 8;
```

Note: The NVMe-oF TCP target and DMA buffer pool must handle up to 1MB transfers. The existing DMA buffer pool allocates per-request, so this is safe. Verify that `acquire_dma_buf_public` can allocate 1MB buffers from the hugepage pool.

- [ ] **Step 6: Set NSFEAT and DLFEAT in build_identify_namespace**

In `build_identify_namespace` (line ~894), add after existing fields:

```rust
// NSFEAT — Namespace Features (offset 24, 1 byte)
// Bit 2: deallocated/unwritten logical blocks behave per DLFEAT
buf[24] = 1 << 2; // 0x04

// DLFEAT — Deallocate Logical Block Features (offset 32, 1 byte)
// Bits [2:0] = 001: read of deallocated LB returns all zeros
buf[32] = 0x01;
```

- [ ] **Step 7: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 8: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" add dataplane/nvmeof-tcp/src/pdu.rs dataplane/nvmeof-tcp/src/target.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] NVMe-oF: dispatch WRITE_ZEROES and DATASET_MGMT to backend

Extend NvmeOfBackend trait with write_zeroes() and unmap().
Parse DSM range descriptors for TRIM/deallocate.
Advertise ONCS (Write Zeroes + DSM) in Identify Controller.
Set NSFEAT/DLFEAT in Identify Namespace for zero-read guarantee."
```

---

## Chunk 3: Multi-Sub-Block Reactor Fast Path

### Task 3: Extend read fast path to handle multi-sub-block within same chunk

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs:1284-1396`

- [ ] **Step 1: Read the dirty bitmap for the chunk**

Before the existing fast-path check at line 1293, we need to load the dirty bitmap. Add a helper function near the top of the file:

```rust
/// Load the dirty bitmap for a chunk from the BdevCtx atomic array.
/// Returns 0 (all clean) if the chunk has no bitmap entry.
unsafe fn load_chunk_bitmap(volume_name: &str, chunk_idx: usize) -> u64 {
    if let Some(ctx) = get_bdev_ctx(volume_name) {
        if chunk_idx < ctx.dirty_bitmaps.len() {
            return ctx.dirty_bitmaps[chunk_idx].load(std::sync::atomic::Ordering::Relaxed);
        }
    }
    0
}
```

- [ ] **Step 2: Extend the fast path condition**

Replace the fast path block at lines 1293-1348. The new condition removes the `sb_idx == end_sb_idx` constraint:

```rust
if aligned && chunk_idx == end_chunk_idx && iovcnt == 1 {
    // ---- REACTOR FAST PATH: single or multi sub-block read within one chunk ----
    if let (Ok(backend), Ok(vol_base)) = (
        get_backend_bdev_name(),
        get_volume_base_offset(&volume_name),
    ) {
        // Check dirty bitmap — which sub-blocks have data?
        let bitmap = load_chunk_bitmap(&volume_name, chunk_idx);
        let sb_mask = if sb_idx == end_sb_idx {
            1u64 << sb_idx
        } else {
            // Mask for sub-blocks sb_idx..=end_sb_idx
            let width = end_sb_idx - sb_idx + 1;
            ((1u64 << width) - 1) << sb_idx
        };

        let iov = &*iovs;
        let dst_base = iov.iov_base as *mut u8;
        let dst_len = length as usize;

        if (bitmap & sb_mask) == 0 {
            // ALL sub-blocks clean: return zeros without backend I/O.
            std::ptr::write_bytes(dst_base, 0, dst_len);
            ffi::spdk_bdev_io_complete(
                bdev_io,
                ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
            );
            return;
        }

        if let Some(cache) = reactor_cache_open(backend) {
            let chunk_base = vol_base + chunk_idx as u64 * CHUNK_SIZE as u64;
            let bdev_offset = sub_block::backend_sub_block_offset(chunk_base, sb_idx)
                + off_in_sb as u64;

            // Align length up to block_size.
            let aligned_len = (length + cache.block_size - 1) & !(cache.block_size - 1);

            let dma_buf = reactor_dispatch::acquire_dma_buf_public(aligned_len as usize);
            if dma_buf.is_null() {
                ffi::spdk_bdev_io_complete(
                    bdev_io,
                    ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_FAILED,
                );
                return;
            }

            // For mixed dirty/clean, we'll zero-fill clean portions in the callback.
            let needs_zero_fill = (bitmap & sb_mask) != sb_mask;

            let rctx = Box::new(ReactorReadCtx {
                parent_io: bdev_io,
                dma_buf,
                dma_len: aligned_len as usize,
                dst_base,
                dst_len,
                buf_offset: 0,
                bitmap,
                sb_start: sb_idx,
                off_in_first_sb: off_in_sb,
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
```

- [ ] **Step 3: Extend ReactorReadCtx for zero-fill of clean sub-blocks**

Add fields to `ReactorReadCtx` at line ~818:

```rust
struct ReactorReadCtx {
    parent_io: *mut ffi::spdk_bdev_io,
    dma_buf: *mut c_void,
    dma_len: usize,
    dst_base: *mut u8,
    dst_len: usize,
    buf_offset: usize,
    /// Dirty bitmap for the chunk (for zero-filling clean sub-blocks).
    bitmap: u64,
    /// First sub-block index in this read.
    sb_start: usize,
    /// Offset within first sub-block.
    off_in_first_sb: usize,
}
```

Update the fast path code to populate these new fields, and update `reactor_read_done_cb` to zero-fill clean sub-block portions:

```rust
// In reactor_read_done_cb, after copying DMA data to dst:
if rctx.bitmap != u64::MAX {
    // Zero-fill any clean sub-block portions in the output.
    let mut pos = 0usize;
    let mut sb = rctx.sb_start;
    let mut off = rctx.off_in_first_sb;
    while pos < rctx.dst_len {
        let sb_remaining = sub_block::SUB_BLOCK_SIZE - off;
        let chunk_remaining = rctx.dst_len - pos;
        let span = sb_remaining.min(chunk_remaining);
        if !sub_block::bitmap_is_set(rctx.bitmap, sb) {
            // Clean sub-block: zero this portion.
            std::ptr::write_bytes(rctx.dst_base.add(pos), 0, span);
        }
        pos += span;
        sb += 1;
        off = 0;
    }
}
```

- [ ] **Step 4: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 5: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" add dataplane/src/bdev/novastor_bdev.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Extend reactor fast path for multi-sub-block reads

Reads up to 4MB within a single chunk now stay on the reactor
thread with zero thread crossings. Dirty bitmap check returns
zeros for clean sub-blocks without backend I/O."
```

---

## Chunk 4: Sequential Readahead

### Task 4: Per-volume readahead state

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs`

- [ ] **Step 1: Define ReadaheadState and storage**

Add near the top of `novastor_bdev.rs`, near other statics:

```rust
/// Per-volume sequential readahead state.
/// Only accessed on the SPDK reactor thread — no synchronization needed.
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
            // In-flight: let completion callback free it via generation check.
            self.prefetch_buf = std::ptr::null_mut();
        }
        self.generation += 1;
        self.sequential_count = 0;
        self.window_sub_blocks = 0;
        self.prefetch_ready = false;
        self.prefetch_len = 0;
    }
}

/// Reactor-local readahead states, keyed by volume name.
/// Only accessed on the reactor thread.
static mut READAHEAD_STATES: Option<HashMap<String, ReadaheadState>> = None;

unsafe fn readahead_state(volume_name: &str) -> &mut ReadaheadState {
    let states = READAHEAD_STATES.get_or_insert_with(HashMap::new);
    states.entry(volume_name.to_string()).or_default()
}
```

- [ ] **Step 2: Add prefetch completion callback**

```rust
struct PrefetchCtx {
    volume_name: String,
    generation: u64,
    buf: *mut c_void,
    buf_size: usize,
    offset: u64,
    len: u64,
}

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

    // Store successful prefetch.
    ra.prefetch_buf = pctx.buf;
    ra.prefetch_buf_size = pctx.buf_size;
    ra.prefetch_offset = pctx.offset;
    ra.prefetch_len = pctx.len;
    ra.prefetch_ready = true;
}
```

- [ ] **Step 3: Integrate readahead into the reactor fast path**

In the reactor fast path (from Task 3), add readahead logic **before** the `spdk_bdev_read` call:

```rust
// --- Readahead check ---
let ra = readahead_state(&volume_name);
let read_end = offset + length;

// Check if this is a sequential read.
if offset == ra.last_end_offset {
    ra.sequential_count += 1;
} else {
    ra.reset();
    ra.sequential_count = 1;
}
ra.last_end_offset = read_end;

// Check if prefetch covers this read.
if ra.prefetch_ready
    && offset >= ra.prefetch_offset
    && read_end <= ra.prefetch_offset + ra.prefetch_len
{
    // HIT: copy from prefetch buffer.
    let src_off = (offset - ra.prefetch_offset) as usize;
    std::ptr::copy_nonoverlapping(
        (ra.prefetch_buf as *const u8).add(src_off),
        dst_base,
        dst_len,
    );
    // Zero-fill clean sub-blocks (prefetch reads raw backend data).
    let bitmap = load_chunk_bitmap(&volume_name, chunk_idx);
    {
        let mut pos = 0usize;
        let mut sb = sb_idx;
        let mut off = off_in_sb;
        while pos < dst_len {
            let sb_remaining = sub_block::SUB_BLOCK_SIZE - off;
            let chunk_remaining = dst_len - pos;
            let span = sb_remaining.min(chunk_remaining);
            if !sub_block::bitmap_is_set(bitmap, sb) {
                std::ptr::write_bytes(dst_base.add(pos), 0, span);
            }
            pos += span;
            sb += 1;
            off = 0;
        }
    }

    ffi::spdk_bdev_io_complete(
        bdev_io,
        ffi::spdk_bdev_io_status_SPDK_BDEV_IO_STATUS_SUCCESS,
    );

    // Trigger next prefetch.
    ra.prefetch_ready = false;
    // (prefetch issuing code below)

    // Issue next prefetch if still sequential.
    if ra.sequential_count >= 2 {
        if ra.window_sub_blocks == 0 {
            ra.window_sub_blocks = 2;
        } else if ra.window_sub_blocks < 32 {
            ra.window_sub_blocks *= 2;
        }
        let pf_offset = read_end;
        let pf_chunk_idx = sub_block::chunk_index(pf_offset) as usize;
        let pf_sb_idx = sub_block::sub_block_index(pf_offset);
        let remaining_in_chunk = (sub_block::SUB_BLOCKS_PER_CHUNK - pf_sb_idx) as u32;
        let pf_sbs = ra.window_sub_blocks.min(remaining_in_chunk);
        if pf_sbs > 0 && pf_chunk_idx == chunk_idx {
            let pf_len = pf_sbs as u64 * sub_block::SUB_BLOCK_SIZE as u64;
            let pf_aligned = (pf_len + cache.block_size - 1) & !(cache.block_size - 1);
            let pf_buf = reactor_dispatch::acquire_dma_buf_public(pf_aligned as usize);
            if !pf_buf.is_null() {
                let pf_bdev_offset = sub_block::backend_sub_block_offset(chunk_base, pf_sb_idx);
                let pf_ctx = Box::new(PrefetchCtx {
                    volume_name: volume_name.to_string(),
                    generation: ra.generation,
                    buf: pf_buf,
                    buf_size: pf_aligned as usize,
                    offset: pf_offset,
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
                    // Failed to issue prefetch — not fatal, just skip.
                    reactor_dispatch::release_dma_buf_public(pf_buf, pf_aligned as usize);
                }
            }
        }
    }

    return; // Served from prefetch buffer.
}

// Not a prefetch hit — issue normal read (existing code).
// But also trigger prefetch if sequential count >= 2.
```

- [ ] **Step 4: Add write invalidation for prefetch**

In the write fast path (the WRITE handler in `bdev_submit_request_cb`) AND the WRITE_ZEROES handler, add after the I/O setup:

```rust
// Invalidate readahead if write/zero overlaps prefetch range.
let ra = readahead_state(&volume_name);
let write_end = offset + length;
if ra.prefetch_ready
    && offset < ra.prefetch_offset + ra.prefetch_len
    && write_end > ra.prefetch_offset
{
    ra.reset();
}
```

For WRITE_ZEROES: since it dispatches to tokio, the invalidation must happen in the `send_to_reactor` completion callback (where we already are on the reactor thread), not in the tokio task. Add the invalidation inside the `send_to_reactor` closure that completes the bdev_io.

- [ ] **Step 5: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 6: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" add dataplane/src/bdev/novastor_bdev.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" commit -m "[Feature] Add sequential readahead with prefetch on reactor thread

Per-volume readahead detects sequential patterns after 2 consecutive
reads. Prefetch window grows from 128KB to 2MB. DMA prefetch buffer
managed via generation counter to prevent use-after-free."
```

---

## Chunk 5: Build, Deploy, Test

### Task 5: Build images, deploy, and run benchmarks

**Files:**
- No source changes — build and verify

- [ ] **Step 1: Build dataplane Docker image**

```bash
cd /Users/pascal/Development/Nova/novastor
podman build -t novastor-dataplane:latest -f deploy/docker/Dockerfile.dataplane .
```

- [ ] **Step 2: Push to local registry**

```bash
DP_TAG="v0.2.0-nvme-$(date +%s)"
podman tag novastor-dataplane:latest 192.168.100.11:30500/novastor/novastor-dataplane:$DP_TAG
podman push --tls-verify=false 192.168.100.11:30500/novastor/novastor-dataplane:$DP_TAG
```

- [ ] **Step 3: Deploy with full cleanup**

Follow the cleanup-before-deploy protocol (see memory `feedback_cleanup_before_deploy.md`):
1. Delete all test pods and PVCs
2. Disconnect all NVMe-oF on all nodes
3. Delete all stale PVs
4. Update dataplane image: `kubectl set image daemonset/novastor-dataplane -n novastor-system dataplane=$REGISTRY/novastor-dataplane:$DP_TAG`
5. Restart dataplane and agent daemonsets
6. Wait for rollout complete
7. Wait for chunk stores to init

- [ ] **Step 4: Verify TRIM/UNMAP support**

```bash
# Create a PVC and pod
# Then verify the kernel sees TRIM support:
kubectl exec -n novastor-system $CSI_POD -c csi-plugin -- sh -c 'cat /sys/block/nvme*n1/queue/discard_max_bytes'
# Should show non-zero value

# Verify blkdiscard works:
kubectl exec bench-pod -- blkdiscard /dev/nvme1n1
# Should succeed (not "operation not supported")
```

- [ ] **Step 5: Run sequential read benchmark**

```bash
# Warmup write
kubectl exec bench-pod -- fio --name=warmup --filename=/data/fio-test --size=256M --rw=write --bs=1M --direct=1 --ioengine=libaio --iodepth=16 --numjobs=1

# Sequential read 128K QD32
kubectl exec bench-pod -- fio --name=seqread --filename=/data/fio-test --size=256M --rw=read --bs=128k --direct=1 --ioengine=libaio --iodepth=32 --numjobs=1 --runtime=30 --time_based --group_reporting
```

Target: >1.5 GB/s sequential read (up from 489-888 MB/s baseline).

- [ ] **Step 6: Run random 4K benchmark to verify no regression**

```bash
kubectl exec bench-pod -- fio --name=randread --filename=/data/fio-test --size=256M --rw=randread --bs=4k --direct=1 --ioengine=libaio --iodepth=32 --numjobs=1 --runtime=30 --time_based --group_reporting
```

Target: ≥38K IOPS random read QD32 (no regression from current baseline).

- [ ] **Step 7: Run TRIM verification test**

```bash
# Write data, then TRIM, then verify zeros
kubectl exec bench-pod -- fio --name=trim-test --filename=/data/fio-test --size=64M --rw=write --bs=4k --direct=1 --ioengine=libaio --iodepth=1 --numjobs=1
kubectl exec bench-pod -- fio --name=trim --filename=/data/fio-test --size=64M --rw=trim --bs=64k --direct=1 --ioengine=libaio --iodepth=1 --numjobs=1
kubectl exec bench-pod -- fio --name=verify-zeros --filename=/data/fio-test --size=64M --rw=read --bs=4k --direct=1 --ioengine=libaio --iodepth=1 --numjobs=1 --verify=pattern --verify_pattern=0x00
```
