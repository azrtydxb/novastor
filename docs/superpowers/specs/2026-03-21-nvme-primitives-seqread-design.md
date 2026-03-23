# NVMe Primitives & Sequential Read Performance Design

## Goal

Implement real WRITE_ZEROES, UNMAP/TRIM, and DATASET_MANAGEMENT NVMe primitives with proper dirty bitmap management, and improve sequential read throughput by extending the reactor fast path to multi-sub-block reads with readahead prefetching.

## Context

Current state:
- WRITE_ZEROES and UNMAP are no-ops in the bdev layer — they return success without modifying state, so previously-written data remains readable after a "zero" or "unmap" operation.
- The NVMe-oF TCP target doesn't dispatch WRITE_ZEROES or DATASET_MGMT to the backend — it stubs them.
- The `NvmeOfBackend` trait only has `read`, `write`, `flush`.
- ONCS is not set in Identify Namespace/Controller, so the kernel doesn't know these operations exist.
- Sequential reads >64KB fall off the reactor fast path into tokio with 2 thread crossings per I/O.
- No readahead or sequential detection exists.

Benchmark baseline (SPDK reactor mode, rep1, worker-23):
- Sequential Read 128K QD32: 489-888 MB/s (varies by node)
- Random Read 4K QD1: 7,340-8,222 IOPS

## Architecture

Four independent changes, each building on the previous:

```
1. Real WRITE_ZEROES & UNMAP (bdev layer)
   └── Dirty bitmap clear + partial sub-block zero write

2. NVMe-oF TCP primitive dispatch
   └── Extend NvmeOfBackend trait + advertise ONCS
   └── Depends on: (1)

3. Reactor fast path for multi-sub-block reads
   └── Remove sb_idx==end_sb_idx constraint + dirty bitmap check
   └── Independent of (1) and (2)

4. Sequential readahead
   └── Per-volume tracker + prefetch on reactor thread
   └── Depends on: (3)
```

---

## Section 1: Real WRITE_ZEROES & UNMAP

### Behavior

**WRITE_ZEROES** for a byte range:
1. Decompose range into sub-block descriptors (reuse `sub_block` decomposition logic).
2. For each **fully covered** sub-block: atomically clear the corresponding bit in the volume's `dirty_bitmap` via `AtomicU64::fetch_and(!mask)`. This marks the sub-block as "unwritten" — subsequent reads return zeros without backend I/O.
3. For **partially covered** sub-blocks (range doesn't span the full 64KB): write actual zeros to the affected portion via the existing `sub_block_write` path. We cannot clear the bitmap because the rest of the sub-block contains real data.
4. If the entire chunk has all 64 bitmap bits cleared after this operation, set `chunk_id` to empty string in the `ChunkMapEntry` (requires write lock on `VOLUME_CHUNK_MAPS`).

**UNMAP** is identical to WRITE_ZEROES at our abstraction level. The NVMe DSM command carries a list of LBA ranges — iterate each range and apply the same logic.

**Cross-chunk WRITE_ZEROES**: Ranges spanning multiple chunks are handled by iterating chunk by chunk, same pattern as `sub_block_write`.

**Never-allocated chunks**: If WRITE_ZEROES targets a chunk with no `ChunkMapEntry` (never written to), the bitmap is already all-zeros — this is a no-op (no bitmap update, no backend I/O).

### Write/Zero Race Ordering

A concurrent write and write_zeroes on the same sub-block must be serialized. Both paths use the existing `sub_block_lock` mechanism: write_zeroes acquires the per-sub-block lock before clearing the bitmap bit. This ensures that if a write is in-flight to the same sub-block, write_zeroes waits for it to complete (and vice versa). Full sub-block write_zeroes (bitmap clear) runs under the same lock to prevent a concurrent write from setting the bit between the backend write and the bitmap update.

### Read Path Change

The read fast path must check the dirty bitmap before issuing backend I/O:
- If a sub-block's bitmap bit is 0 (unwritten/unmapped), return zeros for that sub-block's portion without touching the backend.
- If ALL sub-blocks in the requested range are clean (bitmap=0), return all-zeros immediately — zero backend I/O.
- Mixed ranges (some dirty, some clean): issue separate backend reads for contiguous dirty runs, zero-fill clean portions directly in the output buffer. This avoids reading unnecessary data from the backend.

### Code Changes

- `novastor_bdev.rs`: New `sub_block_write_zeroes(volume_name, offset, length)` function
- `novastor_bdev.rs`: Update WRITE_ZEROES and UNMAP handlers to call `sub_block_write_zeroes`
- `novastor_bdev.rs`: Read fast path — check dirty bitmap, return zeros for cleared sub-blocks
- `sub_block.rs`: New `clear_bitmap_bit(bitmap: &AtomicU64, sb_idx: usize)` using `fetch_and(!(1 << sb_idx))`

---

## Section 2: NVMe-oF TCP Target Primitive Dispatch

### Backend Trait Extension

Add two methods to `NvmeOfBackend` (with `#[async_trait]`, consistent with existing pattern):

```rust
async fn write_zeroes(&self, offset: u64, length: u32) -> Result<()> {
    Ok(())  // default no-op for backends that don't support it
}

async fn unmap(&self, ranges: &[(u64, u32)]) -> Result<()> {
    Ok(())  // default no-op
}
```

### NVMe-oF TCP Command Dispatch

Both commands are dispatched in the I/O command match block (lines 257-302 in `target.rs`), alongside READ/WRITE/FLUSH.

**WRITE_ZEROES (opcode 0x08)**:
- CDW10-11: Starting LBA (64-bit, same layout as WRITE)
- CDW12 bits [15:0]: Number of Logical Blocks minus 1
- No data transfer (unlike WRITE — no R2T/H2CData flow)
- Call `backend.write_zeroes(lba * block_size, nlb * block_size)`
- Return CapsuleResp with success/error CQE

**DATASET_MGMT (opcode 0x09)**:
- CDW10 bits [7:0]: Number of ranges minus 1 (NR)
- CDW11 bit 2: Attribute — Deallocate (AD)
- Inline data (arrives in the command capsule, same as small WRITE inline data): Array of 16-byte range descriptors in little-endian format:
  - Bytes 0-3: Context attributes (ignored)
  - Bytes 4-7: Length in logical blocks (u32 LE)
  - Bytes 8-15: Starting LBA (u64 LE)
- Parse NR+1 range descriptors from the capsule's inline data buffer
- If AD bit is set, convert ranges to `(offset_bytes, length_bytes)` tuples and call `backend.unmap(&ranges)`
- If AD bit is not set, return success (context attributes are advisory only)

### Identify Responses

**Identify Controller** (`build_identify_controller`):
- ONCS at **offset 520** (2 bytes, LE): Set bit 2 (DSM) and bit 3 (WRITE_ZEROES) = `0x000C`
- WZSL at **offset 4396** (1 byte): Set to 0 (no limit beyond MDTS, since WRITE_ZEROES doesn't transfer data — the kernel will use its own heuristics)

**Identify Namespace** (`build_identify_namespace`):
- NSFEAT at offset 24 (1 byte): Set bit 2 (deallocated/unwritten blocks behave per DLFEAT)
- DLFEAT at offset 32 (1 byte): Set bits [2:0] = 0x01 (read of deallocated LB returns all zeros)

This combination tells the kernel: (a) it can send TRIM/discard and WRITE_ZEROES commands, (b) deallocated blocks deterministically return zeros, and (c) mkfs won't pre-zero blocks since reads of unwritten blocks are guaranteed zeros.

### NDP Backend

`frontend/src/ndp_backend.rs` implements `write_zeroes` and `unmap` by forwarding to the chunk engine via NDP protocol. This requires adding WRITE_ZEROES and UNMAP op codes to the NDP message format. Since the frontend is currently only used in userspace NVMe-oF mode (not SPDK reactor mode), this is a follow-up — the SPDK bdev path handles it directly.

---

## Section 3: Reactor Fast Path for Multi-Sub-Block Reads

### Current Fast Path Condition

```rust
aligned && chunk_idx == end_chunk_idx && sb_idx == end_sb_idx && iovcnt == 1
```

Only handles reads within a single 64KB sub-block.

### New Fast Path Condition

```rust
aligned && chunk_idx == end_chunk_idx && iovcnt == 1
```

Removes the `sb_idx == end_sb_idx` constraint. Since sub-blocks within a chunk are laid out contiguously on the backend bdev, a read spanning SB 0 through SB 1 (128KB) is a single contiguous range — one `spdk_bdev_read` call on the reactor.

### Implementation

1. Calculate `bdev_offset` from the first sub-block's position on the backend.
2. Calculate total length from first sub-block start to last sub-block end (respecting partial start/end offsets).
3. **Dirty bitmap check**: Load the chunk's `AtomicU64` dirty bitmap once. Create a mask for the affected sub-block range. Check:
   - `(bitmap & mask) == 0`: All clean — zero-fill output buffer, complete immediately, no backend I/O.
   - `(bitmap & mask) == mask`: All dirty — issue single `spdk_bdev_read` for the full range.
   - Otherwise (mixed): issue separate `spdk_bdev_read` calls for each contiguous dirty run, zero-fill clean portions in the completion callback.
4. Allocate one DMA buffer for the full range.
5. Issue single `spdk_bdev_read` on the reactor — zero thread crossings.
6. Completion callback copies from DMA buffer to caller's iov, zero-fills any unmapped sub-block ranges, completes the bdev_io.

### Impact

128K sequential reads go from 2 thread crossings + async dispatch to zero thread crossings. Reads up to 4MB (full chunk) stay on the reactor. Only cross-chunk reads fall to tokio (rare — 4MB chunks are large relative to typical I/O).

---

## Section 4: Sequential Readahead

### Per-Volume Readahead State

Stored in a reactor-local `HashMap<String, ReadaheadState>` — only accessed on the SPDK reactor thread, no synchronization needed:

```rust
struct ReadaheadState {
    last_end_offset: u64,       // end offset of last completed read
    sequential_count: u32,      // consecutive sequential reads detected
    window_sub_blocks: u32,     // current readahead window (sub-blocks)
    prefetch_offset: u64,       // start offset of prefetched data
    prefetch_len: u64,          // length of prefetched data
    prefetch_buf: *mut u8,      // DMA buffer holding prefetched data (may be null)
    prefetch_ready: bool,       // true when prefetch I/O has completed
    generation: u64,            // incremented on each reset, used by completion callbacks
}
```

### Detection

A read is "sequential" if `read.start_offset == last_end_offset`. After 2 consecutive sequential reads, readahead activates.

### Window Growth

- Starts at 2 sub-blocks (128KB) after activation
- Doubles each sequential hit: 2 → 4 → 8 → 16 → 32
- Caps at 32 sub-blocks (2MB)
- Resets to 0 on any non-sequential read (discards prefetch buffer)

### Prefetch Mechanism

1. On a sequential read hitting the reactor fast path, after issuing the current I/O's `spdk_bdev_read`, issue a second non-blocking `spdk_bdev_read` for the next `window_sub_blocks` sub-blocks into a dedicated prefetch DMA buffer. Set `prefetch_ready = false`.
2. The prefetch completion callback checks `generation` — if it matches current generation, set `prefetch_ready = true`. If generation has changed (reset occurred), release the DMA buffer and do nothing.
3. On the next sequential read:
   - If `prefetch_ready == true` and the prefetch covers the requested range: copy from prefetch buffer (zero backend I/O for this read), then trigger the next prefetch.
   - If `prefetch_ready == false` (still in-flight) or prefetch doesn't cover: fall through to normal read. Do NOT free the prefetch buffer — let the completion callback handle it via generation check. Increment generation and reset state.
4. Prefetch never crosses chunk boundaries. If the remaining sub-blocks in the current chunk are fewer than `window_sub_blocks`, prefetch only up to the chunk boundary.

### Prefetch Buffer Lifetime

The DMA buffer is owned by the readahead state. On reset:
- If `prefetch_ready == true`: free the buffer immediately.
- If `prefetch_ready == false` (in-flight I/O): increment `generation`, set `prefetch_buf = null`. The in-flight completion callback sees the stale generation and frees the buffer itself. No use-after-free.

### Write Invalidation

Writes go through the reactor thread (fast path) or signal back to the reactor (tokio path). On the reactor thread, the write path checks if the write range overlaps the prefetch range — if so, increment `generation` and mark prefetch invalid. This is safe because both the write fast path and the readahead state are reactor-local.

For tokio-path writes (multi-sub-block), the completion callback already runs on the reactor via `send_to_reactor`. The invalidation check happens in that callback, maintaining reactor-thread-only access to `ReadaheadState`.

### Memory Budget

- One 2MB DMA buffer per volume with active readahead
- With 8 active volumes: 16MB total — negligible vs hugepage pool
- Prefetch buffer is allocated on first readahead activation, freed when readahead resets

### Constraints

- Readahead is read-only — no prefetch for writes
- Prefetch does not cross chunk boundaries
- Prefetch respects dirty bitmap — clean sub-blocks return zeros without backend I/O

---

## Files Changed

| File | Changes |
|------|---------|
| `dataplane/src/bdev/novastor_bdev.rs` | New `sub_block_write_zeroes`, update WRITE_ZEROES/UNMAP handlers, read fast path dirty bitmap check, multi-sub-block fast path, readahead state + prefetch |
| `dataplane/src/bdev/sub_block.rs` | New `clear_bitmap_bit` helper using `AtomicU64::fetch_and` |
| `dataplane/nvmeof-tcp/src/target.rs` | WRITE_ZEROES + DATASET_MGMT dispatch in I/O command match, Identify Controller ONCS at offset 520, Identify Namespace NSFEAT/DLFEAT |
| `dataplane/nvmeof-tcp/src/pdu.rs` | DSM range descriptor parsing helper (16-byte LE struct) |
| `frontend/src/ndp_backend.rs` | `write_zeroes` + `unmap` on NDP backend (follow-up — not needed for SPDK reactor mode) |

## Testing

- fio with `--trim` and `--verify` to confirm UNMAP returns zeros on re-read
- `blkdiscard` on a mounted volume, verify freed space
- Sequential read benchmark before/after: target >1.5 GB/s seq read 128K QD32
- Verify no regression on random 4K performance
- Mixed sequential + random workload to verify readahead doesn't hurt random I/O
- Write-then-write_zeroes-then-read cycle to verify bitmap clearing works
- Concurrent write + write_zeroes stress test to verify locking
