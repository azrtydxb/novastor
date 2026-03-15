//! Per-chunk write coalescing buffer.
//!
//! Accumulates sub-chunk writes in memory and flushes the full 4MB chunk
//! to the ChunkEngine only when the chunk is complete or a flush is triggered.
//! This eliminates redundant read-modify-write cycles for sequential workloads
//! (e.g., mkfs, dd, fio sequential) where many small writes fill the same chunk.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use log::{debug, info, warn};

/// Default chunk size: 4MB.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Default flush timeout in milliseconds. Aggressive to minimize data-loss window.
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 100;

/// Per-chunk write coalescing buffer.
///
/// Accumulates sub-chunk writes in memory and flushes the full 4MB chunk
/// to the ChunkEngine only when the chunk is complete or a flush is triggered.
pub struct WriteBuffer {
    /// Buffered chunks: (volume_name, chunk_idx) -> ChunkBuffer
    buffers: Mutex<HashMap<(String, usize), ChunkBuffer>>,
    /// How long to hold a dirty buffer before flushing (ms).
    flush_timeout_ms: u64,
}

/// Internal buffer state for a single 4MB chunk being assembled.
struct ChunkBuffer {
    /// The 4MB buffer being assembled.
    data: Vec<u8>,
    /// Bitmap tracking which byte ranges have been written.
    /// Sorted, non-overlapping (start, end) pairs after each insertion.
    dirty_ranges: Vec<(usize, usize)>,
    /// Whether we've loaded the existing chunk data from storage.
    base_loaded: bool,
    /// Timestamp of first write to this buffer.
    first_write: Instant,
    /// Total bytes written into this buffer (for stats).
    bytes_written: u64,
}

/// Result of a write operation into the buffer.
#[derive(Debug, PartialEq)]
pub enum WriteResult {
    /// The write was buffered; no flush needed yet.
    Buffered,
    /// The entire chunk has been written; the caller should flush
    /// without reading the old chunk (no R in RMW).
    ChunkComplete,
}

/// Information needed to flush a chunk buffer to storage.
pub struct FlushAction {
    /// The volume this chunk belongs to.
    pub volume: String,
    /// The chunk index within the volume.
    pub chunk_idx: usize,
    /// The assembled 4MB data buffer.
    pub data: Vec<u8>,
    /// Whether the entire chunk was written (skip read in RMW).
    pub fully_written: bool,
    /// Byte ranges that were actually modified (for partial-chunk flushes).
    pub dirty_ranges: Vec<(usize, usize)>,
}

impl ChunkBuffer {
    fn new() -> Self {
        Self {
            data: vec![0u8; CHUNK_SIZE],
            dirty_ranges: Vec::new(),
            base_loaded: false,
            first_write: Instant::now(),
            bytes_written: 0,
        }
    }

    /// Overlay data at the given offset within the chunk.
    fn write(&mut self, offset: usize, data: &[u8]) {
        let end = offset + data.len();
        debug_assert!(end <= CHUNK_SIZE, "write exceeds chunk boundary");
        self.data[offset..end].copy_from_slice(data);
        self.bytes_written += data.len() as u64;
        self.add_dirty_range(offset, end);
    }

    /// Merge a new dirty range into the sorted, non-overlapping list.
    fn add_dirty_range(&mut self, start: usize, end: usize) {
        // Insert the new range and merge overlapping/adjacent entries.
        let mut merged = Vec::with_capacity(self.dirty_ranges.len() + 1);
        let mut new_start = start;
        let mut new_end = end;
        let mut inserted = false;

        for &(rs, re) in &self.dirty_ranges {
            if re < new_start {
                // Existing range is entirely before the new range.
                merged.push((rs, re));
            } else if rs > new_end {
                // Existing range is entirely after — insert new range first.
                if !inserted {
                    merged.push((new_start, new_end));
                    inserted = true;
                }
                merged.push((rs, re));
            } else {
                // Overlapping or adjacent — extend the new range.
                new_start = new_start.min(rs);
                new_end = new_end.max(re);
            }
        }

        if !inserted {
            merged.push((new_start, new_end));
        }

        self.dirty_ranges = merged;
    }

    /// Returns true if dirty_ranges cover the entire chunk [0..CHUNK_SIZE).
    fn is_fully_written(&self) -> bool {
        self.dirty_ranges.len() == 1
            && self.dirty_ranges[0].0 == 0
            && self.dirty_ranges[0].1 >= CHUNK_SIZE
    }

    /// Returns the age of this buffer since its first write.
    fn age_ms(&self) -> u64 {
        self.first_write.elapsed().as_millis() as u64
    }

    /// Mark the base data as loaded from storage.
    fn set_base_loaded(&mut self) {
        self.base_loaded = true;
    }

    /// Load base data from an existing chunk read.
    /// Only copies into byte ranges NOT already written (preserving dirty data).
    fn load_base(&mut self, base_data: &[u8]) {
        debug_assert_eq!(base_data.len(), CHUNK_SIZE);
        // Fill in gaps between dirty ranges with the base data.
        let mut cursor = 0usize;
        for &(start, end) in &self.dirty_ranges {
            if cursor < start {
                self.data[cursor..start].copy_from_slice(&base_data[cursor..start]);
            }
            cursor = end;
        }
        if cursor < CHUNK_SIZE {
            self.data[cursor..CHUNK_SIZE].copy_from_slice(&base_data[cursor..CHUNK_SIZE]);
        }
        self.base_loaded = true;
    }
}

impl WriteBuffer {
    /// Create a new WriteBuffer with the default flush timeout (100ms).
    pub fn new() -> Self {
        Self {
            buffers: Mutex::new(HashMap::new()),
            flush_timeout_ms: DEFAULT_FLUSH_TIMEOUT_MS,
        }
    }

    /// Create a new WriteBuffer with a custom flush timeout.
    pub fn with_timeout(flush_timeout_ms: u64) -> Self {
        Self {
            buffers: Mutex::new(HashMap::new()),
            flush_timeout_ms,
        }
    }

    /// Accumulate a sub-chunk write into the buffer.
    ///
    /// Returns `WriteResult::ChunkComplete` if the entire chunk has been written
    /// and should be flushed without reading the old data.
    pub fn write(
        &self,
        volume: &str,
        chunk_idx: usize,
        offset_in_chunk: usize,
        data: &[u8],
    ) -> WriteResult {
        let end = offset_in_chunk + data.len();
        if end > CHUNK_SIZE {
            warn!(
                "write_buffer: write exceeds chunk boundary: offset={} len={} chunk_size={}",
                offset_in_chunk,
                data.len(),
                CHUNK_SIZE,
            );
            return WriteResult::Buffered;
        }

        let key = (volume.to_string(), chunk_idx);
        let mut buffers = self.buffers.lock().unwrap();
        let buf = buffers.entry(key).or_insert_with(ChunkBuffer::new);
        buf.write(offset_in_chunk, data);

        if buf.is_fully_written() {
            debug!(
                "write_buffer: chunk {}:{} fully written ({} bytes accumulated)",
                volume, chunk_idx, buf.bytes_written,
            );
            WriteResult::ChunkComplete
        } else {
            WriteResult::Buffered
        }
    }

    /// Load base data into a buffered chunk (from a ChunkEngine read).
    /// This fills in the gaps between dirty ranges so the full 4MB can be flushed.
    pub fn load_base(&self, volume: &str, chunk_idx: usize, base_data: &[u8]) {
        let key = (volume.to_string(), chunk_idx);
        let mut buffers = self.buffers.lock().unwrap();
        if let Some(buf) = buffers.get_mut(&key) {
            buf.load_base(base_data);
        }
    }

    /// Check if a chunk buffer needs its base loaded from storage.
    pub fn needs_base_load(&self, volume: &str, chunk_idx: usize) -> bool {
        let key = (volume.to_string(), chunk_idx);
        let buffers = self.buffers.lock().unwrap();
        if let Some(buf) = buffers.get(&key) {
            !buf.base_loaded && !buf.is_fully_written()
        } else {
            false
        }
    }

    /// Take (remove) a chunk buffer and return it as a FlushAction.
    /// The caller is responsible for writing the data to the ChunkEngine.
    pub fn take_flush(&self, volume: &str, chunk_idx: usize) -> Option<FlushAction> {
        let key = (volume.to_string(), chunk_idx);
        let mut buffers = self.buffers.lock().unwrap();
        buffers.remove(&key).map(|buf| FlushAction {
            volume: volume.to_string(),
            chunk_idx,
            fully_written: buf.is_fully_written(),
            dirty_ranges: buf.dirty_ranges,
            data: buf.data,
        })
    }

    /// Flush all dirty buffers for a given volume.
    /// Returns a list of FlushActions that the caller must write to the ChunkEngine.
    pub fn flush_all(&self, volume: &str) -> Vec<FlushAction> {
        let mut buffers = self.buffers.lock().unwrap();
        let keys_to_flush: Vec<(String, usize)> = buffers
            .keys()
            .filter(|(v, _)| v == volume)
            .cloned()
            .collect();

        let mut actions = Vec::with_capacity(keys_to_flush.len());
        for key in keys_to_flush {
            if let Some(buf) = buffers.remove(&key) {
                info!(
                    "write_buffer: flushing chunk {}:{} ({} bytes, fully_written={})",
                    key.0,
                    key.1,
                    buf.bytes_written,
                    buf.is_fully_written(),
                );
                actions.push(FlushAction {
                    volume: key.0,
                    chunk_idx: key.1,
                    fully_written: buf.is_fully_written(),
                    dirty_ranges: buf.dirty_ranges,
                    data: buf.data,
                });
            }
        }
        actions
    }

    /// Flush buffers that have been accumulating longer than the flush timeout.
    /// Returns a list of FlushActions for expired buffers.
    pub fn flush_expired(&self) -> Vec<FlushAction> {
        let mut buffers = self.buffers.lock().unwrap();
        let keys_to_flush: Vec<(String, usize)> = buffers
            .iter()
            .filter(|(_, buf)| buf.age_ms() >= self.flush_timeout_ms)
            .map(|(key, _)| key.clone())
            .collect();

        let mut actions = Vec::with_capacity(keys_to_flush.len());
        for key in keys_to_flush {
            if let Some(buf) = buffers.remove(&key) {
                debug!(
                    "write_buffer: expired chunk {}:{} (age={}ms, {} bytes, fully_written={})",
                    key.0,
                    key.1,
                    buf.age_ms(),
                    buf.bytes_written,
                    buf.is_fully_written(),
                );
                actions.push(FlushAction {
                    volume: key.0,
                    chunk_idx: key.1,
                    fully_written: buf.is_fully_written(),
                    dirty_ranges: buf.dirty_ranges,
                    data: buf.data,
                });
            }
        }
        actions
    }

    /// Remove all buffers for a volume (e.g., on volume delete).
    /// Returns the number of buffers dropped.
    pub fn drop_volume(&self, volume: &str) -> usize {
        let mut buffers = self.buffers.lock().unwrap();
        let before = buffers.len();
        buffers.retain(|(v, _), _| v != volume);
        let dropped = before - buffers.len();
        if dropped > 0 {
            info!(
                "write_buffer: dropped {} buffers for volume {}",
                dropped, volume
            );
        }
        dropped
    }

    /// Returns the number of currently buffered chunks.
    pub fn buffered_count(&self) -> usize {
        self.buffers.lock().unwrap().len()
    }

    /// Returns the number of buffered chunks for a specific volume.
    pub fn buffered_count_for_volume(&self, volume: &str) -> usize {
        self.buffers
            .lock()
            .unwrap()
            .keys()
            .filter(|(v, _)| v == volume)
            .count()
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_write_is_buffered() {
        let wb = WriteBuffer::new();
        let result = wb.write("vol1", 0, 0, &[0xAA; 512]);
        assert_eq!(result, WriteResult::Buffered);
        assert_eq!(wb.buffered_count(), 1);
    }

    #[test]
    fn test_full_chunk_write_returns_complete() {
        let wb = WriteBuffer::new();
        let data = vec![0xBB; CHUNK_SIZE];
        let result = wb.write("vol1", 0, 0, &data);
        assert_eq!(result, WriteResult::ChunkComplete);
    }

    #[test]
    fn test_sequential_fills_complete_chunk() {
        let wb = WriteBuffer::new();
        let block_size = 4096;
        let blocks = CHUNK_SIZE / block_size;

        for i in 0..blocks - 1 {
            let result = wb.write("vol1", 0, i * block_size, &[0xCC; 4096]);
            assert_eq!(result, WriteResult::Buffered);
        }

        // Last block completes the chunk.
        let result = wb.write("vol1", 0, (blocks - 1) * block_size, &[0xCC; 4096]);
        assert_eq!(result, WriteResult::ChunkComplete);
    }

    #[test]
    fn test_overlapping_writes_merge_ranges() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 1000]);
        wb.write("vol1", 0, 500, &[0xBB; 1000]);

        // After merging, should have one range [0..1500).
        let action = wb.take_flush("vol1", 0).unwrap();
        assert_eq!(action.dirty_ranges.len(), 1);
        assert_eq!(action.dirty_ranges[0], (0, 1500));
        // The overlapping region should have the second write's data.
        assert_eq!(action.data[999], 0xBB);
        assert_eq!(action.data[0], 0xAA);
    }

    #[test]
    fn test_flush_all_returns_all_volume_buffers() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 512]);
        wb.write("vol1", 1, 0, &[0xBB; 512]);
        wb.write("vol2", 0, 0, &[0xCC; 512]);

        let actions = wb.flush_all("vol1");
        assert_eq!(actions.len(), 2);
        assert_eq!(wb.buffered_count(), 1); // vol2 remains
    }

    #[test]
    fn test_drop_volume_removes_all_buffers() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 512]);
        wb.write("vol1", 1, 0, &[0xBB; 512]);
        wb.write("vol2", 0, 0, &[0xCC; 512]);

        let dropped = wb.drop_volume("vol1");
        assert_eq!(dropped, 2);
        assert_eq!(wb.buffered_count(), 1);
    }

    #[test]
    fn test_load_base_fills_gaps() {
        let wb = WriteBuffer::new();
        // Write to the middle of the chunk.
        wb.write("vol1", 0, 1000, &[0xFF; 500]);

        // Load base data — should fill gaps but preserve dirty region.
        let base = vec![0x11; CHUNK_SIZE];
        wb.load_base("vol1", 0, &base);

        let action = wb.take_flush("vol1", 0).unwrap();
        // Before dirty region: should be base data.
        assert_eq!(action.data[0], 0x11);
        assert_eq!(action.data[999], 0x11);
        // Dirty region: should be our write.
        assert_eq!(action.data[1000], 0xFF);
        assert_eq!(action.data[1499], 0xFF);
        // After dirty region: should be base data.
        assert_eq!(action.data[1500], 0x11);
    }

    #[test]
    fn test_needs_base_load() {
        let wb = WriteBuffer::new();
        // No buffer yet — no base load needed.
        assert!(!wb.needs_base_load("vol1", 0));

        // Partial write — needs base load.
        wb.write("vol1", 0, 0, &[0xAA; 512]);
        assert!(wb.needs_base_load("vol1", 0));

        // Full chunk write — does NOT need base load.
        let wb2 = WriteBuffer::new();
        wb2.write("vol2", 0, 0, &vec![0xBB; CHUNK_SIZE]);
        assert!(!wb2.needs_base_load("vol2", 0));
    }

    #[test]
    fn test_take_flush_removes_buffer() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 512]);

        let action = wb.take_flush("vol1", 0);
        assert!(action.is_some());
        assert_eq!(wb.buffered_count(), 0);

        // Second take returns None.
        assert!(wb.take_flush("vol1", 0).is_none());
    }

    #[test]
    fn test_flush_expired_with_zero_timeout() {
        let wb = WriteBuffer::with_timeout(0);
        wb.write("vol1", 0, 0, &[0xAA; 512]);

        // With 0ms timeout, buffer is immediately expired.
        let actions = wb.flush_expired();
        assert_eq!(actions.len(), 1);
        assert_eq!(wb.buffered_count(), 0);
    }

    #[test]
    fn test_multiple_chunks_independent() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 512]);
        wb.write("vol1", 1, 0, &[0xBB; 512]);

        assert_eq!(wb.buffered_count_for_volume("vol1"), 2);

        let action0 = wb.take_flush("vol1", 0).unwrap();
        assert_eq!(action0.data[0], 0xAA);
        assert_eq!(wb.buffered_count_for_volume("vol1"), 1);

        let action1 = wb.take_flush("vol1", 1).unwrap();
        assert_eq!(action1.data[0], 0xBB);
        assert_eq!(wb.buffered_count_for_volume("vol1"), 0);
    }

    #[test]
    fn test_dirty_ranges_adjacent_merge() {
        let wb = WriteBuffer::new();
        // Two adjacent writes should merge.
        wb.write("vol1", 0, 0, &[0xAA; 1000]);
        wb.write("vol1", 0, 1000, &[0xBB; 1000]);

        let action = wb.take_flush("vol1", 0).unwrap();
        assert_eq!(action.dirty_ranges.len(), 1);
        assert_eq!(action.dirty_ranges[0], (0, 2000));
    }

    #[test]
    fn test_dirty_ranges_gap_preserved() {
        let wb = WriteBuffer::new();
        // Two writes with a gap.
        wb.write("vol1", 0, 0, &[0xAA; 100]);
        wb.write("vol1", 0, 200, &[0xBB; 100]);

        let action = wb.take_flush("vol1", 0).unwrap();
        assert_eq!(action.dirty_ranges.len(), 2);
        assert_eq!(action.dirty_ranges[0], (0, 100));
        assert_eq!(action.dirty_ranges[1], (200, 300));
    }

    #[test]
    fn test_fully_written_flag() {
        let wb = WriteBuffer::new();
        let data = vec![0xDD; CHUNK_SIZE];
        wb.write("vol1", 0, 0, &data);

        let action = wb.take_flush("vol1", 0).unwrap();
        assert!(action.fully_written);
    }

    #[test]
    fn test_partial_write_not_fully_written() {
        let wb = WriteBuffer::new();
        wb.write("vol1", 0, 0, &[0xAA; 512]);

        let action = wb.take_flush("vol1", 0).unwrap();
        assert!(!action.fully_written);
    }
}
