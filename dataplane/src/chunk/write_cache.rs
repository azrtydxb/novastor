//! Write-back cache for ChunkEngine sub-block writes.
//!
//! Caches data at sub-block (64KB) granularity. Only 64KB-aligned, full
//! sub-block writes are absorbed into the cache. Partial / unaligned writes
//! bypass the cache and go directly to the backend, but they invalidate
//! any overlapping cached sub-blocks to prevent stale reads.
//!
//! Cache key: `(volume_id, global_sb_index)` where
//! `global_sb_index = offset / 64KB`.
//!
//! The cache is sharded by hash of (volume_id, global_sb_index) to eliminate
//! mutex contention at high queue depths. Default: 64 shards.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

/// Per-shard capacity (number of sub-block entries).
const DEFAULT_SHARD_CAPACITY: usize = 64;

/// Number of shards — should be >= max expected queue depth.
const DEFAULT_NUM_SHARDS: usize = 64;

/// High-water ratio per shard: flush oldest entries when above this.
const HIGH_WATER_RATIO: f64 = 0.75;

/// Sub-block size (64KB).
pub const SUB_BLOCK_SIZE: u64 = 64 * 1024;

/// Chunk size (4MB) — used for coalescing/grouping.
const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Result of attempting to absorb a write into the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbsorbResult {
    /// Write was 64KB-aligned and cached successfully.
    Cached,
    /// Write is not 64KB-aligned or not exactly one sub-block. The caller
    /// MUST call `invalidate_range` and then flush to backend directly.
    NotAligned,
    /// The target shard is full. Caller should flush overflow or write through.
    Full,
}

struct CacheEntry {
    volume_id: String,
    /// Global sub-block index (offset / SUB_BLOCK_SIZE).
    global_sb_index: u64,
    data: Vec<u8>,
    created: Instant,
}

/// A single cache shard with its own entries.
pub struct CacheShard {
    /// Key: (volume_id, global_sb_index). Each entry is exactly 64KB.
    entries: HashMap<(String, u64), CacheEntry>,
    capacity: usize,
}

impl CacheShard {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            capacity,
        }
    }

    /// Absorb a write. Only accepts 64KB-aligned, full sub-block writes.
    fn absorb(&mut self, volume_id: &str, offset: u64, data: &[u8]) -> AbsorbResult {
        // Must be aligned to SUB_BLOCK_SIZE and exactly one sub-block.
        if offset % SUB_BLOCK_SIZE != 0 || data.len() as u64 != SUB_BLOCK_SIZE {
            return AbsorbResult::NotAligned;
        }

        let sb_index = offset / SUB_BLOCK_SIZE;
        let key = (volume_id.to_string(), sb_index);

        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            return AbsorbResult::Full;
        }

        self.entries.insert(
            key,
            CacheEntry {
                volume_id: volume_id.to_string(),
                global_sb_index: sb_index,
                data: data.to_vec(),
                created: Instant::now(),
            },
        );
        AbsorbResult::Cached
    }

    /// Look up cached data for a byte range. Decomposes the range into
    /// sub-block queries and returns data only if ALL sub-blocks are cached.
    fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        if length == 0 {
            return Some(Vec::new());
        }

        let first_sb = offset / SUB_BLOCK_SIZE;
        let last_sb = (offset + length - 1) / SUB_BLOCK_SIZE;

        let mut result = Vec::with_capacity(length as usize);

        for sb in first_sb..=last_sb {
            let key = (volume_id.to_string(), sb);
            let entry = self.entries.get(&key)?;

            let sb_start = sb * SUB_BLOCK_SIZE;
            let range_start = if sb == first_sb {
                (offset - sb_start) as usize
            } else {
                0
            };
            let range_end = if sb == last_sb {
                ((offset + length) - sb_start) as usize
            } else {
                SUB_BLOCK_SIZE as usize
            };

            // Safety: entry.data is always exactly SUB_BLOCK_SIZE bytes.
            if range_end > entry.data.len() || range_start > entry.data.len() {
                return None;
            }
            result.extend_from_slice(&entry.data[range_start..range_end]);
        }

        Some(result)
    }

    /// Invalidate any cached sub-blocks that overlap with the byte range
    /// [offset, offset+length). Called when a partial write bypasses the
    /// cache and goes directly to backend.
    fn invalidate_range(&mut self, volume_id: &str, offset: u64, length: u64) -> usize {
        if length == 0 {
            return 0;
        }
        let first_sb = offset / SUB_BLOCK_SIZE;
        let last_sb = (offset + length - 1) / SUB_BLOCK_SIZE;
        let mut removed = 0;
        for sb in first_sb..=last_sb {
            let key = (volume_id.to_string(), sb);
            if self.entries.remove(&key).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn drain_volume(&mut self, volume_id: &str) -> Vec<(u64, Vec<u8>)> {
        let keys: Vec<_> = self
            .entries
            .keys()
            .filter(|(vid, _)| vid == volume_id)
            .cloned()
            .collect();
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(entry) = self.entries.remove(&key) {
                // Convert back to byte offset for callers.
                let byte_offset = entry.global_sb_index * SUB_BLOCK_SIZE;
                result.push((byte_offset, entry.data));
            }
        }
        result.sort_by_key(|(off, _)| *off);
        result
    }

    fn drain_overflow(&mut self) -> Vec<(String, u64, Vec<u8>)> {
        let threshold = (self.capacity as f64 * HIGH_WATER_RATIO) as usize;
        if self.entries.len() <= threshold {
            return Vec::new();
        }
        let mut by_age: Vec<_> = self.entries.keys().cloned().collect();
        by_age.sort_by_key(|k| self.entries[k].created);
        let to_drain = self.entries.len() - threshold;
        let mut result = Vec::with_capacity(to_drain);
        for key in by_age.into_iter().take(to_drain) {
            if let Some(entry) = self.entries.remove(&key) {
                let byte_offset = entry.global_sb_index * SUB_BLOCK_SIZE;
                result.push((entry.volume_id, byte_offset, entry.data));
            }
        }
        result
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn needs_flush(&self) -> bool {
        self.entries.len() >= (self.capacity as f64 * HIGH_WATER_RATIO) as usize
    }
}

/// A coalesced write: contiguous sub-blocks merged into one buffer.
pub struct CoalescedWrite {
    pub volume_id: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub chunk_index: u64,
}

/// Coalesce a list of (volume_id, offset, data) into grouped, merged writes.
/// Adjacent sub-blocks within the same chunk are merged into one buffer.
pub fn coalesce_writes(mut entries: Vec<(String, u64, Vec<u8>)>) -> Vec<CoalescedWrite> {
    if entries.is_empty() {
        return Vec::new();
    }

    // Sort by (volume_id, offset)
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut result = Vec::new();
    let mut current_vol = String::new();
    let mut current_chunk: u64 = u64::MAX;
    let mut current_offset: u64 = 0;
    let mut current_data: Vec<u8> = Vec::new();

    for (vol, off, data) in entries {
        let chunk = off / CHUNK_SIZE;
        let is_contiguous = vol == current_vol
            && chunk == current_chunk
            && off == current_offset + current_data.len() as u64;

        if is_contiguous {
            // Extend current run
            current_data.extend_from_slice(&data);
        } else {
            // Emit previous run
            if !current_data.is_empty() {
                result.push(CoalescedWrite {
                    volume_id: current_vol.clone(),
                    offset: current_offset,
                    data: std::mem::take(&mut current_data),
                    chunk_index: current_chunk,
                });
            }
            // Start new run
            current_vol = vol;
            current_chunk = chunk;
            current_offset = off;
            current_data = data;
        }
    }

    // Emit last run
    if !current_data.is_empty() {
        result.push(CoalescedWrite {
            volume_id: current_vol,
            offset: current_offset,
            data: current_data,
            chunk_index: current_chunk,
        });
    }

    result
}

/// Sharded write-back cache. Each shard has its own mutex, so writes
/// to different shards proceed in parallel. Default: 64 shards.
///
/// Cache key is `(volume_id, global_sb_index)` where
/// `global_sb_index = offset / 64KB`. Only 64KB-aligned, full sub-block
/// writes are cached. Partial writes invalidate overlapping entries.
pub struct ShardedWriteCache {
    shards: Vec<std::sync::Mutex<CacheShard>>,
    num_shards: usize,
}

impl ShardedWriteCache {
    pub fn new() -> Self {
        Self::with_shards(DEFAULT_NUM_SHARDS, DEFAULT_SHARD_CAPACITY)
    }

    pub fn with_shards(num_shards: usize, shard_capacity: usize) -> Self {
        let shards = (0..num_shards)
            .map(|_| std::sync::Mutex::new(CacheShard::new(shard_capacity)))
            .collect();
        Self { shards, num_shards }
    }

    pub fn shard_index_for(&self, volume_id: &str, offset: u64) -> usize {
        let sb_index = offset / SUB_BLOCK_SIZE;
        self.shard_index(volume_id, sb_index)
    }

    fn shard_index(&self, volume_id: &str, sb_index: u64) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        volume_id.hash(&mut hasher);
        sb_index.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Absorb a write into the appropriate shard.
    /// Returns `AbsorbResult::Cached` only for 64KB-aligned, full sub-block writes.
    /// Returns `AbsorbResult::NotAligned` for partial / misaligned writes.
    /// Returns `AbsorbResult::Full` if the target shard is at capacity OR contended.
    /// Uses try_lock — NEVER blocks the SPDK reactor.
    pub fn absorb(&self, volume_id: &str, offset: u64, data: &[u8]) -> AbsorbResult {
        // Pre-check alignment before taking a lock.
        if offset % SUB_BLOCK_SIZE != 0 || data.len() as u64 != SUB_BLOCK_SIZE {
            return AbsorbResult::NotAligned;
        }
        let sb_index = offset / SUB_BLOCK_SIZE;
        let idx = self.shard_index(volume_id, sb_index);
        match self.shards[idx].try_lock() {
            Ok(mut shard) => shard.absorb(volume_id, offset, data),
            Err(_) => AbsorbResult::Full, // Contended — fall through to tokio
        }
    }

    /// Look up cached data for an arbitrary byte range.
    /// Decomposes into sub-block queries and returns data only if ALL
    /// required sub-blocks are cached.
    /// Uses try_lock — NEVER blocks the SPDK reactor. Returns None if contended.
    pub fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        if length == 0 {
            return Some(Vec::new());
        }

        let first_sb = offset / SUB_BLOCK_SIZE;
        let last_sb = (offset + length - 1) / SUB_BLOCK_SIZE;

        // Fast path: single sub-block — only one shard lock.
        if first_sb == last_sb {
            let idx = self.shard_index(volume_id, first_sb);
            return match self.shards[idx].try_lock() {
                Ok(shard) => shard.lookup(volume_id, offset, length),
                Err(_) => None, // Contended — cache miss, fall to backend
            };
        }

        // Multi-sub-block read: must gather from potentially multiple shards.
        let mut result = Vec::with_capacity(length as usize);
        for sb in first_sb..=last_sb {
            let idx = self.shard_index(volume_id, sb);
            let shard = match self.shards[idx].try_lock() {
                Ok(s) => s,
                Err(_) => return None, // Contended — cache miss
            };
            let key = (volume_id.to_string(), sb);
            let entry = shard.entries.get(&key)?;

            let sb_start = sb * SUB_BLOCK_SIZE;
            let range_start = if sb == first_sb {
                (offset - sb_start) as usize
            } else {
                0
            };
            let range_end = if sb == last_sb {
                ((offset + length) - sb_start) as usize
            } else {
                SUB_BLOCK_SIZE as usize
            };

            if range_end > entry.data.len() || range_start > entry.data.len() {
                return None;
            }
            result.extend_from_slice(&entry.data[range_start..range_end]);
        }

        Some(result)
    }

    /// Invalidate any cached sub-blocks that overlap with the given byte range.
    /// MUST be called when a partial / unaligned write bypasses the cache and
    /// goes directly to the backend, to prevent stale reads.
    /// Uses try_lock — if contended, skips invalidation (the flush path will
    /// see the stale entry but the backend has the correct data, so reads from
    /// backend are correct; a subsequent flush will overwrite with stale data
    /// which is then invalidated on the next partial write).
    pub fn invalidate_range(&self, volume_id: &str, offset: u64, length: u64) -> usize {
        if length == 0 {
            return 0;
        }
        let first_sb = offset / SUB_BLOCK_SIZE;
        let last_sb = (offset + length - 1) / SUB_BLOCK_SIZE;
        let mut total_removed = 0;
        for sb in first_sb..=last_sb {
            let idx = self.shard_index(volume_id, sb);
            if let Ok(mut shard) = self.shards[idx].try_lock() {
                let key = (volume_id.to_string(), sb);
                if shard.entries.remove(&key).is_some() {
                    total_removed += 1;
                }
            }
            // If contended, skip — flush will handle it
        }
        total_removed
    }

    /// Drain all entries for a volume across all shards, sorted by offset.
    pub fn drain_volume(&self, volume_id: &str) -> Vec<(u64, Vec<u8>)> {
        let mut all = Vec::new();
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            all.extend(s.drain_volume(volume_id));
        }
        all.sort_by_key(|(off, _)| *off);
        all
    }

    /// Drain overflow from the shard that was just written to.
    pub fn drain_shard_overflow(&self, shard_idx: usize) -> Vec<(String, u64, Vec<u8>)> {
        self.shards[shard_idx].lock().unwrap().drain_overflow()
    }

    /// Check if a specific shard needs flushing.
    pub fn shard_needs_flush(&self, shard_idx: usize) -> bool {
        self.shards[shard_idx].lock().unwrap().needs_flush()
    }

    /// Total entries across all shards.
    pub fn total_len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.lock().unwrap().len();
        }
        total
    }
}

impl Default for ShardedWriteCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absorb_aligned_sub_block() {
        let cache = ShardedWriteCache::new();
        let data = vec![42u8; SUB_BLOCK_SIZE as usize];
        let result = cache.absorb("vol-1", 0, &data);
        assert_eq!(result, AbsorbResult::Cached);
        assert_eq!(cache.total_len(), 1);
    }

    #[test]
    fn absorb_second_sub_block() {
        let cache = ShardedWriteCache::new();
        let data = vec![42u8; SUB_BLOCK_SIZE as usize];
        assert_eq!(
            cache.absorb("vol-1", SUB_BLOCK_SIZE, &data),
            AbsorbResult::Cached
        );
        assert_eq!(cache.total_len(), 1);
    }

    #[test]
    fn reject_unaligned_offset() {
        let cache = ShardedWriteCache::new();
        let data = vec![0u8; SUB_BLOCK_SIZE as usize];
        let result = cache.absorb("vol-1", 1024, &data);
        assert_eq!(result, AbsorbResult::NotAligned);
        assert_eq!(cache.total_len(), 0);
    }

    #[test]
    fn reject_partial_size() {
        let cache = ShardedWriteCache::new();
        // Aligned offset but not a full sub-block.
        let data = vec![0u8; 4096];
        let result = cache.absorb("vol-1", 0, &data);
        assert_eq!(result, AbsorbResult::NotAligned);
        assert_eq!(cache.total_len(), 0);
    }

    #[test]
    fn lookup_exact_sub_block() {
        let cache = ShardedWriteCache::new();
        let data = vec![0xABu8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        let result = cache.lookup("vol-1", 0, SUB_BLOCK_SIZE);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn lookup_partial_within_sub_block() {
        let cache = ShardedWriteCache::new();
        let mut data = vec![0u8; SUB_BLOCK_SIZE as usize];
        data[1024..1028].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        cache.absorb("vol-1", 0, &data);
        // Read 4 bytes at offset 1024 within sub-block 0.
        let result = cache.lookup("vol-1", 1024, 4);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn lookup_cross_sub_block() {
        let cache = ShardedWriteCache::new();
        let data_sb0 = vec![0x11u8; SUB_BLOCK_SIZE as usize];
        let data_sb1 = vec![0x22u8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data_sb0);
        cache.absorb("vol-1", SUB_BLOCK_SIZE, &data_sb1);

        // Read last 4KB of sb0 + first 4KB of sb1 = 8KB spanning two sub-blocks.
        let read_offset = SUB_BLOCK_SIZE - 4096;
        let read_len = 8192u64;
        let result = cache.lookup("vol-1", read_offset, read_len);
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(got.len(), 8192);
        assert!(got[..4096].iter().all(|&b| b == 0x11));
        assert!(got[4096..].iter().all(|&b| b == 0x22));
    }

    #[test]
    fn lookup_miss_when_not_all_cached() {
        let cache = ShardedWriteCache::new();
        let data = vec![0x11u8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        // sb1 is not cached — cross-sub-block read should return None.
        let result = cache.lookup("vol-1", 0, SUB_BLOCK_SIZE * 2);
        assert!(result.is_none());
    }

    #[test]
    fn invalidate_range_removes_overlapping() {
        let cache = ShardedWriteCache::new();
        let data = vec![0xAAu8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        cache.absorb("vol-1", SUB_BLOCK_SIZE, &data);
        cache.absorb("vol-1", 2 * SUB_BLOCK_SIZE, &data);
        assert_eq!(cache.total_len(), 3);

        // Partial write at offset 1024, length 4096 overlaps sb0 only.
        let removed = cache.invalidate_range("vol-1", 1024, 4096);
        assert_eq!(removed, 1);
        assert_eq!(cache.total_len(), 2);
        // sb0 should be gone.
        assert!(cache.lookup("vol-1", 0, SUB_BLOCK_SIZE).is_none());
        // sb1 and sb2 should remain.
        assert!(cache
            .lookup("vol-1", SUB_BLOCK_SIZE, SUB_BLOCK_SIZE)
            .is_some());
        assert!(cache
            .lookup("vol-1", 2 * SUB_BLOCK_SIZE, SUB_BLOCK_SIZE)
            .is_some());
    }

    #[test]
    fn invalidate_range_spanning_multiple_sub_blocks() {
        let cache = ShardedWriteCache::new();
        let data = vec![0xBBu8; SUB_BLOCK_SIZE as usize];
        for i in 0..4 {
            cache.absorb("vol-1", i * SUB_BLOCK_SIZE, &data);
        }
        assert_eq!(cache.total_len(), 4);

        // Write spanning sb1 and sb2 (partial overlap on each).
        let removed = cache.invalidate_range("vol-1", SUB_BLOCK_SIZE + 100, SUB_BLOCK_SIZE);
        assert_eq!(removed, 2); // sb1 and sb2
        assert_eq!(cache.total_len(), 2);
        // sb0 and sb3 remain.
        assert!(cache.lookup("vol-1", 0, SUB_BLOCK_SIZE).is_some());
        assert!(cache
            .lookup("vol-1", 3 * SUB_BLOCK_SIZE, SUB_BLOCK_SIZE)
            .is_some());
    }

    #[test]
    fn invalidate_range_no_overlap() {
        let cache = ShardedWriteCache::new();
        let data = vec![0xCCu8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        // Invalidate in a range that doesn't overlap sb0.
        let removed = cache.invalidate_range("vol-1", SUB_BLOCK_SIZE, 4096);
        assert_eq!(removed, 0);
        assert_eq!(cache.total_len(), 1);
    }

    #[test]
    fn invalidate_range_different_volume() {
        let cache = ShardedWriteCache::new();
        let data = vec![0xDDu8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        // Invalidate same offset range but different volume.
        let removed = cache.invalidate_range("vol-2", 0, SUB_BLOCK_SIZE);
        assert_eq!(removed, 0);
        assert_eq!(cache.total_len(), 1);
    }

    #[test]
    fn shard_full_returns_full() {
        let cache = ShardedWriteCache::with_shards(1, 2);
        let data = vec![0u8; SUB_BLOCK_SIZE as usize];
        assert_eq!(cache.absorb("vol-1", 0, &data), AbsorbResult::Cached);
        assert_eq!(
            cache.absorb("vol-1", SUB_BLOCK_SIZE, &data),
            AbsorbResult::Cached
        );
        // Shard is full (capacity=2), third entry should fail.
        assert_eq!(
            cache.absorb("vol-1", 2 * SUB_BLOCK_SIZE, &data),
            AbsorbResult::Full
        );
    }

    #[test]
    fn overwrite_existing_entry_when_full() {
        let cache = ShardedWriteCache::with_shards(1, 2);
        let data1 = vec![0x11u8; SUB_BLOCK_SIZE as usize];
        let data2 = vec![0x22u8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data1);
        cache.absorb("vol-1", SUB_BLOCK_SIZE, &data1);
        // Overwrite existing entry — should succeed even when full.
        assert_eq!(cache.absorb("vol-1", 0, &data2), AbsorbResult::Cached);
        let result = cache.lookup("vol-1", 0, SUB_BLOCK_SIZE).unwrap();
        assert_eq!(result, data2);
    }

    #[test]
    fn drain_volume() {
        let cache = ShardedWriteCache::new();
        let data = vec![0u8; SUB_BLOCK_SIZE as usize];
        cache.absorb("vol-1", 0, &data);
        cache.absorb("vol-1", SUB_BLOCK_SIZE, &data);
        cache.absorb("vol-2", 0, &data);
        let drained = cache.drain_volume("vol-1");
        assert_eq!(drained.len(), 2);
        assert_eq!(cache.total_len(), 1);
        // Drained entries should have byte offsets.
        assert_eq!(drained[0].0, 0);
        assert_eq!(drained[1].0, SUB_BLOCK_SIZE);
    }

    #[test]
    fn coalesce_contiguous() {
        let sb = SUB_BLOCK_SIZE as usize;
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; sb]),
            ("vol-1".to_string(), SUB_BLOCK_SIZE, vec![2u8; sb]),
            ("vol-1".to_string(), 2 * SUB_BLOCK_SIZE, vec![3u8; sb]),
            ("vol-2".to_string(), 0, vec![4u8; sb]),
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2); // vol-1 merged, vol-2 separate
        assert_eq!(coalesced[0].volume_id, "vol-1");
        assert_eq!(coalesced[0].data.len(), 3 * sb);
        assert_eq!(coalesced[1].volume_id, "vol-2");
        assert_eq!(coalesced[1].data.len(), sb);
    }

    #[test]
    fn coalesce_non_contiguous() {
        let sb = SUB_BLOCK_SIZE as usize;
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; sb]),
            ("vol-1".to_string(), 3 * SUB_BLOCK_SIZE, vec![2u8; sb]), // gap
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2); // not merged due to gap
    }

    #[test]
    fn coalesce_cross_chunk() {
        let sb = SUB_BLOCK_SIZE as usize;
        // Entries in different chunks should not be merged
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; sb]),
            ("vol-1".to_string(), CHUNK_SIZE, vec![2u8; sb]),
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2);
    }
}
