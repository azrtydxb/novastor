//! Write-back cache for ChunkEngine sub-block writes.
//!
//! Absorbs writes and returns immediately. Data is flushed to backends
//! on host FLUSH commands or when the cache reaches high-water mark.
//!
//! The cache is sharded by hash of (volume_id, offset) to eliminate
//! mutex contention at high queue depths. Default: 64 shards.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

/// Per-shard capacity.
const DEFAULT_SHARD_CAPACITY: usize = 64;

/// Number of shards — should be >= max expected queue depth.
const DEFAULT_NUM_SHARDS: usize = 64;

/// High-water ratio per shard: flush oldest entries when above this.
const HIGH_WATER_RATIO: f64 = 0.75;

/// Sub-block size (64KB) — used for coalescing.
const SUB_BLOCK_SIZE: u64 = 64 * 1024;

/// Chunk size (4MB) — used for grouping.
const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

struct CacheEntry {
    volume_id: String,
    offset: u64,
    data: Vec<u8>,
    created: Instant,
}

/// A single cache shard with its own entries.
pub struct CacheShard {
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

    fn absorb(&mut self, volume_id: &str, offset: u64, data: &[u8]) -> bool {
        let key = (volume_id.to_string(), offset);
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            return false;
        }
        self.entries.insert(
            key,
            CacheEntry {
                volume_id: volume_id.to_string(),
                offset,
                data: data.to_vec(),
                created: Instant::now(),
            },
        );
        true
    }

    fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let key = (volume_id.to_string(), offset);
        self.entries.get(&key).map(|e| {
            let end = std::cmp::min(length as usize, e.data.len());
            e.data[..end].to_vec()
        })
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
                result.push((entry.offset, entry.data));
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
                result.push((entry.volume_id, entry.offset, entry.data));
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
        self.shard_index(volume_id, offset)
    }

    fn shard_index(&self, volume_id: &str, offset: u64) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        volume_id.hash(&mut hasher);
        offset.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Absorb a write into the appropriate shard.
    /// Returns true if cached, false if that shard is full.
    /// Sync — safe to call from SPDK reactor (sub-microsecond lock hold).
    pub fn absorb(&self, volume_id: &str, offset: u64, data: &[u8]) -> bool {
        let idx = self.shard_index(volume_id, offset);
        self.shards[idx]
            .lock()
            .unwrap()
            .absorb(volume_id, offset, data)
    }

    /// Look up cached data from the appropriate shard.
    /// Sync — safe to call from SPDK reactor.
    pub fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let idx = self.shard_index(volume_id, offset);
        self.shards[idx]
            .lock()
            .unwrap()
            .lookup(volume_id, offset, length)
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
    fn shard_absorb_and_lookup() {
        let cache = ShardedWriteCache::new();
        let data = vec![42u8; 64 * 1024];
        assert!(cache.absorb("vol-1", 0, &data));
        let result = cache.lookup("vol-1", 0, 64 * 1024);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        assert!(cache.lookup("vol-1", 65536, 64 * 1024).is_none());
    }

    #[test]
    fn shard_drain_volume() {
        let cache = ShardedWriteCache::new();
        cache.absorb("vol-1", 0, &[1; 100]);
        cache.absorb("vol-1", 65536, &[2; 100]);
        cache.absorb("vol-2", 0, &[3; 100]);
        let drained = cache.drain_volume("vol-1");
        assert_eq!(drained.len(), 2);
        assert_eq!(cache.total_len(), 1);
    }

    #[test]
    fn shard_parallel_no_contention() {
        let cache = ShardedWriteCache::new();
        for i in 0..64u64 {
            assert!(cache.absorb("vol-1", i * 65536, &[i as u8; 100]));
        }
        assert_eq!(cache.total_len(), 64);
    }

    #[test]
    fn coalesce_contiguous() {
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; 100]),
            ("vol-1".to_string(), 100, vec![2u8; 100]),
            ("vol-1".to_string(), 200, vec![3u8; 100]),
            ("vol-2".to_string(), 0, vec![4u8; 50]),
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2); // vol-1 merged, vol-2 separate
        assert_eq!(coalesced[0].volume_id, "vol-1");
        assert_eq!(coalesced[0].data.len(), 300);
        assert_eq!(coalesced[1].volume_id, "vol-2");
        assert_eq!(coalesced[1].data.len(), 50);
    }

    #[test]
    fn coalesce_non_contiguous() {
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; 100]),
            ("vol-1".to_string(), 500, vec![2u8; 100]), // gap
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2); // not merged due to gap
    }

    #[test]
    fn coalesce_cross_chunk() {
        // Entries in different chunks should not be merged
        let entries = vec![
            ("vol-1".to_string(), 0u64, vec![1u8; 100]),
            ("vol-1".to_string(), CHUNK_SIZE, vec![2u8; 100]), // different chunk
        ];
        let coalesced = coalesce_writes(entries);
        assert_eq!(coalesced.len(), 2);
    }
}
