//! Write-back cache for ChunkEngine sub-block writes.
//!
//! Absorbs writes and returns immediately. Data is flushed to backends
//! on host FLUSH commands or when the cache reaches high-water mark.

use std::collections::HashMap;
use std::time::Instant;

const DEFAULT_CAPACITY: usize = 128;
const HIGH_WATER_RATIO: f64 = 0.75;

struct CacheEntry {
    volume_id: String,
    offset: u64,
    data: Vec<u8>,
    created: Instant,
}

pub struct WriteCache {
    entries: HashMap<(String, u64), CacheEntry>,
    capacity: usize,
}

impl WriteCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            capacity: DEFAULT_CAPACITY,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
        }
    }

    /// Absorb a write. Returns true if cached, false if full.
    pub fn absorb(&mut self, volume_id: &str, offset: u64, data: &[u8]) -> bool {
        if self.entries.len() >= self.capacity
            && !self.entries.contains_key(&(volume_id.to_string(), offset))
        {
            return false;
        }
        self.entries.insert(
            (volume_id.to_string(), offset),
            CacheEntry {
                volume_id: volume_id.to_string(),
                offset,
                data: data.to_vec(),
                created: Instant::now(),
            },
        );
        true
    }

    /// Look up cached data. Returns clone if fully covered.
    pub fn lookup(&self, volume_id: &str, offset: u64, length: u64) -> Option<Vec<u8>> {
        let key = (volume_id.to_string(), offset);
        self.entries.get(&key).map(|e| {
            let end = std::cmp::min(length as usize, e.data.len());
            e.data[..end].to_vec()
        })
    }

    /// Drain all entries for a volume, sorted by offset.
    pub fn drain_volume(&mut self, volume_id: &str) -> Vec<(u64, Vec<u8>)> {
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

    /// Drain oldest entries when above high-water mark.
    pub fn drain_overflow(&mut self) -> Vec<(String, u64, Vec<u8>)> {
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

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn needs_flush(&self) -> bool {
        self.entries.len() >= (self.capacity as f64 * HIGH_WATER_RATIO) as usize
    }
}

impl Default for WriteCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absorb_and_lookup() {
        let mut cache = WriteCache::new();
        let data = vec![42u8; 64 * 1024];
        assert!(cache.absorb("vol-1", 0, &data));
        assert_eq!(cache.len(), 1);
        let result = cache.lookup("vol-1", 0, 64 * 1024);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        assert!(cache.lookup("vol-1", 65536, 64 * 1024).is_none());
    }

    #[test]
    fn overwrite_same_key() {
        let mut cache = WriteCache::new();
        cache.absorb("vol-1", 0, &[1; 100]);
        cache.absorb("vol-1", 0, &[2; 100]); // overwrite
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.lookup("vol-1", 0, 100).unwrap(), vec![2u8; 100]);
    }

    #[test]
    fn drain_volume() {
        let mut cache = WriteCache::new();
        cache.absorb("vol-1", 0, &[1; 100]);
        cache.absorb("vol-1", 65536, &[2; 100]);
        cache.absorb("vol-2", 0, &[3; 100]);
        let drained = cache.drain_volume("vol-1");
        assert_eq!(drained.len(), 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn capacity_limit() {
        let mut cache = WriteCache::with_capacity(2);
        assert!(cache.absorb("v", 0, &[1]));
        assert!(cache.absorb("v", 1, &[2]));
        assert!(!cache.absorb("v", 2, &[3]));
    }

    #[test]
    fn drain_overflow() {
        let mut cache = WriteCache::with_capacity(4);
        cache.absorb("v", 0, &[1]);
        cache.absorb("v", 1, &[2]);
        cache.absorb("v", 2, &[3]);
        assert!(!cache.needs_flush());
        cache.absorb("v", 3, &[4]);
        assert!(cache.needs_flush());
        let drained = cache.drain_overflow();
        assert_eq!(drained.len(), 1);
        assert_eq!(cache.len(), 3);
    }
}
