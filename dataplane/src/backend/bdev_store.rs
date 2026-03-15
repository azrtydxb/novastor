//! BdevChunkStore — chunk storage on SPDK bdevs (raw device or lvol).
//!
//! Uses a bitmap allocator for 4MB-aligned slot management and stores
//! chunk_id → slot mappings in an on-disk index. Supports both raw block
//! devices and LVM logical volumes — the BdevChunkStore doesn't care
//! which type of bdev it operates on.

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, ChunkStoreStats, CHUNK_SIZE};

/// On-disk slot size: data payload (CHUNK_SIZE) + per-chunk header,
/// rounded up to a 512-byte boundary so all offsets are sector-aligned.
const SLOT_SIZE: u64 = ((CHUNK_SIZE as u64 + ChunkHeader::SIZE as u64) + 511) & !511;
use crate::error::{DataPlaneError, Result};
use crate::spdk::reactor_dispatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::RwLock;

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
        if slot >= self.total_slots {
            return;
        }
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
        if slot >= self.total_slots {
            return false;
        }
        let byte_idx = (slot / 8) as usize;
        let bit_idx = (slot % 8) as u8;
        (self.bitmap[byte_idx] >> bit_idx) & 1 == 1
    }

    pub fn total_slots(&self) -> u64 {
        self.total_slots
    }
    pub fn used_slots(&self) -> u64 {
        self.used_slots
    }
    pub fn free_slots(&self) -> u64 {
        self.total_slots - self.used_slots
    }

    /// Get the raw bitmap bytes (for persistence).
    pub fn bitmap_bytes(&self) -> &[u8] {
        &self.bitmap
    }

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
    pub magic: [u8; 8], // b"NVACHUNK"
    pub version: u32,
    pub generation: u64, // Monotonically increasing for A/B recovery.
    pub chunk_size: u32,
    pub total_slots: u64,
    pub used_slots: u64,
    pub bitmap_bytes: u32,  // Size of bitmap region in bytes.
    pub index_entries: u32, // Number of chunk_id→slot entries.
    pub checksum: u32,      // CRC-32C of all preceding fields.
}

impl Superblock {
    /// Size of serialized superblock: 4KB (4096 bytes), matching the spec.
    /// The first 48 bytes contain fields, followed by zero padding to 4KB.
    pub const SIZE: usize = 4096;

    /// Byte offset where fields end. CRC covers buf[0..FIELDS_END].
    /// Checksum is stored at FIELDS_END..FIELDS_END+4.
    const FIELDS_END: usize = 48;

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; Self::SIZE];
        let mut off = 0;

        buf[off..off + 8].copy_from_slice(&self.magic);
        off += 8;
        buf[off..off + 4].copy_from_slice(&self.version.to_le_bytes());
        off += 4;
        buf[off..off + 8].copy_from_slice(&self.generation.to_le_bytes());
        off += 8;
        buf[off..off + 4].copy_from_slice(&self.chunk_size.to_le_bytes());
        off += 4;
        buf[off..off + 8].copy_from_slice(&self.total_slots.to_le_bytes());
        off += 8;
        buf[off..off + 8].copy_from_slice(&self.used_slots.to_le_bytes());
        off += 8;
        buf[off..off + 4].copy_from_slice(&self.bitmap_bytes.to_le_bytes());
        off += 4;
        buf[off..off + 4].copy_from_slice(&self.index_entries.to_le_bytes());
        // off is now 48 == FIELDS_END
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
        magic.copy_from_slice(&buf[off..off + 8]);
        off += 8;
        let version = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;
        let generation = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let chunk_size = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;
        let total_slots = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let used_slots = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        off += 8;
        let bitmap_bytes = u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        off += 4;
        let index_entries =
            u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
        // off is now 48 == FIELDS_END. Checksum stored at bytes 48..52.
        let _ = off; // intentional: off == FIELDS_END, use constant directly
        let checksum = u32::from_le_bytes([
            buf[Self::FIELDS_END],
            buf[Self::FIELDS_END + 1],
            buf[Self::FIELDS_END + 2],
            buf[Self::FIELDS_END + 3],
        ]);

        // Verify checksum covers bytes 0..48 (fields only, not the checksum itself).
        let expected_crc = crc32c::crc32c(&buf[..Self::FIELDS_END]);
        if checksum != expected_crc {
            return Err(DataPlaneError::BdevError(format!(
                "superblock checksum mismatch: stored={checksum:#x}, computed={expected_crc:#x}"
            )));
        }

        Ok(Self {
            magic,
            version,
            generation,
            chunk_size,
            total_slots,
            used_slots,
            bitmap_bytes,
            index_entries,
            checksum,
        })
    }
}

/// On-disk index entry: chunk_id → slot number.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub chunk_id: String, // 64-char hex SHA-256.
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

/// Round up `val` to the next multiple of `align`.
fn align_up(val: u64, align: u64) -> u64 {
    if align == 0 {
        return val;
    }
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
        let estimated_slots = capacity_bytes / SLOT_SIZE;
        let bitmap_size = (estimated_slots + 7) / 8;
        let index_size = estimated_slots * IndexEntry::SIZE as u64;
        let per_copy = Superblock::SIZE as u64 + bitmap_size + index_size;
        // Align total reserved (2 copies) up to 4MB boundary for clean data region start.
        let reserved = align_up(per_copy * 2, SLOT_SIZE);

        let data_capacity = capacity_bytes.saturating_sub(reserved);
        let total_slots = data_capacity / SLOT_SIZE;

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
        self.data_region_offset + slot * SLOT_SIZE
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
            log::debug!("BdevChunkStore::put dedup hit chunk={}", &chunk_id[..16]);
            return Ok(());
        }

        // Allocate a slot.
        let slot = self.allocator.lock().unwrap().allocate().ok_or_else(|| {
            DataPlaneError::BdevError(format!("bdev {} out of space", self.bdev_name))
        })?;

        let offset = self.slot_to_offset(slot);
        log::debug!(
            "BdevChunkStore::put chunk={} slot={} offset={} bdev={} data_len={}",
            &chunk_id[..16],
            slot,
            offset,
            self.bdev_name,
            data.len()
        );

        // Write data to the bdev via async reactor dispatch.
        if let Err(e) = reactor_dispatch::bdev_write_async(&self.bdev_name, offset, data).await {
            log::error!(
                "BdevChunkStore::put bdev_write_async FAILED: {} bdev={} offset={}",
                e,
                self.bdev_name,
                offset
            );
            // Roll back allocation on failure.
            self.allocator.lock().unwrap().free(slot);
            return Err(e);
        }
        log::debug!(
            "BdevChunkStore::put bdev_write_async OK chunk={}",
            &chunk_id[..16]
        );

        // Record in index.
        self.index
            .write()
            .unwrap()
            .insert(chunk_id.to_string(), slot);
        Ok(())
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let slot = {
            let index = self.index.read().unwrap();
            *index.get(chunk_id).ok_or_else(|| {
                DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &chunk_id[..16.min(chunk_id.len())]
                ))
            })?
        };

        let offset = self.slot_to_offset(slot);
        reactor_dispatch::bdev_read_async(&self.bdev_name, offset, SLOT_SIZE).await
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        let slot = {
            let mut index = self.index.write().unwrap();
            index.remove(chunk_id).ok_or_else(|| {
                DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &chunk_id[..16.min(chunk_id.len())]
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
            used_bytes: alloc.used_slots() * SLOT_SIZE,
            data_bytes: index.len() as u64 * SLOT_SIZE,
            chunk_count: index.len() as u64,
        })
    }
}

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

    #[test]
    fn index_entry_roundtrip() {
        let entry = IndexEntry {
            chunk_id: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                .to_string(),
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
}
