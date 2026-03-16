//! Chunk I/O — content-addressed 4MB chunk storage through SPDK bdevs.
//!
//! This module implements the core "everything is chunks" data path. Each chunk
//! is a 4MB block of data identified by the SHA-256 hash of its content, with a
//! CRC-32C integrity checksum. All I/O goes through the reactor dispatch layer
//! which ensures SPDK operations run on the reactor thread.

use crate::error::{DataPlaneError, Result};
use crate::spdk::reactor_dispatch;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::RwLock;

/// Default chunk size: 4 MiB.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// SHA-256 hex-encoded chunk ID (64 characters).
pub type ChunkID = String;

/// Metadata for a stored chunk.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMeta {
    pub id: ChunkID,
    pub size: u32,
    pub checksum: u32,
    /// Byte offset on the bdev where this chunk's data is stored.
    pub offset: u64,
}

/// A chunk store backed by an SPDK bdev.
///
/// Manages a mapping from chunk IDs to bdev offsets and handles all I/O
/// operations. Uses a bump allocator with a free list so that deleted chunk
/// offsets are reclaimed and reused before bumping the watermark.
pub struct ChunkStore {
    bdev_name: String,
    /// Chunk metadata index: chunk ID → meta.
    index: RwLock<HashMap<ChunkID, ChunkMeta>>,
    /// Next free offset for allocation (bump allocator watermark).
    next_offset: std::sync::Mutex<u64>,
    /// Free list: offsets reclaimed from deleted chunks, available for reuse.
    /// Each entry is an aligned offset that can hold one CHUNK_SIZE block.
    free_offsets: std::sync::Mutex<Vec<u64>>,
    /// Total capacity in bytes.
    capacity_bytes: u64,
}

impl ChunkStore {
    /// Create a new chunk store backed by the named bdev.
    pub fn new(bdev_name: &str, capacity_bytes: u64) -> Self {
        info!(
            "chunk store created on bdev={}, capacity={}B",
            bdev_name, capacity_bytes
        );
        Self {
            bdev_name: bdev_name.to_string(),
            index: RwLock::new(HashMap::new()),
            next_offset: std::sync::Mutex::new(0),
            free_offsets: std::sync::Mutex::new(Vec::new()),
            capacity_bytes,
        }
    }

    /// Compute the SHA-256 chunk ID for the given data.
    /// Uses ring for hardware-accelerated SHA-256 (auto-detects ARM SHA extensions).
    pub fn compute_chunk_id(data: &[u8]) -> ChunkID {
        let hash = ring::digest::digest(&ring::digest::SHA256, data);
        hex::encode(hash.as_ref())
    }

    /// Compute CRC-32C checksum for the given data.
    pub fn compute_checksum(data: &[u8]) -> u32 {
        crc32c::crc32c(data)
    }

    /// Write a chunk to the bdev. Returns the chunk ID.
    ///
    /// The chunk is content-addressed: if a chunk with the same ID already
    /// exists, this is a no-op (deduplication).
    pub fn write_chunk(&self, data: &[u8]) -> Result<ChunkID> {
        if data.len() > CHUNK_SIZE {
            return Err(DataPlaneError::BdevError(format!(
                "chunk too large: {} > {}",
                data.len(),
                CHUNK_SIZE
            )));
        }

        let chunk_id = Self::compute_chunk_id(data);
        let checksum = Self::compute_checksum(data);

        // Deduplication: if chunk already exists, return immediately.
        if self.index.read().unwrap().contains_key(&chunk_id) {
            debug!("chunk {} already exists (dedup hit)", &chunk_id[..16]);
            return Ok(chunk_id);
        }

        // Allocate space on the bdev.
        let offset = self.allocate_offset(data.len() as u64)?;

        reactor_dispatch::bdev_write(&self.bdev_name, offset, data)?;

        // Record metadata.
        let meta = ChunkMeta {
            id: chunk_id.clone(),
            size: data.len() as u32,
            checksum,
            offset,
        };
        self.index.write().unwrap().insert(chunk_id.clone(), meta);

        debug!(
            "chunk {} written ({} bytes, checksum={})",
            &chunk_id[..16],
            data.len(),
            checksum
        );
        Ok(chunk_id)
    }

    /// Read a chunk from the bdev by its ID. Verifies the CRC-32C checksum.
    pub fn read_chunk(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let meta = {
            let index = self.index.read().unwrap();
            index.get(chunk_id).cloned().ok_or_else(|| {
                DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &chunk_id[..16.min(chunk_id.len())]
                ))
            })?
        };

        let data = reactor_dispatch::bdev_read(&self.bdev_name, meta.offset, meta.size as u64)?;

        // Verify checksum.
        let actual_checksum = Self::compute_checksum(&data);
        if actual_checksum != meta.checksum {
            return Err(DataPlaneError::BdevError(format!(
                "chunk {} checksum mismatch: expected={}, got={}",
                &chunk_id[..16.min(chunk_id.len())],
                meta.checksum,
                actual_checksum
            )));
        }

        debug!(
            "chunk {} read ({} bytes, checksum verified)",
            &chunk_id[..16.min(chunk_id.len())],
            data.len()
        );
        Ok(data)
    }

    /// Delete a chunk from the bdev and return its offset to the free list.
    pub fn delete_chunk(&self, chunk_id: &str) -> Result<()> {
        let removed = self.index.write().unwrap().remove(chunk_id);
        let meta = match removed {
            Some(m) => m,
            None => {
                return Err(DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &chunk_id[..16.min(chunk_id.len())]
                )));
            }
        };

        // Return the offset to the free list so it can be reused.
        self.free_offsets.lock().unwrap().push(meta.offset);
        debug!(
            "chunk {} deleted, offset {} returned to free list",
            &chunk_id[..16.min(chunk_id.len())],
            meta.offset
        );
        Ok(())
    }

    /// Check if a chunk exists.
    pub fn has_chunk(&self, chunk_id: &str) -> bool {
        self.index.read().unwrap().contains_key(chunk_id)
    }

    /// List all chunk IDs.
    pub fn list_chunks(&self) -> Vec<ChunkID> {
        self.index.read().unwrap().keys().cloned().collect()
    }

    /// Get metadata for a chunk.
    pub fn get_chunk_meta(&self, chunk_id: &str) -> Option<ChunkMeta> {
        self.index.read().unwrap().get(chunk_id).cloned()
    }

    /// Return the underlying bdev name.
    pub fn bdev_name(&self) -> &str {
        &self.bdev_name
    }

    /// Return storage statistics.
    pub fn stats(&self) -> ChunkStoreStats {
        let index = self.index.read().unwrap();
        let chunk_count = index.len() as u64;
        let data_bytes: u64 = index.values().map(|m| m.size as u64).sum();
        let used_bytes = *self.next_offset.lock().unwrap();
        let free_slots = self.free_offsets.lock().unwrap().len() as u64;
        let reclaimable_bytes = free_slots * CHUNK_SIZE as u64;
        ChunkStoreStats {
            bdev_name: self.bdev_name.clone(),
            capacity_bytes: self.capacity_bytes,
            used_bytes,
            data_bytes,
            chunk_count,
            free_slots,
            reclaimable_bytes,
        }
    }

    /// Verify integrity of all stored chunks by re-reading and checking CRC.
    pub fn scrub(&self) -> Result<ScrubResult> {
        let chunks: Vec<(ChunkID, ChunkMeta)> = {
            let index = self.index.read().unwrap();
            index.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };

        let mut checked = 0u64;
        let mut errors = Vec::new();

        for (chunk_id, meta) in &chunks {
            checked += 1;

            match reactor_dispatch::bdev_read(&self.bdev_name, meta.offset, meta.size as u64) {
                Ok(data) => {
                    let actual = Self::compute_checksum(&data);
                    if actual != meta.checksum {
                        warn!("scrub: chunk {} checksum mismatch", &chunk_id[..16]);
                        errors.push(chunk_id.clone());
                    }
                }
                Err(e) => {
                    warn!("scrub: chunk {} read error: {}", &chunk_id[..16], e);
                    errors.push(chunk_id.clone());
                }
            }
        }

        let error_count = errors.len() as u64;
        info!(
            "scrub complete: checked={}, errors={}",
            checked, error_count
        );
        Ok(ScrubResult {
            chunks_checked: checked,
            errors_found: error_count,
            corrupted_chunks: errors,
        })
    }

    /// Split a large data blob into chunks and write each one.
    /// Returns a list of (chunk_id, size) pairs in order.
    pub fn split_and_write(&self, data: &[u8]) -> Result<Vec<(ChunkID, u32)>> {
        let mut result = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let end = (offset + CHUNK_SIZE).min(data.len());
            let chunk_data = &data[offset..end];
            let chunk_id = self.write_chunk(chunk_data)?;
            result.push((chunk_id, chunk_data.len() as u32));
            offset = end;
        }
        Ok(result)
    }

    /// Reassemble data from an ordered list of chunk IDs.
    pub fn reassemble(&self, chunk_ids: &[String]) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        for chunk_id in chunk_ids {
            let chunk_data = self.read_chunk(chunk_id)?;
            data.extend_from_slice(&chunk_data);
        }
        Ok(data)
    }

    /// Allocate a bdev offset for storing `size` bytes.
    ///
    /// Checks the free list first (offsets reclaimed from deleted chunks).
    /// Falls back to bumping the watermark if no free slots are available.
    fn allocate_offset(&self, size: u64) -> Result<u64> {
        // All chunks are stored at CHUNK_SIZE-aligned boundaries.
        let aligned_size = ((size + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) * CHUNK_SIZE as u64;

        // Try the free list first — reuse space from deleted chunks.
        {
            let mut free = self.free_offsets.lock().unwrap();
            if let Some(offset) = free.pop() {
                debug!(
                    "allocate_offset: reusing free offset {} (free list size={})",
                    offset,
                    free.len()
                );
                return Ok(offset);
            }
        }

        // No free slots — bump the watermark.
        let mut next = self.next_offset.lock().unwrap();
        if *next + aligned_size > self.capacity_bytes {
            let free_count = self.free_offsets.lock().unwrap().len();
            return Err(DataPlaneError::BdevError(format!(
                "bdev {} out of space: need {}B, watermark={}B, capacity={}B, free_slots={}",
                self.bdev_name, aligned_size, *next, self.capacity_bytes, free_count
            )));
        }
        let offset = *next;
        *next += aligned_size;
        Ok(offset)
    }
}

/// Storage statistics for a chunk store.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ChunkStoreStats {
    pub bdev_name: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub data_bytes: u64,
    pub chunk_count: u64,
    /// Number of free slots available for reuse (from deleted chunks).
    pub free_slots: u64,
    /// Bytes reclaimable from the free list.
    pub reclaimable_bytes: u64,
}

/// Result of a scrub (integrity verification) operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScrubResult {
    pub chunks_checked: u64,
    pub errors_found: u64,
    pub corrupted_chunks: Vec<ChunkID>,
}
