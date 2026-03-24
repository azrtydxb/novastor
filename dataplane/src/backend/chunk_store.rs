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

/// A null chunk store for frontend-only nodes that have no local storage.
///
/// All local I/O operations return errors, because a frontend-only node has
/// no backend bdev. The ChunkEngine's CRUSH placement will route all chunks
/// to remote nodes that have actual backends.
pub struct NullChunkStore;

#[async_trait]
impl ChunkStore for NullChunkStore {
    async fn put(&self, chunk_id: &str, _data: &[u8]) -> Result<()> {
        Err(crate::error::DataPlaneError::ChunkEngineError(format!(
            "frontend-only node has no local storage — cannot store chunk '{}'",
            chunk_id
        )))
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        Err(crate::error::DataPlaneError::ChunkEngineError(format!(
            "frontend-only node has no local storage — cannot read chunk '{}'",
            chunk_id
        )))
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        Err(crate::error::DataPlaneError::ChunkEngineError(format!(
            "frontend-only node has no local storage — cannot delete chunk '{}'",
            chunk_id
        )))
    }

    async fn exists(&self, _chunk_id: &str) -> Result<bool> {
        Ok(false)
    }

    async fn stats(&self) -> Result<ChunkStoreStats> {
        Ok(ChunkStoreStats {
            backend_name: "null".to_string(),
            total_bytes: 0,
            used_bytes: 0,
            data_bytes: 0,
            chunk_count: 0,
        })
    }
}

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
        let checksum = parsed.checksum;
        assert_eq!(checksum, 0xDEADBEEF);
        let data_len = parsed.data_len;
        assert_eq!(data_len, 4 * 1024 * 1024);
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
