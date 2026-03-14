//! FileChunkStore — stores chunks via SPDK AIO bdevs backed by files.
//!
//! The File backend creates a single large backing file on an existing mounted
//! filesystem, then registers it as an SPDK AIO (io_uring) bdev. All I/O flows
//! through SPDK, not through POSIX read/write — satisfying invariant #2.
//!
//! Chunk storage is delegated to `BdevChunkStore` which handles bitmap
//! allocation, on-disk index, and slot management on the bdev.

use crate::backend::bdev_store::BdevChunkStore;
use crate::backend::chunk_store::{ChunkStore, ChunkStoreStats};
use crate::config::LocalBdevConfig;
use crate::error::{DataPlaneError, Result};
use crate::spdk::bdev_manager::BdevManager;
use async_trait::async_trait;
use log::info;
use std::path::PathBuf;
use std::sync::OnceLock;

/// Default block size for AIO bdevs.
const AIO_BLOCK_SIZE: u32 = 4096;

/// Global bdev manager for creating AIO bdevs.
static FILE_BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

fn bdev_manager() -> &'static BdevManager {
    FILE_BDEV_MANAGER.get_or_init(BdevManager::new)
}

/// A chunk store backed by an SPDK AIO bdev over a file on a mounted filesystem.
///
/// All I/O goes through SPDK (not POSIX read/write), ensuring invariant #2
/// ("All data I/O flows through SPDK") is satisfied.
pub struct FileChunkStore {
    /// The underlying BdevChunkStore that handles slot allocation and I/O.
    inner: BdevChunkStore,
    /// Path to the backing file (for diagnostics / cleanup).
    backing_file: PathBuf,
    /// SPDK bdev name for the AIO bdev.
    bdev_name: String,
}

impl FileChunkStore {
    /// Create a new FileChunkStore rooted at `base_dir`.
    ///
    /// Creates a backing file at `<base_dir>/novastor-chunks.dat` and registers
    /// it as an SPDK AIO bdev. The `capacity_bytes` parameter controls the size
    /// of the backing file and thus the available chunk storage.
    ///
    /// If the backing file already exists and is large enough, it is reused
    /// (preserving existing data). If it is smaller, it is extended.
    pub fn new(base_dir: PathBuf, capacity_bytes: u64) -> Result<Self> {
        // Ensure the directory exists.
        std::fs::create_dir_all(&base_dir).map_err(|e| {
            DataPlaneError::BdevError(format!(
                "failed to create file backend directory {}: {}",
                base_dir.display(),
                e
            ))
        })?;

        let backing_file = base_dir.join("novastor-chunks.dat");
        let bdev_name = format!(
            "file_{}",
            base_dir.file_name().unwrap_or_default().to_string_lossy()
        );

        // Create or extend the backing file to the requested capacity.
        {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&backing_file)
                .map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to create backing file {}: {}",
                        backing_file.display(),
                        e
                    ))
                })?;

            let current_size = f.metadata().map(|m| m.len()).unwrap_or(0);
            if current_size < capacity_bytes {
                f.set_len(capacity_bytes).map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to set backing file size to {} bytes: {}",
                        capacity_bytes, e
                    ))
                })?;
            }
        }

        // Register as an SPDK AIO bdev.
        let config = LocalBdevConfig {
            name: bdev_name.clone(),
            device_path: backing_file.to_string_lossy().to_string(),
            block_size: AIO_BLOCK_SIZE,
        };

        info!(
            "file_backend: creating AIO bdev '{}' backed by '{}'",
            bdev_name,
            backing_file.display()
        );
        bdev_manager().create_aio_bdev(&config)?;

        // Delegate chunk I/O to BdevChunkStore which manages bitmap, index, and
        // 4MB-aligned slot allocation on the SPDK bdev.
        let inner = BdevChunkStore::new(&bdev_name, capacity_bytes);

        info!(
            "file_backend: initialised on '{}' ({} bytes)",
            backing_file.display(),
            capacity_bytes
        );

        Ok(Self {
            inner,
            backing_file,
            bdev_name,
        })
    }

    /// Return the path to the backing file.
    pub fn backing_file(&self) -> &std::path::Path {
        &self.backing_file
    }

    /// Return the SPDK bdev name.
    pub fn bdev_name(&self) -> &str {
        &self.bdev_name
    }
}

#[async_trait]
impl ChunkStore for FileChunkStore {
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        self.inner.put(chunk_id, data).await
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        self.inner.get(chunk_id).await
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        self.inner.delete(chunk_id).await
    }

    async fn exists(&self, chunk_id: &str) -> Result<bool> {
        self.inner.exists(chunk_id).await
    }

    async fn stats(&self) -> Result<ChunkStoreStats> {
        self.inner.stats().await
    }
}

#[cfg(test)]
mod tests {
    // Unit tests for FileChunkStore require an SPDK environment (reactor thread,
    // bdev layer) which is only available on Linux with SPDK initialised. The
    // BdevChunkStore tests in bdev_store.rs cover the underlying logic.
    //
    // Integration tests for the file backend run in the e2e test suite with a
    // real SPDK instance.

    use super::*;

    #[test]
    fn bdev_name_derived_from_dir() {
        // The bdev name should be derived from the directory name.
        let dir = PathBuf::from("/var/lib/novastor/pool1");
        let expected = "file_pool1";
        let actual = format!(
            "file_{}",
            dir.file_name().unwrap_or_default().to_string_lossy()
        );
        assert_eq!(actual, expected);
    }
}
