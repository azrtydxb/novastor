//! FileChunkStore — stores chunks as files on an existing filesystem.
//!
//! Layout: `<base_dir>/chunks/<ab>/<cd>/<abcdef0123456789...>`
//! Two-level hash-prefix subdirectories for even distribution.
//!
//! I/O uses std::fs wrapped in tokio::task::spawn_blocking.
//! Writes use atomic rename: write to temp file, then rename.

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, ChunkStoreStats};
use crate::error::{DataPlaneError, Result};
use async_trait::async_trait;
use std::path::PathBuf;

/// A chunk store backed by POSIX files in hash-prefix directories.
pub struct FileChunkStore {
    base_dir: PathBuf,
    chunks_dir: PathBuf,
}

impl FileChunkStore {
    /// Create a new FileChunkStore rooted at `base_dir`.
    /// Creates the `chunks/` subdirectory if it doesn't exist.
    pub async fn new(base_dir: PathBuf) -> Result<Self> {
        let chunks_dir = base_dir.join("chunks");
        let cd = chunks_dir.clone();
        tokio::task::spawn_blocking(move || std::fs::create_dir_all(&cd))
            .await
            .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
            .map_err(|e| DataPlaneError::IoError(e))?;

        Ok(Self {
            base_dir,
            chunks_dir,
        })
    }

    /// Build the file path for a chunk: `<chunks_dir>/<ab>/<cd>/<chunk_id>`
    fn chunk_path(&self, chunk_id: &str) -> PathBuf {
        let p1 = &chunk_id[0..2];
        let p2 = &chunk_id[2..4];
        self.chunks_dir.join(p1).join(p2).join(chunk_id)
    }
}

#[async_trait]
impl ChunkStore for FileChunkStore {
    async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        let path = self.chunk_path(chunk_id);
        let data = data.to_vec();

        tokio::task::spawn_blocking(move || {
            // Idempotent: if chunk already exists, skip.
            if path.exists() {
                return Ok(());
            }

            // Ensure parent directory exists.
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            // Atomic write: temp file + rename.
            let tmp_path = path.with_extension("tmp");
            std::fs::write(&tmp_path, &data)?;
            std::fs::rename(&tmp_path, &path)?;
            Ok(())
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let path = self.chunk_path(chunk_id);
        let cid = chunk_id.to_string();

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &cid[..16.min(cid.len())]
                )));
            }
            std::fs::read(&path).map_err(|e| DataPlaneError::IoError(e))
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn delete(&self, chunk_id: &str) -> Result<()> {
        let path = self.chunk_path(chunk_id);
        let cid = chunk_id.to_string();

        tokio::task::spawn_blocking(move || {
            if !path.exists() {
                return Err(DataPlaneError::BdevError(format!(
                    "chunk not found: {}",
                    &cid[..16.min(cid.len())]
                )));
            }
            std::fs::remove_file(&path).map_err(|e| DataPlaneError::IoError(e))
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn exists(&self, chunk_id: &str) -> Result<bool> {
        let path = self.chunk_path(chunk_id);
        tokio::task::spawn_blocking(move || Ok(path.exists()))
            .await
            .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }

    async fn stats(&self) -> Result<ChunkStoreStats> {
        let chunks_dir = self.chunks_dir.clone();
        let base_dir = self.base_dir.clone();

        tokio::task::spawn_blocking(move || {
            let mut chunk_count = 0u64;
            let mut data_bytes = 0u64;

            // Walk the 2-level directory tree.
            if let Ok(entries) = std::fs::read_dir(&chunks_dir) {
                for l1 in entries.flatten() {
                    if !l1.path().is_dir() {
                        continue;
                    }
                    if let Ok(l2_entries) = std::fs::read_dir(l1.path()) {
                        for l2 in l2_entries.flatten() {
                            if !l2.path().is_dir() {
                                continue;
                            }
                            if let Ok(files) = std::fs::read_dir(l2.path()) {
                                for file in files.flatten() {
                                    if file.path().is_file() {
                                        chunk_count += 1;
                                        if let Ok(meta) = file.metadata() {
                                            data_bytes += meta.len();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Get filesystem capacity via statvfs.
            let (total_bytes, used_bytes) = get_fs_usage(&base_dir);

            Ok(ChunkStoreStats {
                backend_name: base_dir.to_string_lossy().to_string(),
                total_bytes,
                used_bytes,
                data_bytes,
                chunk_count,
            })
        })
        .await
        .map_err(|e| DataPlaneError::BdevError(format!("spawn_blocking join: {e}")))?
    }
}

/// Get filesystem total and used bytes.
/// Uses libc::statvfs on Linux. Returns (0, 0) on unsupported platforms.
#[cfg(target_os = "linux")]
fn get_fs_usage(path: &std::path::Path) -> (u64, u64) {
    use std::ffi::CString;
    let path_c = match CString::new(path.to_string_lossy().as_bytes()) {
        Ok(c) => c,
        Err(_) => return (0, 0),
    };
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path_c.as_ptr(), &mut stat) == 0 {
            let total = stat.f_blocks * stat.f_frsize;
            let free = stat.f_bfree * stat.f_frsize;
            (total, total.saturating_sub(free))
        } else {
            (0, 0)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn get_fs_usage(_path: &std::path::Path) -> (u64, u64) {
    (0, 0) // Filesystem stats not available on this platform.
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> (tempfile::TempDir, FileChunkStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FileChunkStore::new(dir.path().to_path_buf()).await.unwrap();
        (dir, store)
    }

    fn fake_chunk_id() -> String {
        "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string()
    }

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let (_dir, store) = make_store().await;
        let id = fake_chunk_id();
        let data = vec![42u8; 100];

        store.put(&id, &data).await.unwrap();
        let got = store.get(&id).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn exists_returns_false_then_true() {
        let (_dir, store) = make_store().await;
        let id = fake_chunk_id();

        assert!(!store.exists(&id).await.unwrap());
        store.put(&id, &[1, 2, 3]).await.unwrap();
        assert!(store.exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn delete_removes_chunk() {
        let (_dir, store) = make_store().await;
        let id = fake_chunk_id();

        store.put(&id, &[1]).await.unwrap();
        assert!(store.exists(&id).await.unwrap());

        store.delete(&id).await.unwrap();
        assert!(!store.exists(&id).await.unwrap());
    }

    #[tokio::test]
    async fn get_nonexistent_returns_error() {
        let (_dir, store) = make_store().await;
        assert!(store.get(&fake_chunk_id()).await.is_err());
    }

    #[tokio::test]
    async fn hash_prefix_directory_layout() {
        let (dir, store) = make_store().await;
        let id = fake_chunk_id();
        store.put(&id, &[1]).await.unwrap();

        // Check that the file exists at chunks/ab/cd/...
        let path = dir.path().join("chunks").join("ab").join("cd").join(&id);
        assert!(path.exists());
    }

    #[tokio::test]
    async fn stats_reports_correctly() {
        let (_dir, store) = make_store().await;
        let id = fake_chunk_id();
        store.put(&id, &[0u8; 256]).await.unwrap();

        let stats = store.stats().await.unwrap();
        assert_eq!(stats.chunk_count, 1);
        assert_eq!(stats.data_bytes, 256);
    }

    #[tokio::test]
    async fn put_is_idempotent() {
        let (_dir, store) = make_store().await;
        let id = fake_chunk_id();
        let data = vec![42u8; 100];

        store.put(&id, &data).await.unwrap();
        // Second put should succeed (idempotent).
        store.put(&id, &data).await.unwrap();
        let got = store.get(&id).await.unwrap();
        assert_eq!(got, data);
    }
}
