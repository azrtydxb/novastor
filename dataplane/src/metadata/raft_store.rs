//! openraft storage implementations backed by redb.

use std::path::Path;
use std::sync::Arc;

use crate::error::{DataPlaneError, Result};
use crate::metadata::store::MetadataStore;

/// Combined Raft storage for a single shard.
pub struct ShardStorage {
    pub state: Arc<MetadataStore>,
    pub data_dir: std::path::PathBuf,
}

impl ShardStorage {
    /// Open (or create) per-shard storage rooted at `data_dir`.
    ///
    /// The directory is created if it does not already exist. The underlying
    /// redb database is stored as `state.redb` inside `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| DataPlaneError::MetadataError(format!("create shard dir: {e}")))?;
        let state_path = data_dir.join("state.redb");
        let state = Arc::new(MetadataStore::open(state_path)?);
        Ok(Self { state, data_dir })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{
        ChunkMapEntry, MetadataRequest, Protection, VolumeDefinition, VolumeStatus,
    };

    fn sample_volume(id: &str) -> VolumeDefinition {
        VolumeDefinition {
            id: id.to_string(),
            name: format!("vol-{id}"),
            size_bytes: 4 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1_700_000_000,
            chunk_count: 16,
        }
    }

    #[test]
    fn shard_storage_open_creates_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-00");

        assert!(!shard_dir.exists(), "dir should not exist before open");

        let storage = ShardStorage::open(&shard_dir).expect("open must succeed");
        assert!(shard_dir.exists(), "dir must exist after open");

        // Verify basic put/get through the underlying MetadataStore.
        let vol = sample_volume("aabbcc-0001");
        storage.state.put_volume(&vol).expect("put_volume");
        let got = storage
            .state
            .get_volume("aabbcc-0001")
            .expect("get_volume")
            .expect("volume present");
        assert_eq!(got.id, vol.id);
    }

    #[test]
    fn shard_storage_apply_request() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ShardStorage::open(tmp.path().join("shard-01")).expect("open");

        let vol = sample_volume("ddeeff-0002");
        let req = MetadataRequest::PutVolume(vol.clone());
        storage.state.apply(&req).expect("apply PutVolume");

        let got = storage
            .state
            .get_volume(&vol.id)
            .expect("get_volume")
            .expect("volume must be present after apply");
        assert_eq!(got.id, vol.id);
        assert_eq!(got.status, VolumeStatus::Available);
    }

    #[test]
    fn shard_storage_apply_chunk_map() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ShardStorage::open(tmp.path().join("shard-02")).expect("open");

        let vol_id = "112233-0003".to_string();
        let entry = ChunkMapEntry {
            chunk_index: 5,
            chunk_id: "cafebabe".to_string(),
            ec_params: None,
        };
        let req = MetadataRequest::PutChunkMap {
            volume_id: vol_id.clone(),
            entry: entry.clone(),
        };
        storage.state.apply(&req).expect("apply PutChunkMap");

        let got = storage
            .state
            .get_chunk_map(&vol_id, 5)
            .expect("get_chunk_map")
            .expect("entry must be present after apply");
        assert_eq!(got.chunk_index, 5);
        assert_eq!(got.chunk_id, "cafebabe");
    }
}
