//! Shard manager — routes metadata operations to the correct shard.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use crate::error::{DataPlaneError, Result};
use crate::metadata::raft_store::ShardStorage;
use crate::metadata::types::{shard_for_volume, ChunkMapEntry, VolumeDefinition};

/// Total number of shards.  One redb database per shard.
pub const SHARD_COUNT: usize = 256;

/// RAII guard that holds a shard mutex and provides access to the inner
/// `ShardStorage`.  The `Deref` impl allows callers to use `guard.state`
/// transparently without any `unwrap()`.
pub struct ShardGuard<'a> {
    _guard: MutexGuard<'a, Option<ShardStorage>>,
}

impl<'a> ShardGuard<'a> {
    /// Build a `ShardGuard` from a `MutexGuard` that is guaranteed to contain
    /// `Some`.  Returns an error if the inner value is unexpectedly `None`.
    fn new(guard: MutexGuard<'a, Option<ShardStorage>>) -> Result<Self> {
        if guard.is_none() {
            return Err(DataPlaneError::MetadataError(
                "shard not initialised (internal bug)".to_string(),
            ));
        }
        Ok(Self { _guard: guard })
    }

    /// Return a reference to the inner `ShardStorage`.
    pub fn storage(&self) -> &ShardStorage {
        // Safety: `new()` verified `Some`.
        self._guard.as_ref().unwrap()
    }
}

/// Routes every metadata operation to one of 256 per-shard `MetadataStore`
/// instances.  Each shard is initialised lazily on first access.
pub struct ShardManager {
    base_dir: PathBuf,
    shards: Vec<Mutex<Option<ShardStorage>>>,
}

impl ShardManager {
    /// Open (or create) a sharded metadata store rooted at `base_dir`.
    ///
    /// The directory is created if it does not already exist.  Individual shard
    /// sub-directories are created lazily on first use.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)?;

        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Mutex::new(None));
        }

        Ok(Self { base_dir, shards })
    }

    /// Return a locked guard for the shard that owns `volume_id`.
    ///
    /// If the shard has not been initialised yet the redb database is opened
    /// (and the sub-directory created) before the guard is returned.
    ///
    /// Returns an error if `volume_id` is invalid (empty or non-hex prefix).
    pub fn shard_for(&self, volume_id: &str) -> Result<ShardGuard<'_>> {
        let shard_id =
            shard_for_volume(volume_id).map_err(|e| DataPlaneError::MetadataError(e))? as usize;

        let mut guard = self.shards[shard_id]
            .lock()
            .map_err(|_| DataPlaneError::MetadataError("shard mutex poisoned".to_string()))?;

        if guard.is_none() {
            let shard_dir = self.base_dir.join(format!("shard_{shard_id:02x}"));
            std::fs::create_dir_all(&shard_dir)?;
            *guard = Some(ShardStorage::open(&shard_dir)?);
        }

        ShardGuard::new(guard)
    }

    // -------------------------------------------------------------------------
    // Volume operations
    // -------------------------------------------------------------------------

    /// Persist a volume definition in the appropriate shard.
    pub fn put_volume(&self, vol: &VolumeDefinition) -> Result<()> {
        let guard = self.shard_for(&vol.id)?;
        guard.storage().state.put_volume(vol)
    }

    /// Retrieve a volume by ID, returning `None` if not found.
    pub fn get_volume(&self, volume_id: &str) -> Result<Option<VolumeDefinition>> {
        let guard = self.shard_for(volume_id)?;
        guard.storage().state.get_volume(volume_id)
    }

    /// Delete a volume by ID (no-op if absent).
    pub fn delete_volume(&self, volume_id: &str) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard.storage().state.delete_volume(volume_id)
    }

    // -------------------------------------------------------------------------
    // Chunk-map operations
    // -------------------------------------------------------------------------

    /// Persist a chunk-map entry, routed by `volume_id`.
    pub fn put_chunk_map(&self, volume_id: &str, entry: &ChunkMapEntry) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard.storage().state.put_chunk_map(volume_id, entry)
    }

    /// Retrieve a single chunk-map entry by volume ID and chunk index.
    pub fn get_chunk_map(
        &self,
        volume_id: &str,
        chunk_index: u64,
    ) -> Result<Option<ChunkMapEntry>> {
        let guard = self.shard_for(volume_id)?;
        guard.storage().state.get_chunk_map(volume_id, chunk_index)
    }

    /// Delete a single chunk-map entry (no-op if absent).
    pub fn delete_chunk_map(&self, volume_id: &str, chunk_index: u64) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard
            .storage()
            .state
            .delete_chunk_map(volume_id, chunk_index)
    }

    /// Return all chunk-map entries for `volume_id` in ascending index order.
    pub fn list_chunk_map(&self, volume_id: &str) -> Result<Vec<ChunkMapEntry>> {
        let guard = self.shard_for(volume_id)?;
        guard.storage().state.list_chunk_map(volume_id)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{Protection, VolumeStatus};

    fn sample_volume(id: &str) -> VolumeDefinition {
        VolumeDefinition {
            id: id.to_string(),
            name: format!("vol-{id}"),
            size_bytes: 4 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1_700_000_000,
            chunk_count: 0,
        }
    }

    #[test]
    fn shard_manager_routes_by_volume_id() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(dir.path()).unwrap();

        let vol = sample_volume("aa112233");
        mgr.put_volume(&vol).unwrap();

        let got = mgr.get_volume("aa112233").unwrap().unwrap();
        assert_eq!(got.id, "aa112233");
        assert_eq!(got.name, "vol-aa112233");
    }

    #[test]
    fn shard_manager_different_volumes_different_shards() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(dir.path()).unwrap();

        let vol00 = sample_volume("00aabbcc");
        let vol11 = sample_volume("11aabbcc");
        let volff = sample_volume("ffaabbcc");

        mgr.put_volume(&vol00).unwrap();
        mgr.put_volume(&vol11).unwrap();
        mgr.put_volume(&volff).unwrap();

        let got00 = mgr.get_volume("00aabbcc").unwrap().unwrap();
        let got11 = mgr.get_volume("11aabbcc").unwrap().unwrap();
        let gotff = mgr.get_volume("ffaabbcc").unwrap().unwrap();

        assert_eq!(got00.id, "00aabbcc");
        assert_eq!(got11.id, "11aabbcc");
        assert_eq!(gotff.id, "ffaabbcc");
    }

    #[test]
    fn shard_manager_chunk_map_operations() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(dir.path()).unwrap();

        let volume_id = "abcdef01";
        let entry = ChunkMapEntry {
            chunk_index: 5,
            chunk_id: "deadbeef".to_string(),
            ec_params: None,
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        };

        // put
        mgr.put_chunk_map(volume_id, &entry).unwrap();

        // get
        let got = mgr.get_chunk_map(volume_id, 5).unwrap().unwrap();
        assert_eq!(got.chunk_index, 5);
        assert_eq!(got.chunk_id, "deadbeef");

        // list
        let list = mgr.list_chunk_map(volume_id).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].chunk_index, 5);

        // delete
        mgr.delete_chunk_map(volume_id, 5).unwrap();
        assert!(mgr.get_chunk_map(volume_id, 5).unwrap().is_none());

        // list after delete
        let list_after = mgr.list_chunk_map(volume_id).unwrap();
        assert!(list_after.is_empty());
    }

    #[test]
    fn shard_count_is_256() {
        assert_eq!(SHARD_COUNT, 256);
    }

    #[test]
    fn shard_manager_rejects_empty_volume_id() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(dir.path()).unwrap();

        let result = mgr.get_volume("");
        assert!(result.is_err());
    }

    #[test]
    fn shard_manager_rejects_non_hex_volume_id() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(dir.path()).unwrap();

        let result = mgr.get_volume("zz-bad-prefix");
        assert!(result.is_err());
    }
}
