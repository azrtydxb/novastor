//! redb-backed local metadata store.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::error::{DataPlaneError, Result};
use crate::metadata::types::{ChunkMapEntry, MetadataRequest, MetadataResponse, VolumeDefinition};

const VOLUMES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");
const CHUNK_MAP_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("chunk_map");

/// redb-backed store for volume and chunk-map metadata.
pub struct MetadataStore {
    db: Database,
}

impl MetadataStore {
    /// Open (or create) the database at `path` and initialise both tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)
            .map_err(|e| DataPlaneError::MetadataError(format!("open database: {e}")))?;

        // Ensure both tables exist so later read transactions never fail with
        // "table does not exist".
        let txn = db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            txn.open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("init volumes table: {e}")))?;
            txn.open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("init chunk_map table: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit init: {e}")))?;

        Ok(Self { db })
    }

    // -------------------------------------------------------------------------
    // Volumes
    // -------------------------------------------------------------------------

    /// Persist a volume definition, keyed by its `id`.
    pub fn put_volume(&self, vol: &VolumeDefinition) -> Result<()> {
        let bytes = serde_json::to_vec(vol)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;
            table
                .insert(vol.id.as_str(), bytes.as_slice())
                .map_err(|e| DataPlaneError::MetadataError(format!("insert volume: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit put_volume: {e}")))?;
        Ok(())
    }

    /// Retrieve a volume by its ID, returning `None` if not found.
    pub fn get_volume(&self, volume_id: &str) -> Result<Option<VolumeDefinition>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(VOLUMES_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;
        match table
            .get(volume_id)
            .map_err(|e| DataPlaneError::MetadataError(format!("get volume: {e}")))?
        {
            None => Ok(None),
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let vol: VolumeDefinition = serde_json::from_slice(bytes)?;
                Ok(Some(vol))
            }
        }
    }

    /// Delete a volume by its ID (no-op if absent).
    pub fn delete_volume(&self, volume_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;
            table
                .remove(volume_id)
                .map_err(|e| DataPlaneError::MetadataError(format!("remove volume: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit delete_volume: {e}")))?;
        Ok(())
    }

    /// Return all stored volumes.
    pub fn list_volumes(&self) -> Result<Vec<VolumeDefinition>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(VOLUMES_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;
        let mut volumes = Vec::new();
        for result in table
            .iter()
            .map_err(|e| DataPlaneError::MetadataError(format!("iter volumes: {e}")))?
        {
            let (_key, value) =
                result.map_err(|e| DataPlaneError::MetadataError(format!("iter item: {e}")))?;
            let bytes: &[u8] = value.value();
            let vol: VolumeDefinition = serde_json::from_slice(bytes)?;
            volumes.push(vol);
        }
        Ok(volumes)
    }

    // -------------------------------------------------------------------------
    // Chunk map
    // -------------------------------------------------------------------------

    /// Build the chunk-map key: `"{volume_id}:{chunk_index:016x}"`.
    fn chunk_map_key(volume_id: &str, chunk_index: u64) -> String {
        format!("{volume_id}:{chunk_index:016x}")
    }

    /// Persist a chunk-map entry.
    pub fn put_chunk_map(&self, volume_id: &str, entry: &ChunkMapEntry) -> Result<()> {
        let key = Self::chunk_map_key(volume_id, entry.chunk_index);
        let bytes = serde_json::to_vec(entry)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| DataPlaneError::MetadataError(format!("insert chunk map: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit put_chunk_map: {e}")))?;
        Ok(())
    }

    /// Retrieve a single chunk-map entry.
    pub fn get_chunk_map(
        &self,
        volume_id: &str,
        chunk_index: u64,
    ) -> Result<Option<ChunkMapEntry>> {
        let key = Self::chunk_map_key(volume_id, chunk_index);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(CHUNK_MAP_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;
        match table
            .get(key.as_str())
            .map_err(|e| DataPlaneError::MetadataError(format!("get chunk map: {e}")))?
        {
            None => Ok(None),
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let entry: ChunkMapEntry = serde_json::from_slice(bytes)?;
                Ok(Some(entry))
            }
        }
    }

    /// Delete a single chunk-map entry (no-op if absent).
    pub fn delete_chunk_map(&self, volume_id: &str, chunk_index: u64) -> Result<()> {
        let key = Self::chunk_map_key(volume_id, chunk_index);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;
            table
                .remove(key.as_str())
                .map_err(|e| DataPlaneError::MetadataError(format!("remove chunk map: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit delete_chunk_map: {e}")))?;
        Ok(())
    }

    /// Return all chunk-map entries for `volume_id`, in ascending chunk-index
    /// order.  Uses a key-range scan with `"{volume_id}:".."{volume_id};"`;
    /// the semicolon (ASCII 0x3B) is exactly one greater than the colon (ASCII
    /// 0x3A), so the range is tight.
    pub fn list_chunk_map(&self, volume_id: &str) -> Result<Vec<ChunkMapEntry>> {
        let prefix_start = format!("{volume_id}:");
        let prefix_end = format!("{volume_id};");
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(CHUNK_MAP_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;
        let mut entries = Vec::new();
        for result in table
            .range(prefix_start.as_str()..prefix_end.as_str())
            .map_err(|e| DataPlaneError::MetadataError(format!("range scan: {e}")))?
        {
            let (_key, value) =
                result.map_err(|e| DataPlaneError::MetadataError(format!("range item: {e}")))?;
            let bytes: &[u8] = value.value();
            let entry: ChunkMapEntry = serde_json::from_slice(bytes)?;
            entries.push(entry);
        }
        Ok(entries)
    }

    // -------------------------------------------------------------------------
    // Request dispatcher
    // -------------------------------------------------------------------------

    /// Apply a single `MetadataRequest` and return the corresponding
    /// `MetadataResponse`.  Each call opens its own transaction.
    pub fn apply(&self, req: &MetadataRequest) -> Result<MetadataResponse> {
        match req {
            MetadataRequest::PutVolume(vol) => {
                self.put_volume(vol)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::DeleteVolume { volume_id } => {
                self.delete_volume(volume_id)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::PutChunkMap { volume_id, entry } => {
                self.put_chunk_map(volume_id, entry)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::DeleteChunkMap {
                volume_id,
                chunk_index,
            } => {
                self.delete_chunk_map(volume_id, *chunk_index)?;
                Ok(MetadataResponse::Ok)
            }
        }
    }

    // -------------------------------------------------------------------------
    // Batch / atomic operations
    // -------------------------------------------------------------------------

    /// Apply a batch of `MetadataRequest`s in a single redb write transaction.
    ///
    /// This guarantees atomicity: either all requests are applied or none are.
    /// Returns one `MetadataResponse` per request, in the same order.
    pub fn apply_batch(&self, requests: &[MetadataRequest]) -> Result<Vec<MetadataResponse>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;

        let mut responses = Vec::with_capacity(requests.len());
        {
            let mut vol_table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;
            let mut chunk_table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;

            for req in requests {
                match req {
                    MetadataRequest::PutVolume(vol) => {
                        let bytes = serde_json::to_vec(vol)?;
                        vol_table
                            .insert(vol.id.as_str(), bytes.as_slice())
                            .map_err(|e| {
                                DataPlaneError::MetadataError(format!("insert volume: {e}"))
                            })?;
                        responses.push(MetadataResponse::Ok);
                    }
                    MetadataRequest::DeleteVolume { volume_id } => {
                        vol_table.remove(volume_id.as_str()).map_err(|e| {
                            DataPlaneError::MetadataError(format!("remove volume: {e}"))
                        })?;
                        responses.push(MetadataResponse::Ok);
                    }
                    MetadataRequest::PutChunkMap { volume_id, entry } => {
                        let key = Self::chunk_map_key(volume_id, entry.chunk_index);
                        let bytes = serde_json::to_vec(entry)?;
                        chunk_table
                            .insert(key.as_str(), bytes.as_slice())
                            .map_err(|e| {
                                DataPlaneError::MetadataError(format!("insert chunk map: {e}"))
                            })?;
                        responses.push(MetadataResponse::Ok);
                    }
                    MetadataRequest::DeleteChunkMap {
                        volume_id,
                        chunk_index,
                    } => {
                        let key = Self::chunk_map_key(volume_id, *chunk_index);
                        chunk_table.remove(key.as_str()).map_err(|e| {
                            DataPlaneError::MetadataError(format!("remove chunk map: {e}"))
                        })?;
                        responses.push(MetadataResponse::Ok);
                    }
                }
            }
        }

        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit apply_batch: {e}")))?;

        Ok(responses)
    }

    /// Atomically replace all volumes and chunk-map entries from a snapshot.
    ///
    /// Deletes every existing volume and chunk-map entry, then inserts all
    /// provided volumes and chunk entries, all within a single redb write
    /// transaction.  A crash at any point either leaves the old state intact
    /// or installs the complete new state.
    pub fn replace_all_from_snapshot(
        &self,
        volumes: &[VolumeDefinition],
        chunks: &[(String, ChunkMapEntry)],
    ) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
        {
            // -- Clear and repopulate volumes --
            let mut vol_table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open volumes table: {e}")))?;

            // Collect existing keys to delete (can't mutate while iterating).
            let existing_keys: Vec<String> = {
                let mut keys = Vec::new();
                for result in vol_table
                    .iter()
                    .map_err(|e| DataPlaneError::MetadataError(format!("iter volumes: {e}")))?
                {
                    let (key, _) = result
                        .map_err(|e| DataPlaneError::MetadataError(format!("iter item: {e}")))?;
                    keys.push(key.value().to_string());
                }
                keys
            };
            for key in &existing_keys {
                vol_table
                    .remove(key.as_str())
                    .map_err(|e| DataPlaneError::MetadataError(format!("remove volume: {e}")))?;
            }

            // Insert snapshot volumes.
            for vol in volumes {
                let bytes = serde_json::to_vec(vol)?;
                vol_table
                    .insert(vol.id.as_str(), bytes.as_slice())
                    .map_err(|e| DataPlaneError::MetadataError(format!("insert volume: {e}")))?;
            }

            // -- Clear and repopulate chunk maps --
            let mut chunk_table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open chunk_map table: {e}")))?;

            let existing_chunk_keys: Vec<String> = {
                let mut keys = Vec::new();
                for result in chunk_table
                    .iter()
                    .map_err(|e| DataPlaneError::MetadataError(format!("iter chunk_map: {e}")))?
                {
                    let (key, _) = result
                        .map_err(|e| DataPlaneError::MetadataError(format!("iter item: {e}")))?;
                    keys.push(key.value().to_string());
                }
                keys
            };
            for key in &existing_chunk_keys {
                chunk_table
                    .remove(key.as_str())
                    .map_err(|e| DataPlaneError::MetadataError(format!("remove chunk map: {e}")))?;
            }

            // Insert snapshot chunk maps.
            for (vol_id, entry) in chunks {
                let key = Self::chunk_map_key(vol_id, entry.chunk_index);
                let bytes = serde_json::to_vec(entry)?;
                chunk_table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| DataPlaneError::MetadataError(format!("insert chunk map: {e}")))?;
            }
        }

        txn.commit().map_err(|e| {
            DataPlaneError::MetadataError(format!("commit replace_all_from_snapshot: {e}"))
        })?;

        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{ErasureParams, Protection, VolumeStatus};

    fn temp_store() -> MetadataStore {
        let dir = tempfile::tempdir().unwrap();
        MetadataStore::open(dir.path().join("test.redb")).unwrap()
    }

    fn sample_volume(id: &str) -> VolumeDefinition {
        VolumeDefinition {
            id: id.to_string(),
            name: format!("vol-{id}"),
            size_bytes: 4 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1_700_000_000,
            chunk_count: 1024,
        }
    }

    fn sample_chunk_entry(index: u64) -> ChunkMapEntry {
        ChunkMapEntry {
            chunk_index: index,
            chunk_id: format!("chunk-{index:04}"),
            ec_params: None,
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        }
    }

    #[test]
    fn put_and_get_volume() {
        let store = temp_store();
        let vol = sample_volume("aabbccdd-0001");
        store.put_volume(&vol).unwrap();
        let got = store.get_volume("aabbccdd-0001").unwrap().unwrap();
        assert_eq!(got.id, vol.id);
        assert_eq!(got.name, vol.name);
        assert_eq!(got.size_bytes, vol.size_bytes);
        assert_eq!(got.status, VolumeStatus::Available);
    }

    #[test]
    fn get_nonexistent_volume_returns_none() {
        let store = temp_store();
        let result = store.get_volume("does-not-exist").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn delete_volume() {
        let store = temp_store();
        let vol = sample_volume("delete-me");
        store.put_volume(&vol).unwrap();
        assert!(store.get_volume("delete-me").unwrap().is_some());
        store.delete_volume("delete-me").unwrap();
        assert!(store.get_volume("delete-me").unwrap().is_none());
    }

    #[test]
    fn list_volumes() {
        let store = temp_store();
        store.put_volume(&sample_volume("vol-alpha")).unwrap();
        store.put_volume(&sample_volume("vol-beta")).unwrap();
        let list = store.list_volumes().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn put_and_get_chunk_map_entry() {
        let store = temp_store();
        let entry = ChunkMapEntry {
            chunk_index: 0,
            chunk_id: "deadbeef".to_string(),
            ec_params: Some(ErasureParams {
                data_shards: 4,
                parity_shards: 2,
            }),
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
        };
        store.put_chunk_map("vol-1", &entry).unwrap();
        let got = store.get_chunk_map("vol-1", 0).unwrap().unwrap();
        assert_eq!(got.chunk_index, 0);
        assert_eq!(got.chunk_id, "deadbeef");
        let params = got.ec_params.unwrap();
        assert_eq!(params.data_shards, 4);
        assert_eq!(params.parity_shards, 2);
    }

    #[test]
    fn list_chunk_map_for_volume() {
        let store = temp_store();
        for i in 0..5 {
            store
                .put_chunk_map("vol-list", &sample_chunk_entry(i))
                .unwrap();
        }
        // An unrelated volume must not appear in the list.
        store
            .put_chunk_map("other-vol", &sample_chunk_entry(0))
            .unwrap();
        let list = store.list_chunk_map("vol-list").unwrap();
        assert_eq!(list.len(), 5);
        for (i, entry) in list.iter().enumerate() {
            assert_eq!(entry.chunk_index, i as u64);
        }
    }

    #[test]
    fn delete_chunk_map_entry() {
        let store = temp_store();
        store
            .put_chunk_map("vol-del", &sample_chunk_entry(7))
            .unwrap();
        assert!(store.get_chunk_map("vol-del", 7).unwrap().is_some());
        store.delete_chunk_map("vol-del", 7).unwrap();
        assert!(store.get_chunk_map("vol-del", 7).unwrap().is_none());
    }

    #[test]
    fn apply_batch_atomically() {
        let store = temp_store();
        let vol_a = sample_volume("batch-vol-a");
        let vol_b = sample_volume("batch-vol-b");
        let chunk = sample_chunk_entry(3);

        let requests = vec![
            MetadataRequest::PutVolume(vol_a.clone()),
            MetadataRequest::PutVolume(vol_b.clone()),
            MetadataRequest::PutChunkMap {
                volume_id: "batch-vol-a".to_string(),
                entry: chunk.clone(),
            },
        ];

        let responses = store.apply_batch(&requests).unwrap();
        assert_eq!(responses.len(), 3);
        for resp in &responses {
            assert!(matches!(resp, MetadataResponse::Ok));
        }

        // Verify all writes landed.
        assert!(store.get_volume("batch-vol-a").unwrap().is_some());
        assert!(store.get_volume("batch-vol-b").unwrap().is_some());
        assert!(store.get_chunk_map("batch-vol-a", 3).unwrap().is_some());
    }

    #[test]
    fn apply_batch_with_delete() {
        let store = temp_store();
        store.put_volume(&sample_volume("del-batch-vol")).unwrap();
        store
            .put_chunk_map("del-batch-vol", &sample_chunk_entry(0))
            .unwrap();

        let requests = vec![
            MetadataRequest::DeleteChunkMap {
                volume_id: "del-batch-vol".to_string(),
                chunk_index: 0,
            },
            MetadataRequest::DeleteVolume {
                volume_id: "del-batch-vol".to_string(),
            },
        ];

        let responses = store.apply_batch(&requests).unwrap();
        assert_eq!(responses.len(), 2);

        assert!(store.get_volume("del-batch-vol").unwrap().is_none());
        assert!(store.get_chunk_map("del-batch-vol", 0).unwrap().is_none());
    }

    #[test]
    fn apply_batch_empty() {
        let store = temp_store();
        let responses = store.apply_batch(&[]).unwrap();
        assert!(responses.is_empty());
    }

    #[test]
    fn replace_all_from_snapshot_replaces_everything() {
        let store = temp_store();

        // Pre-populate with some data that should be wiped.
        store.put_volume(&sample_volume("old-vol-1")).unwrap();
        store.put_volume(&sample_volume("old-vol-2")).unwrap();
        store
            .put_chunk_map("old-vol-1", &sample_chunk_entry(0))
            .unwrap();

        // Snapshot data — completely different volumes and chunks.
        let new_vols = vec![sample_volume("new-vol-x"), sample_volume("new-vol-y")];
        let new_chunks = vec![
            ("new-vol-x".to_string(), sample_chunk_entry(10)),
            ("new-vol-y".to_string(), sample_chunk_entry(20)),
        ];

        store
            .replace_all_from_snapshot(&new_vols, &new_chunks)
            .unwrap();

        // Old data must be gone.
        assert!(store.get_volume("old-vol-1").unwrap().is_none());
        assert!(store.get_volume("old-vol-2").unwrap().is_none());
        assert!(store.get_chunk_map("old-vol-1", 0).unwrap().is_none());

        // New data must be present.
        assert_eq!(
            store.get_volume("new-vol-x").unwrap().unwrap().id,
            "new-vol-x"
        );
        assert_eq!(
            store.get_volume("new-vol-y").unwrap().unwrap().id,
            "new-vol-y"
        );
        assert!(store.get_chunk_map("new-vol-x", 10).unwrap().is_some());
        assert!(store.get_chunk_map("new-vol-y", 20).unwrap().is_some());

        // Total volume count must be exactly 2.
        assert_eq!(store.list_volumes().unwrap().len(), 2);
    }

    #[test]
    fn replace_all_from_snapshot_with_empty_snapshot() {
        let store = temp_store();

        // Pre-populate.
        store.put_volume(&sample_volume("wipe-vol")).unwrap();
        store
            .put_chunk_map("wipe-vol", &sample_chunk_entry(0))
            .unwrap();

        // Replace with empty snapshot.
        store.replace_all_from_snapshot(&[], &[]).unwrap();

        assert!(store.list_volumes().unwrap().is_empty());
        assert!(store.list_chunk_map("wipe-vol").unwrap().is_empty());
    }
}
