//! redb-backed persistent store for chunk locations and references.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use crate::error::{DataPlaneError, Result};
use crate::policy::types::{ChunkLocation, ChunkRef};

const CHUNK_LOCATIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("chunk_locations");
const CHUNK_REFS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("chunk_refs");

/// Persistent store tracking where chunks live (which nodes) and which
/// volumes reference each chunk.
pub struct ChunkLocationStore {
    db: Database,
}

impl ChunkLocationStore {
    /// Open (or create) the database at `path` and initialise both tables.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path)
            .map_err(|e| DataPlaneError::PolicyError(format!("open database: {e}")))?;

        let txn = db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            txn.open_table(CHUNK_LOCATIONS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("init locations table: {e}")))?;
            txn.open_table(CHUNK_REFS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("init refs table: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit init: {e}")))?;

        Ok(Self { db })
    }

    // -------------------------------------------------------------------------
    // Location methods
    // -------------------------------------------------------------------------

    /// Persist a chunk location, keyed by its `chunk_id`.
    pub fn put_location(&self, loc: &ChunkLocation) -> Result<()> {
        let bytes = serde_json::to_vec(loc)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_LOCATIONS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open locations table: {e}")))?;
            table
                .insert(loc.chunk_id.as_str(), bytes.as_slice())
                .map_err(|e| DataPlaneError::PolicyError(format!("insert location: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit put_location: {e}")))?;
        Ok(())
    }

    /// Retrieve a chunk location by chunk ID, returning `None` if not found.
    pub fn get_location(&self, chunk_id: &str) -> Result<Option<ChunkLocation>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(CHUNK_LOCATIONS_TABLE)
            .map_err(|e| DataPlaneError::PolicyError(format!("open locations table: {e}")))?;
        match table
            .get(chunk_id)
            .map_err(|e| DataPlaneError::PolicyError(format!("get location: {e}")))?
        {
            None => Ok(None),
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let loc: ChunkLocation = serde_json::from_slice(bytes)?;
                Ok(Some(loc))
            }
        }
    }

    /// Add a node to a chunk's location. If the chunk has no existing location
    /// entry, a new one is created.
    pub fn add_node_to_chunk(&self, chunk_id: &str, node_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_LOCATIONS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open locations table: {e}")))?;

            let mut loc = match table
                .get(chunk_id)
                .map_err(|e| DataPlaneError::PolicyError(format!("get location: {e}")))?
            {
                Some(guard) => {
                    let bytes: &[u8] = guard.value();
                    serde_json::from_slice::<ChunkLocation>(bytes)?
                }
                None => ChunkLocation::new(chunk_id.to_string()),
            };

            loc.add_node(node_id.to_string());

            let bytes = serde_json::to_vec(&loc)?;
            table
                .insert(chunk_id, bytes.as_slice())
                .map_err(|e| DataPlaneError::PolicyError(format!("insert location: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit add_node_to_chunk: {e}")))?;
        Ok(())
    }

    /// Remove a node from a chunk's location. If the chunk has no existing
    /// location entry, this is a no-op.
    pub fn remove_node_from_chunk(&self, chunk_id: &str, node_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_LOCATIONS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open locations table: {e}")))?;

            let maybe_loc: Option<ChunkLocation> = table
                .get(chunk_id)
                .map_err(|e| DataPlaneError::PolicyError(format!("get location: {e}")))?
                .map(|guard| serde_json::from_slice(guard.value()))
                .transpose()?;

            if let Some(mut loc) = maybe_loc {
                loc.remove_node(node_id);

                let bytes = serde_json::to_vec(&loc)?;
                table
                    .insert(chunk_id, bytes.as_slice())
                    .map_err(|e| DataPlaneError::PolicyError(format!("insert location: {e}")))?;
            }
        }
        txn.commit().map_err(|e| {
            DataPlaneError::PolicyError(format!("commit remove_node_from_chunk: {e}"))
        })?;
        Ok(())
    }

    /// Return all stored chunk locations.
    pub fn list_locations(&self) -> Result<Vec<ChunkLocation>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(CHUNK_LOCATIONS_TABLE)
            .map_err(|e| DataPlaneError::PolicyError(format!("open locations table: {e}")))?;
        let mut locations = Vec::new();
        for result in table
            .iter()
            .map_err(|e| DataPlaneError::PolicyError(format!("iter locations: {e}")))?
        {
            let (_key, value) =
                result.map_err(|e| DataPlaneError::PolicyError(format!("iter item: {e}")))?;
            let bytes: &[u8] = value.value();
            let loc: ChunkLocation = serde_json::from_slice(bytes)?;
            locations.push(loc);
        }
        Ok(locations)
    }

    // -------------------------------------------------------------------------
    // Reference methods
    // -------------------------------------------------------------------------

    /// Persist a chunk reference, keyed by its `chunk_id`.
    pub fn put_ref(&self, chunk_ref: &ChunkRef) -> Result<()> {
        let bytes = serde_json::to_vec(chunk_ref)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_REFS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open refs table: {e}")))?;
            table
                .insert(chunk_ref.chunk_id.as_str(), bytes.as_slice())
                .map_err(|e| DataPlaneError::PolicyError(format!("insert ref: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit put_ref: {e}")))?;
        Ok(())
    }

    /// Retrieve a chunk reference by chunk ID, returning `None` if not found.
    pub fn get_ref(&self, chunk_id: &str) -> Result<Option<ChunkRef>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin read: {e}")))?;
        let table = txn
            .open_table(CHUNK_REFS_TABLE)
            .map_err(|e| DataPlaneError::PolicyError(format!("open refs table: {e}")))?;
        match table
            .get(chunk_id)
            .map_err(|e| DataPlaneError::PolicyError(format!("get ref: {e}")))?
        {
            None => Ok(None),
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let cr: ChunkRef = serde_json::from_slice(bytes)?;
                Ok(Some(cr))
            }
        }
    }

    /// Add a volume reference to a chunk. If no ref entry exists for the chunk,
    /// a new one is created.
    pub fn add_volume_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_REFS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open refs table: {e}")))?;

            let mut cr = match table
                .get(chunk_id)
                .map_err(|e| DataPlaneError::PolicyError(format!("get ref: {e}")))?
            {
                Some(guard) => {
                    let bytes: &[u8] = guard.value();
                    serde_json::from_slice::<ChunkRef>(bytes)?
                }
                None => ChunkRef::new(chunk_id.to_string()),
            };

            cr.add_volume(volume_id.to_string());

            let bytes = serde_json::to_vec(&cr)?;
            table
                .insert(chunk_id, bytes.as_slice())
                .map_err(|e| DataPlaneError::PolicyError(format!("insert ref: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit add_volume_ref: {e}")))?;
        Ok(())
    }

    /// Remove a volume reference from a chunk. If no ref entry exists, this is
    /// a no-op.
    pub fn remove_volume_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::PolicyError(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_REFS_TABLE)
                .map_err(|e| DataPlaneError::PolicyError(format!("open refs table: {e}")))?;

            let maybe_ref: Option<ChunkRef> = table
                .get(chunk_id)
                .map_err(|e| DataPlaneError::PolicyError(format!("get ref: {e}")))?
                .map(|guard| serde_json::from_slice(guard.value()))
                .transpose()?;

            if let Some(mut cr) = maybe_ref {
                cr.remove_volume(volume_id);

                let bytes = serde_json::to_vec(&cr)?;
                table
                    .insert(chunk_id, bytes.as_slice())
                    .map_err(|e| DataPlaneError::PolicyError(format!("insert ref: {e}")))?;
            }
        }
        txn.commit()
            .map_err(|e| DataPlaneError::PolicyError(format!("commit remove_volume_ref: {e}")))?;
        Ok(())
    }

    /// Return the number of volumes referencing a chunk. Returns 0 if the
    /// chunk has no ref entry.
    pub fn ref_count(&self, chunk_id: &str) -> Result<u32> {
        match self.get_ref(chunk_id)? {
            Some(cr) => Ok(cr.ref_count()),
            None => Ok(0),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_store() -> ChunkLocationStore {
        let dir = tempfile::tempdir().unwrap();
        ChunkLocationStore::open(dir.path().join("test.redb")).unwrap()
    }

    #[test]
    fn open_creates_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.redb");
        assert!(!path.exists());
        let store = ChunkLocationStore::open(&path).unwrap();
        assert!(path.exists());
        // Verify basic operations work on the freshly opened store.
        assert!(store.list_locations().unwrap().is_empty());
    }

    #[test]
    fn put_and_get_location() {
        let store = temp_store();
        let mut loc = ChunkLocation::new("chunk-aaa".to_string());
        loc.add_node("node-1".to_string());
        loc.add_node("node-2".to_string());

        store.put_location(&loc).unwrap();

        let got = store.get_location("chunk-aaa").unwrap().unwrap();
        assert_eq!(got.chunk_id, "chunk-aaa");
        assert_eq!(got.node_ids.len(), 2);
        assert!(got.node_ids.contains(&"node-1".to_string()));
        assert!(got.node_ids.contains(&"node-2".to_string()));
    }

    #[test]
    fn add_node_to_existing_chunk() {
        let store = temp_store();
        let mut loc = ChunkLocation::new("chunk-bbb".to_string());
        loc.add_node("node-1".to_string());
        store.put_location(&loc).unwrap();

        store.add_node_to_chunk("chunk-bbb", "node-2").unwrap();

        let got = store.get_location("chunk-bbb").unwrap().unwrap();
        assert_eq!(got.node_ids.len(), 2);
        assert!(got.node_ids.contains(&"node-1".to_string()));
        assert!(got.node_ids.contains(&"node-2".to_string()));
    }

    #[test]
    fn add_node_to_new_chunk() {
        let store = temp_store();

        // No existing entry for this chunk.
        assert!(store.get_location("chunk-new").unwrap().is_none());

        store.add_node_to_chunk("chunk-new", "node-x").unwrap();

        let got = store.get_location("chunk-new").unwrap().unwrap();
        assert_eq!(got.chunk_id, "chunk-new");
        assert_eq!(got.node_ids, vec!["node-x".to_string()]);
    }

    #[test]
    fn remove_node_from_chunk() {
        let store = temp_store();
        let mut loc = ChunkLocation::new("chunk-ccc".to_string());
        loc.add_node("node-1".to_string());
        loc.add_node("node-2".to_string());
        store.put_location(&loc).unwrap();

        store.remove_node_from_chunk("chunk-ccc", "node-1").unwrap();

        let got = store.get_location("chunk-ccc").unwrap().unwrap();
        assert_eq!(got.node_ids.len(), 1);
        assert_eq!(got.node_ids[0], "node-2");
    }

    #[test]
    fn remove_node_from_nonexistent_chunk() {
        let store = temp_store();
        // Should not error.
        store
            .remove_node_from_chunk("does-not-exist", "node-1")
            .unwrap();
    }

    #[test]
    fn list_locations() {
        let store = temp_store();
        for i in 0..3 {
            let mut loc = ChunkLocation::new(format!("chunk-{i}"));
            loc.add_node(format!("node-{i}"));
            store.put_location(&loc).unwrap();
        }

        let all = store.list_locations().unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn put_and_get_ref() {
        let store = temp_store();
        let mut cr = ChunkRef::new("chunk-ref-1".to_string());
        cr.add_volume("vol-a".to_string());
        cr.add_volume("vol-b".to_string());

        store.put_ref(&cr).unwrap();

        let got = store.get_ref("chunk-ref-1").unwrap().unwrap();
        assert_eq!(got.chunk_id, "chunk-ref-1");
        assert_eq!(got.volume_ids.len(), 2);
        assert!(got.volume_ids.contains(&"vol-a".to_string()));
        assert!(got.volume_ids.contains(&"vol-b".to_string()));
    }

    #[test]
    fn add_volume_ref() {
        let store = temp_store();

        // add_volume_ref on a nonexistent chunk creates the entry.
        store.add_volume_ref("chunk-vr", "vol-1").unwrap();

        let got = store.get_ref("chunk-vr").unwrap().unwrap();
        assert_eq!(got.chunk_id, "chunk-vr");
        assert_eq!(got.volume_ids, vec!["vol-1".to_string()]);
    }

    #[test]
    fn remove_volume_ref() {
        let store = temp_store();
        let mut cr = ChunkRef::new("chunk-rvr".to_string());
        cr.add_volume("vol-a".to_string());
        cr.add_volume("vol-b".to_string());
        store.put_ref(&cr).unwrap();

        store.remove_volume_ref("chunk-rvr", "vol-a").unwrap();

        let got = store.get_ref("chunk-rvr").unwrap().unwrap();
        assert_eq!(got.volume_ids.len(), 1);
        assert_eq!(got.volume_ids[0], "vol-b");
    }

    #[test]
    fn ref_count() {
        let store = temp_store();
        store.add_volume_ref("chunk-rc", "vol-1").unwrap();
        store.add_volume_ref("chunk-rc", "vol-2").unwrap();
        store.add_volume_ref("chunk-rc", "vol-3").unwrap();

        assert_eq!(store.ref_count("chunk-rc").unwrap(), 3);
    }

    #[test]
    fn ref_count_nonexistent() {
        let store = temp_store();
        assert_eq!(store.ref_count("no-such-chunk").unwrap(), 0);
    }
}
