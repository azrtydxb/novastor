//! openraft storage implementations backed by redb.
//!
//! Provides [`ShardStorage`] which bundles:
//! - A **log store** (`RaftLogStorage`) backed by a dedicated redb database for
//!   Raft log entries, vote state, and committed index.
//! - A **state machine** (`RaftStateMachine`) wrapping [`MetadataStore`] for
//!   applying committed entries and producing snapshots.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::error::{DataPlaneError, Result};
use crate::metadata::raft_types::{RaftNode, TypeConfig};
use crate::metadata::store::MetadataStore;
use crate::metadata::types::{ChunkMapEntry, MetadataResponse, VolumeDefinition};

// ---------------------------------------------------------------------------
// redb table definitions for the Raft log database
// ---------------------------------------------------------------------------

/// Raft log entries: log_index (u64) -> serialized `Entry<TypeConfig>` (JSON bytes).
const RAFT_LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// Raft metadata: string key -> JSON bytes.
/// Keys: "vote", "committed", "last_purged_log_id"
const RAFT_META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

// ---------------------------------------------------------------------------
// Helper: convert any Error into a StorageIOError
// ---------------------------------------------------------------------------

fn to_sto_err<E: std::error::Error + 'static>(e: E) -> StorageIOError<u64> {
    StorageIOError::read(anyerror::AnyError::new(&e))
}

fn to_sto_write_err<E: std::error::Error + 'static>(e: E) -> StorageIOError<u64> {
    StorageIOError::write(anyerror::AnyError::new(&e))
}

// ---------------------------------------------------------------------------
// Snapshot data types
// ---------------------------------------------------------------------------

/// Serialisable snapshot of the full state machine.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StateMachineSnapshot {
    last_applied_log: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, RaftNode>,
    volumes: Vec<VolumeDefinition>,
    /// Flat list of (volume_id, entry) pairs.
    chunk_maps: Vec<(String, ChunkMapEntry)>,
}

// ---------------------------------------------------------------------------
// ShardStorage -- combined log + state machine for one shard
// ---------------------------------------------------------------------------

/// Combined Raft storage for a single shard.
///
/// Wraps a redb-backed log store and a [`MetadataStore`]-backed state machine.
/// The log database is stored in `data_dir/raft_log.redb`, while the state
/// machine database lives at `data_dir/state.redb`.
pub struct ShardStorage {
    /// Underlying state machine (redb-backed volume/chunk-map store).
    pub state: Arc<MetadataStore>,
    /// Filesystem path of the shard data directory.
    pub data_dir: std::path::PathBuf,
    /// Separate redb database used exclusively for Raft log entries and metadata.
    pub log_db: Database,
}

impl ShardStorage {
    /// Open (or create) per-shard storage rooted at `data_dir`.
    ///
    /// The directory is created if it does not already exist. The underlying
    /// redb databases are stored as `state.redb` (state machine) and
    /// `raft_log.redb` (log + vote) inside `data_dir`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| DataPlaneError::MetadataError(format!("create shard dir: {e}")))?;

        let state_path = data_dir.join("state.redb");
        let state = Arc::new(MetadataStore::open(&state_path)?);

        let log_path = data_dir.join("raft_log.redb");
        let log_db = Database::create(&log_path)
            .map_err(|e| DataPlaneError::MetadataError(format!("open raft log db: {e}")))?;

        // Ensure tables exist.
        {
            let txn = log_db
                .begin_write()
                .map_err(|e| DataPlaneError::MetadataError(format!("begin write: {e}")))?;
            txn.open_table(RAFT_LOG_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("init raft_log table: {e}")))?;
            txn.open_table(RAFT_META_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("init raft_meta table: {e}")))?;
            txn.commit()
                .map_err(|e| DataPlaneError::MetadataError(format!("commit init: {e}")))?;
        }

        Ok(Self {
            state,
            data_dir,
            log_db,
        })
    }

    /// Create the log store half from a shared reference.
    pub fn log_store(shared: &Arc<RwLock<Self>>) -> LogStore {
        LogStore {
            shared: Arc::clone(shared),
        }
    }

    /// Create the state machine half from a shared reference.
    pub fn state_machine(shared: &Arc<RwLock<Self>>) -> StateMachine {
        StateMachine {
            shared: Arc::clone(shared),
        }
    }
}

// ===========================================================================
// LogStore -- implements RaftLogReader + RaftLogStorage
// ===========================================================================

/// Raft log store backed by a redb database inside [`ShardStorage`].
#[derive(Clone)]
pub struct LogStore {
    shared: Arc<RwLock<ShardStorage>>,
}

impl LogStore {
    // -- helpers --

    fn read_meta_json<T: serde::de::DeserializeOwned>(
        db: &Database,
        key: &str,
    ) -> std::result::Result<Option<T>, StorageIOError<u64>> {
        let txn = db.begin_read().map_err(to_sto_err)?;
        let table = txn.open_table(RAFT_META_TABLE).map_err(to_sto_err)?;
        match table.get(key).map_err(to_sto_err)? {
            None => Ok(None),
            Some(guard) => {
                let bytes: &[u8] = guard.value();
                let val: T = serde_json::from_slice(bytes).map_err(to_sto_err)?;
                Ok(Some(val))
            }
        }
    }

    fn write_meta_json<T: serde::Serialize>(
        db: &Database,
        key: &str,
        value: &T,
    ) -> std::result::Result<(), StorageIOError<u64>> {
        let bytes = serde_json::to_vec(value).map_err(to_sto_write_err)?;
        let txn = db.begin_write().map_err(to_sto_write_err)?;
        {
            let mut table = txn.open_table(RAFT_META_TABLE).map_err(to_sto_write_err)?;
            table
                .insert(key, bytes.as_slice())
                .map_err(to_sto_write_err)?;
        }
        txn.commit().map_err(to_sto_write_err)?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let guard = self.shared.read().await;
        let db = &guard.log_db;
        let txn = db.begin_read().map_err(|e| StorageError::IO {
            source: to_sto_err(e),
        })?;
        let table = txn
            .open_table(RAFT_LOG_TABLE)
            .map_err(|e| StorageError::IO {
                source: to_sto_err(e),
            })?;

        let mut entries = Vec::new();
        for result in table.range(range).map_err(|e| StorageError::IO {
            source: to_sto_err(e),
        })? {
            let (_key, value) = result.map_err(|e| StorageError::IO {
                source: to_sto_err(e),
            })?;
            let bytes: &[u8] = value.value();
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(bytes).map_err(|e| StorageError::IO {
                    source: to_sto_err(e),
                })?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<u64>> {
        let guard = self.shared.read().await;
        let db = &guard.log_db;

        let last_purged: Option<LogId<u64>> = Self::read_meta_json(db, "last_purged_log_id")
            .map_err(|e| StorageError::IO { source: e })?;

        let txn = db.begin_read().map_err(|e| StorageError::IO {
            source: to_sto_err(e),
        })?;
        let table = txn
            .open_table(RAFT_LOG_TABLE)
            .map_err(|e| StorageError::IO {
                source: to_sto_err(e),
            })?;

        let last_log_id = match table.last().map_err(|e| StorageError::IO {
            source: to_sto_err(e),
        })? {
            Some((_key, value)) => {
                let bytes: &[u8] = value.value();
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(bytes).map_err(|e| StorageError::IO {
                        source: to_sto_err(e),
                    })?;
                Some(entry.log_id)
            }
            None => last_purged.clone(),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        let guard = self.shared.read().await;
        Self::write_meta_json(&guard.log_db, "vote", vote)
            .map_err(|e| StorageError::IO { source: e })
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.shared.read().await;
        Self::read_meta_json(&guard.log_db, "vote").map_err(|e| StorageError::IO { source: e })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let guard = self.shared.read().await;
        Self::write_meta_json(&guard.log_db, "committed", &committed)
            .map_err(|e| StorageError::IO { source: e })
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        let guard = self.shared.read().await;
        Self::read_meta_json(&guard.log_db, "committed").map_err(|e| StorageError::IO { source: e })
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let guard = self.shared.read().await;
        let db = &guard.log_db;
        let txn = db.begin_write().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;
        {
            let mut table = txn
                .open_table(RAFT_LOG_TABLE)
                .map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;
            for entry in entries {
                let index = entry.log_id.index;
                let bytes = serde_json::to_vec(&entry).map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;
                table
                    .insert(index, bytes.as_slice())
                    .map_err(|e| StorageError::IO {
                        source: to_sto_write_err(e),
                    })?;
            }
        }
        txn.commit().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;

        // Data is persisted (redb is durable on commit), signal completion.
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let guard = self.shared.read().await;
        let db = &guard.log_db;
        let txn = db.begin_write().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;
        {
            let mut table = txn
                .open_table(RAFT_LOG_TABLE)
                .map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;

            // Collect keys to delete (from log_id.index to the end).
            let keys_to_delete: Vec<u64> = {
                let mut keys = Vec::new();
                for result in table.range(log_id.index..).map_err(|e| StorageError::IO {
                    source: to_sto_err(e),
                })? {
                    let (key, _) = result.map_err(|e| StorageError::IO {
                        source: to_sto_err(e),
                    })?;
                    keys.push(key.value());
                }
                keys
            };

            for key in keys_to_delete {
                table.remove(key).map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;
            }
        }
        txn.commit().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let guard = self.shared.read().await;
        let db = &guard.log_db;

        // Delete all entries up to and including log_id.index.
        let txn = db.begin_write().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;
        {
            let mut table = txn
                .open_table(RAFT_LOG_TABLE)
                .map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;

            let keys_to_delete: Vec<u64> = {
                let mut keys = Vec::new();
                for result in table.range(..=log_id.index).map_err(|e| StorageError::IO {
                    source: to_sto_err(e),
                })? {
                    let (key, _) = result.map_err(|e| StorageError::IO {
                        source: to_sto_err(e),
                    })?;
                    keys.push(key.value());
                }
                keys
            };

            for key in keys_to_delete {
                table.remove(key).map_err(|e| StorageError::IO {
                    source: to_sto_write_err(e),
                })?;
            }
        }
        txn.commit().map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;

        // Persist the purged log id so we can report it on restart.
        Self::write_meta_json(db, "last_purged_log_id", &log_id)
            .map_err(|e| StorageError::IO { source: e })?;

        Ok(())
    }
}

// ===========================================================================
// StateMachine -- implements RaftStateMachine
// ===========================================================================

/// Raft state machine wrapping [`MetadataStore`].
#[derive(Clone)]
pub struct StateMachine {
    shared: Arc<RwLock<ShardStorage>>,
}

/// Snapshot builder that captures a point-in-time view and serialises it.
pub struct SmSnapshotBuilder {
    shared: Arc<RwLock<ShardStorage>>,
}

impl RaftSnapshotBuilder<TypeConfig> for SmSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let guard = self.shared.read().await;

        let last_applied_log: Option<LogId<u64>> =
            read_sm_meta(&guard.log_db, "sm_last_applied_log")
                .map_err(|e| StorageError::IO { source: e })?;

        let last_membership: StoredMembership<u64, RaftNode> =
            read_sm_meta(&guard.log_db, "sm_last_membership")
                .map_err(|e| StorageError::IO { source: e })?
                .unwrap_or_default();

        let volumes = guard.state.list_volumes().map_err(|e| StorageError::IO {
            source: to_sto_err(e),
        })?;

        // Collect all chunk maps for all volumes.
        let mut chunk_maps = Vec::new();
        for vol in &volumes {
            let entries = guard
                .state
                .list_chunk_map(&vol.id)
                .map_err(|e| StorageError::IO {
                    source: to_sto_err(e),
                })?;
            for entry in entries {
                chunk_maps.push((vol.id.clone(), entry));
            }
        }

        let snap = StateMachineSnapshot {
            last_applied_log,
            last_membership: last_membership.clone(),
            volumes,
            chunk_maps,
        };

        let data = serde_json::to_vec(&snap).map_err(|e| StorageError::IO {
            source: to_sto_write_err(e),
        })?;

        let snapshot_id = format!(
            "{}-{}",
            last_applied_log.map(|l| l.index).unwrap_or(0),
            uuid::Uuid::new_v4()
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = Cursor::new(data);

        // Persist the snapshot for get_current_snapshot().
        write_sm_meta(&guard.log_db, "current_snapshot_meta", &meta)
            .map_err(|e| StorageError::IO { source: e })?;
        write_sm_meta(&guard.log_db, "current_snapshot_data", &snapshot.get_ref())
            .map_err(|e| StorageError::IO { source: e })?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(snapshot),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = SmSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<(Option<LogId<u64>>, StoredMembership<u64, RaftNode>), StorageError<u64>>
    {
        let guard = self.shared.read().await;
        let last_applied: Option<LogId<u64>> = read_sm_meta(&guard.log_db, "sm_last_applied_log")
            .map_err(|e| StorageError::IO { source: e })?;
        let membership: StoredMembership<u64, RaftNode> =
            read_sm_meta(&guard.log_db, "sm_last_membership")
                .map_err(|e| StorageError::IO { source: e })?
                .unwrap_or_default();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<MetadataResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let guard = self.shared.read().await;

        // Separate entries into normal requests (batched atomically) and
        // membership/blank entries that only touch raft metadata.
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();

        // Collect normal requests for batch application.
        let mut normal_requests = Vec::new();
        // Track the entry types in order so we can reassemble responses.
        enum EntryKind {
            Blank,
            Normal(usize), // index into normal_requests
            Membership(openraft::Membership<u64, RaftNode>),
        }
        let mut entry_kinds = Vec::with_capacity(entries.len());
        let mut last_log_id = None;

        for entry in &entries {
            last_log_id = Some(entry.log_id);
            match &entry.payload {
                EntryPayload::Blank => {
                    entry_kinds.push(EntryKind::Blank);
                }
                EntryPayload::Normal(req) => {
                    let idx = normal_requests.len();
                    normal_requests.push(req.clone());
                    entry_kinds.push(EntryKind::Normal(idx));
                }
                EntryPayload::Membership(membership) => {
                    entry_kinds.push(EntryKind::Membership(membership.clone()));
                }
            }
        }

        // Apply all normal requests in a single atomic transaction.
        let batch_responses = if !normal_requests.is_empty() {
            guard
                .state
                .apply_batch(&normal_requests)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write(anyerror::AnyError::new(&e)),
                })?
        } else {
            Vec::new()
        };

        // Reassemble responses in entry order.
        let mut responses = Vec::with_capacity(entry_kinds.len());
        for kind in &entry_kinds {
            match kind {
                EntryKind::Blank => responses.push(MetadataResponse::Ok),
                EntryKind::Normal(idx) => responses.push(batch_responses[*idx].clone()),
                EntryKind::Membership(_) => {
                    // Membership metadata is written below, after the batch.
                    responses.push(MetadataResponse::Ok);
                }
            }
        }

        // Write membership and last_applied_log metadata.
        // These go to the raft_meta table (separate db), so we write them
        // after the state batch succeeds.
        for (entry, kind) in entries.iter().zip(entry_kinds.iter()) {
            if let EntryKind::Membership(membership) = kind {
                let stored = StoredMembership::new(Some(entry.log_id), membership.clone());
                write_sm_meta(&guard.log_db, "sm_last_membership", &stored)
                    .map_err(|e| StorageError::IO { source: e })?;
            }
        }

        // Update last applied log id once for the entire batch.
        if let Some(log_id) = last_log_id {
            write_sm_meta(&guard.log_db, "sm_last_applied_log", &log_id)
                .map_err(|e| StorageError::IO { source: e })?;
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SmSnapshotBuilder {
            shared: Arc::clone(&self.shared),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, RaftNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let guard = self.shared.read().await;
        let data = snapshot.into_inner();

        let snap: StateMachineSnapshot =
            serde_json::from_slice(&data).map_err(|e| StorageError::IO {
                source: to_sto_err(e),
            })?;

        // Atomically replace all state from the snapshot in a single
        // redb write transaction.
        guard
            .state
            .replace_all_from_snapshot(&snap.volumes, &snap.chunk_maps)
            .map_err(|e| StorageError::IO {
                source: to_sto_write_err(e),
            })?;

        // Update state machine metadata.
        write_sm_meta(&guard.log_db, "sm_last_applied_log", &snap.last_applied_log)
            .map_err(|e| StorageError::IO { source: e })?;
        write_sm_meta(&guard.log_db, "sm_last_membership", &snap.last_membership)
            .map_err(|e| StorageError::IO { source: e })?;

        // Persist the snapshot for get_current_snapshot().
        write_sm_meta(&guard.log_db, "current_snapshot_meta", meta)
            .map_err(|e| StorageError::IO { source: e })?;
        write_sm_meta(&guard.log_db, "current_snapshot_data", &data)
            .map_err(|e| StorageError::IO { source: e })?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let guard = self.shared.read().await;
        let meta: Option<SnapshotMeta<u64, RaftNode>> =
            read_sm_meta(&guard.log_db, "current_snapshot_meta")
                .map_err(|e| StorageError::IO { source: e })?;

        match meta {
            None => Ok(None),
            Some(meta) => {
                let data: Option<Vec<u8>> = read_sm_meta(&guard.log_db, "current_snapshot_data")
                    .map_err(|e| StorageError::IO { source: e })?;
                match data {
                    None => Ok(None),
                    Some(d) => Ok(Some(Snapshot {
                        meta,
                        snapshot: Box::new(Cursor::new(d)),
                    })),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers for state machine metadata stored in the raft_meta table
// ---------------------------------------------------------------------------

fn read_sm_meta<T: serde::de::DeserializeOwned>(
    db: &Database,
    key: &str,
) -> std::result::Result<Option<T>, StorageIOError<u64>> {
    let txn = db.begin_read().map_err(to_sto_err)?;
    let table = txn.open_table(RAFT_META_TABLE).map_err(to_sto_err)?;
    match table.get(key).map_err(to_sto_err)? {
        None => Ok(None),
        Some(guard) => {
            let bytes: &[u8] = guard.value();
            let val: T = serde_json::from_slice(bytes).map_err(to_sto_err)?;
            Ok(Some(val))
        }
    }
}

fn write_sm_meta<T: serde::Serialize>(
    db: &Database,
    key: &str,
    value: &T,
) -> std::result::Result<(), StorageIOError<u64>> {
    let bytes = serde_json::to_vec(value).map_err(to_sto_write_err)?;
    let txn = db.begin_write().map_err(to_sto_write_err)?;
    {
        let mut table = txn.open_table(RAFT_META_TABLE).map_err(to_sto_write_err)?;
        table
            .insert(key, bytes.as_slice())
            .map_err(to_sto_write_err)?;
    }
    txn.commit().map_err(to_sto_write_err)?;
    Ok(())
}

// ===========================================================================
// Helper: construct a LogId for tests
// ===========================================================================

/// Helper to create a `LogId` from term, node_id, and index.
#[cfg(test)]
fn test_log_id(term: u64, node_id: u64, index: u64) -> LogId<u64> {
    LogId::new(openraft::CommittedLeaderId::new(term, node_id), index)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{
        ChunkMapEntry, MetadataRequest, Protection, VolumeDefinition, VolumeStatus,
    };
    use openraft::{Entry, EntryPayload, Membership, Vote};
    use std::collections::{BTreeMap, BTreeSet};

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

    fn open_shard(dir: &std::path::Path) -> Arc<RwLock<ShardStorage>> {
        Arc::new(RwLock::new(ShardStorage::open(dir).expect("open")))
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
            dirty_bitmap: 0,
            placements: vec![],
            generation: 0,
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

    #[tokio::test]
    async fn log_store_vote_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-vote"));
        let mut log = LogStore {
            shared: Arc::clone(&shared),
        };

        // Initially no vote.
        assert!(log.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 42);
        log.save_vote(&vote).await.unwrap();

        let got = log.read_vote().await.unwrap().unwrap();
        assert_eq!(got, vote);
    }

    #[tokio::test]
    async fn log_store_committed_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-committed"));
        let mut log = LogStore {
            shared: Arc::clone(&shared),
        };

        assert!(log.read_committed().await.unwrap().is_none());

        let lid = test_log_id(1, 0, 42);
        log.save_committed(Some(lid)).await.unwrap();

        let got = log.read_committed().await.unwrap().unwrap();
        assert_eq!(got, lid);
    }

    #[tokio::test]
    async fn log_store_get_log_state_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-empty"));
        let mut log = LogStore {
            shared: Arc::clone(&shared),
        };

        let state = log.get_log_state().await.unwrap();
        assert!(state.last_purged_log_id.is_none());
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn state_machine_apply_and_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-sm"));
        let mut sm = StateMachine {
            shared: Arc::clone(&shared),
        };

        // Apply a PutVolume entry.
        let vol = sample_volume("snap-vol-01");
        let lid = test_log_id(1, 0, 1);
        let entry = Entry::<TypeConfig> {
            log_id: lid,
            payload: EntryPayload::Normal(MetadataRequest::PutVolume(vol.clone())),
        };
        let responses = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(responses.len(), 1);

        // Check state.
        let (last_applied, _membership) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(lid));

        // Build a snapshot.
        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(lid));

        // Verify the snapshot data contains the volume.
        let data = snapshot.snapshot.into_inner();
        let snap: StateMachineSnapshot = serde_json::from_slice(&data).unwrap();
        assert_eq!(snap.volumes.len(), 1);
        assert_eq!(snap.volumes[0].id, "snap-vol-01");
    }

    #[tokio::test]
    async fn state_machine_install_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-install"));
        let mut sm = StateMachine {
            shared: Arc::clone(&shared),
        };

        // Create a snapshot with one volume.
        let vol = sample_volume("installed-vol");
        let lid = test_log_id(2, 0, 10);
        let snap = StateMachineSnapshot {
            last_applied_log: Some(lid),
            last_membership: StoredMembership::default(),
            volumes: vec![vol.clone()],
            chunk_maps: vec![],
        };
        let data = serde_json::to_vec(&snap).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(lid),
            last_membership: StoredMembership::default(),
            snapshot_id: "test-snap-1".to_string(),
        };

        RaftStateMachine::<TypeConfig>::install_snapshot(
            &mut sm,
            &meta,
            Box::new(Cursor::new(data)),
        )
        .await
        .unwrap();

        // Verify state was restored.
        let (last_applied, _) = RaftStateMachine::<TypeConfig>::applied_state(&mut sm)
            .await
            .unwrap();
        assert_eq!(last_applied, Some(lid));

        // Verify the volume exists in the state machine.
        let guard = shared.read().await;
        let got = guard.state.get_volume("installed-vol").unwrap().unwrap();
        assert_eq!(got.id, "installed-vol");
    }

    #[tokio::test]
    async fn state_machine_membership_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let shared = open_shard(&tmp.path().join("shard-membership"));
        let mut sm = StateMachine {
            shared: Arc::clone(&shared),
        };

        let mut voter_set = BTreeSet::new();
        voter_set.insert(1u64);

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1u64,
            RaftNode {
                address: "10.0.0.1".to_string(),
                port: 7000,
            },
        );
        let membership = Membership::new(vec![voter_set], nodes);

        let lid = test_log_id(1, 0, 1);
        let entry = Entry::<TypeConfig> {
            log_id: lid,
            payload: EntryPayload::Membership(membership),
        };

        let responses = RaftStateMachine::<TypeConfig>::apply(&mut sm, vec![entry])
            .await
            .unwrap();
        assert_eq!(responses.len(), 1);

        let (_, stored_membership) = RaftStateMachine::<TypeConfig>::applied_state(&mut sm)
            .await
            .unwrap();
        assert!(stored_membership.log_id().is_some());
    }
}
