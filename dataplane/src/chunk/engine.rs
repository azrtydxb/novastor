//! Chunk engine — volume I/O → content-addressed chunks → CRUSH dispatch.
//!
//! Implements the core invariant: "Owner fans out to replicas via gRPC
//! (Rust-to-Rust)." The chunk engine selects placement nodes via CRUSH,
//! then replicates full chunks (or distributes EC shards) to those nodes.

use std::collections::HashMap;
use std::sync::Arc;

use log::warn;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, CHUNK_SIZE};
use crate::error::{DataPlaneError, Result};
use crate::metadata::crush;
use crate::metadata::topology::ClusterMap;
use crate::metadata::types::{ChunkMapEntry, ErasureParams, Protection};
use crate::policy::engine::PolicyEngine;
use crate::transport::chunk_client::ChunkClient;

pub struct ChunkEngine {
    node_id: String,
    local_store: Arc<dyn ChunkStore>,
    topology: ClusterMap,
    /// Data protection scheme for this engine instance.
    protection: Protection,
    /// Cached gRPC connections to remote nodes, keyed by address.
    connections: Mutex<HashMap<String, ChunkClient>>,
    /// Optional policy engine for tracking chunk locations and references.
    policy: Option<Arc<PolicyEngine>>,
}

impl ChunkEngine {
    /// Create a ChunkEngine with default protection (single replica, no redundancy).
    pub fn new(node_id: String, local_store: Arc<dyn ChunkStore>, topology: ClusterMap) -> Self {
        Self {
            node_id,
            local_store,
            topology,
            protection: Protection::Replication { factor: 1 },
            connections: Mutex::new(HashMap::new()),
            policy: None,
        }
    }

    /// Create a ChunkEngine with policy tracking enabled.
    pub fn with_policy(
        node_id: String,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
        policy: Arc<PolicyEngine>,
    ) -> Self {
        Self {
            node_id,
            local_store,
            topology,
            protection: Protection::Replication { factor: 1 },
            connections: Mutex::new(HashMap::new()),
            policy: Some(policy),
        }
    }

    /// Create a ChunkEngine with a specific protection scheme.
    pub fn with_protection(
        node_id: String,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
        protection: Protection,
    ) -> Self {
        Self {
            node_id,
            local_store,
            topology,
            protection,
            connections: Mutex::new(HashMap::new()),
            policy: None,
        }
    }

    /// Create a ChunkEngine with both protection scheme and policy engine.
    pub fn with_protection_and_policy(
        node_id: String,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
        protection: Protection,
        policy: Arc<PolicyEngine>,
    ) -> Self {
        Self {
            node_id,
            local_store,
            topology,
            protection,
            connections: Mutex::new(HashMap::new()),
            policy: Some(policy),
        }
    }

    /// Set the protection scheme (useful when the scheme is determined at volume
    /// creation time rather than engine construction).
    pub fn set_protection(&mut self, protection: Protection) {
        self.protection = protection;
    }

    /// Return the current protection scheme.
    pub fn protection(&self) -> &Protection {
        &self.protection
    }

    /// Get or create a cached ChunkClient for the given address.
    async fn get_client(&self, addr: &str) -> Result<ChunkClient> {
        let mut cache = self.connections.lock().await;
        if let Some(client) = cache.get(addr) {
            return Ok(client.clone());
        }
        let client = ChunkClient::connect(addr).await?;
        cache.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// Content-addressed chunk ID (SHA-256 hex of raw data).
    pub fn compute_chunk_id(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Prepend ChunkHeader to raw data.
    pub fn prepare_chunk(data: &[u8]) -> Vec<u8> {
        let header = ChunkHeader {
            magic: *b"NVAC",
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(data),
            data_len: data.len() as u32,
            _reserved: [0; 2],
        };
        let mut buf = Vec::with_capacity(ChunkHeader::SIZE + data.len());
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(data);
        buf
    }

    /// Verify chunk CRC-32C integrity.
    pub fn verify_chunk(chunk_with_header: &[u8]) -> Result<()> {
        if chunk_with_header.len() < ChunkHeader::SIZE {
            return Err(DataPlaneError::ChunkEngineError("chunk too small".into()));
        }
        let header_bytes: [u8; ChunkHeader::SIZE] = chunk_with_header[..ChunkHeader::SIZE]
            .try_into()
            .map_err(|_| DataPlaneError::ChunkEngineError("header read failed".into()))?;
        let header = ChunkHeader::from_bytes(&header_bytes)?;
        let data_len = header.data_len as usize;
        if chunk_with_header.len() < ChunkHeader::SIZE + data_len {
            return Err(DataPlaneError::ChunkEngineError("chunk truncated".into()));
        }
        let data = &chunk_with_header[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len];
        let actual = crc32c::crc32c(data);
        let stored = header.checksum;
        if stored != actual {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "CRC mismatch: stored={stored:#010x}, actual={actual:#010x}"
            )));
        }
        Ok(())
    }

    /// Split data into CHUNK_SIZE-aligned slices.
    pub fn split_into_chunks(data: &[u8]) -> Vec<&[u8]> {
        data.chunks(CHUNK_SIZE).collect()
    }

    // -----------------------------------------------------------------------
    // Write path
    // -----------------------------------------------------------------------

    /// Write volume data, returning chunk map entries.
    ///
    /// Depending on the protection scheme, this either:
    /// - **Replication**: fans out each chunk to N nodes via CRUSH, acks after
    ///   majority quorum.
    /// - **Erasure coding**: RS-encodes each chunk into K+M shards and
    ///   distributes shards to distinct nodes via CRUSH.
    pub async fn write(
        &self,
        volume_id: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<Vec<ChunkMapEntry>> {
        let start_chunk_index = offset / CHUNK_SIZE as u64;
        let chunks = Self::split_into_chunks(data);
        log::debug!(
            "ChunkEngine::write vol={} offset={} data_len={} chunks={}",
            volume_id,
            offset,
            data.len(),
            chunks.len()
        );
        let mut entries = Vec::with_capacity(chunks.len());

        for (i, raw_chunk) in chunks.iter().enumerate() {
            let chunk_index = start_chunk_index + i as u64;
            let chunk_id = Self::compute_chunk_id(raw_chunk);

            let ec_params = match &self.protection {
                Protection::Replication { factor } => {
                    let prepared = Self::prepare_chunk(raw_chunk);
                    self.write_replicated(&chunk_id, &prepared, *factor, volume_id)
                        .await?;
                    None
                }
                Protection::ErasureCoding {
                    data_shards,
                    parity_shards,
                } => {
                    self.write_erasure_coded(
                        &chunk_id,
                        raw_chunk,
                        *data_shards,
                        *parity_shards,
                        volume_id,
                    )
                    .await?;
                    Some(ErasureParams {
                        data_shards: *data_shards,
                        parity_shards: *parity_shards,
                    })
                }
            };

            entries.push(ChunkMapEntry {
                chunk_index,
                chunk_id,
                ec_params,
            });
        }

        Ok(entries)
    }

    /// Write a chunk to N replica nodes selected by CRUSH, ack after quorum.
    async fn write_replicated(
        &self,
        chunk_id: &str,
        prepared: &[u8],
        factor: u32,
        volume_id: &str,
    ) -> Result<()> {
        let placements = crush::select(chunk_id, factor as usize, &self.topology);
        log::debug!(
            "write_replicated chunk={} prepared_len={} factor={} placements={}",
            &chunk_id[..16],
            prepared.len(),
            factor,
            placements.len()
        );
        if placements.is_empty() {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "CRUSH returned no placement for chunk {chunk_id}"
            )));
        }

        // Majority quorum: floor(N/2) + 1.
        let quorum = (placements.len() / 2) + 1;
        let mut successes = 0usize;
        let mut last_err = None;

        for (target_node, _backend) in &placements {
            let result = self
                .put_chunk_to_node(chunk_id, prepared, target_node)
                .await;
            match result {
                Ok(()) => {
                    successes += 1;
                    if let Some(policy) = &self.policy {
                        let _ = policy.record_chunk_location(chunk_id, target_node);
                        let _ = policy.record_chunk_ref(chunk_id, volume_id);
                    }
                }
                Err(e) => {
                    warn!(
                        "chunk {} write to node {} failed: {}",
                        chunk_id, target_node, e
                    );
                    last_err = Some(e);
                }
            }
        }

        if successes < quorum {
            return Err(last_err.unwrap_or_else(|| {
                DataPlaneError::ChunkEngineError(format!(
                    "write quorum not met for chunk {}: {}/{} succeeded, need {}",
                    chunk_id,
                    successes,
                    placements.len(),
                    quorum
                ))
            }));
        }

        Ok(())
    }

    /// RS-encode a chunk into K+M shards and distribute to distinct nodes.
    async fn write_erasure_coded(
        &self,
        chunk_id: &str,
        raw_data: &[u8],
        data_shards: u32,
        parity_shards: u32,
        volume_id: &str,
    ) -> Result<()> {
        let total_shards = (data_shards + parity_shards) as usize;
        let placements = crush::select(chunk_id, total_shards, &self.topology);

        if placements.len() < data_shards as usize {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "insufficient nodes for EC: need at least {} data shard nodes, CRUSH returned {}",
                data_shards,
                placements.len()
            )));
        }

        // RS-encode the raw chunk data into shards.
        let shard_size = {
            let raw = (raw_data.len() + data_shards as usize - 1) / data_shards as usize;
            // Round up to even (reed-solomon-simd requirement).
            (raw + 1) & !1
        };
        let padded_len = shard_size * data_shards as usize;
        let mut padded = raw_data.to_vec();
        padded.resize(padded_len, 0);

        let data_pieces: Vec<&[u8]> = padded.chunks(shard_size).collect();
        let parity = reed_solomon_simd::encode(
            data_shards as usize,
            parity_shards as usize,
            data_pieces.iter().copied(),
        )
        .map_err(|e| DataPlaneError::ChunkEngineError(format!("RS encode failed: {e}")))?;

        let mut all_shards: Vec<Vec<u8>> = data_pieces.iter().map(|s| s.to_vec()).collect();
        all_shards.extend(parity);

        // Distribute shards to nodes. All shards must succeed for EC write.
        let mut successes = 0usize;
        for (shard_idx, shard_data) in all_shards.iter().enumerate() {
            if shard_idx >= placements.len() {
                break;
            }
            let (target_node, _backend) = &placements[shard_idx];
            let shard_id = format!("{chunk_id}:shard:{shard_idx}");
            let prepared = Self::prepare_chunk(shard_data);

            match self
                .put_chunk_to_node(&shard_id, &prepared, target_node)
                .await
            {
                Ok(()) => {
                    successes += 1;
                    if let Some(policy) = &self.policy {
                        let _ = policy.record_chunk_location(&shard_id, target_node);
                        let _ = policy.record_chunk_ref(&shard_id, volume_id);
                    }
                }
                Err(e) => {
                    warn!(
                        "EC shard {} write to node {} failed: {}",
                        shard_id, target_node, e
                    );
                }
            }
        }

        // For EC, all shards must be written (we can tolerate up to parity_shards
        // failures on read, but initial write must be complete).
        if successes < total_shards {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "EC write incomplete for chunk {}: {}/{} shards written",
                chunk_id, successes, total_shards
            )));
        }

        Ok(())
    }

    /// Put a prepared chunk to a specific node (local or remote via gRPC).
    async fn put_chunk_to_node(
        &self,
        chunk_id: &str,
        prepared: &[u8],
        target_node: &str,
    ) -> Result<()> {
        log::debug!(
            "put_chunk_to_node chunk={} len={} target={} local={}",
            &chunk_id[..16],
            prepared.len(),
            target_node,
            target_node == self.node_id
        );
        if target_node == self.node_id {
            let r = self.local_store.put(chunk_id, prepared).await;
            log::debug!("put_chunk_to_node local done: {:?}", r.is_ok());
            r
        } else {
            let node = self
                .topology
                .nodes()
                .iter()
                .find(|n| n.id == target_node)
                .ok_or_else(|| {
                    DataPlaneError::ChunkEngineError(format!(
                        "node not found in topology: {target_node}"
                    ))
                })?;
            let addr = format!("http://{}:{}", node.address, node.port);
            let client = self.get_client(&addr).await?;
            client.put(chunk_id, prepared).await
        }
    }

    /// Get a chunk from a specific node (local or remote via gRPC).
    async fn get_chunk_from_node(&self, chunk_id: &str, target_node: &str) -> Result<Vec<u8>> {
        if target_node == self.node_id {
            self.local_store.get(chunk_id).await
        } else {
            let node = self
                .topology
                .nodes()
                .iter()
                .find(|n| n.id == target_node)
                .ok_or_else(|| {
                    DataPlaneError::ChunkEngineError(format!(
                        "node not found in topology: {target_node}"
                    ))
                })?;
            let addr = format!("http://{}:{}", node.address, node.port);
            let client = self.get_client(&addr).await?;
            client.get(chunk_id).await
        }
    }

    // -----------------------------------------------------------------------
    // Read path
    // -----------------------------------------------------------------------

    /// Read volume data using a chunk map.
    ///
    /// For replicated chunks, tries each replica in CRUSH order until one
    /// succeeds (automatic failover). For EC chunks, reads data shards and
    /// reconstructs from parity if needed.
    pub async fn read(
        &self,
        _volume_id: &str,
        _offset: u64,
        chunk_map: &[ChunkMapEntry],
    ) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        for entry in chunk_map {
            let raw_data = if let Some(ec) = &entry.ec_params {
                self.read_erasure_coded(&entry.chunk_id, ec.data_shards, ec.parity_shards)
                    .await?
            } else {
                self.read_replicated(&entry.chunk_id).await?
            };
            result.extend_from_slice(&raw_data);
        }

        Ok(result)
    }

    /// Read a replicated chunk, trying each CRUSH replica in order.
    async fn read_replicated(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let factor = match &self.protection {
            Protection::Replication { factor } => *factor as usize,
            // If called for a replicated chunk but engine is in EC mode,
            // use a single placement (legacy compatibility).
            Protection::ErasureCoding { .. } => 1,
        };
        let placements = crush::select(chunk_id, factor, &self.topology);
        if placements.is_empty() {
            return Err(DataPlaneError::ChunkEngineError(
                "CRUSH returned no placement".into(),
            ));
        }

        let mut last_err = None;
        for (target_node, _) in &placements {
            match self.get_chunk_from_node(chunk_id, target_node).await {
                Ok(chunk_data) => {
                    Self::verify_chunk(&chunk_data)?;
                    let header_bytes: [u8; ChunkHeader::SIZE] =
                        chunk_data[..ChunkHeader::SIZE].try_into().map_err(|_| {
                            DataPlaneError::ChunkEngineError("header read failed".into())
                        })?;
                    let header = ChunkHeader::from_bytes(&header_bytes)?;
                    let data_len = header.data_len as usize;
                    return Ok(chunk_data[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len].to_vec());
                }
                Err(e) => {
                    warn!(
                        "chunk {} read from node {} failed: {}",
                        chunk_id, target_node, e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            DataPlaneError::ChunkEngineError(format!(
                "all replica reads failed for chunk {}",
                chunk_id
            ))
        }))
    }

    /// Read an erasure-coded chunk by fetching shards and reconstructing.
    async fn read_erasure_coded(
        &self,
        chunk_id: &str,
        data_shards: u32,
        parity_shards: u32,
    ) -> Result<Vec<u8>> {
        let total_shards = (data_shards + parity_shards) as usize;
        let placements = crush::select(chunk_id, total_shards, &self.topology);

        let mut available: Vec<(usize, Vec<u8>)> = Vec::new();

        for (shard_idx, (target_node, _)) in placements.iter().enumerate() {
            if shard_idx >= total_shards {
                break;
            }
            let shard_id = format!("{chunk_id}:shard:{shard_idx}");
            match self.get_chunk_from_node(&shard_id, target_node).await {
                Ok(shard_with_header) => {
                    Self::verify_chunk(&shard_with_header)?;
                    let header_bytes: [u8; ChunkHeader::SIZE] = shard_with_header
                        [..ChunkHeader::SIZE]
                        .try_into()
                        .map_err(|_| {
                            DataPlaneError::ChunkEngineError("shard header read failed".into())
                        })?;
                    let header = ChunkHeader::from_bytes(&header_bytes)?;
                    let data_len = header.data_len as usize;
                    let shard_data =
                        shard_with_header[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len].to_vec();
                    available.push((shard_idx, shard_data));
                }
                Err(e) => {
                    warn!(
                        "EC shard {} read from node {} failed: {}",
                        shard_idx, target_node, e
                    );
                }
            }
        }

        if available.len() < data_shards as usize {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "insufficient EC shards for chunk {}: have {}, need {}",
                chunk_id,
                available.len(),
                data_shards
            )));
        }

        // Check if all data shards are available (fast path).
        let has_all_data =
            (0..data_shards as usize).all(|i| available.iter().any(|(idx, _)| *idx == i));

        if has_all_data {
            // Fast path: concatenate data shards in order, then figure out
            // original size from the first shard.
            let mut reconstructed = Vec::new();
            for i in 0..data_shards as usize {
                let shard = available.iter().find(|(idx, _)| *idx == i).unwrap();
                reconstructed.extend_from_slice(&shard.1);
            }
            // Trim padding (original chunk is at most CHUNK_SIZE).
            reconstructed.truncate(CHUNK_SIZE);
            return Ok(reconstructed);
        }

        // Slow path: RS decode to reconstruct missing data shards.
        let mut originals: Vec<(usize, &[u8])> = Vec::new();
        let mut recovery: Vec<(usize, &[u8])> = Vec::new();

        for (idx, data) in &available {
            if *idx < data_shards as usize {
                originals.push((*idx, data.as_slice()));
            } else {
                let recovery_idx = *idx - data_shards as usize;
                recovery.push((recovery_idx, data.as_slice()));
            }
        }

        let recovered = reed_solomon_simd::decode(
            data_shards as usize,
            parity_shards as usize,
            originals,
            recovery,
        )
        .map_err(|e| DataPlaneError::ChunkEngineError(format!("RS decode failed: {e}")))?;

        let mut reconstructed = Vec::new();
        for i in 0..data_shards as usize {
            if let Some((_, data)) = available.iter().find(|(idx, _)| *idx == i) {
                reconstructed.extend_from_slice(data);
            } else if let Some(shard_data) = recovered.get(&i) {
                reconstructed.extend_from_slice(shard_data);
            } else {
                return Err(DataPlaneError::ChunkEngineError(format!(
                    "shard {} missing from both available and recovered",
                    i
                )));
            }
        }

        reconstructed.truncate(CHUNK_SIZE);
        Ok(reconstructed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::file_store::FileChunkStore;
    use crate::metadata::topology::{Backend, BackendType, ClusterMap, Node, NodeStatus};

    fn local_topology(node_id: &str) -> ClusterMap {
        let mut map = ClusterMap::new(0);
        map.add_node(Node {
            id: node_id.to_string(),
            address: "127.0.0.1".to_string(),
            port: 9500,
            backends: vec![Backend {
                id: format!("{node_id}-be1"),
                node_id: node_id.to_string(),
                capacity_bytes: 100 * 1024 * 1024 * 1024,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::File,
            }],
            status: NodeStatus::Online,
        });
        map
    }

    #[test]
    fn content_addressing_produces_deterministic_id() {
        let data = b"hello chunk engine";
        let id1 = ChunkEngine::compute_chunk_id(data);
        let id2 = ChunkEngine::compute_chunk_id(data);
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 64); // SHA-256 hex
    }

    #[test]
    fn prepare_chunk_adds_header() {
        let data = b"raw chunk data";
        let prepared = ChunkEngine::prepare_chunk(data);
        assert_eq!(prepared.len(), ChunkHeader::SIZE + data.len());
        assert_eq!(&prepared[..4], b"NVAC");
    }

    #[test]
    fn verify_chunk_validates_crc() {
        let data = b"verified data";
        let prepared = ChunkEngine::prepare_chunk(data);
        assert!(ChunkEngine::verify_chunk(&prepared).is_ok());

        let mut corrupted = prepared.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF;
        assert!(ChunkEngine::verify_chunk(&corrupted).is_err());
    }

    #[test]
    fn split_data_into_chunks() {
        let data = vec![0xABu8; 10 * 1024 * 1024]; // 10MB
        let chunks = ChunkEngine::split_into_chunks(&data);
        assert_eq!(chunks.len(), 3); // 4MB + 4MB + 2MB
        assert_eq!(chunks[0].len(), 4 * 1024 * 1024);
        assert_eq!(chunks[1].len(), 4 * 1024 * 1024);
        assert_eq!(chunks[2].len(), 2 * 1024 * 1024);
    }

    #[tokio::test]
    async fn write_and_read_local_volume() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileChunkStore::new(dir.path().to_path_buf()).await.unwrap());
        let topology = local_topology("node-1");

        let engine = ChunkEngine::new("node-1".to_string(), store, topology);

        let volume_id = "aabb000000000000";
        let data = vec![0x42u8; 8 * 1024 * 1024]; // 8MB = 2 chunks
        let chunk_map = engine.write(volume_id, 0, &data).await.unwrap();
        assert_eq!(chunk_map.len(), 2);

        let read_data = engine.read(volume_id, 0, &chunk_map).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn write_records_policy_location_and_ref() {
        use crate::policy::engine::PolicyEngine;
        use crate::policy::location_store::ChunkLocationStore;

        let chunk_dir = tempfile::tempdir().unwrap();
        let policy_dir = tempfile::tempdir().unwrap();

        let store = Arc::new(
            FileChunkStore::new(chunk_dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let loc_store =
            Arc::new(ChunkLocationStore::open(policy_dir.path().join("policy.redb")).unwrap());
        let topology = local_topology("node-1");

        let policy_engine = Arc::new(PolicyEngine::new(
            "node-1".to_string(),
            loc_store.clone(),
            store.clone(),
            topology.clone(),
        ));

        let engine = ChunkEngine::with_policy("node-1".to_string(), store, topology, policy_engine);

        let volume_id = "aabb000000000000";
        let data = vec![0x42u8; 4 * 1024 * 1024]; // 1 chunk
        let chunk_map = engine.write(volume_id, 0, &data).await.unwrap();
        assert_eq!(chunk_map.len(), 1);

        // Verify policy engine recorded the chunk location
        let loc = loc_store
            .get_location(&chunk_map[0].chunk_id)
            .unwrap()
            .expect("location should be recorded");
        assert!(loc.node_ids.contains(&"node-1".to_string()));

        // Verify policy engine recorded the volume reference
        let chunk_ref = loc_store
            .get_ref(&chunk_map[0].chunk_id)
            .unwrap()
            .expect("ref should be recorded");
        assert!(chunk_ref.volume_ids.contains(&volume_id.to_string()));
    }

    #[tokio::test]
    async fn dedup_identical_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileChunkStore::new(dir.path().to_path_buf()).await.unwrap());
        let topology = local_topology("node-1");

        let engine = ChunkEngine::new("node-1".to_string(), store, topology);

        let data = vec![0x99u8; 4 * 1024 * 1024]; // exactly 1 chunk
        let map1 = engine.write("vol1-aabb00000000", 0, &data).await.unwrap();
        let map2 = engine.write("vol2-ccdd00000000", 0, &data).await.unwrap();
        assert_eq!(map1[0].chunk_id, map2[0].chunk_id); // same content = same ID
    }

    #[tokio::test]
    async fn write_and_read_with_replication_single_node() {
        // Replication factor 3 on a single-node topology writes one replica
        // (capped by available nodes) — write should still succeed.
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileChunkStore::new(dir.path().to_path_buf()).await.unwrap());
        let topology = local_topology("node-1");

        let engine = ChunkEngine::with_protection(
            "node-1".to_string(),
            store,
            topology,
            Protection::Replication { factor: 3 },
        );

        let volume_id = "aabb000000000000";
        let data = vec![0x42u8; 4 * 1024 * 1024];
        let chunk_map = engine.write(volume_id, 0, &data).await.unwrap();
        assert_eq!(chunk_map.len(), 1);

        let read_data = engine.read(volume_id, 0, &chunk_map).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn protection_scheme_accessors() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileChunkStore::new(dir.path().to_path_buf()).await.unwrap());
        let topology = local_topology("node-1");

        let mut engine = ChunkEngine::new("node-1".to_string(), store, topology);

        // Default is Replication { factor: 1 }
        match engine.protection() {
            Protection::Replication { factor } => assert_eq!(*factor, 1),
            _ => panic!("expected Replication"),
        }

        engine.set_protection(Protection::ErasureCoding {
            data_shards: 4,
            parity_shards: 2,
        });
        match engine.protection() {
            Protection::ErasureCoding {
                data_shards,
                parity_shards,
            } => {
                assert_eq!(*data_shards, 4);
                assert_eq!(*parity_shards, 2);
            }
            _ => panic!("expected ErasureCoding"),
        }
    }
}
