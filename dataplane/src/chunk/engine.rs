//! Chunk engine — volume I/O → content-addressed chunks → CRUSH dispatch.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, CHUNK_SIZE};
use crate::error::{DataPlaneError, Result};
use crate::metadata::crush;
use crate::metadata::topology::ClusterMap;
use crate::metadata::types::ChunkMapEntry;
use crate::transport::chunk_client::ChunkClient;

pub struct ChunkEngine {
    node_id: String,
    local_store: Arc<dyn ChunkStore>,
    topology: ClusterMap,
}

impl ChunkEngine {
    pub fn new(node_id: String, local_store: Arc<dyn ChunkStore>, topology: ClusterMap) -> Self {
        Self {
            node_id,
            local_store,
            topology,
        }
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

    /// Write volume data, returning chunk map entries.
    pub async fn write(
        &self,
        _volume_id: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<Vec<ChunkMapEntry>> {
        let start_chunk_index = offset / CHUNK_SIZE as u64;
        let chunks = Self::split_into_chunks(data);
        let mut entries = Vec::with_capacity(chunks.len());

        for (i, raw_chunk) in chunks.iter().enumerate() {
            let chunk_index = start_chunk_index + i as u64;
            let chunk_id = Self::compute_chunk_id(raw_chunk);
            let prepared = Self::prepare_chunk(raw_chunk);

            let placements = crush::select(&chunk_id, 1, &self.topology);

            if let Some((target_node, _backend)) = placements.first() {
                if target_node == &self.node_id {
                    self.local_store.put(&chunk_id, &prepared).await?;
                } else {
                    let node = self
                        .topology
                        .nodes()
                        .iter()
                        .find(|n| n.id == *target_node)
                        .ok_or_else(|| {
                            DataPlaneError::ChunkEngineError(format!(
                                "node not found in topology: {target_node}"
                            ))
                        })?;
                    let addr = format!("http://{}:{}", node.address, node.port);
                    let client = ChunkClient::connect(addr).await?;
                    client.put(&chunk_id, &prepared).await?;
                }
            }

            entries.push(ChunkMapEntry {
                chunk_index,
                chunk_id,
                ec_params: None,
            });
        }

        Ok(entries)
    }

    /// Read volume data using a chunk map.
    pub async fn read(
        &self,
        _volume_id: &str,
        _offset: u64,
        chunk_map: &[ChunkMapEntry],
    ) -> Result<Vec<u8>> {
        let mut result = Vec::new();

        for entry in chunk_map {
            let placements = crush::select(&entry.chunk_id, 1, &self.topology);

            let chunk_data = if let Some((target_node, _)) = placements.first() {
                if target_node == &self.node_id {
                    self.local_store.get(&entry.chunk_id).await?
                } else {
                    let node = self
                        .topology
                        .nodes()
                        .iter()
                        .find(|n| n.id == *target_node)
                        .ok_or_else(|| {
                            DataPlaneError::ChunkEngineError(format!(
                                "node not found: {target_node}"
                            ))
                        })?;
                    let addr = format!("http://{}:{}", node.address, node.port);
                    let client = ChunkClient::connect(addr).await?;
                    client.get(&entry.chunk_id).await?
                }
            } else {
                return Err(DataPlaneError::ChunkEngineError(
                    "CRUSH returned no placement".into(),
                ));
            };

            Self::verify_chunk(&chunk_data)?;

            let header_bytes: [u8; ChunkHeader::SIZE] = chunk_data[..ChunkHeader::SIZE]
                .try_into()
                .map_err(|_| DataPlaneError::ChunkEngineError("header read failed".into()))?;
            let header = ChunkHeader::from_bytes(&header_bytes)?;
            let data_len = header.data_len as usize;
            result.extend_from_slice(&chunk_data[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len]);
        }

        Ok(result)
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
}
