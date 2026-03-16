//! Chunk-level operations for the policy engine.
//!
//! Executes replication and removal actions using the local `ChunkStore`
//! for same-node I/O and `ChunkClient` for remote nodes.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::backend::chunk_store::ChunkStore;
use crate::error::Result;
use crate::transport::chunk_client::ChunkClient;

/// Executes chunk-level operations for the policy engine.
///
/// Maintains a cache of gRPC connections to remote nodes. Local operations
/// go directly through the `ChunkStore` trait without network overhead.
pub struct ChunkOperations {
    local_node_id: String,
    local_store: Arc<dyn ChunkStore>,
    connections: Mutex<HashMap<String, ChunkClient>>,
}

impl ChunkOperations {
    pub fn new(local_node_id: String, local_store: Arc<dyn ChunkStore>) -> Self {
        Self {
            local_node_id,
            local_store,
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a cached `ChunkClient` for the given address.
    async fn get_client(&self, addr: &str) -> Result<ChunkClient> {
        let mut cache = self.connections.lock().await;
        if let Some(client) = cache.get(addr) {
            return Ok(client.clone());
        }
        let client = ChunkClient::connect(addr).await?;
        cache.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// Replicate a chunk from one node to another.
    ///
    /// If the source is this node, reads from the local store.
    /// If the target is this node, writes to the local store.
    /// Remote nodes are reached via gRPC `ChunkClient`.
    pub async fn replicate_chunk(
        &self,
        chunk_id: &str,
        source_node_id: &str,
        source_addr: &str,
        target_node_id: &str,
        target_addr: &str,
    ) -> Result<()> {
        // Read from source
        let data = if source_node_id == self.local_node_id {
            self.local_store.get(chunk_id).await?
        } else {
            let client = self.get_client(source_addr).await?;
            client.get(chunk_id).await?
        };

        // Write to target
        if target_node_id == self.local_node_id {
            self.local_store.put(chunk_id, &data).await?;
        } else {
            let client = self.get_client(target_addr).await?;
            client.put(chunk_id, &data).await?;
        }

        Ok(())
    }

    /// Reconstruct a missing EC shard from surviving shards using
    /// Reed-Solomon decoding, then write the result to the target node.
    ///
    /// `shard_addrs` maps shard index → (node_id, address) for all
    /// surviving shards. The method reads the surviving shards, runs RS
    /// decode, and writes the reconstructed shard to `target_addr`.
    pub async fn reconstruct_shard(
        &self,
        chunk_id: &str,
        shard_index: usize,
        data_shards: u32,
        parity_shards: u32,
        shard_addrs: &[(usize, String, String)], // (shard_idx, node_id, addr)
        target_node_id: &str,
        target_addr: &str,
    ) -> Result<()> {
        use crate::backend::chunk_store::ChunkHeader;

        // Read all surviving shards.
        let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
        for (idx, node_id, addr) in shard_addrs {
            let shard_id = format!("{chunk_id}:shard:{idx}");
            let shard_with_header = if *node_id == self.local_node_id {
                self.local_store.get(&shard_id).await?
            } else {
                let client = self.get_client(addr).await?;
                client.get(&shard_id).await?
            };
            // Strip the chunk header to get raw shard data.
            if shard_with_header.len() < ChunkHeader::SIZE {
                continue;
            }
            let header_bytes: [u8; ChunkHeader::SIZE] = shard_with_header[..ChunkHeader::SIZE]
                .try_into()
                .map_err(|_| {
                    crate::error::DataPlaneError::ChunkEngineError(
                        "shard header read failed".into(),
                    )
                })?;
            let header = ChunkHeader::from_bytes(&header_bytes)?;
            let data_len = header.data_len as usize;
            if shard_with_header.len() < ChunkHeader::SIZE + data_len {
                continue;
            }
            let raw = shard_with_header[ChunkHeader::SIZE..ChunkHeader::SIZE + data_len].to_vec();
            available.push((*idx, raw));
        }

        if available.len() < data_shards as usize {
            return Err(crate::error::DataPlaneError::ChunkEngineError(format!(
                "insufficient surviving shards for reconstruction: have {}, need {}",
                available.len(),
                data_shards
            )));
        }

        // Separate into original (data) and recovery (parity) shard sets.
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

        // If the missing shard is a data shard, use RS decode.
        // If it's a parity shard, use RS encode to regenerate parity.
        let reconstructed_shard = if shard_index < data_shards as usize {
            // Missing data shard — decode.
            let recovered = reed_solomon_simd::decode(
                data_shards as usize,
                parity_shards as usize,
                originals,
                recovery,
            )
            .map_err(|e| {
                crate::error::DataPlaneError::ChunkEngineError(format!("RS decode failed: {e}"))
            })?;
            recovered.get(&shard_index).cloned().ok_or_else(|| {
                crate::error::DataPlaneError::ChunkEngineError(format!(
                    "RS decode did not produce shard {shard_index}"
                ))
            })?
        } else {
            // Missing parity shard — re-encode from data shards.
            // First, ensure we have all data shards.
            let mut data_pieces: Vec<Vec<u8>> = vec![Vec::new(); data_shards as usize];
            for (idx, data) in &available {
                if *idx < data_shards as usize {
                    data_pieces[*idx] = data.clone();
                }
            }
            let all_data_present = data_pieces.iter().all(|d| !d.is_empty());
            if !all_data_present {
                return Err(crate::error::DataPlaneError::ChunkEngineError(
                    "cannot reconstruct parity: not all data shards available".into(),
                ));
            }
            let data_refs: Vec<&[u8]> = data_pieces.iter().map(|d| d.as_slice()).collect();
            let parity = reed_solomon_simd::encode(
                data_shards as usize,
                parity_shards as usize,
                data_refs.into_iter(),
            )
            .map_err(|e| {
                crate::error::DataPlaneError::ChunkEngineError(format!("RS encode failed: {e}"))
            })?;
            let parity_idx = shard_index - data_shards as usize;
            parity.into_iter().nth(parity_idx).ok_or_else(|| {
                crate::error::DataPlaneError::ChunkEngineError(format!(
                    "RS encode did not produce parity shard {parity_idx}"
                ))
            })?
        };

        // Prepare the shard with a chunk header and write to target.
        let header = ChunkHeader {
            magic: *b"NVAC",
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(&reconstructed_shard),
            data_len: reconstructed_shard.len() as u32,
            _reserved: [0; 2],
        };
        let mut prepared = Vec::with_capacity(ChunkHeader::SIZE + reconstructed_shard.len());
        prepared.extend_from_slice(&header.to_bytes());
        prepared.extend_from_slice(&reconstructed_shard);

        let shard_id = format!("{chunk_id}:shard:{shard_index}");
        if target_node_id == self.local_node_id {
            self.local_store.put(&shard_id, &prepared).await?;
        } else {
            let client = self.get_client(target_addr).await?;
            client.put(&shard_id, &prepared).await?;
        }

        Ok(())
    }

    /// Remove a chunk replica from a node.
    ///
    /// If the node is this node, deletes from the local store.
    /// Otherwise, issues a remote delete via gRPC.
    pub async fn remove_replica(
        &self,
        chunk_id: &str,
        node_id: &str,
        node_addr: &str,
    ) -> Result<()> {
        if node_id == self.local_node_id {
            self.local_store.delete(chunk_id).await?;
        } else {
            let client = self.get_client(node_addr).await?;
            client.delete(chunk_id).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::ChunkHeader;
    use crate::backend::file_store::FileChunkStore;

    const LOCAL_NODE: &str = "local-node-1";

    fn make_chunk_data(data: &[u8]) -> Vec<u8> {
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

    fn fake_chunk_id() -> String {
        "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233".to_string()
    }

    async fn make_ops() -> (tempfile::TempDir, ChunkOperations) {
        let dir = tempfile::tempdir().unwrap();
        let store = FileChunkStore::new(dir.path().to_path_buf()).await.unwrap();
        let ops = ChunkOperations::new(LOCAL_NODE.to_string(), Arc::new(store));
        (dir, ops)
    }

    #[tokio::test]
    async fn replicate_local_to_local() {
        let (_dir, ops) = make_ops().await;
        let chunk_id = fake_chunk_id();
        let data = make_chunk_data(b"hello replication");

        // Seed the chunk in local store.
        ops.local_store.put(&chunk_id, &data).await.unwrap();

        // Replicate local -> local (idempotent write).
        ops.replicate_chunk(&chunk_id, LOCAL_NODE, "", LOCAL_NODE, "")
            .await
            .unwrap();

        // Data must still be readable.
        let got = ops.local_store.get(&chunk_id).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn replicate_preserves_data() {
        let (_dir, ops) = make_ops().await;
        let chunk_id = fake_chunk_id();
        let payload = b"exact data must survive replication";
        let data = make_chunk_data(payload);

        ops.local_store.put(&chunk_id, &data).await.unwrap();

        // Replicate (local source, local target — exercises the read-then-write path).
        ops.replicate_chunk(&chunk_id, LOCAL_NODE, "", LOCAL_NODE, "")
            .await
            .unwrap();

        let got = ops.local_store.get(&chunk_id).await.unwrap();
        assert_eq!(got.len(), data.len());
        assert_eq!(got, data);

        // Verify header + payload are intact.
        let header_bytes: [u8; ChunkHeader::SIZE] = got[..ChunkHeader::SIZE].try_into().unwrap();
        let header = ChunkHeader::from_bytes(&header_bytes).unwrap();
        let data_len = header.data_len;
        assert_eq!(data_len as usize, payload.len());
        let checksum = header.checksum;
        assert_eq!(checksum, crc32c::crc32c(payload));
    }

    #[tokio::test]
    async fn remove_local_replica() {
        let (_dir, ops) = make_ops().await;
        let chunk_id = fake_chunk_id();
        let data = make_chunk_data(b"will be removed");

        ops.local_store.put(&chunk_id, &data).await.unwrap();
        assert!(ops.local_store.exists(&chunk_id).await.unwrap());

        ops.remove_replica(&chunk_id, LOCAL_NODE, "").await.unwrap();
        assert!(!ops.local_store.exists(&chunk_id).await.unwrap());
    }

    #[tokio::test]
    async fn remove_nonexistent_chunk_returns_error() {
        let (_dir, ops) = make_ops().await;
        let chunk_id = fake_chunk_id();

        // Removing a chunk that doesn't exist should propagate the store's error.
        let result = ops.remove_replica(&chunk_id, LOCAL_NODE, "").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replicate_nonexistent_source_returns_error() {
        let (_dir, ops) = make_ops().await;
        let chunk_id = fake_chunk_id();

        // Reading a chunk that doesn't exist should fail.
        let result = ops
            .replicate_chunk(&chunk_id, LOCAL_NODE, "", LOCAL_NODE, "")
            .await;
        assert!(result.is_err());
    }
}
