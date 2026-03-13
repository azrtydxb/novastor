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
