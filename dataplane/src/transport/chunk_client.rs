//! gRPC ChunkService client for remote chunk dispatch.

use crate::error::{DataPlaneError, Result};
use crate::transport::chunk_proto::chunk_service_client::ChunkServiceClient;
use crate::transport::chunk_proto::*;

/// Fragment size for streaming put requests (1MB).
const PUT_FRAGMENT_SIZE: usize = 1024 * 1024;

/// Client for a remote node's ChunkService.
#[derive(Clone)]
pub struct ChunkClient {
    inner: ChunkServiceClient<tonic::transport::Channel>,
}

impl ChunkClient {
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let inner = ChunkServiceClient::connect(addr.into())
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("connect: {e}")))?;
        Ok(Self { inner })
    }

    pub async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        let mut client = self.inner.clone();
        let mut requests = Vec::new();
        for (i, fragment) in data.chunks(PUT_FRAGMENT_SIZE).enumerate() {
            requests.push(PutChunkRequest {
                chunk_id: if i == 0 {
                    chunk_id.to_string()
                } else {
                    String::new()
                },
                data: fragment.to_vec(),
            });
        }
        if requests.is_empty() {
            requests.push(PutChunkRequest {
                chunk_id: chunk_id.to_string(),
                data: Vec::new(),
            });
        }
        client
            .put_chunk(tokio_stream::iter(requests))
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("put_chunk: {e}")))?;
        Ok(())
    }

    /// Maximum payload size for a get response (8MB — chunk + header).
    const MAX_GET_PAYLOAD: usize = 8 * 1024 * 1024;

    pub async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let mut client = self.inner.clone();
        let response = client
            .get_chunk(GetChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("get_chunk: {e}")))?;

        let mut stream = response.into_inner();
        let mut data = Vec::new();
        while let Some(msg) = stream
            .message()
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("get_chunk stream: {e}")))?
        {
            data.extend_from_slice(&msg.data);
            if data.len() > Self::MAX_GET_PAYLOAD {
                return Err(DataPlaneError::TransportError(format!(
                    "get_chunk response exceeds {}MB limit",
                    Self::MAX_GET_PAYLOAD / (1024 * 1024)
                )));
            }
        }
        Ok(data)
    }

    pub async fn delete(&self, chunk_id: &str) -> Result<()> {
        let mut client = self.inner.clone();
        client
            .delete_chunk(DeleteChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("delete_chunk: {e}")))?;
        Ok(())
    }

    pub async fn has(&self, chunk_id: &str) -> Result<bool> {
        let mut client = self.inner.clone();
        let response = client
            .has_chunk(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("has_chunk: {e}")))?;
        Ok(response.into_inner().exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::ChunkHeader;
    use crate::backend::file_store::FileChunkStore;
    use crate::transport::chunk_proto::chunk_service_server::ChunkServiceServer;
    use crate::transport::chunk_service::ChunkServiceImpl;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tempfile::TempDir;

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

    async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.keep();
        let store = FileChunkStore::new(dir_path).await.unwrap();
        let svc = ChunkServiceImpl::new(Arc::new(store));
        let server = ChunkServiceServer::new(svc);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr, handle)
    }

    #[tokio::test]
    async fn client_put_and_get() {
        let (addr, _handle) = start_server().await;
        let client = ChunkClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let chunk_id = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233";
        let data = make_chunk_data(b"remote chunk data");

        client.put(chunk_id, &data).await.unwrap();
        let got = client.get(chunk_id).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn client_has_and_delete() {
        let (addr, _handle) = start_server().await;
        let client = ChunkClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let chunk_id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let data = make_chunk_data(b"to be deleted");

        assert!(!client.has(chunk_id).await.unwrap());
        client.put(chunk_id, &data).await.unwrap();
        assert!(client.has(chunk_id).await.unwrap());
        client.delete(chunk_id).await.unwrap();
        assert!(!client.has(chunk_id).await.unwrap());
    }
}
