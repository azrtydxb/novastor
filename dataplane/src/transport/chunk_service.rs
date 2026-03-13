//! gRPC ChunkService server implementation.
//!
//! Wraps a `dyn ChunkStore` and exposes chunk I/O over gRPC.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::backend::chunk_store::ChunkStore;
use crate::transport::chunk_proto::chunk_service_server::ChunkService;
use crate::transport::chunk_proto::{
    DeleteChunkRequest, DeleteChunkResponse, GetChunkRequest, GetChunkResponse, HasChunkRequest,
    HasChunkResponse, PutChunkRequest, PutChunkResponse,
};

/// Maximum payload size for a streaming put_chunk request (8MB — chunk + header).
const MAX_PUT_PAYLOAD: usize = 8 * 1024 * 1024;

pub struct ChunkServiceImpl {
    store: Arc<dyn ChunkStore>,
}

impl ChunkServiceImpl {
    pub fn new(store: Arc<dyn ChunkStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl ChunkService for ChunkServiceImpl {
    type GetChunkStream = tokio_stream::wrappers::ReceiverStream<Result<GetChunkResponse, Status>>;

    async fn put_chunk(
        &self,
        request: Request<Streaming<PutChunkRequest>>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let mut chunk_id = String::new();
        let mut data = Vec::new();

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if chunk_id.is_empty() {
                chunk_id = msg.chunk_id;
            }
            data.extend_from_slice(&msg.data);
            if data.len() > MAX_PUT_PAYLOAD {
                return Err(Status::resource_exhausted(format!(
                    "put_chunk payload exceeds {}MB limit",
                    MAX_PUT_PAYLOAD / (1024 * 1024)
                )));
            }
        }

        if chunk_id.is_empty() {
            return Err(Status::invalid_argument("missing chunk_id"));
        }

        let bytes_written = data.len() as u64;
        self.store
            .put(&chunk_id, &data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutChunkResponse {
            chunk_id,
            bytes_written,
        }))
    }

    async fn get_chunk(
        &self,
        request: Request<GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        let chunk_id = request.into_inner().chunk_id;

        let data = self
            .store
            .get(&chunk_id)
            .await
            .map_err(|e| Status::not_found(e.to_string()))?;

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        let cid = chunk_id.clone();
        tokio::spawn(async move {
            const FRAGMENT_SIZE: usize = 1024 * 1024; // 1MB
            for (i, fragment) in data.chunks(FRAGMENT_SIZE).enumerate() {
                let msg = GetChunkResponse {
                    chunk_id: if i == 0 { cid.clone() } else { String::new() },
                    data: fragment.to_vec(),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let chunk_id = request.into_inner().chunk_id;
        self.store
            .delete(&chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(DeleteChunkResponse {}))
    }

    async fn has_chunk(
        &self,
        request: Request<HasChunkRequest>,
    ) -> Result<Response<HasChunkResponse>, Status> {
        let chunk_id = request.into_inner().chunk_id;
        let exists = self
            .store
            .exists(&chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(HasChunkResponse { exists }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::ChunkHeader;
    use crate::backend::file_store::FileChunkStore;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

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

    async fn setup() -> (ChunkServiceImpl, PathBuf) {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.keep();
        let store = FileChunkStore::new(dir_path.clone()).await.unwrap();
        let svc = ChunkServiceImpl::new(Arc::new(store));
        (svc, dir_path)
    }

    /// Test get_chunk streaming response by putting data via the store directly,
    /// then reading it back through the gRPC get_chunk method.
    #[tokio::test]
    async fn get_chunk_streams_data() {
        let (svc, _dir) = setup().await;
        let chunk_id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let data = make_chunk_data(b"hello world");

        // Put data directly via the store.
        svc.store.put(chunk_id, &data).await.unwrap();

        // Read back via gRPC get_chunk.
        let request = Request::new(GetChunkRequest {
            chunk_id: chunk_id.to_string(),
        });
        let response = svc.get_chunk(request).await.unwrap();
        let mut stream = response.into_inner();
        let mut received = Vec::new();
        let mut got_chunk_id = String::new();
        while let Some(msg) = stream.next().await {
            let msg = msg.unwrap();
            if !msg.chunk_id.is_empty() {
                got_chunk_id = msg.chunk_id;
            }
            received.extend_from_slice(&msg.data);
        }
        assert_eq!(got_chunk_id, chunk_id);
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn has_chunk_returns_false_then_true() {
        let (svc, _dir) = setup().await;
        let chunk_id = "1111111111111111111111111111111111111111111111111111111111111111";

        // Should not exist initially.
        let resp = svc
            .has_chunk(Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(!resp.into_inner().exists);

        // Put data via the store.
        let data = make_chunk_data(b"test");
        svc.store.put(chunk_id, &data).await.unwrap();

        // Should exist now.
        let resp = svc
            .has_chunk(Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(resp.into_inner().exists);
    }

    #[tokio::test]
    async fn delete_chunk_removes_data() {
        let (svc, _dir) = setup().await;
        let chunk_id = "2222222222222222222222222222222222222222222222222222222222222222";
        let data = make_chunk_data(b"delete me");

        // Put data via the store.
        svc.store.put(chunk_id, &data).await.unwrap();

        // Delete via gRPC.
        svc.delete_chunk(Request::new(DeleteChunkRequest {
            chunk_id: chunk_id.to_string(),
        }))
        .await
        .unwrap();

        // Verify it's gone.
        let resp = svc
            .has_chunk(Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(!resp.into_inner().exists);
    }
}
