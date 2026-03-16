//! gRPC client for remote chunk dispatch (inter-dataplane replication).
//!
//! Uses the DataplaneService PutChunk/GetChunk RPCs for chunk transfer
//! between dataplanes during replication. These are unary RPCs on the
//! DataplaneService, which is always registered in production (unlike
//! the standalone ChunkService which is only available when a chunk
//! store is provided at server construction time).

use crate::error::{DataPlaneError, Result};
use crate::transport::dataplane_proto::dataplane_service_client::DataplaneServiceClient;
use crate::transport::dataplane_proto::{DeleteChunkRequest, GetChunkRequest, PutChunkRequest};

/// Client for a remote node's DataplaneService (chunk transfer RPCs).
#[derive(Clone)]
pub struct ChunkClient {
    inner: DataplaneServiceClient<tonic::transport::Channel>,
}

impl ChunkClient {
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let endpoint = tonic::transport::Channel::from_shared(addr.into())
            .map_err(|e| DataPlaneError::TransportError(format!("invalid endpoint: {e}")))?
            // Allow large messages for 4MB+ chunk transfers.
            .initial_connection_window_size(8 * 1024 * 1024);
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("connect: {e}")))?;
        // Set max message sizes to accommodate 4MB chunks + header overhead.
        let inner = DataplaneServiceClient::new(channel)
            .max_decoding_message_size(8 * 1024 * 1024)
            .max_encoding_message_size(8 * 1024 * 1024);
        Ok(Self { inner })
    }

    /// Store a chunk on the remote dataplane.
    pub async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        let mut client = self.inner.clone();
        client
            .put_chunk(PutChunkRequest {
                chunk_id: chunk_id.to_string(),
                data: data.to_vec(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("put_chunk: {e}")))?;
        Ok(())
    }

    /// Retrieve a chunk from the remote dataplane.
    pub async fn get(&self, chunk_id: &str) -> Result<Vec<u8>> {
        let mut client = self.inner.clone();
        let response = client
            .get_chunk(GetChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("get_chunk: {e}")))?;

        Ok(response.into_inner().data)
    }

    /// Delete a chunk on the remote dataplane.
    pub async fn delete(&self, chunk_id: &str) -> Result<()> {
        let mut client = self.inner.clone();
        client
            .delete_chunk(DeleteChunkRequest {
                chunk_id: chunk_id.to_string(),
                bdev_name: String::new(),
            })
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("delete_chunk: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::{ChunkHeader, ChunkStore};
    use crate::backend::file_store::FileChunkStore;
    use crate::transport::dataplane_proto::dataplane_service_server::{
        DataplaneService, DataplaneServiceServer,
    };
    use crate::transport::dataplane_proto::*;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio_stream::Stream;
    use tonic::{Request, Response, Status, Streaming};

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

    /// Minimal DataplaneService implementation for testing PutChunk/GetChunk.
    /// Only implements the chunk transfer RPCs; all others return Unimplemented.
    struct TestDataplaneService {
        store: Arc<dyn ChunkStore>,
    }

    type StreamResult<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

    #[tonic::async_trait]
    impl DataplaneService for TestDataplaneService {
        async fn put_chunk(
            &self,
            request: Request<PutChunkRequest>,
        ) -> Result<Response<PutChunkResponse>, Status> {
            let req = request.into_inner();
            self.store
                .put(&req.chunk_id, &req.data)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            Ok(Response::new(PutChunkResponse {}))
        }

        async fn get_chunk(
            &self,
            request: Request<GetChunkRequest>,
        ) -> Result<Response<GetChunkResponse>, Status> {
            let req = request.into_inner();
            let data = self
                .store
                .get(&req.chunk_id)
                .await
                .map_err(|e| Status::not_found(e.to_string()))?;
            Ok(Response::new(GetChunkResponse { data }))
        }

        // --- Stubs for all other RPCs (not used in chunk transfer tests) ---

        async fn create_aio_bdev(
            &self,
            _: Request<CreateAioBdevRequest>,
        ) -> Result<Response<BdevInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_malloc_bdev(
            &self,
            _: Request<CreateMallocBdevRequest>,
        ) -> Result<Response<BdevInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_lvol_store(
            &self,
            _: Request<CreateLvolStoreRequest>,
        ) -> Result<Response<LvolStoreInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_lvol(
            &self,
            _: Request<CreateLvolRequest>,
        ) -> Result<Response<BdevInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn delete_bdev(
            &self,
            _: Request<DeleteBdevRequest>,
        ) -> Result<Response<DeleteBdevResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn list_bdevs(
            &self,
            _: Request<ListBdevsRequest>,
        ) -> Result<Response<ListBdevsResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn get_bdev_info(
            &self,
            _: Request<GetBdevInfoRequest>,
        ) -> Result<Response<BdevInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn list_lvol_stores(
            &self,
            _: Request<ListLvolStoresRequest>,
        ) -> Result<Response<ListLvolStoresResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn init_transport(
            &self,
            _: Request<InitTransportRequest>,
        ) -> Result<Response<InitTransportResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_nvmf_target(
            &self,
            _: Request<CreateNvmfTargetRequest>,
        ) -> Result<Response<NvmfTargetInfo>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn delete_nvmf_target(
            &self,
            _: Request<DeleteNvmfTargetRequest>,
        ) -> Result<Response<DeleteNvmfTargetResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn set_ana_state(
            &self,
            _: Request<SetAnaStateRequest>,
        ) -> Result<Response<SetAnaStateResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn get_ana_state(
            &self,
            _: Request<GetAnaStateRequest>,
        ) -> Result<Response<GetAnaStateResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn list_subsystems(
            &self,
            _: Request<ListSubsystemsRequest>,
        ) -> Result<Response<ListSubsystemsResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn export_bdev(
            &self,
            _: Request<ExportBdevRequest>,
        ) -> Result<Response<ExportBdevResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn connect_initiator(
            &self,
            _: Request<ConnectInitiatorRequest>,
        ) -> Result<Response<ConnectInitiatorResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn disconnect_initiator(
            &self,
            _: Request<DisconnectInitiatorRequest>,
        ) -> Result<Response<DisconnectInitiatorResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_replica_bdev(
            &self,
            _: Request<CreateReplicaBdevRequest>,
        ) -> Result<Response<CreateReplicaBdevResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn replica_status(
            &self,
            _: Request<ReplicaStatusRequest>,
        ) -> Result<Response<ReplicaStatusResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn init_chunk_store(
            &self,
            _: Request<InitChunkStoreRequest>,
        ) -> Result<Response<InitChunkStoreResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn chunk_store_stats(
            &self,
            _: Request<ChunkStoreStatsRequest>,
        ) -> Result<Response<ChunkStoreStatsResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn write_chunk(
            &self,
            _: Request<Streaming<WriteChunkRequest>>,
        ) -> Result<Response<WriteChunkResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        type ReadChunkStream = StreamResult<ReadChunkResponse>;
        async fn read_chunk(
            &self,
            _: Request<ReadChunkRequest>,
        ) -> Result<Response<Self::ReadChunkStream>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn delete_chunk(
            &self,
            _: Request<DeleteChunkRequest>,
        ) -> Result<Response<DeleteChunkResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn chunk_exists(
            &self,
            _: Request<ChunkExistsRequest>,
        ) -> Result<Response<ChunkExistsResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn list_chunks(
            &self,
            _: Request<ListChunksRequest>,
        ) -> Result<Response<ListChunksResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn garbage_collect(
            &self,
            _: Request<GarbageCollectRequest>,
        ) -> Result<Response<GarbageCollectResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn init_backend(
            &self,
            _: Request<InitBackendRequest>,
        ) -> Result<Response<InitBackendResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn create_volume(
            &self,
            _: Request<CreateVolumeRequest>,
        ) -> Result<Response<CreateVolumeResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn delete_volume(
            &self,
            _: Request<DeleteVolumeRequest>,
        ) -> Result<Response<DeleteVolumeResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn set_volume_policy(
            &self,
            _: Request<SetVolumePolicyRequest>,
        ) -> Result<Response<SetVolumePolicyResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn reconstruct_shard(
            &self,
            _: Request<ReconstructShardRequest>,
        ) -> Result<Response<ReconstructShardResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn get_chunk_maps(
            &self,
            _: Request<GetChunkMapsRequest>,
        ) -> Result<Response<GetChunkMapsResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn update_topology(
            &self,
            _: Request<UpdateTopologyRequest>,
        ) -> Result<Response<UpdateTopologyResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn get_version(
            &self,
            _: Request<GetVersionRequest>,
        ) -> Result<Response<GetVersionResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn heartbeat(
            &self,
            _: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
        async fn put_sub_block(
            &self,
            _: Request<PutSubBlockRequest>,
        ) -> Result<Response<PutSubBlockResponse>, Status> {
            Err(Status::unimplemented("test stub"))
        }
    }

    async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.keep();
        let store = FileChunkStore::new(dir_path).await.unwrap();
        let store = Arc::new(store);

        let svc = TestDataplaneService {
            store: store.clone(),
        };
        let server = DataplaneServiceServer::new(svc)
            .max_decoding_message_size(8 * 1024 * 1024)
            .max_encoding_message_size(8 * 1024 * 1024);

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
}
