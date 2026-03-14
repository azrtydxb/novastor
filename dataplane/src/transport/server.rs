//! gRPC server — binds ChunkService, DataplaneService, and RaftService to a TCP listener.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::backend::chunk_store::ChunkStore;
use crate::error::Result;
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::nvmf_manager::NvmfManager;
use crate::transport::chunk_proto::chunk_service_server::ChunkServiceServer;
use crate::transport::chunk_service::ChunkServiceImpl;
use crate::transport::dataplane_proto::dataplane_service_server::DataplaneServiceServer;
use crate::transport::dataplane_service::DataplaneServiceImpl;

/// Configuration for the gRPC inter-node server.
pub struct GrpcServerConfig {
    pub listen_address: String,
    pub port: u16,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0".to_string(),
            port: 9500,
        }
    }
}

/// Handle returned by [`GrpcServer::start`] for managing the server lifecycle.
pub struct GrpcServerHandle {
    /// The address the server is bound to.
    pub addr: SocketAddr,
    /// Join handle for the server task.
    task: tokio::task::JoinHandle<()>,
}

impl GrpcServerHandle {
    /// Abort the server task, triggering a graceful shutdown.
    pub fn shutdown(self) {
        self.task.abort();
    }
}

/// The gRPC server hosting all inter-node services.
pub struct GrpcServer {
    config: GrpcServerConfig,
    chunk_store: Arc<dyn ChunkStore>,
    bdev_manager: Option<Arc<BdevManager>>,
    nvmf_manager: Option<Arc<NvmfManager>>,
}

impl GrpcServer {
    pub fn new(config: GrpcServerConfig, chunk_store: Arc<dyn ChunkStore>) -> Self {
        Self {
            config,
            chunk_store,
            bdev_manager: None,
            nvmf_manager: None,
        }
    }

    /// Set the BdevManager and NvmfManager for the DataplaneService.
    /// When both are set, the DataplaneService will be registered on the server.
    pub fn with_dataplane(
        mut self,
        bdev_manager: Arc<BdevManager>,
        nvmf_manager: Arc<NvmfManager>,
    ) -> Self {
        self.bdev_manager = Some(bdev_manager);
        self.nvmf_manager = Some(nvmf_manager);
        self
    }

    /// Start the gRPC server. Returns a handle with the bound address and shutdown control.
    pub async fn start(&self) -> Result<GrpcServerHandle> {
        let addr: SocketAddr = format!("{}:{}", self.config.listen_address, self.config.port)
            .parse()
            .map_err(|e| {
                crate::error::DataPlaneError::TransportError(format!("invalid listen address: {e}"))
            })?;

        let chunk_svc = ChunkServiceImpl::new(self.chunk_store.clone());
        let chunk_server = ChunkServiceServer::new(chunk_svc);

        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            crate::error::DataPlaneError::TransportError(format!("bind failed: {e}"))
        })?;
        let bound_addr = listener.local_addr().map_err(|e| {
            crate::error::DataPlaneError::TransportError(format!("local_addr: {e}"))
        })?;

        log::info!("gRPC server listening on {}", bound_addr);

        let bdev_mgr = self.bdev_manager.clone();
        let nvmf_mgr = self.nvmf_manager.clone();

        let task = tokio::spawn(async move {
            let mut builder = tonic::transport::Server::builder();
            let router = builder.add_service(chunk_server);

            // Register DataplaneService if managers are available.
            if let (Some(bdev_mgr), Some(nvmf_mgr)) = (bdev_mgr, nvmf_mgr) {
                let dp_svc = DataplaneServiceImpl::new(bdev_mgr, nvmf_mgr);
                let dp_server = DataplaneServiceServer::new(dp_svc);
                router
                    .add_service(dp_server)
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
            } else {
                router
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                    .await
                    .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
            }
        });

        Ok(GrpcServerHandle {
            addr: bound_addr,
            task,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::chunk_proto::chunk_service_client::ChunkServiceClient;
    use crate::transport::chunk_proto::HasChunkRequest;

    #[test]
    fn default_grpc_config() {
        let config = GrpcServerConfig::default();
        assert_eq!(config.listen_address, "0.0.0.0");
        assert_eq!(config.port, 9500);
    }

    #[tokio::test]
    async fn server_starts_and_accepts_client() {
        let dir = tempfile::tempdir().unwrap().keep();
        let store = crate::backend::file_store::FileChunkStore::new(dir)
            .await
            .unwrap();

        let config = GrpcServerConfig {
            listen_address: "127.0.0.1".to_string(),
            port: 0, // OS assigns port
        };

        let server = GrpcServer::new(config, Arc::new(store));
        let handle = server.start().await.unwrap();
        let addr = handle.addr;
        assert_ne!(addr.port(), 0);

        // Server should be serving — verify by connecting a client
        let mut client = ChunkServiceClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        // HasChunk on nonexistent chunk should return false
        let resp = client
            .has_chunk(HasChunkRequest {
                chunk_id: "0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            })
            .await
            .unwrap();
        assert!(!resp.into_inner().exists);
    }
}
