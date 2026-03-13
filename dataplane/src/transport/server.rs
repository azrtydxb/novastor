//! gRPC server — binds ChunkService and RaftService to a TCP listener.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::backend::chunk_store::ChunkStore;
use crate::error::Result;
use crate::transport::chunk_proto::chunk_service_server::ChunkServiceServer;
use crate::transport::chunk_service::ChunkServiceImpl;

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

/// The gRPC server hosting all inter-node services.
pub struct GrpcServer {
    config: GrpcServerConfig,
    chunk_store: Arc<dyn ChunkStore>,
}

impl GrpcServer {
    pub fn new(config: GrpcServerConfig, chunk_store: Arc<dyn ChunkStore>) -> Self {
        Self {
            config,
            chunk_store,
        }
    }

    /// Start the gRPC server. Returns the bound address.
    pub async fn start(&self) -> Result<SocketAddr> {
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

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(chunk_server)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
        });

        Ok(bound_addr)
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
        let addr = server.start().await.unwrap();
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
