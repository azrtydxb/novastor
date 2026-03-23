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
    /// Path to CA certificate for mTLS (empty = no TLS).
    pub tls_ca_cert: String,
    /// Path to server certificate.
    pub tls_server_cert: String,
    /// Path to server private key.
    pub tls_server_key: String,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            listen_address: "::".to_string(),
            port: 9500,
            tls_ca_cert: String::new(),
            tls_server_cert: String::new(),
            tls_server_key: String::new(),
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
    chunk_store: Option<Arc<dyn ChunkStore>>,
    bdev_manager: Option<Arc<BdevManager>>,
    nvmf_manager: Option<Arc<NvmfManager>>,
}

impl GrpcServer {
    pub fn new(config: GrpcServerConfig, chunk_store: Arc<dyn ChunkStore>) -> Self {
        Self {
            config,
            chunk_store: Some(chunk_store),
            bdev_manager: None,
            nvmf_manager: None,
        }
    }

    /// Create a server without a chunk store (dataplane management only).
    pub fn new_management_only(config: GrpcServerConfig) -> Self {
        Self {
            config,
            chunk_store: None,
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

        // Configure mTLS if cert paths are provided.
        let tls_config = if !self.config.tls_ca_cert.is_empty()
            && !self.config.tls_server_cert.is_empty()
            && !self.config.tls_server_key.is_empty()
        {
            let ca_cert = std::fs::read(&self.config.tls_ca_cert).map_err(|e| {
                crate::error::DataPlaneError::TransportError(format!(
                    "read CA cert {}: {e}",
                    self.config.tls_ca_cert
                ))
            })?;
            let server_cert = std::fs::read(&self.config.tls_server_cert).map_err(|e| {
                crate::error::DataPlaneError::TransportError(format!(
                    "read server cert {}: {e}",
                    self.config.tls_server_cert
                ))
            })?;
            let server_key = std::fs::read(&self.config.tls_server_key).map_err(|e| {
                crate::error::DataPlaneError::TransportError(format!(
                    "read server key {}: {e}",
                    self.config.tls_server_key
                ))
            })?;

            let identity = tonic::transport::Identity::from_pem(server_cert, server_key);
            let client_ca = tonic::transport::Certificate::from_pem(ca_cert);

            let tls = tonic::transport::ServerTlsConfig::new()
                .identity(identity)
                .client_ca_root(client_ca);

            log::info!("gRPC server TLS enabled (mTLS with client CA verification)");
            Some(tls)
        } else {
            log::warn!("gRPC server TLS DISABLED — no cert paths configured");
            None
        };

        let chunk_store = self.chunk_store.clone();

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
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

            // Build the server, optionally with TLS.
            let mut builder = tonic::transport::Server::builder();
            if let Some(tls) = tls_config {
                builder = builder.tls_config(tls).unwrap_or_else(|e| {
                    log::error!("gRPC TLS config error: {}", e);
                    tonic::transport::Server::builder()
                });
            }

            // Tonic's add_service changes the router type at compile time, so
            // we branch based on which services are available.
            match (chunk_store, bdev_mgr, nvmf_mgr) {
                // Full services: ChunkService + DataplaneService
                (Some(cs), Some(bm), Some(nm)) => {
                    builder
                        .add_service(ChunkServiceServer::new(ChunkServiceImpl::new(cs)))
                        .add_service(
                            DataplaneServiceServer::new(DataplaneServiceImpl::new(bm, nm))
                                .max_decoding_message_size(8 * 1024 * 1024)
                                .max_encoding_message_size(8 * 1024 * 1024),
                        )
                        .serve_with_incoming(incoming)
                        .await
                        .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
                }
                // Chunk store unavailable — DataplaneService only (production path).
                (None, Some(bm), Some(nm)) => {
                    builder
                        .add_service(
                            DataplaneServiceServer::new(DataplaneServiceImpl::new(bm, nm))
                                .max_decoding_message_size(8 * 1024 * 1024)
                                .max_encoding_message_size(8 * 1024 * 1024),
                        )
                        .serve_with_incoming(incoming)
                        .await
                        .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
                }
                // ChunkService only (no dataplane managers)
                (Some(cs), _, _) => {
                    builder
                        .add_service(ChunkServiceServer::new(ChunkServiceImpl::new(cs)))
                        .serve_with_incoming(incoming)
                        .await
                        .unwrap_or_else(|e| log::error!("gRPC server error: {}", e));
                }
                // Nothing available
                (None, _, _) => {
                    log::error!("no services available for gRPC server");
                }
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
        assert_eq!(config.listen_address, "::");
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
            ..Default::default()
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
