//! RaftService gRPC server and openraft network factory.
//!
//! Two responsibilities:
//! 1. **RaftServiceImpl** — tonic server that receives Raft messages from peers
//!    and routes them to the correct local shard via a [`RaftMessageHandler`].
//! 2. **GrpcRaftNetworkFactory** — creates [`GrpcRaftNetwork`] instances that
//!    openraft uses to send messages to remote peers over gRPC.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::metadata::raft_types::RaftNode;
use crate::transport::raft_proto::raft_service_server::RaftService;
use crate::transport::raft_proto::{RaftRequest, RaftResponse, SnapshotRequest, SnapshotResponse};

// ---------------------------------------------------------------------------
// Handler trait
// ---------------------------------------------------------------------------

/// Handler trait for incoming Raft RPCs.
///
/// Implementors route messages to the correct local shard's Raft instance.
/// The `shard_id` identifies which Raft group the message belongs to,
/// `rpc_type` indicates the openraft RPC kind (e.g. "vote", "append_entries"),
/// and `payload` carries the JSON-serialized openraft request.
#[async_trait::async_trait]
pub trait RaftMessageHandler: Send + Sync + 'static {
    /// Handle a forwarded Raft RPC (vote, append_entries, etc.).
    async fn handle_forward(
        &self,
        shard_id: u32,
        rpc_type: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, String>;

    /// Handle an incoming snapshot stream (already fully assembled).
    async fn handle_snapshot(&self, shard_id: u32, payload: Vec<u8>) -> Result<Vec<u8>, String>;
}

// ---------------------------------------------------------------------------
// RaftServiceImpl — tonic server
// ---------------------------------------------------------------------------

/// Maximum payload size for a streaming install_snapshot request (64MB).
const MAX_SNAPSHOT_PAYLOAD: usize = 64 * 1024 * 1024;

/// gRPC server implementation for [`RaftService`].
///
/// Delegates all incoming RPCs to the provided [`RaftMessageHandler`].
pub struct RaftServiceImpl {
    handler: Arc<dyn RaftMessageHandler>,
}

impl RaftServiceImpl {
    /// Create a new `RaftServiceImpl` backed by the given handler.
    pub fn new(handler: Arc<dyn RaftMessageHandler>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn forward(
        &self,
        request: Request<RaftRequest>,
    ) -> Result<Response<RaftResponse>, Status> {
        let req = request.into_inner();
        let payload = self
            .handler
            .handle_forward(req.shard_id, req.rpc_type, req.payload)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(RaftResponse { payload }))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotRequest>>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let mut shard_id = 0u32;
        let mut payload = Vec::new();

        while let Some(msg) = stream.next().await {
            let msg = msg?;
            if payload.is_empty() {
                shard_id = msg.shard_id;
            }
            payload.extend_from_slice(&msg.payload);
            if payload.len() > MAX_SNAPSHOT_PAYLOAD {
                return Err(Status::resource_exhausted(format!(
                    "snapshot payload exceeds {}MB limit",
                    MAX_SNAPSHOT_PAYLOAD / (1024 * 1024)
                )));
            }
        }

        let resp_payload = self
            .handler
            .handle_snapshot(shard_id, payload)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(SnapshotResponse {
            payload: resp_payload,
        }))
    }
}

// ---------------------------------------------------------------------------
// GrpcRaftNetworkFactory — creates per-node gRPC network clients
// ---------------------------------------------------------------------------

/// Factory that creates gRPC-backed [`GrpcRaftNetwork`] instances for openraft.
///
/// Each call to [`new_client`](GrpcRaftNetworkFactory::new_client) returns a
/// network handle pre-configured with the target node's address.
pub struct GrpcRaftNetworkFactory;

impl GrpcRaftNetworkFactory {
    /// Create a new factory.
    pub fn new() -> Self {
        Self
    }

    /// Create a [`GrpcRaftNetwork`] for the given target node.
    pub fn new_client(&self, _target: u64, node: &RaftNode) -> GrpcRaftNetwork {
        GrpcRaftNetwork {
            address: format!("http://{}:{}", node.address, node.port),
        }
    }
}

impl Default for GrpcRaftNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

/// A gRPC-backed network handle for a specific remote Raft peer.
///
/// Holds the target address. The full `RaftNetwork<TypeConfig>` trait
/// implementation will be added in a future integration task once Raft
/// groups are running.
pub struct GrpcRaftNetwork {
    /// The target node's gRPC endpoint (e.g. `http://10.0.0.1:9100`).
    pub address: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::raft_proto::raft_service_server::RaftService as RaftServiceTrait;

    // -- GrpcRaftNetworkFactory tests --

    #[test]
    fn raft_network_factory_creates_network() {
        let factory = GrpcRaftNetworkFactory::new();
        let node = RaftNode {
            address: "127.0.0.1".to_string(),
            port: 9100,
        };
        let network = factory.new_client(0, &node);
        assert_eq!(network.address, "http://127.0.0.1:9100");
    }

    #[test]
    fn raft_network_factory_different_nodes() {
        let factory = GrpcRaftNetworkFactory::new();

        let node_a = RaftNode {
            address: "10.0.0.1".to_string(),
            port: 7000,
        };
        let node_b = RaftNode {
            address: "10.0.0.2".to_string(),
            port: 8000,
        };

        let net_a = factory.new_client(1, &node_a);
        let net_b = factory.new_client(2, &node_b);

        assert_eq!(net_a.address, "http://10.0.0.1:7000");
        assert_eq!(net_b.address, "http://10.0.0.2:8000");
    }

    #[test]
    fn raft_network_factory_default() {
        let factory = GrpcRaftNetworkFactory::default();
        let node = RaftNode {
            address: "localhost".to_string(),
            port: 5000,
        };
        let network = factory.new_client(0, &node);
        assert_eq!(network.address, "http://localhost:5000");
    }

    // -- Serialization roundtrip test --

    #[tokio::test]
    async fn raft_request_serialization_roundtrip() {
        let vote = openraft::Vote::<u64>::new(1, 0);
        let serialized = serde_json::to_vec(&vote).unwrap();
        let deserialized: openraft::Vote<u64> = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(vote, deserialized);
    }

    // -- RaftServiceImpl tests --

    /// A test handler that echoes back the payload with metadata prepended.
    struct EchoHandler;

    #[async_trait::async_trait]
    impl RaftMessageHandler for EchoHandler {
        async fn handle_forward(
            &self,
            shard_id: u32,
            rpc_type: String,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            let response = format!("shard={},type={},len={}", shard_id, rpc_type, payload.len());
            Ok(response.into_bytes())
        }

        async fn handle_snapshot(
            &self,
            shard_id: u32,
            payload: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            let response = format!("snapshot:shard={},len={}", shard_id, payload.len());
            Ok(response.into_bytes())
        }
    }

    /// A test handler that always returns an error.
    struct ErrorHandler;

    #[async_trait::async_trait]
    impl RaftMessageHandler for ErrorHandler {
        async fn handle_forward(
            &self,
            _shard_id: u32,
            _rpc_type: String,
            _payload: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            Err("forward failed".to_string())
        }

        async fn handle_snapshot(
            &self,
            _shard_id: u32,
            _payload: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            Err("snapshot failed".to_string())
        }
    }

    #[tokio::test]
    async fn forward_routes_to_handler() {
        let service = RaftServiceImpl::new(Arc::new(EchoHandler));
        let request = Request::new(RaftRequest {
            shard_id: 42,
            rpc_type: "vote".to_string(),
            payload: vec![1, 2, 3],
        });

        let response = RaftServiceTrait::forward(&service, request).await.unwrap();
        let body = String::from_utf8(response.into_inner().payload).unwrap();
        assert_eq!(body, "shard=42,type=vote,len=3");
    }

    #[tokio::test]
    async fn forward_returns_error_on_handler_failure() {
        let service = RaftServiceImpl::new(Arc::new(ErrorHandler));
        let request = Request::new(RaftRequest {
            shard_id: 1,
            rpc_type: "append".to_string(),
            payload: vec![],
        });

        let err = RaftServiceTrait::forward(&service, request)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("forward failed"));
    }

    #[tokio::test]
    async fn snapshot_handler_assembles_payload() {
        let handler = EchoHandler;
        let payload = [vec![10u8, 20], vec![30, 40, 50]].concat();
        let result = handler.handle_snapshot(7, payload).await.unwrap();
        let body = String::from_utf8(result).unwrap();
        assert_eq!(body, "snapshot:shard=7,len=5");
    }

    #[tokio::test]
    async fn snapshot_handler_returns_error() {
        let handler = ErrorHandler;
        let err = handler.handle_snapshot(1, vec![1]).await.unwrap_err();
        assert!(err.contains("snapshot failed"));
    }
}
