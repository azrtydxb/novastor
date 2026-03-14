# Chunk Engine Redesign — Plan 3: gRPC Transport + Chunk Engine

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add gRPC inter-node transport (tonic) for remote chunk I/O and Raft message forwarding, then build the chunk engine that uses CRUSH placement to dispatch chunk operations to the correct node (local or remote).

**Architecture:** Two new subsystems: (1) A tonic gRPC server exposes `ChunkService` (remote chunk put/get/delete) and `RaftService` (openraft append-entries/vote forwarding). (2) The chunk engine translates volume-level I/O into content-addressed chunk operations, uses CRUSH to determine placement, and dispatches locally or via gRPC to remote nodes. The existing `ChunkStore` trait implementations (`FileChunkStore`, `BdevChunkStore`) handle actual I/O. The metadata store (Plan 2) provides volume chunk maps and cluster topology.

**Tech Stack:** Rust, tonic, prost, tonic-build, openraft, tokio

**Spec:** `docs/specs/2026-03-13-chunk-engine-redesign.md` (sections 2.3, 3, and 5)

**Plan series:** This is Plan 3 of 4. Plan 1 (Backend Engine) and Plan 2 (Metadata Store) are complete. Plan 4 will add the policy engine + cleanup.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `proto/chunk_service.proto` | Rust-specific proto for chunk I/O between dataplane nodes |
| `proto/raft_service.proto` | Proto for openraft message transport between shard replicas |
| `src/transport/mod.rs` | Module declarations for transport layer |
| `src/transport/chunk_service.rs` | tonic `ChunkService` server — wraps `dyn ChunkStore` |
| `src/transport/chunk_client.rs` | tonic `ChunkService` client — for remote chunk dispatch |
| `src/transport/raft_service.rs` | tonic `RaftService` server + openraft `RaftNetworkFactory` |
| `src/transport/server.rs` | gRPC server startup, binds all services |
| `src/chunk/mod.rs` | Module declarations for chunk engine |
| `src/chunk/engine.rs` | Chunk engine — volume I/O → CRUSH placement → local/remote dispatch |

### Modified files

| File | Change |
|------|--------|
| `Cargo.toml` | Add tonic, prost, tonic-build, prost-types dependencies |
| `build.rs` | Add tonic-build proto compilation step |
| `src/main.rs` | Add `mod transport; mod chunk;`, start gRPC server on tokio |
| `src/error.rs` | Add `TransportError` and `ChunkEngineError` variants |

---

## Chunk 1: gRPC Infrastructure + ChunkService

### Task 1: Add tonic/prost dependencies and proto build infrastructure

**Files:**
- Modify: `Cargo.toml`
- Modify: `build.rs`
- Create: `proto/chunk_service.proto`
- Create: `proto/raft_service.proto`

This task adds the gRPC toolchain to the Rust dataplane. Proto definitions are Rust-specific (separate from the Go protos in `api/proto/`). The Go management plane uses its own proto files; the Rust dataplane nodes communicate with each other via these protos.

- [ ] **Step 1: Add dependencies to Cargo.toml**

Add to `[dependencies]`:
```toml
tonic = "0.13"
prost = "0.13"
prost-types = "0.13"
tokio-stream = "0.1"
```

Add to `[build-dependencies]`:
```toml
tonic-build = "0.13"
```

- [ ] **Step 2: Create chunk_service.proto**

Create `proto/chunk_service.proto`:

```protobuf
syntax = "proto3";
package novastor.chunk;

// Inter-node chunk I/O service.
// Used by the chunk engine to dispatch chunk operations to remote nodes
// when CRUSH places a chunk on a different node.
service ChunkService {
  // Store a chunk on this node's local backend.
  // Streaming request: chunk_id + metadata in first message, data in subsequent messages.
  rpc PutChunk(stream PutChunkRequest) returns (PutChunkResponse);

  // Retrieve a chunk from this node's local backend.
  // Streaming response: chunk_id + metadata in first message, data in subsequent messages.
  rpc GetChunk(GetChunkRequest) returns (stream GetChunkResponse);

  // Delete a chunk from this node's local backend.
  rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);

  // Check if a chunk exists on this node.
  rpc HasChunk(HasChunkRequest) returns (HasChunkResponse);
}

message PutChunkRequest {
  // First message: chunk_id (required). Subsequent messages: data fragments.
  string chunk_id = 1;
  bytes data = 2;
}

message PutChunkResponse {
  string chunk_id = 1;
  uint64 bytes_written = 2;
}

message GetChunkRequest {
  string chunk_id = 1;
}

message GetChunkResponse {
  string chunk_id = 1;
  bytes data = 2;
}

message DeleteChunkRequest {
  string chunk_id = 1;
}

message DeleteChunkResponse {}

message HasChunkRequest {
  string chunk_id = 1;
}

message HasChunkResponse {
  bool exists = 1;
}
```

- [ ] **Step 3: Create raft_service.proto**

Create `proto/raft_service.proto`:

```protobuf
syntax = "proto3";
package novastor.raft;

// Inter-node Raft message transport.
// Each shard is an independent openraft group. This service forwards
// Raft RPCs (append entries, vote, install snapshot) between shard replicas.
service RaftService {
  // Forward an openraft RPC to the target node.
  // The request/response bodies are opaque serialized openraft messages.
  // The shard_id field routes to the correct shard on the target node.
  rpc Forward(RaftRequest) returns (RaftResponse);

  // Install a snapshot on the target node's shard.
  // Streaming request: metadata in first message, snapshot data in subsequent messages.
  rpc InstallSnapshot(stream SnapshotRequest) returns (SnapshotResponse);
}

message RaftRequest {
  // Shard identifier (0-255).
  uint32 shard_id = 1;
  // Serialized openraft RPC (AppendEntries, Vote, etc).
  // The receiver deserializes based on the rpc_type field.
  string rpc_type = 2;
  bytes payload = 3;
}

message RaftResponse {
  bytes payload = 1;
}

message SnapshotRequest {
  uint32 shard_id = 1;
  bytes payload = 2;
}

message SnapshotResponse {
  bytes payload = 1;
}
```

- [ ] **Step 4: Add proto compilation to build.rs**

Add to `build.rs`, **before** the existing SPDK feature gate:

```rust
// Compile gRPC proto files for tonic.
tonic_build::configure()
    .build_server(true)
    .build_client(true)
    .out_dir("src/transport/generated")
    .compile_protos(
        &["proto/chunk_service.proto", "proto/raft_service.proto"],
        &["proto/"],
    )
    .expect("failed to compile proto files");
```

Create the output directory `src/transport/generated/` and add a `.gitkeep` file so git tracks it.

**Note**: tonic-build generates Rust code from .proto files at build time. The generated code is written to `src/transport/generated/` and included via `include!()` or `tonic::include_proto!()` macros.

- [ ] **Step 5: Add error variants**

Add to `src/error.rs`:

```rust
TransportError(String),
ChunkEngineError(String),
```

Also add a `From<tonic::Status>` impl:

```rust
impl From<tonic::Status> for DataPlaneError {
    fn from(status: tonic::Status) -> Self {
        DataPlaneError::TransportError(format!("gRPC error: {}", status.message()))
    }
}
```

And an `Into<tonic::Status>` for DataPlaneError:

```rust
impl From<DataPlaneError> for tonic::Status {
    fn from(err: DataPlaneError) -> Self {
        match &err {
            DataPlaneError::TransportError(msg) => tonic::Status::internal(msg.clone()),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}
```

- [ ] **Step 6: Create transport module skeleton**

Create `src/transport/mod.rs`:

```rust
//! gRPC transport layer for inter-node communication.
//!
//! Provides `ChunkService` for remote chunk I/O and `RaftService`
//! for openraft message forwarding between shard replicas.

pub mod chunk_client;
pub mod chunk_service;
pub mod raft_service;
pub mod server;

// Re-export generated proto types.
pub mod chunk_proto {
    tonic::include_proto!("novastor.chunk");
}

pub mod raft_proto {
    tonic::include_proto!("novastor.raft");
}
```

Create `src/chunk/mod.rs`:

```rust
//! Chunk engine — translates volume I/O into content-addressed chunk operations.

pub mod engine;
```

Add to `src/main.rs`:

```rust
mod transport;
mod chunk;
```

- [ ] **Step 7: Verify proto compilation**

Run: `cargo check` (or `cargo build` locally if on Linux with SPDK)
Expected: Proto files compile, generated code is produced in `src/transport/generated/`.

**Note**: On macOS without SPDK, this requires the `spdk-sys` feature gate from Plan 2. Run `cargo check` (without `--features spdk-sys`) — the proto compilation should work regardless of SPDK availability.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock build.rs proto/ src/transport/mod.rs src/chunk/mod.rs src/main.rs src/error.rs
git commit -m "[Feature] Add tonic/prost gRPC infrastructure and proto definitions"
```

---

### Task 2: ChunkService gRPC server

**Files:**
- Create: `src/transport/chunk_service.rs`

Implements the `ChunkService` gRPC server. This is a thin wrapper around `dyn ChunkStore` — it receives chunks via streaming gRPC and writes them to the local backend, or reads chunks and streams them back.

- [ ] **Step 1: Write tests**

Tests use a `FileChunkStore` in a temp directory as the backing store.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::{ChunkHeader, CHUNK_HEADER_SIZE, CHUNK_MAGIC};
    use crate::backend::file_store::FileChunkStore;
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    fn make_chunk_data(data: &[u8]) -> Vec<u8> {
        let header = ChunkHeader {
            magic: CHUNK_MAGIC,
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(data),
            data_len: data.len() as u32,
            _reserved: [0; 2],
        };
        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + data.len());
        buf.extend_from_slice(unsafe {
            std::slice::from_raw_parts(
                &header as *const ChunkHeader as *const u8,
                CHUNK_HEADER_SIZE,
            )
        });
        buf.extend_from_slice(data);
        buf
    }

    async fn setup() -> (ChunkServiceImpl, String) {
        let dir = tempdir().unwrap();
        // Leak the tempdir so it lives for the test duration
        let dir_path = dir.into_path();
        let store = FileChunkStore::new(dir_path.to_str().unwrap()).unwrap();
        let svc = ChunkServiceImpl::new(Arc::new(store));
        (svc, dir_path.to_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn put_and_get_chunk() {
        let (svc, _dir) = setup().await;
        let chunk_id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let data = make_chunk_data(b"hello world");

        // Put via streaming request
        let requests = vec![
            PutChunkRequest {
                chunk_id: chunk_id.to_string(),
                data: data.clone(),
            },
        ];
        let request = tonic::Request::new(tokio_stream::iter(requests));
        let response = svc.put_chunk(request).await.unwrap();
        assert_eq!(response.into_inner().chunk_id, chunk_id);

        // Get via streaming response
        let request = tonic::Request::new(GetChunkRequest {
            chunk_id: chunk_id.to_string(),
        });
        let response = svc.get_chunk(request).await.unwrap();
        let mut stream = response.into_inner();
        let mut received = Vec::new();
        while let Some(msg) = stream.next().await {
            received.extend_from_slice(&msg.unwrap().data);
        }
        assert_eq!(received, data);
    }

    #[tokio::test]
    async fn has_chunk() {
        let (svc, _dir) = setup().await;
        let chunk_id = "1111111111111111111111111111111111111111111111111111111111111111";

        // Should not exist yet
        let resp = svc
            .has_chunk(tonic::Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(!resp.into_inner().exists);

        // Put it
        let data = make_chunk_data(b"test");
        let requests = vec![PutChunkRequest {
            chunk_id: chunk_id.to_string(),
            data,
        }];
        svc.put_chunk(tonic::Request::new(tokio_stream::iter(requests)))
            .await
            .unwrap();

        // Now it should exist
        let resp = svc
            .has_chunk(tonic::Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(resp.into_inner().exists);
    }

    #[tokio::test]
    async fn delete_chunk() {
        let (svc, _dir) = setup().await;
        let chunk_id = "2222222222222222222222222222222222222222222222222222222222222222";
        let data = make_chunk_data(b"delete me");

        // Put
        let requests = vec![PutChunkRequest {
            chunk_id: chunk_id.to_string(),
            data,
        }];
        svc.put_chunk(tonic::Request::new(tokio_stream::iter(requests)))
            .await
            .unwrap();

        // Delete
        let resp = svc
            .delete_chunk(tonic::Request::new(DeleteChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(resp.into_inner().into() == ());

        // Should no longer exist
        let resp = svc
            .has_chunk(tonic::Request::new(HasChunkRequest {
                chunk_id: chunk_id.to_string(),
            }))
            .await
            .unwrap();
        assert!(!resp.into_inner().exists);
    }
}
```

- [ ] **Step 2: Implement ChunkServiceImpl**

```rust
//! gRPC ChunkService server implementation.
//!
//! Wraps a `dyn ChunkStore` and exposes chunk I/O over gRPC.
//! Used by remote nodes to store/retrieve chunks placed on this node by CRUSH.

use std::sync::Arc;

use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::backend::chunk_store::ChunkStore;
use crate::transport::chunk_proto::chunk_service_server::ChunkService;
use crate::transport::chunk_proto::*;

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

        // Stream the data in fragments (e.g., 1MB each) to avoid
        // holding the entire chunk in a single gRPC message.
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

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
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
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib transport::chunk_service`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/transport/chunk_service.rs
git commit -m "[Feature] Add ChunkService gRPC server wrapping ChunkStore"
```

---

### Task 3: ChunkService gRPC client

**Files:**
- Create: `src/transport/chunk_client.rs`

The client side of `ChunkService` — used by the chunk engine to dispatch chunk operations to remote nodes when CRUSH places a chunk on a different node.

- [ ] **Step 1: Write tests**

Tests start a real tonic server in the background and connect a client to it.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::{ChunkHeader, CHUNK_HEADER_SIZE, CHUNK_MAGIC};
    use crate::backend::file_store::FileChunkStore;
    use crate::transport::chunk_service::ChunkServiceImpl;
    use crate::transport::chunk_proto::chunk_service_server::ChunkServiceServer;
    use tempfile::tempdir;
    use std::net::SocketAddr;

    fn make_chunk_data(data: &[u8]) -> Vec<u8> {
        let header = ChunkHeader {
            magic: CHUNK_MAGIC,
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(data),
            data_len: data.len() as u32,
            _reserved: [0; 2],
        };
        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + data.len());
        buf.extend_from_slice(unsafe {
            std::slice::from_raw_parts(
                &header as *const ChunkHeader as *const u8,
                CHUNK_HEADER_SIZE,
            )
        });
        buf.extend_from_slice(data);
        buf
    }

    async fn start_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let dir = tempdir().unwrap().into_path();
        let store = FileChunkStore::new(dir.to_str().unwrap()).unwrap();
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

        // Give server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr, handle)
    }

    #[tokio::test]
    async fn client_put_and_get() {
        let (addr, _handle) = start_server().await;
        let client = ChunkClient::connect(format!("http://{}", addr)).await.unwrap();

        let chunk_id = "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233";
        let data = make_chunk_data(b"remote chunk data");

        client.put(chunk_id, &data).await.unwrap();
        let got = client.get(chunk_id).await.unwrap();
        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn client_has_and_delete() {
        let (addr, _handle) = start_server().await;
        let client = ChunkClient::connect(format!("http://{}", addr)).await.unwrap();

        let chunk_id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let data = make_chunk_data(b"to be deleted");

        assert!(!client.has(chunk_id).await.unwrap());
        client.put(chunk_id, &data).await.unwrap();
        assert!(client.has(chunk_id).await.unwrap());
        client.delete(chunk_id).await.unwrap();
        assert!(!client.has(chunk_id).await.unwrap());
    }
}
```

- [ ] **Step 2: Implement ChunkClient**

```rust
//! gRPC ChunkService client.
//!
//! Used by the chunk engine to dispatch chunk I/O to remote nodes.
//! Wraps tonic-generated client with a convenient async API matching
//! the `ChunkStore` trait methods.

use std::sync::Arc;

use crate::error::{DataPlaneError, Result};
use crate::transport::chunk_proto::chunk_service_client::ChunkServiceClient;
use crate::transport::chunk_proto::*;

/// Client for a remote node's ChunkService.
pub struct ChunkClient {
    inner: ChunkServiceClient<tonic::transport::Channel>,
}

impl ChunkClient {
    /// Connect to a remote node's ChunkService.
    pub async fn connect(addr: impl Into<String>) -> Result<Self> {
        let inner = ChunkServiceClient::connect(addr.into())
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("connect: {e}")))?;
        Ok(Self { inner })
    }

    /// Store a chunk on the remote node.
    pub async fn put(&self, chunk_id: &str, data: &[u8]) -> Result<()> {
        let mut client = self.inner.clone();
        let requests = vec![PutChunkRequest {
            chunk_id: chunk_id.to_string(),
            data: data.to_vec(),
        }];
        client
            .put_chunk(tokio_stream::iter(requests))
            .await
            .map_err(|e| DataPlaneError::TransportError(format!("put_chunk: {e}")))?;
        Ok(())
    }

    /// Retrieve a chunk from the remote node.
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
        }
        Ok(data)
    }

    /// Delete a chunk from the remote node.
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

    /// Check if a chunk exists on the remote node.
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
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib transport::chunk_client`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/transport/chunk_client.rs
git commit -m "[Feature] Add ChunkService gRPC client for remote chunk dispatch"
```

---

## Chunk 2: RaftService + Server + Chunk Engine

### Task 4: RaftService gRPC server and openraft network factory

**Files:**
- Create: `src/transport/raft_service.rs`

Implements the `RaftService` gRPC server that receives Raft messages from peer nodes and routes them to the correct local shard. Also implements openraft's `RaftNetworkFactory` trait so that openraft can send messages to remote shard replicas via gRPC.

**Note to implementer**: openraft 0.9's `RaftNetworkFactory` trait requires creating `RaftNetwork` instances per target node. The `RaftNetwork` trait has methods like `append_entries`, `vote`, `install_snapshot`. Each method should serialize the openraft message, send it via gRPC `Forward` RPC, and deserialize the response.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_network_factory_creates_network() {
        // Verify the factory can create a network instance for a target node.
        let factory = GrpcRaftNetworkFactory::new();
        let node = crate::metadata::raft_types::RaftNode {
            address: "127.0.0.1".to_string(),
            port: 9100,
        };
        // Factory creation should not fail — connection is lazy.
        let _network = factory.new_client(0, &node);
    }

    #[tokio::test]
    async fn raft_request_serialization_roundtrip() {
        // Verify that openraft messages can be serialized to proto and back.
        let vote = openraft::Vote::<u64>::new(1, 0);
        let serialized = serde_json::to_vec(&vote).unwrap();
        let deserialized: openraft::Vote<u64> = serde_json::from_slice(&serialized).unwrap();
        assert_eq!(vote, deserialized);
    }
}
```

- [ ] **Step 2: Implement RaftService and GrpcRaftNetworkFactory**

The implementation should:

1. **`RaftServiceImpl`**: A tonic service that:
   - Receives `RaftRequest` messages
   - Extracts `shard_id` and `rpc_type`
   - Deserializes the `payload` as the appropriate openraft message type
   - Forwards to the local shard's openraft `Raft` instance
   - Serializes the response and returns it

   For now, the service can hold a reference to a `ShardManager` or a map of `shard_id → Raft<TypeConfig>` instances. Since we don't have running Raft instances yet (they'll be wired up later), the server implementation can accept a callback/handler trait.

2. **`GrpcRaftNetworkFactory`**: Implements openraft's `RaftNetworkFactory<TypeConfig>` trait:
   - `new_client(target, node)` creates a `GrpcRaftNetwork` that connects to the target node's `RaftService`
   - `GrpcRaftNetwork` implements `RaftNetwork<TypeConfig>` with methods:
     - `append_entries`: Serialize → gRPC Forward → deserialize response
     - `vote`: Same pattern
     - `install_snapshot`: Same pattern via streaming RPC

**Note**: The full Raft integration (starting Raft groups, handling leader election) is not needed in this task. We just need the network plumbing so that openraft CAN send messages. The actual Raft group startup will be in a future integration task.

- [ ] **Step 3: Run tests**

Run: `cargo test --lib transport::raft_service`
Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/transport/raft_service.rs
git commit -m "[Feature] Add RaftService gRPC and openraft network factory"
```

---

### Task 5: gRPC server startup

**Files:**
- Create: `src/transport/server.rs`
- Modify: `src/main.rs`

Wires together all gRPC services into a single tonic server and starts it on the tokio runtime. The server binds to a configurable address and port for inter-node communication.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_grpc_config() {
        let config = GrpcServerConfig::default();
        assert_eq!(config.listen_address, "0.0.0.0");
        assert_eq!(config.port, 9500);
    }

    #[tokio::test]
    async fn server_starts_and_stops() {
        let dir = tempfile::tempdir().unwrap().into_path();
        let store = crate::backend::file_store::FileChunkStore::new(
            dir.to_str().unwrap()
        ).unwrap();

        let config = GrpcServerConfig {
            listen_address: "127.0.0.1".to_string(),
            port: 0, // OS assigns port
        };

        let server = GrpcServer::new(config, Arc::new(store));
        let addr = server.start().await.unwrap();
        assert_ne!(addr.port(), 0);

        // Server should be serving — verify by connecting a client
        let client = crate::transport::chunk_client::ChunkClient::connect(
            format!("http://{}", addr)
        ).await.unwrap();

        // HasChunk on nonexistent chunk should return false
        let exists = client.has("0000000000000000000000000000000000000000000000000000000000000000").await.unwrap();
        assert!(!exists);
    }
}
```

- [ ] **Step 2: Implement GrpcServer**

```rust
//! gRPC server — binds ChunkService and RaftService to a TCP listener.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::backend::chunk_store::ChunkStore;
use crate::error::Result;
use crate::transport::chunk_service::ChunkServiceImpl;
use crate::transport::chunk_proto::chunk_service_server::ChunkServiceServer;

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
        Self { config, chunk_store }
    }

    /// Start the gRPC server. Returns the bound address.
    pub async fn start(&self) -> Result<SocketAddr> {
        let addr: SocketAddr = format!("{}:{}", self.config.listen_address, self.config.port)
            .parse()
            .map_err(|e| crate::error::DataPlaneError::TransportError(
                format!("invalid listen address: {e}")
            ))?;

        let chunk_svc = ChunkServiceImpl::new(self.chunk_store.clone());
        let chunk_server = ChunkServiceServer::new(chunk_svc);

        let listener = tokio::net::TcpListener::bind(addr).await
            .map_err(|e| crate::error::DataPlaneError::TransportError(
                format!("bind failed: {e}")
            ))?;
        let bound_addr = listener.local_addr()
            .map_err(|e| crate::error::DataPlaneError::TransportError(
                format!("local_addr: {e}")
            ))?;

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
```

- [ ] **Step 3: Add gRPC config to DataPlaneConfig**

In `src/config.rs`, add:

```rust
/// gRPC inter-node listen port (default: 9500)
pub grpc_port: u16,
```

In `src/main.rs`, add a CLI argument:

```rust
/// gRPC inter-node listen port
#[arg(long, default_value_t = 9500)]
grpc_port: u16,
```

And pass it to the config struct.

- [ ] **Step 4: Run tests**

Run: `cargo test --lib transport::server`
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/transport/server.rs src/config.rs src/main.rs
git commit -m "[Feature] Add gRPC server startup with ChunkService binding"
```

---

### Task 6: Chunk engine

**Files:**
- Create: `src/chunk/engine.rs`

The chunk engine is the core I/O path. It translates volume-level operations into content-addressed chunk operations and dispatches them to the correct node using CRUSH placement.

The engine:
1. Takes a write request (volume_id, offset, data)
2. Splits data into 4MB-aligned chunks
3. Computes SHA-256 chunk IDs (content addressing)
4. Prepends ChunkHeader with CRC-32C checksum
5. Uses CRUSH to determine target node(s) for each chunk
6. If target is local: calls `ChunkStore::put()` directly
7. If target is remote: calls `ChunkClient::put()` via gRPC
8. Updates the volume chunk map in the metadata store

For reads, the reverse: look up chunk map → CRUSH → fetch from local or remote → verify CRC → assemble.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::file_store::FileChunkStore;
    use crate::metadata::topology::{Backend, BackendType, ClusterMap, Node, NodeStatus};

    fn local_topology(node_id: &str) -> ClusterMap {
        let mut map = ClusterMap::new();
        let node = Node {
            id: node_id.to_string(),
            address: "127.0.0.1".to_string(),
            port: 9500,
            status: NodeStatus::Online,
            backends: vec![Backend {
                id: format!("{node_id}-be1"),
                backend_type: BackendType::File,
                capacity_bytes: 100 * 1024 * 1024 * 1024,
                used_bytes: 0,
                weight: 100,
            }],
        };
        map.add_node(node);
        map
    }

    #[tokio::test]
    async fn content_addressing_produces_deterministic_id() {
        let data = b"hello chunk engine";
        let chunk_id = ChunkEngine::compute_chunk_id(data);
        let chunk_id2 = ChunkEngine::compute_chunk_id(data);
        assert_eq!(chunk_id, chunk_id2);
        assert_eq!(chunk_id.len(), 64); // SHA-256 hex
    }

    #[tokio::test]
    async fn prepare_chunk_adds_header() {
        let data = b"raw chunk data";
        let prepared = ChunkEngine::prepare_chunk(data);
        assert!(prepared.len() > data.len());
        // First 4 bytes should be NVAC magic
        assert_eq!(&prepared[..4], b"NVAC");
    }

    #[tokio::test]
    async fn verify_chunk_validates_crc() {
        let data = b"verified data";
        let prepared = ChunkEngine::prepare_chunk(data);
        assert!(ChunkEngine::verify_chunk(&prepared).is_ok());

        // Corrupt the data
        let mut corrupted = prepared.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF;
        assert!(ChunkEngine::verify_chunk(&corrupted).is_err());
    }

    #[tokio::test]
    async fn split_data_into_chunks() {
        // 10MB of data should produce 3 chunks (4MB + 4MB + 2MB)
        let data = vec![0xABu8; 10 * 1024 * 1024];
        let chunks = ChunkEngine::split_into_chunks(&data);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 4 * 1024 * 1024);
        assert_eq!(chunks[1].len(), 4 * 1024 * 1024);
        assert_eq!(chunks[2].len(), 2 * 1024 * 1024);
    }

    #[tokio::test]
    async fn write_and_read_local_volume() {
        let dir = tempfile::tempdir().unwrap().into_path();
        let store = Arc::new(FileChunkStore::new(dir.to_str().unwrap()).unwrap());
        let topology = local_topology("node-1");

        let engine = ChunkEngine::new(
            "node-1".to_string(),
            store,
            topology,
        );

        // Write 8MB to a volume starting at offset 0
        let volume_id = "aabb000000000000";
        let data = vec![0x42u8; 8 * 1024 * 1024];
        let chunk_map = engine.write(volume_id, 0, &data).await.unwrap();
        assert_eq!(chunk_map.len(), 2); // Two 4MB chunks

        // Read it back
        let read_data = engine.read(volume_id, 0, &chunk_map).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn dedup_identical_chunks() {
        let dir = tempfile::tempdir().unwrap().into_path();
        let store = Arc::new(FileChunkStore::new(dir.to_str().unwrap()).unwrap());
        let topology = local_topology("node-1");

        let engine = ChunkEngine::new(
            "node-1".to_string(),
            store,
            topology,
        );

        // Write the same data twice — should produce same chunk IDs
        let data = vec![0x99u8; 4 * 1024 * 1024];
        let map1 = engine.write("vol1-aabb00000000", 0, &data).await.unwrap();
        let map2 = engine.write("vol2-ccdd00000000", 0, &data).await.unwrap();
        assert_eq!(map1[0].chunk_id, map2[0].chunk_id);
    }
}
```

- [ ] **Step 2: Implement ChunkEngine**

```rust
//! Chunk engine — volume I/O → content-addressed chunks → CRUSH dispatch.
//!
//! The engine does not own volume metadata. It returns chunk map entries
//! that the caller persists to the sharded Raft metadata store.

use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::backend::chunk_store::{ChunkHeader, ChunkStore, CHUNK_HEADER_SIZE, CHUNK_MAGIC, CHUNK_SIZE};
use crate::error::{DataPlaneError, Result};
use crate::metadata::crush;
use crate::metadata::topology::ClusterMap;
use crate::metadata::types::ChunkMapEntry;
use crate::transport::chunk_client::ChunkClient;

/// Chunk engine — translates volume I/O into chunk operations.
pub struct ChunkEngine {
    /// This node's ID (for CRUSH local-vs-remote decision).
    node_id: String,
    /// Local chunk store for chunks placed on this node.
    local_store: Arc<dyn ChunkStore>,
    /// Current cluster topology (nodes, backends, weights).
    topology: ClusterMap,
}

impl ChunkEngine {
    pub fn new(
        node_id: String,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
    ) -> Self {
        Self {
            node_id,
            local_store,
            topology,
        }
    }

    /// Compute the content-addressed chunk ID (SHA-256 hex).
    pub fn compute_chunk_id(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    /// Prepend a ChunkHeader to raw data.
    pub fn prepare_chunk(data: &[u8]) -> Vec<u8> {
        let header = ChunkHeader {
            magic: CHUNK_MAGIC,
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(data),
            data_len: data.len() as u32,
            _reserved: [0; 2],
        };
        let mut buf = Vec::with_capacity(CHUNK_HEADER_SIZE + data.len());
        // Safety: ChunkHeader is repr(C, packed), copying raw bytes is valid.
        buf.extend_from_slice(unsafe {
            std::slice::from_raw_parts(
                &header as *const ChunkHeader as *const u8,
                CHUNK_HEADER_SIZE,
            )
        });
        buf.extend_from_slice(data);
        buf
    }

    /// Verify a chunk's CRC-32C checksum.
    pub fn verify_chunk(chunk_with_header: &[u8]) -> Result<()> {
        if chunk_with_header.len() < CHUNK_HEADER_SIZE {
            return Err(DataPlaneError::ChunkEngineError("chunk too small".into()));
        }
        if &chunk_with_header[..4] != CHUNK_MAGIC {
            return Err(DataPlaneError::ChunkEngineError("invalid magic".into()));
        }
        // Copy header fields to avoid unaligned access on packed struct
        let mut checksum_bytes = [0u8; 4];
        checksum_bytes.copy_from_slice(&chunk_with_header[6..10]);
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        let mut data_len_bytes = [0u8; 4];
        data_len_bytes.copy_from_slice(&chunk_with_header[10..14]);
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;

        let data = &chunk_with_header[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + data_len];
        let actual_checksum = crc32c::crc32c(data);

        if stored_checksum != actual_checksum {
            return Err(DataPlaneError::ChunkEngineError(format!(
                "CRC mismatch: stored={stored_checksum:#010x}, actual={actual_checksum:#010x}"
            )));
        }
        Ok(())
    }

    /// Split data into 4MB-aligned chunks.
    pub fn split_into_chunks(data: &[u8]) -> Vec<&[u8]> {
        data.chunks(CHUNK_SIZE).collect()
    }

    /// Write volume data, returning chunk map entries for the caller to persist.
    ///
    /// The caller (e.g., ChunkBackend or the JSON-RPC handler) is responsible
    /// for persisting the returned entries to the sharded Raft metadata store.
    pub async fn write(
        &self,
        volume_id: &str,
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

            // Use CRUSH to determine placement (replication_factor=1 for now).
            let placements = crush::select(&chunk_id, 1, &self.topology);

            if let Some((target_node, _backend)) = placements.first() {
                if target_node == &self.node_id {
                    // Local write
                    self.local_store.put(&chunk_id, &prepared).await?;
                } else {
                    // Remote write via gRPC
                    let node = self.topology.nodes().iter()
                        .find(|n| n.id == *target_node)
                        .ok_or_else(|| DataPlaneError::ChunkEngineError(
                            format!("node not found in topology: {target_node}")
                        ))?;
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

    /// Read volume data using a previously stored chunk map.
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
                    let node = self.topology.nodes().iter()
                        .find(|n| n.id == *target_node)
                        .ok_or_else(|| DataPlaneError::ChunkEngineError(
                            format!("node not found: {target_node}")
                        ))?;
                    let addr = format!("http://{}:{}", node.address, node.port);
                    let client = ChunkClient::connect(addr).await?;
                    client.get(&entry.chunk_id).await?
                }
            } else {
                return Err(DataPlaneError::ChunkEngineError(
                    "CRUSH returned no placement".into(),
                ));
            };

            // Verify integrity
            Self::verify_chunk(&chunk_data)?;

            // Extract raw data (skip header)
            let mut data_len_bytes = [0u8; 4];
            data_len_bytes.copy_from_slice(&chunk_data[10..14]);
            let data_len = u32::from_le_bytes(data_len_bytes) as usize;
            result.extend_from_slice(&chunk_data[CHUNK_HEADER_SIZE..CHUNK_HEADER_SIZE + data_len]);
        }

        Ok(result)
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib chunk::engine`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/chunk/engine.rs
git commit -m "[Feature] Add chunk engine with CRUSH placement and local/remote dispatch"
```

---

### Task 7: Docker verification

- [ ] **Step 1: Run all transport and chunk engine tests locally**

Run: `cargo test --lib transport chunk`
Expected: All tests pass.

- [ ] **Step 2: Run Docker build + test**

Run: `cd dataplane && DOCKER=podman make test`
Expected: All tests pass (101 from Plan 2 + new transport and chunk engine tests).

- [ ] **Step 3: Fix any compilation or test issues**

If Docker build reveals issues (e.g., tonic/prost compilation, proto generation, dependency resolution), fix and re-run.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "[Fix] Address Docker build issues for gRPC transport"
```
