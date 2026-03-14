//! Error types for the NovaStor data plane.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataPlaneError {
    #[error("SPDK initialization failed: {0}")]
    SpdkInit(String),
    #[error("bdev error: {0}")]
    BdevError(String),
    #[error("NVMe-oF target error: {0}")]
    NvmfTargetError(String),
    #[error("NVMe-oF initiator error: {0}")]
    NvmfInitiatorError(String),
    #[error("blobstore error: {0}")]
    BlobstoreError(String),
    #[error("lvol error: {0}")]
    LvolError(String),
    #[error("replica bdev error: {0}")]
    ReplicaError(String),
    #[error("erasure coding error: {0}")]
    ErasureError(String),
    #[error("metadata error: {0}")]
    MetadataError(String),
    #[error("raft error: {0}")]
    RaftError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("transport error: {0}")]
    TransportError(String),
    #[error("chunk engine error: {0}")]
    ChunkEngineError(String),
    #[error("policy error: {0}")]
    PolicyError(String),
}

impl From<tonic::Status> for DataPlaneError {
    fn from(status: tonic::Status) -> Self {
        DataPlaneError::TransportError(format!("gRPC error: {}", status.message()))
    }
}

impl From<DataPlaneError> for tonic::Status {
    fn from(err: DataPlaneError) -> Self {
        match &err {
            DataPlaneError::TransportError(msg) => tonic::Status::internal(msg.clone()),
            _ => tonic::Status::internal(err.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, DataPlaneError>;
