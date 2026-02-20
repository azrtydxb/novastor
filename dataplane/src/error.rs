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
    #[error("JSON-RPC error: {0}")]
    JsonRpcError(String),
    #[error("replica bdev error: {0}")]
    ReplicaError(String),
    #[error("erasure coding error: {0}")]
    ErasureError(String),
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, DataPlaneError>;
