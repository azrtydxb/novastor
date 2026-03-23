//! Error types for the NVMe-oF TCP target.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NvmeOfError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, NvmeOfError>;
