use thiserror::Error;

#[derive(Debug, Error)]
pub enum NdpError {
    #[error("invalid magic: expected 0x4E565354, got 0x{0:08X}")]
    InvalidMagic(u32),
    #[error("header CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    HeaderCrcMismatch { expected: u32, actual: u32 },
    #[error("data CRC mismatch: expected 0x{expected:08X}, got 0x{actual:08X}")]
    DataCrcMismatch { expected: u32, actual: u32 },
    #[error("unknown op: 0x{0:02X}")]
    UnknownOp(u8),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection closed")]
    ConnectionClosed,
    #[error("request timeout")]
    Timeout,
    #[error("pool error: {0}")]
    Pool(String),
}

pub type Result<T> = std::result::Result<T, NdpError>;
