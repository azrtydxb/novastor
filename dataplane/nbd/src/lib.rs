//! Lightweight NBD (Network Block Device) server for Linux.
//!
//! Implements the NBD protocol (newstyle fixed) over Unix domain sockets
//! or TCP. The server provides a block device backend trait that the
//! caller implements to handle read/write/trim/flush operations.
//!
//! Usage:
//! 1. Implement [`BlockDevice`] trait for your backend
//! 2. Call [`serve`] with a listener and your backend
//! 3. Connect the kernel NBD client: `nbd-client -u /path/to/socket /dev/nbdX`

pub mod protocol;
pub mod server;

pub use protocol::{NbdCommand, NbdRequest};
pub use server::{BlockDevice, NbdServer};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum NbdError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, NbdError>;
