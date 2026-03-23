//! Userspace NVMe-oF TCP target library.
//!
//! Multi-subsystem NVMe-oF TCP target — one listener, many volumes.
//! Each volume is identified by its NQN during the Fabric Connect command.

pub mod error;
pub mod pdu;
pub mod target;

pub use error::{NvmeOfError, Result};
pub use target::{NvmeOfBackend, NvmeOfTarget, SubsystemConfig};
