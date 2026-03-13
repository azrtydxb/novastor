//! NovaStor SPDK Data Plane
//!
//! High-performance storage data plane built on SPDK. Provides NVMe-oF/TCP
//! targets with custom bdev modules for replication and erasure coding.
//! Controlled by the Go agent via JSON-RPC over a Unix domain socket.

pub mod backend;
#[cfg(feature = "spdk-sys")]
pub mod bdev;
pub mod chunk;
pub mod config;
pub mod error;
#[cfg(feature = "spdk-sys")]
pub mod jsonrpc;
pub mod metadata;
pub mod policy;
#[cfg(feature = "spdk-sys")]
pub mod spdk;
pub mod transport;
