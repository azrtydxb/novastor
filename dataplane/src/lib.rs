//! NovaStor SPDK Data Plane
//!
//! High-performance storage data plane built on SPDK. Provides NVMe-oF/TCP
//! targets with custom bdev modules for replication and erasure coding.
//! Controlled by the Go agent via gRPC (invariant #5: gRPC is the only
//! communication protocol).

pub mod backend;
#[cfg(feature = "spdk-sys")]
pub mod bdev;
pub mod chunk;
pub mod config;
pub mod error;
pub mod metadata;
pub mod policy;
#[cfg(feature = "spdk-sys")]
pub mod spdk;
#[cfg(not(feature = "spdk-sys"))]
#[path = "bdev/sub_block.rs"]
pub mod sub_block;
pub mod tracing_init;
pub mod transport;

/// Global tokio runtime handle — set by the binary entry point before SPDK init.
static TOKIO_HANDLE: std::sync::OnceLock<tokio::runtime::Handle> = std::sync::OnceLock::new();

/// Register the tokio runtime handle (called from main.rs).
pub fn set_tokio_handle(handle: tokio::runtime::Handle) {
    TOKIO_HANDLE.set(handle).expect("tokio handle already set");
}

/// Get the global tokio runtime handle.
pub fn tokio_handle() -> &'static tokio::runtime::Handle {
    TOKIO_HANDLE.get().expect("tokio runtime not initialized")
}
