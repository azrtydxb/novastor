//! Unified storage backend interface.
//!
//! All three data backends (raw disk, LVM, chunk) implement the same
//! `StorageBackend` trait so the Go management plane can use any backend
//! through the gRPC DataplaneService.

#[cfg(feature = "spdk-sys")]
pub mod bdev_store;
#[cfg(feature = "spdk-sys")]
pub mod chunk;
pub mod chunk_store;
#[cfg(feature = "spdk-sys")]
pub mod file_store;
#[cfg(feature = "spdk-sys")]
pub mod lvm;
#[cfg(feature = "spdk-sys")]
pub mod raw_disk;
pub mod traits;
