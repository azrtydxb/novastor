//! Unified storage backend interface.
//!
//! Three backend types (Raw, LVM, File) implement the `StorageBackend` trait.
//! All produce SPDK bdevs. The ChunkEngine sits above backends and stores
//! content-addressed 4MB chunks on them via the async `ChunkStore` trait.

#[cfg(feature = "spdk-sys")]
pub mod bdev_store;
pub mod chunk_store;
#[cfg(feature = "spdk-sys")]
pub mod file_store;
#[cfg(feature = "spdk-sys")]
pub mod lvm;
#[cfg(feature = "spdk-sys")]
pub mod raw_disk;
pub mod traits;
