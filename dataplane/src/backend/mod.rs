//! Unified storage backend interface.
//!
//! All three data backends (raw disk, LVM, chunk) implement the same
//! `StorageBackend` trait so the Go management plane can use any backend
//! through a single JSON-RPC interface.

pub mod chunk;
pub mod chunk_store;
pub mod file_store;
pub mod lvm;
pub mod raw_disk;
pub mod traits;
