//! Unified storage backend trait.
//!
//! Every backend (Raw, LVM, File) implements this trait identically.
//! All three produce SPDK bdevs. The ChunkEngine sits above backends
//! and stores content-addressed 4MB chunks on them.

use crate::error::Result;
use serde::{Deserialize, Serialize};

/// Volume metadata returned by stat and list operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeInfo {
    /// Unique volume name within this backend.
    pub name: String,
    /// Which backend owns this volume.
    pub backend: BackendType,
    /// Total provisioned size in bytes.
    pub size_bytes: u64,
    /// Bytes actually consumed on disk (may differ with thin provisioning).
    pub used_bytes: u64,
    /// Block size used for I/O alignment.
    pub block_size: u32,
    /// Whether the volume is healthy and available for I/O.
    pub healthy: bool,
    /// Whether this volume is a snapshot (read-only).
    pub is_snapshot: bool,
    /// Parent snapshot name, if this volume was cloned.
    pub parent_snapshot: Option<String>,
    /// Whether thin provisioning is in effect.
    pub thin_provisioned: bool,
}

/// Snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot name.
    pub name: String,
    /// Source volume this snapshot was taken from.
    pub source_volume: String,
    /// Size in bytes (same as source at snapshot time).
    pub size_bytes: u64,
    /// Bytes consumed by snapshot data.
    pub used_bytes: u64,
    /// Timestamp (seconds since epoch) when the snapshot was created.
    pub created_at: u64,
}

/// Backend type discriminator.
///
/// There are exactly three backend types. The ChunkEngine is NOT a backend —
/// it is the layer above backends that stores content-addressed 4MB chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendType {
    /// Raw block device (io_uring bdev on NVMe/block device).
    RawDisk,
    /// SPDK lvol store on a block device.
    Lvm,
    /// File on a mounted filesystem (SPDK AIO bdev).
    File,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendType::RawDisk => write!(f, "raw"),
            BackendType::Lvm => write!(f, "lvm"),
            BackendType::File => write!(f, "file"),
        }
    }
}

/// Unified storage backend interface.
///
/// All operations are synchronous from the caller's perspective. Internally
/// they may use SPDK async callbacks with the `Completion<T>` pattern to
/// block until the reactor thread completes the operation.
pub trait StorageBackend: Send + Sync {
    /// Which backend type this is.
    fn backend_type(&self) -> BackendType;

    // ----- Volume lifecycle -----

    /// Create a new volume.
    ///
    /// `thin` controls whether the volume is thin-provisioned (allocate on
    /// first write) or thick-provisioned (allocate all space up front).
    fn create_volume(&self, name: &str, size_bytes: u64, thin: bool) -> Result<VolumeInfo>;

    /// Delete a volume and free all associated storage.
    fn delete_volume(&self, name: &str) -> Result<()>;

    /// Resize a volume. `new_size_bytes` must be >= current size for grow,
    /// or <= current size for shrink. Not all backends support shrink.
    fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<VolumeInfo>;

    /// Get metadata for a single volume.
    fn stat_volume(&self, name: &str) -> Result<VolumeInfo>;

    /// List all volumes managed by this backend.
    fn list_volumes(&self) -> Result<Vec<VolumeInfo>>;

    // ----- Data I/O -----

    /// Read `length` bytes starting at `offset` from the named volume.
    fn read(&self, name: &str, offset: u64, length: u64) -> Result<Vec<u8>>;

    /// Write `data` to the named volume starting at `offset`.
    fn write(&self, name: &str, offset: u64, data: &[u8]) -> Result<()>;

    // ----- Snapshots & clones -----

    /// Create a point-in-time read-only snapshot of a volume.
    fn create_snapshot(&self, volume_name: &str, snapshot_name: &str) -> Result<SnapshotInfo>;

    /// Delete a snapshot. Fails if clones still reference it.
    fn delete_snapshot(&self, snapshot_name: &str) -> Result<()>;

    /// List all snapshots for a volume.
    fn list_snapshots(&self, volume_name: &str) -> Result<Vec<SnapshotInfo>>;

    /// Create a writable clone from a snapshot.
    fn clone(&self, snapshot_name: &str, clone_name: &str) -> Result<VolumeInfo>;
}
