//! Raw Disk backend — direct block I/O on SPDK bdevs.
//!
//! The simplest backend: a volume is a 1:1 mapping to an SPDK bdev (AIO or
//! NVMe). Reads and writes go directly to the device at the requested offset.
//!
//! Production volumes use io_uring (AIO) bdevs backed by real block devices or
//! files on the host filesystem. Snapshots use full block copies.

use crate::config::LocalBdevConfig;
use crate::error::{DataPlaneError, Result};
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::reactor_dispatch;
use log::info;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::traits::*;

/// Directory where raw disk volume files are stored.
/// Each volume gets a sparse file at `<base_path>/<volume_name>.raw`.
const RAW_VOLUME_BASE_PATH: &str = "/var/lib/novastor/raw";

/// Global bdev manager for creating AIO bdevs.
static RAW_BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

fn bdev_manager() -> &'static BdevManager {
    RAW_BDEV_MANAGER.get_or_init(BdevManager::new)
}

/// Tracks a raw disk volume's metadata.
struct RawVolume {
    name: String,
    /// The underlying SPDK bdev name.
    bdev_name: String,
    /// Path to the backing file on the host filesystem.
    device_path: String,
    size_bytes: u64,
    block_size: u32,
    num_blocks: u64,
    is_snapshot: bool,
    parent_snapshot: Option<String>,
}

/// Tracks a snapshot.
struct RawSnapshot {
    name: String,
    source_volume: String,
    bdev_name: String,
    device_path: String,
    size_bytes: u64,
    created_at: u64,
}

pub struct RawDiskBackend {
    volumes: Mutex<HashMap<String, RawVolume>>,
    snapshots: Mutex<HashMap<String, RawSnapshot>>,
}

impl RawDiskBackend {
    pub fn new() -> Self {
        Self {
            volumes: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    /// Full block copy from one bdev to another (for snapshots).
    fn full_copy(src_bdev: &str, dst_bdev: &str, size_bytes: u64) -> Result<()> {
        // Copy in 1MB chunks to avoid huge DMA allocations.
        const COPY_BLOCK: u64 = 1024 * 1024;
        let mut offset = 0u64;
        while offset < size_bytes {
            let len = std::cmp::min(COPY_BLOCK, size_bytes - offset);
            let data = reactor_dispatch::bdev_read(src_bdev, offset, len)?;
            reactor_dispatch::bdev_write(dst_bdev, offset, &data)?;
            offset += len;
        }
        Ok(())
    }

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn volume_info(&self, vol: &RawVolume) -> VolumeInfo {
        VolumeInfo {
            name: vol.name.clone(),
            backend: BackendType::RawDisk,
            size_bytes: vol.size_bytes,
            used_bytes: vol.size_bytes,
            block_size: vol.block_size,
            healthy: true,
            is_snapshot: vol.is_snapshot,
            parent_snapshot: vol.parent_snapshot.clone(),
            thin_provisioned: false,
        }
    }
}

impl StorageBackend for RawDiskBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::RawDisk
    }

    fn create_volume(&self, name: &str, size_bytes: u64, _thin: bool) -> Result<VolumeInfo> {
        info!("raw_disk: creating volume '{}' ({}B)", name, size_bytes);

        let bdev_name = format!("raw_{}", name);
        let block_size: u32 = 512;
        let num_blocks = size_bytes / block_size as u64;
        let device_path = format!("{}/{}.raw", RAW_VOLUME_BASE_PATH, name);

        // Ensure the base directory exists.
        std::fs::create_dir_all(RAW_VOLUME_BASE_PATH).map_err(|e| {
            DataPlaneError::BdevError(format!(
                "failed to create raw volume directory {}: {}",
                RAW_VOLUME_BASE_PATH, e
            ))
        })?;

        // Create a sparse file of the requested size for the volume backing.
        {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&device_path)
                .map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to create backing file {}: {}",
                        device_path, e
                    ))
                })?;
            f.set_len(size_bytes).map_err(|e| {
                DataPlaneError::BdevError(format!(
                    "failed to set size on backing file {}: {}",
                    device_path, e
                ))
            })?;
        }

        // Create an io_uring bdev backed by the real file on disk.
        let config = LocalBdevConfig {
            name: bdev_name.clone(),
            device_path: device_path.clone(),
            block_size,
        };
        bdev_manager().create_aio_bdev(&config)?;

        let vol = RawVolume {
            name: name.to_string(),
            bdev_name: bdev_name.clone(),
            device_path: device_path.clone(),
            size_bytes,
            block_size,
            num_blocks,
            is_snapshot: false,
            parent_snapshot: None,
        };
        self.volumes.lock().unwrap().insert(name.to_string(), vol);

        Ok(VolumeInfo {
            name: name.to_string(),
            backend: BackendType::RawDisk,
            size_bytes,
            used_bytes: size_bytes,
            block_size,
            healthy: true,
            is_snapshot: false,
            parent_snapshot: None,
            thin_provisioned: false,
        })
    }

    fn delete_volume(&self, name: &str) -> Result<()> {
        info!("raw_disk: deleting volume '{}'", name);
        let vol = self
            .volumes
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        // Unregister the SPDK bdev.
        bdev_manager().delete_bdev(&vol.bdev_name)?;

        // Remove the backing file.
        if !vol.device_path.is_empty() {
            if let Err(e) = std::fs::remove_file(&vol.device_path) {
                info!(
                    "raw_disk: failed to remove backing file {} (may already be gone): {}",
                    vol.device_path, e
                );
            }
        }
        Ok(())
    }

    fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<VolumeInfo> {
        // Raw disk resize: grow the backing file, recreate the bdev.
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        if new_size_bytes < vol.size_bytes {
            return Err(DataPlaneError::BdevError(
                "raw_disk: shrink not supported".into(),
            ));
        }
        if new_size_bytes == vol.size_bytes {
            return Ok(self.volume_info(vol));
        }

        let old_bdev = vol.bdev_name.clone();
        let device_path = vol.device_path.clone();
        let block_size = vol.block_size;
        let new_blocks = new_size_bytes / block_size as u64;
        drop(volumes);

        // Delete old SPDK bdev.
        let _ = bdev_manager().delete_bdev(&old_bdev);

        // Grow the backing file (data is preserved, new space is zeroed).
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&device_path)
                .map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to open backing file for resize {}: {}",
                        device_path, e
                    ))
                })?;
            f.set_len(new_size_bytes).map_err(|e| {
                DataPlaneError::BdevError(format!(
                    "failed to resize backing file {}: {}",
                    device_path, e
                ))
            })?;
        }

        // Recreate AIO bdev with new size.
        let config = LocalBdevConfig {
            name: old_bdev.clone(),
            device_path: device_path.clone(),
            block_size,
        };
        bdev_manager().create_aio_bdev(&config)?;

        // Update volume record.
        let mut volumes = self.volumes.lock().unwrap();
        if let Some(vol) = volumes.get_mut(name) {
            vol.size_bytes = new_size_bytes;
            vol.num_blocks = new_blocks;
            return Ok(self.volume_info(vol));
        }
        Err(DataPlaneError::BdevError(
            "volume disappeared during resize".into(),
        ))
    }

    fn stat_volume(&self, name: &str) -> Result<VolumeInfo> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        Ok(self.volume_info(vol))
    }

    fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let volumes = self.volumes.lock().unwrap();
        Ok(volumes.values().map(|v| self.volume_info(v)).collect())
    }

    fn read(&self, name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        if offset + length > vol.size_bytes {
            return Err(DataPlaneError::BdevError(format!(
                "read past end: offset={} length={} size={}",
                offset, length, vol.size_bytes
            )));
        }
        let bdev = vol.bdev_name.clone();
        drop(volumes);
        reactor_dispatch::bdev_read(&bdev, offset, length)
    }

    fn write(&self, name: &str, offset: u64, data: &[u8]) -> Result<()> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;
        if vol.is_snapshot {
            return Err(DataPlaneError::BdevError("cannot write to snapshot".into()));
        }
        if offset + data.len() as u64 > vol.size_bytes {
            return Err(DataPlaneError::BdevError(format!(
                "write past end: offset={} length={} size={}",
                offset,
                data.len(),
                vol.size_bytes
            )));
        }
        let bdev = vol.bdev_name.clone();
        drop(volumes);
        reactor_dispatch::bdev_write(&bdev, offset, data)
    }

    fn create_snapshot(&self, volume_name: &str, snapshot_name: &str) -> Result<SnapshotInfo> {
        info!(
            "raw_disk: snapshot '{}' of '{}'",
            snapshot_name, volume_name
        );
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes.get(volume_name).ok_or_else(|| {
            DataPlaneError::BdevError(format!("volume '{}' not found", volume_name))
        })?;
        let src_bdev = vol.bdev_name.clone();
        let size = vol.size_bytes;
        let block_size = vol.block_size;
        drop(volumes);

        // Create a backing file for the snapshot.
        let snap_bdev = format!("rawsnap_{}", snapshot_name);
        let snap_path = format!("{}/snap_{}.raw", RAW_VOLUME_BASE_PATH, snapshot_name);

        std::fs::create_dir_all(RAW_VOLUME_BASE_PATH).map_err(|e| {
            DataPlaneError::BdevError(format!("failed to create snapshot directory: {}", e))
        })?;
        {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&snap_path)
                .map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to create snapshot backing file {}: {}",
                        snap_path, e
                    ))
                })?;
            f.set_len(size).map_err(|e| {
                DataPlaneError::BdevError(format!("failed to set snapshot file size: {}", e))
            })?;
        }

        let config = LocalBdevConfig {
            name: snap_bdev.clone(),
            device_path: snap_path.clone(),
            block_size,
        };
        bdev_manager().create_aio_bdev(&config)?;

        // Full copy from source to snapshot.
        Self::full_copy(&src_bdev, &snap_bdev, size)?;

        let created_at = Self::now_epoch();
        self.snapshots.lock().unwrap().insert(
            snapshot_name.to_string(),
            RawSnapshot {
                name: snapshot_name.to_string(),
                source_volume: volume_name.to_string(),
                bdev_name: snap_bdev,
                device_path: snap_path,
                size_bytes: size,
                created_at,
            },
        );

        Ok(SnapshotInfo {
            name: snapshot_name.to_string(),
            source_volume: volume_name.to_string(),
            size_bytes: size,
            used_bytes: size,
            created_at,
        })
    }

    fn delete_snapshot(&self, snapshot_name: &str) -> Result<()> {
        info!("raw_disk: deleting snapshot '{}'", snapshot_name);

        // Check no clones reference this snapshot.
        let volumes = self.volumes.lock().unwrap();
        for vol in volumes.values() {
            if vol.parent_snapshot.as_deref() == Some(snapshot_name) {
                return Err(DataPlaneError::BdevError(format!(
                    "snapshot '{}' has active clone '{}'",
                    snapshot_name, vol.name
                )));
            }
        }
        drop(volumes);

        let snap = self
            .snapshots
            .lock()
            .unwrap()
            .remove(snapshot_name)
            .ok_or_else(|| {
                DataPlaneError::BdevError(format!("snapshot '{}' not found", snapshot_name))
            })?;

        let _ = bdev_manager().delete_bdev(&snap.bdev_name);
        if !snap.device_path.is_empty() {
            let _ = std::fs::remove_file(&snap.device_path);
        }
        Ok(())
    }

    fn list_snapshots(&self, volume_name: &str) -> Result<Vec<SnapshotInfo>> {
        let snapshots = self.snapshots.lock().unwrap();
        Ok(snapshots
            .values()
            .filter(|s| s.source_volume == volume_name)
            .map(|s| SnapshotInfo {
                name: s.name.clone(),
                source_volume: s.source_volume.clone(),
                size_bytes: s.size_bytes,
                used_bytes: s.size_bytes,
                created_at: s.created_at,
            })
            .collect())
    }

    fn clone(&self, snapshot_name: &str, clone_name: &str) -> Result<VolumeInfo> {
        info!(
            "raw_disk: clone '{}' from snapshot '{}'",
            clone_name, snapshot_name
        );
        let snapshots = self.snapshots.lock().unwrap();
        let snap = snapshots.get(snapshot_name).ok_or_else(|| {
            DataPlaneError::BdevError(format!("snapshot '{}' not found", snapshot_name))
        })?;
        let src_bdev = snap.bdev_name.clone();
        let size = snap.size_bytes;
        drop(snapshots);

        // Create a backing file and AIO bdev for the clone.
        let clone_bdev = format!("raw_{}", clone_name);
        let clone_path = format!("{}/{}.raw", RAW_VOLUME_BASE_PATH, clone_name);
        let block_size: u32 = 512;

        {
            let f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&clone_path)
                .map_err(|e| {
                    DataPlaneError::BdevError(format!(
                        "failed to create clone backing file {}: {}",
                        clone_path, e
                    ))
                })?;
            f.set_len(size).map_err(|e| {
                DataPlaneError::BdevError(format!("failed to set clone file size: {}", e))
            })?;
        }

        let config = LocalBdevConfig {
            name: clone_bdev.clone(),
            device_path: clone_path.clone(),
            block_size,
        };
        bdev_manager().create_aio_bdev(&config)?;

        Self::full_copy(&src_bdev, &clone_bdev, size)?;

        let vol = RawVolume {
            name: clone_name.to_string(),
            bdev_name: clone_bdev,
            device_path: clone_path,
            size_bytes: size,
            block_size,
            num_blocks: size / block_size as u64,
            is_snapshot: false,
            parent_snapshot: Some(snapshot_name.to_string()),
        };
        let info = self.volume_info(&vol);
        self.volumes
            .lock()
            .unwrap()
            .insert(clone_name.to_string(), vol);
        Ok(info)
    }
}
