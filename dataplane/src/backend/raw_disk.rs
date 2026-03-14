//! Raw Disk backend — direct block I/O on NVMe bdevs.
//!
//! The Raw backend attaches unbound NVMe devices directly as SPDK NVMe bdevs.
//! No filesystem is created on the device. Volumes are 1:1 mappings to the
//! underlying NVMe namespace.
//!
//! This is the highest-performance backend: zero overhead from filesystems or
//! logical volume management. Suitable for dedicated NVMe devices that should
//! be used entirely by NovaStor.
//!
//! Snapshots use full block copies between bdevs.

use crate::config::{LocalBdevConfig, NvmeBdevConfig};
use crate::error::{DataPlaneError, Result};
use crate::spdk::bdev_manager::BdevManager;
use crate::spdk::reactor_dispatch;
use log::info;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::traits::*;

/// Global bdev manager for creating/managing NVMe bdevs.
static RAW_BDEV_MANAGER: OnceLock<BdevManager> = OnceLock::new();

fn bdev_manager() -> &'static BdevManager {
    RAW_BDEV_MANAGER.get_or_init(BdevManager::new)
}

/// Tracks a raw NVMe volume's metadata.
struct RawVolume {
    name: String,
    /// The underlying SPDK bdev name (e.g., "nvme0n1").
    bdev_name: String,
    /// PCIe BDF address of the NVMe device.
    pcie_addr: String,
    size_bytes: u64,
    block_size: u32,
    num_blocks: u64,
    is_snapshot: bool,
    parent_snapshot: Option<String>,
}

/// Tracks a snapshot (backed by an AIO bdev for the full copy).
struct RawSnapshot {
    name: String,
    source_volume: String,
    bdev_name: String,
    /// Snapshot backing file path (full copies stored as files).
    backing_file: String,
    size_bytes: u64,
    created_at: u64,
}

/// Directory where snapshot backing files are stored.
const SNAPSHOT_BASE_PATH: &str = "/var/lib/novastor/raw-snapshots";

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

        // The volume name must include a PCIe address in the format
        // "<name>@<pcie_addr>" (e.g., "vol1@0000:00:04.0"). The Go agent
        // passes this after discovering available NVMe devices.
        let (vol_name, pcie_addr) = if let Some(idx) = name.find('@') {
            (&name[..idx], &name[idx + 1..])
        } else {
            return Err(DataPlaneError::BdevError(format!(
                "raw_disk: volume name must be '<name>@<pcie_addr>', got '{}'",
                name
            )));
        };

        let controller_name = format!("raw_{}", vol_name);

        // Attach the NVMe device via SPDK.
        let config = NvmeBdevConfig {
            name: controller_name.clone(),
            pcie_addr: pcie_addr.to_string(),
        };
        let bdev_info = bdev_manager().attach_nvme_bdev(&config)?;

        let actual_size = bdev_info.num_blocks * bdev_info.block_size as u64;
        if actual_size < size_bytes {
            // Device is smaller than requested — unregister and fail.
            let _ = bdev_manager().delete_bdev(&bdev_info.name);
            return Err(DataPlaneError::BdevError(format!(
                "raw_disk: device at {} is {}B, need {}B",
                pcie_addr, actual_size, size_bytes
            )));
        }

        let vol = RawVolume {
            name: vol_name.to_string(),
            bdev_name: bdev_info.name.clone(),
            pcie_addr: pcie_addr.to_string(),
            size_bytes: actual_size,
            block_size: bdev_info.block_size,
            num_blocks: bdev_info.num_blocks,
            is_snapshot: false,
            parent_snapshot: None,
        };
        let info = self.volume_info(&vol);
        self.volumes
            .lock()
            .unwrap()
            .insert(vol_name.to_string(), vol);

        Ok(info)
    }

    fn delete_volume(&self, name: &str) -> Result<()> {
        info!("raw_disk: deleting volume '{}'", name);
        let vol = self
            .volumes
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        // Unregister the SPDK NVMe bdev.
        bdev_manager().delete_bdev(&vol.bdev_name)?;
        Ok(())
    }

    fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<VolumeInfo> {
        // Raw NVMe devices cannot be resized — they have a fixed physical size.
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::BdevError(format!("volume '{}' not found", name)))?;

        if new_size_bytes != vol.size_bytes {
            return Err(DataPlaneError::BdevError(
                "raw_disk: NVMe devices cannot be resized".into(),
            ));
        }
        Ok(self.volume_info(vol))
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
        drop(volumes);

        // Snapshots of raw NVMe devices use full block copies to an AIO bdev
        // backed by a file, since NVMe devices don't support COW snapshots.
        let snap_bdev = format!("rawsnap_{}", snapshot_name);
        let snap_path = format!("{}/snap_{}.raw", SNAPSHOT_BASE_PATH, snapshot_name);
        let block_size: u32 = 4096;

        std::fs::create_dir_all(SNAPSHOT_BASE_PATH).map_err(|e| {
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

        // Full copy from source NVMe bdev to snapshot AIO bdev.
        Self::full_copy(&src_bdev, &snap_bdev, size)?;

        let created_at = Self::now_epoch();
        self.snapshots.lock().unwrap().insert(
            snapshot_name.to_string(),
            RawSnapshot {
                name: snapshot_name.to_string(),
                source_volume: volume_name.to_string(),
                bdev_name: snap_bdev,
                backing_file: snap_path,
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
        if !snap.backing_file.is_empty() {
            let _ = std::fs::remove_file(&snap.backing_file);
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

        // Clone creates an AIO bdev backed by a file (same as snapshot).
        let clone_bdev = format!("raw_{}", clone_name);
        let clone_path = format!("{}/{}.raw", SNAPSHOT_BASE_PATH, clone_name);
        let block_size: u32 = 4096;

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
            pcie_addr: String::new(),
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
