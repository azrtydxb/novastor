//! LVM backend — SPDK blobstore/lvol logical volume management.
//!
//! Volumes are SPDK logical volumes (lvols) backed by a blobstore (lvol store)
//! on a physical bdev. Snapshots and clones are native SPDK lvol operations
//! (copy-on-write, instant).

use crate::error::{DataPlaneError, Result};
use crate::spdk::context::Completion;
use crate::spdk::reactor_dispatch;
use log::info;
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::traits::*;

#[allow(
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    dead_code,
    improper_ctypes
)]
mod ffi {
    include!(concat!(env!("OUT_DIR"), "/spdk_bindings.rs"));
}

/// Internal volume tracking.
struct LvmVolume {
    name: String,
    lvol_name: String,
    lvol_store: String,
    size_bytes: u64,
    block_size: u32,
    thin: bool,
    is_snapshot: bool,
    parent_snapshot: Option<String>,
}

/// Internal snapshot tracking.
struct LvmSnapshot {
    name: String,
    source_volume: String,
    lvol_name: String,
    size_bytes: u64,
    created_at: u64,
}

pub struct LvmBackend {
    /// The lvol store name this backend operates on.
    lvol_store: String,
    volumes: Mutex<HashMap<String, LvmVolume>>,
    snapshots: Mutex<HashMap<String, LvmSnapshot>>,
}

impl LvmBackend {
    /// Create a new LVM backend operating on the given lvol store.
    ///
    /// The lvol store must already exist (created via `bdev_manager::create_lvol_store`).
    pub fn new(lvol_store: &str) -> Self {
        info!("lvm backend initialised on lvol store '{}'", lvol_store);
        Self {
            lvol_store: lvol_store.to_string(),
            volumes: Mutex::new(HashMap::new()),
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    fn now_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Full bdev name for an lvol: "lvstore/volname".
    fn lvol_bdev_name(&self, vol_name: &str) -> String {
        format!("{}/{}", self.lvol_store, vol_name)
    }
}

impl StorageBackend for LvmBackend {
    fn backend_type(&self) -> BackendType {
        BackendType::Lvm
    }

    fn create_volume(&self, name: &str, size_bytes: u64, thin: bool) -> Result<VolumeInfo> {
        info!(
            "lvm: creating volume '{}' ({}B, thin={})",
            name, size_bytes, thin
        );

        let lvs_ptr = reactor_dispatch::get_lvol_store_ptr(&self.lvol_store).ok_or_else(|| {
            DataPlaneError::LvolError(format!(
                "lvol store '{}' pointer not found in registry",
                self.lvol_store
            ))
        })?;

        let lvol_name = name.to_string();
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let lvol_name_c = std::ffi::CString::new(lvol_name.as_str()).unwrap();
            unsafe {
                ffi::vbdev_lvol_create(
                    lvs_ptr.as_ptr() as *mut ffi::spdk_lvol_store,
                    lvol_name_c.as_ptr() as *const c_char,
                    size_bytes,
                    thin,
                    ffi::lvol_clear_method_LVOL_CLEAR_WITH_UNMAP,
                    Some(lvol_op_cb),
                    comp.as_ptr(),
                );
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::LvolError(format!(
                "vbdev_lvol_create failed: rc={rc}"
            )));
        }

        // Find the actual bdev name SPDK registered for this lvol.
        // vbdev_lvol registers the bdev with a UUID-based primary name
        // and an alias "lvs_name/lvol_name".
        let vol_name_str = name.to_string();
        let actual_bdev_name: String = reactor_dispatch::dispatch_sync(move || {
            unsafe {
                let mut bdev = ffi::spdk_bdev_first();
                while !bdev.is_null() {
                    let name_ptr = ffi::spdk_bdev_get_name(bdev);
                    if !name_ptr.is_null() {
                        let bname = std::ffi::CStr::from_ptr(name_ptr).to_string_lossy();
                        if bname.ends_with(&format!("/{}", vol_name_str)) {
                            return bname.into_owned();
                        }
                    }
                    bdev = ffi::spdk_bdev_next(bdev);
                }
            }
            String::new()
        });

        let bdev_name = if actual_bdev_name.is_empty() {
            self.lvol_bdev_name(name)
        } else {
            actual_bdev_name
        };
        info!("lvm: lvol bdev name = '{}'", bdev_name);
        let block_size = 512u32;

        let vol = LvmVolume {
            name: name.to_string(),
            lvol_name: bdev_name.clone(),
            lvol_store: self.lvol_store.clone(),
            size_bytes,
            block_size,
            thin,
            is_snapshot: false,
            parent_snapshot: None,
        };

        let info = VolumeInfo {
            name: name.to_string(),
            backend: BackendType::Lvm,
            size_bytes,
            used_bytes: if thin { 0 } else { size_bytes },
            block_size,
            healthy: true,
            is_snapshot: false,
            parent_snapshot: None,
            thin_provisioned: thin,
        };

        self.volumes.lock().unwrap().insert(name.to_string(), vol);
        Ok(info)
    }

    fn delete_volume(&self, name: &str) -> Result<()> {
        info!("lvm: deleting volume '{}'", name);
        let vol = self
            .volumes
            .lock()
            .unwrap()
            .remove(name)
            .ok_or_else(|| DataPlaneError::LvolError(format!("volume '{}' not found", name)))?;

        let lvol_name = vol.lvol_name.clone();
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let name_c = std::ffi::CString::new(lvol_name.as_str()).unwrap();
            unsafe {
                let bdev = ffi::spdk_bdev_get_by_name(name_c.as_ptr() as *const c_char);
                if !bdev.is_null() {
                    ffi::spdk_bdev_unregister(bdev, Some(bdev_unregister_cb), comp.as_ptr());
                } else {
                    comp.complete(0); // already gone
                }
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::LvolError(format!(
                "bdev_unregister failed: rc={rc}"
            )));
        }
        Ok(())
    }

    fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<VolumeInfo> {
        info!("lvm: resizing volume '{}' to {}B", name, new_size_bytes);
        let mut volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get_mut(name)
            .ok_or_else(|| DataPlaneError::LvolError(format!("volume '{}' not found", name)))?;

        let lvol_name = vol.lvol_name.clone();
        drop(volumes);

        // Verify the bdev exists on the reactor thread.
        let exists: bool = reactor_dispatch::dispatch_sync({
            let lvol_name = lvol_name.clone();
            move || {
                let name_c = std::ffi::CString::new(lvol_name.as_str()).unwrap();
                unsafe { !ffi::spdk_bdev_get_by_name(name_c.as_ptr() as *const c_char).is_null() }
            }
        });

        if !exists {
            return Err(DataPlaneError::LvolError(format!(
                "lvol bdev '{}' not found",
                lvol_name
            )));
        }

        // TODO(gap-18): vbdev_lvol_resize requires a *mut spdk_lvol pointer.
        // SPDK's spdk_bdev_get_module_ctx takes a bdev_desc (opened bdev),
        // not a raw bdev pointer. To implement this properly:
        // 1. Change lvol_op_cb to a new lvol_op_with_handle_cb that stores the
        //    spdk_lvol pointer in a global registry keyed by bdev name
        // 2. Look up the lvol pointer at resize time from the registry
        // 3. Call vbdev_lvol_resize with the lvol pointer
        //
        // For now, return an error. This only affects LVM backend volume
        // expansion — raw/file backends and chunk-level operations work fine.
        Err(DataPlaneError::LvolError(
            "lvol resize requires spdk_lvol pointer caching (gap-18: needs lvol_op_with_handle_cb)"
                .into(),
        ))
    }

    fn stat_volume(&self, name: &str) -> Result<VolumeInfo> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::LvolError(format!("volume '{}' not found", name)))?;
        Ok(VolumeInfo {
            name: vol.name.clone(),
            backend: BackendType::Lvm,
            size_bytes: vol.size_bytes,
            used_bytes: if vol.thin { 0 } else { vol.size_bytes },
            block_size: vol.block_size,
            healthy: true,
            is_snapshot: vol.is_snapshot,
            parent_snapshot: vol.parent_snapshot.clone(),
            thin_provisioned: vol.thin,
        })
    }

    fn list_volumes(&self) -> Result<Vec<VolumeInfo>> {
        let volumes = self.volumes.lock().unwrap();
        Ok(volumes
            .values()
            .map(|v| VolumeInfo {
                name: v.name.clone(),
                backend: BackendType::Lvm,
                size_bytes: v.size_bytes,
                used_bytes: if v.thin { 0 } else { v.size_bytes },
                block_size: v.block_size,
                healthy: true,
                is_snapshot: v.is_snapshot,
                parent_snapshot: v.parent_snapshot.clone(),
                thin_provisioned: v.thin,
            })
            .collect())
    }

    fn read(&self, name: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::LvolError(format!("volume '{}' not found", name)))?;
        if offset + length > vol.size_bytes {
            return Err(DataPlaneError::LvolError(format!(
                "read past end: offset={} length={} size={}",
                offset, length, vol.size_bytes
            )));
        }
        let bdev = vol.lvol_name.clone();
        drop(volumes);
        reactor_dispatch::bdev_read(&bdev, offset, length)
    }

    fn write(&self, name: &str, offset: u64, data: &[u8]) -> Result<()> {
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes
            .get(name)
            .ok_or_else(|| DataPlaneError::LvolError(format!("volume '{}' not found", name)))?;
        if vol.is_snapshot {
            return Err(DataPlaneError::LvolError("cannot write to snapshot".into()));
        }
        if offset + data.len() as u64 > vol.size_bytes {
            return Err(DataPlaneError::LvolError(format!(
                "write past end: offset={} length={} size={}",
                offset,
                data.len(),
                vol.size_bytes
            )));
        }
        let bdev = vol.lvol_name.clone();
        drop(volumes);
        reactor_dispatch::bdev_write(&bdev, offset, data)
    }

    fn create_snapshot(&self, volume_name: &str, snapshot_name: &str) -> Result<SnapshotInfo> {
        info!("lvm: snapshot '{}' of '{}'", snapshot_name, volume_name);
        let volumes = self.volumes.lock().unwrap();
        let vol = volumes.get(volume_name).ok_or_else(|| {
            DataPlaneError::LvolError(format!("volume '{}' not found", volume_name))
        })?;
        let size = vol.size_bytes;
        let lvol_bdev = vol.lvol_name.clone();
        drop(volumes);

        let snap_name = snapshot_name.to_string();
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let snap_name_c = std::ffi::CString::new(snap_name.as_str()).unwrap();
            let lvol_bdev_c = std::ffi::CString::new(lvol_bdev.as_str()).unwrap();
            unsafe {
                let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
                let rc = ffi::spdk_bdev_open_ext(
                    lvol_bdev_c.as_ptr() as *const c_char,
                    true, // write access needed for snapshot
                    Some(bdev_event_cb),
                    std::ptr::null_mut(),
                    &mut desc,
                );
                if rc != 0 {
                    log::warn!("snapshot: open_ext('{}') failed: rc={}", lvol_bdev, rc);
                    comp.complete(rc);
                    return;
                }
                // Get the lvol* from the bdev's ctxt field (set by vbdev_lvol).
                let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
                if bdev.is_null() {
                    ffi::spdk_bdev_close(desc);
                    comp.complete(-1);
                    return;
                }
                let lvol = (*bdev).ctxt as *mut ffi::spdk_lvol;
                if lvol.is_null() {
                    log::warn!("snapshot: bdev.ctxt is null for '{}'", lvol_bdev);
                    ffi::spdk_bdev_close(desc);
                    comp.complete(-1);
                    return;
                }

                ffi::vbdev_lvol_create_snapshot(
                    lvol,
                    snap_name_c.as_ptr() as *const c_char,
                    Some(lvol_op_with_handle_cb),
                    comp.as_ptr(),
                );
                ffi::spdk_bdev_close(desc);
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::LvolError(format!(
                "spdk_lvol_create_snapshot failed: rc={rc}"
            )));
        }

        let created_at = Self::now_epoch();
        let snap_bdev = self.lvol_bdev_name(snapshot_name);

        // Track the snapshot as a read-only volume.
        self.volumes.lock().unwrap().insert(
            snapshot_name.to_string(),
            LvmVolume {
                name: snapshot_name.to_string(),
                lvol_name: snap_bdev,
                lvol_store: self.lvol_store.clone(),
                size_bytes: size,
                block_size: 512,
                thin: true,
                is_snapshot: true,
                parent_snapshot: None,
            },
        );

        self.snapshots.lock().unwrap().insert(
            snapshot_name.to_string(),
            LvmSnapshot {
                name: snapshot_name.to_string(),
                source_volume: volume_name.to_string(),
                lvol_name: snapshot_name.to_string(),
                size_bytes: size,
                created_at,
            },
        );

        Ok(SnapshotInfo {
            name: snapshot_name.to_string(),
            source_volume: volume_name.to_string(),
            size_bytes: size,
            used_bytes: 0, // CoW — no extra space until original is modified
            created_at,
        })
    }

    fn delete_snapshot(&self, snapshot_name: &str) -> Result<()> {
        info!("lvm: deleting snapshot '{}'", snapshot_name);

        // Check no clones reference this snapshot.
        let volumes = self.volumes.lock().unwrap();
        for vol in volumes.values() {
            if vol.parent_snapshot.as_deref() == Some(snapshot_name) {
                return Err(DataPlaneError::LvolError(format!(
                    "snapshot '{}' has active clone '{}'",
                    snapshot_name, vol.name
                )));
            }
        }
        drop(volumes);

        self.snapshots.lock().unwrap().remove(snapshot_name);
        // Remove from volume tracking and delete the lvol.
        if let Some(vol) = self.volumes.lock().unwrap().remove(snapshot_name) {
            let lvol_name = vol.lvol_name.clone();
            let completion = Arc::new(Completion::<i32>::new());
            let comp = completion.clone();

            reactor_dispatch::send_to_reactor(move || {
                let name_c = std::ffi::CString::new(lvol_name.as_str()).unwrap();
                unsafe {
                    let bdev = ffi::spdk_bdev_get_by_name(name_c.as_ptr() as *const c_char);
                    if !bdev.is_null() {
                        ffi::spdk_bdev_unregister(bdev, Some(bdev_unregister_cb), comp.as_ptr());
                    } else {
                        comp.complete(0);
                    }
                }
            });

            let _ = completion.wait();
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
                used_bytes: 0,
                created_at: s.created_at,
            })
            .collect())
    }

    fn clone(&self, snapshot_name: &str, clone_name: &str) -> Result<VolumeInfo> {
        info!(
            "lvm: clone '{}' from snapshot '{}'",
            clone_name, snapshot_name
        );

        let snapshots = self.snapshots.lock().unwrap();
        let snap = snapshots.get(snapshot_name).ok_or_else(|| {
            DataPlaneError::LvolError(format!("snapshot '{}' not found", snapshot_name))
        })?;
        let size = snap.size_bytes;
        let snap_lvol_bdev = self.lvol_bdev_name(&snap.lvol_name);
        drop(snapshots);

        let clone_name_str = clone_name.to_string();
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let clone_name_c = std::ffi::CString::new(clone_name_str.as_str()).unwrap();
            let snap_bdev_c = std::ffi::CString::new(snap_lvol_bdev.as_str()).unwrap();
            unsafe {
                let mut desc: *mut ffi::spdk_bdev_desc = std::ptr::null_mut();
                let rc = ffi::spdk_bdev_open_ext(
                    snap_bdev_c.as_ptr() as *const c_char,
                    false,
                    Some(bdev_event_cb),
                    std::ptr::null_mut(),
                    &mut desc,
                );
                if rc != 0 {
                    comp.complete(rc);
                    return;
                }
                let bdev = ffi::spdk_bdev_desc_get_bdev(desc);
                if bdev.is_null() {
                    ffi::spdk_bdev_close(desc);
                    comp.complete(-1);
                    return;
                }
                let lvol = (*bdev).ctxt as *mut ffi::spdk_lvol;
                if lvol.is_null() {
                    ffi::spdk_bdev_close(desc);
                    comp.complete(-1);
                    return;
                }

                ffi::vbdev_lvol_create_clone(
                    lvol,
                    clone_name_c.as_ptr() as *const c_char,
                    Some(lvol_op_with_handle_cb),
                    comp.as_ptr(),
                );
                ffi::spdk_bdev_close(desc);
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::LvolError(format!(
                "spdk_lvol_create_clone failed: rc={rc}"
            )));
        }

        let clone_bdev = self.lvol_bdev_name(clone_name);
        let vol = LvmVolume {
            name: clone_name.to_string(),
            lvol_name: clone_bdev,
            lvol_store: self.lvol_store.clone(),
            size_bytes: size,
            block_size: 512,
            thin: true,
            is_snapshot: false,
            parent_snapshot: Some(snapshot_name.to_string()),
        };

        let info = VolumeInfo {
            name: clone_name.to_string(),
            backend: BackendType::Lvm,
            size_bytes: size,
            used_bytes: 0,
            block_size: 512,
            healthy: true,
            is_snapshot: false,
            parent_snapshot: Some(snapshot_name.to_string()),
            thin_provisioned: true,
        };

        self.volumes
            .lock()
            .unwrap()
            .insert(clone_name.to_string(), vol);
        Ok(info)
    }
}

// ---------------------------------------------------------------------------
// SPDK FFI callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn bdev_event_cb(
    _type_: ffi::spdk_bdev_event_type,
    _bdev: *mut ffi::spdk_bdev,
    _event_ctx: *mut std::os::raw::c_void,
) {
    // No-op — required non-null in SPDK v24.09.
}

unsafe extern "C" fn bdev_unregister_cb(ctx: *mut std::os::raw::c_void, rc: i32) {
    let completion = Completion::<i32>::from_ptr(ctx);
    completion.complete(rc);
}

unsafe extern "C" fn lvol_op_cb(
    ctx: *mut std::os::raw::c_void,
    _lvol: *mut ffi::spdk_lvol,
    rc: i32,
) {
    let completion = Completion::<i32>::from_ptr(ctx);
    completion.complete(rc);
}

unsafe extern "C" fn lvol_op_with_handle_cb(
    ctx: *mut std::os::raw::c_void,
    _lvol: *mut ffi::spdk_lvol,
    rc: i32,
) {
    let completion = Completion::<i32>::from_ptr(ctx);
    completion.complete(rc);
}
