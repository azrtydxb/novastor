//! SPDK bdev management — AIO, malloc, blobstore lvols.

use crate::config::{BlobstoreConfig, LocalBdevConfig, LvolConfig};
use crate::error::{DataPlaneError, Result};
use crate::spdk::reactor_dispatch;
use log::info;
use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};

use super::context::Completion;

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

#[derive(Debug, Clone)]
pub struct BdevInfo {
    pub name: String,
    pub device_path: String,
    pub block_size: u32,
    pub num_blocks: u64,
    pub bdev_type: String,
}

#[derive(Debug, Clone)]
pub struct LvolStoreInfo {
    pub name: String,
    pub base_bdev: String,
    pub cluster_size: u32,
    pub total_clusters: u64,
    pub free_clusters: u64,
}

pub struct BdevManager {
    bdevs: Mutex<HashMap<String, BdevInfo>>,
    lvol_stores: Mutex<HashMap<String, LvolStoreInfo>>,
}

impl BdevManager {
    pub fn new() -> Self {
        Self {
            bdevs: Mutex::new(HashMap::new()),
            lvol_stores: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_aio_bdev(&self, config: &LocalBdevConfig) -> Result<BdevInfo> {
        info!(
            "creating uring bdev (io_uring): name={}, device={}",
            config.name, config.device_path
        );

        let name = config.name.clone();
        let device_path = config.device_path.clone();
        let block_size = config.block_size;

        reactor_dispatch::dispatch_sync(move || -> Result<()> {
            let bdev_name = std::ffi::CString::new(name.as_str())
                .map_err(|e| DataPlaneError::BdevError(format!("invalid bdev name: {e}")))?;
            let device_path_c = std::ffi::CString::new(device_path.as_str())
                .map_err(|e| DataPlaneError::BdevError(format!("invalid device path: {e}")))?;

            unsafe {
                let bdev = ffi::novastor_create_uring_bdev(
                    bdev_name.as_ptr() as *const c_char,
                    device_path_c.as_ptr() as *const c_char,
                    block_size,
                );
                if bdev.is_null() {
                    return Err(DataPlaneError::BdevError(
                        "create_uring_bdev returned null".into(),
                    ));
                }
            }
            Ok(())
        })?;

        let (num_blocks, actual_bs) = self.query_bdev_size(&config.name)?;

        let info = BdevInfo {
            name: config.name.clone(),
            device_path: config.device_path.clone(),
            block_size: actual_bs,
            num_blocks,
            bdev_type: "uring".to_string(),
        };
        self.bdevs
            .lock()
            .unwrap()
            .insert(config.name.clone(), info.clone());
        Ok(info)
    }

    pub fn create_malloc_bdev(
        &self,
        name: &str,
        size_mb: u64,
        block_size: u32,
    ) -> Result<BdevInfo> {
        info!("creating malloc bdev: name={}, size={}MB", name, size_mb);

        let name_str = name.to_string();
        let total_bytes = size_mb * 1024 * 1024;
        let num_blocks = total_bytes / block_size as u64;

        reactor_dispatch::dispatch_sync(move || -> Result<()> {
            let bdev_name = std::ffi::CString::new(name_str.as_str())
                .map_err(|e| DataPlaneError::BdevError(format!("invalid bdev name: {e}")))?;

            unsafe {
                let mut opts: ffi::malloc_bdev_opts = std::mem::zeroed();
                opts.name = bdev_name.as_ptr() as *mut c_char;
                opts.num_blocks = num_blocks;
                opts.block_size = block_size;

                let mut bdev_ptr: *mut ffi::spdk_bdev = std::ptr::null_mut();
                let rc = ffi::create_malloc_disk(&mut bdev_ptr, &opts);
                if rc != 0 {
                    return Err(DataPlaneError::BdevError(format!(
                        "create_malloc_disk failed: rc={rc}"
                    )));
                }
            }
            Ok(())
        })?;

        let (num_blocks, actual_bs) = self.query_bdev_size(name)?;
        let info = BdevInfo {
            name: name.to_string(),
            device_path: String::new(),
            block_size: actual_bs,
            num_blocks,
            bdev_type: "malloc".to_string(),
        };
        self.bdevs
            .lock()
            .unwrap()
            .insert(name.to_string(), info.clone());
        Ok(info)
    }

    pub fn create_lvol_store(&self, config: &BlobstoreConfig) -> Result<LvolStoreInfo> {
        info!("creating lvol store on bdev: {}", config.base_bdev);

        let base_bdev = config.base_bdev.clone();
        let lvs_name_str = format!("lvs_{}", config.base_bdev);
        let lvs_name_for_closure = lvs_name_str.clone();
        let cluster_size = config.cluster_size;
        let completion = Arc::new(Completion::<LvsInitResult>::new());
        let comp = completion.clone();

        // Use vbdev_lvs_create which registers the lvs-bdev pair needed
        // by vbdev_lvol_create later.
        reactor_dispatch::send_to_reactor(move || {
            let base_bdev_c = std::ffi::CString::new(base_bdev.as_str()).unwrap();
            let lvs_name_c = std::ffi::CString::new(lvs_name_for_closure.as_str()).unwrap();
            unsafe {
                ffi::vbdev_lvs_create(
                    base_bdev_c.as_ptr() as *const c_char,
                    lvs_name_c.as_ptr() as *const c_char,
                    cluster_size,
                    ffi::lvs_clear_method_LVS_CLEAR_WITH_UNMAP,
                    0, // num_md_pages_per_cluster_ratio — 0 = default
                    Some(lvs_init_cb),
                    comp.as_ptr(),
                );
            }
        });

        let result = completion.wait();
        if result.rc != 0 {
            return Err(DataPlaneError::BlobstoreError(format!(
                "vbdev_lvs_create failed: rc={}",
                result.rc
            )));
        }

        let store_name = lvs_name_str;

        // Register the lvs pointer so create_lvol can find it.
        reactor_dispatch::register_lvol_store(&store_name, result.lvs_ptr.as_ptr());

        let info = LvolStoreInfo {
            name: store_name.clone(),
            base_bdev: config.base_bdev.clone(),
            cluster_size: config.cluster_size,
            total_clusters: result.total_clusters,
            free_clusters: result.free_clusters,
        };
        self.lvol_stores
            .lock()
            .unwrap()
            .insert(store_name, info.clone());
        Ok(info)
    }

    pub fn create_lvol(&self, config: &LvolConfig) -> Result<BdevInfo> {
        info!(
            "creating lvol: volume_id={}, size={}B, store={}",
            config.volume_id, config.size_bytes, config.lvol_store
        );
        let bdev_name = format!("{}/{}", config.lvol_store, config.volume_id);

        // Verify the lvol store exists.
        let stores = self.lvol_stores.lock().unwrap();
        if !stores.contains_key(&config.lvol_store) {
            return Err(DataPlaneError::LvolError(format!(
                "lvol store '{}' not found",
                config.lvol_store
            )));
        }
        drop(stores);

        let lvs_ptr =
            reactor_dispatch::get_lvol_store_ptr(&config.lvol_store).ok_or_else(|| {
                DataPlaneError::LvolError(format!(
                    "lvol store '{}' pointer not found in registry",
                    config.lvol_store
                ))
            })?;

        let volume_id = config.volume_id.clone();
        let size_bytes = config.size_bytes;
        let thin_provision = config.thin_provision;
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let lvol_name = std::ffi::CString::new(volume_id.as_str()).unwrap();
            unsafe {
                ffi::vbdev_lvol_create(
                    lvs_ptr.as_ptr() as *mut ffi::spdk_lvol_store,
                    lvol_name.as_ptr() as *const c_char,
                    size_bytes,
                    thin_provision,
                    ffi::lvol_clear_method_LVOL_CLEAR_WITH_UNMAP,
                    Some(lvol_create_cb),
                    comp.as_ptr(),
                );
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::LvolError(format!(
                "spdk_lvol_create failed: rc={rc}"
            )));
        }

        let info = BdevInfo {
            name: bdev_name.clone(),
            device_path: String::new(),
            block_size: 512,
            num_blocks: config.size_bytes / 512,
            bdev_type: "lvol".to_string(),
        };
        self.bdevs.lock().unwrap().insert(bdev_name, info.clone());

        // Update free clusters in the lvol store.
        if let Some(store) = self.lvol_stores.lock().unwrap().get_mut(&config.lvol_store) {
            let clusters_used = config.size_bytes / store.cluster_size as u64;
            store.free_clusters = store.free_clusters.saturating_sub(clusters_used);
        }

        Ok(info)
    }

    pub fn delete_bdev(&self, name: &str) -> Result<()> {
        info!("deleting bdev: {}", name);

        let name_str = name.to_string();
        let completion = Arc::new(Completion::<i32>::new());
        let comp = completion.clone();

        reactor_dispatch::send_to_reactor(move || {
            let bdev_name = std::ffi::CString::new(name_str.as_str()).unwrap();
            unsafe {
                let bdev = ffi::spdk_bdev_get_by_name(bdev_name.as_ptr() as *const c_char);
                if bdev.is_null() {
                    // Already gone — treat as success.
                    comp.complete(0);
                    return;
                }
                ffi::spdk_bdev_unregister(bdev, Some(bdev_unregister_cb), comp.as_ptr());
            }
        });

        let rc = completion.wait();
        if rc != 0 {
            return Err(DataPlaneError::BdevError(format!(
                "spdk_bdev_unregister failed: rc={rc}"
            )));
        }

        self.bdevs.lock().unwrap().remove(name);
        Ok(())
    }

    pub fn list_bdevs(&self) -> Vec<BdevInfo> {
        self.bdevs.lock().unwrap().values().cloned().collect()
    }

    pub fn get_bdev(&self, name: &str) -> Option<BdevInfo> {
        self.bdevs.lock().unwrap().get(name).cloned()
    }

    pub fn list_lvol_stores(&self) -> Vec<LvolStoreInfo> {
        self.lvol_stores.lock().unwrap().values().cloned().collect()
    }

    pub fn get_lvol_store(&self, name: &str) -> Option<LvolStoreInfo> {
        self.lvol_stores.lock().unwrap().get(name).cloned()
    }

    /// Query a bdev's block count and block size from SPDK.
    fn query_bdev_size(&self, name: &str) -> Result<(u64, u32)> {
        let name = name.to_string();
        reactor_dispatch::dispatch_sync(move || {
            let bdev_name = std::ffi::CString::new(name.as_str()).unwrap();
            unsafe {
                let bdev = ffi::spdk_bdev_get_by_name(bdev_name.as_ptr() as *const c_char);
                if bdev.is_null() {
                    Err(DataPlaneError::BdevError(format!(
                        "bdev '{}' not found after creation",
                        name
                    )))
                } else {
                    let num_blocks = ffi::spdk_bdev_get_num_blocks(bdev);
                    let block_size = ffi::spdk_bdev_get_block_size(bdev);
                    Ok((num_blocks, block_size))
                }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// SPDK FFI callbacks
// ---------------------------------------------------------------------------

struct LvsInitResult {
    rc: i32,
    uuid: Option<String>,
    total_clusters: u64,
    free_clusters: u64,
    lvs_ptr: reactor_dispatch::SendPtr,
}

/// Callback for spdk_lvs_init.
unsafe extern "C" fn lvs_init_cb(
    ctx: *mut std::os::raw::c_void,
    lvs: *mut ffi::spdk_lvol_store,
    rc: i32,
) {
    let result = if rc != 0 || lvs.is_null() {
        LvsInitResult {
            rc: if rc != 0 { rc } else { -1 },
            uuid: None,
            total_clusters: 0,
            free_clusters: 0,
            lvs_ptr: reactor_dispatch::SendPtr::new(std::ptr::null_mut()),
        }
    } else {
        LvsInitResult {
            rc: 0,
            uuid: Some(format!("lvs_{:p}", lvs)),
            total_clusters: 0,
            free_clusters: 0,
            lvs_ptr: reactor_dispatch::SendPtr::new(lvs as *mut std::os::raw::c_void),
        }
    };

    let completion = Completion::<LvsInitResult>::from_ptr(ctx);
    completion.complete(result);
}

/// Callback for spdk_lvol_create.
unsafe extern "C" fn lvol_create_cb(
    ctx: *mut std::os::raw::c_void,
    _lvol: *mut ffi::spdk_lvol,
    rc: i32,
) {
    let completion = Completion::<i32>::from_ptr(ctx);
    completion.complete(rc);
}

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
