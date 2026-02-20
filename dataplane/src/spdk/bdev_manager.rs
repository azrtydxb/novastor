//! SPDK bdev management — AIO, malloc, blobstore lvols.

use crate::config::{BlobstoreConfig, LocalBdevConfig, LvolConfig};
use crate::error::Result;
use log::info;
use std::collections::HashMap;
use std::sync::Mutex;

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
        info!("creating AIO bdev: name={}, device={}", config.name, config.device_path);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_bdev_aio_create") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            let info = BdevInfo {
                name: config.name.clone(),
                device_path: config.device_path.clone(),
                block_size: config.block_size,
                num_blocks: 0,
                bdev_type: "aio".to_string(),
            };
            self.bdevs.lock().unwrap().insert(config.name.clone(), info.clone());
            Ok(info)
        }
    }

    pub fn create_malloc_bdev(&self, name: &str, size_mb: u64, block_size: u32) -> Result<BdevInfo> {
        info!("creating malloc bdev: name={}, size={}MB", name, size_mb);
        let num_blocks = (size_mb * 1024 * 1024) / block_size as u64;
        let info = BdevInfo {
            name: name.to_string(),
            device_path: String::new(),
            block_size,
            num_blocks,
            bdev_type: "malloc".to_string(),
        };
        self.bdevs.lock().unwrap().insert(name.to_string(), info.clone());
        Ok(info)
    }

    pub fn create_lvol_store(&self, config: &BlobstoreConfig) -> Result<LvolStoreInfo> {
        info!("creating lvol store on bdev: {}", config.base_bdev);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_lvs_init") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            let store_name = format!("lvs_{}", config.base_bdev);
            let info = LvolStoreInfo {
                name: store_name.clone(),
                base_bdev: config.base_bdev.clone(),
                cluster_size: config.cluster_size,
                total_clusters: 0,
                free_clusters: 0,
            };
            self.lvol_stores.lock().unwrap().insert(store_name.clone(), info.clone());
            Ok(info)
        }
    }

    pub fn create_lvol(&self, config: &LvolConfig) -> Result<BdevInfo> {
        info!("creating lvol: volume_id={}, size={}B, store={}", config.volume_id, config.size_bytes, config.lvol_store);
        let bdev_name = format!("{}/{}", config.lvol_store, config.volume_id);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_lvol_create") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            let info = BdevInfo {
                name: bdev_name.clone(),
                device_path: String::new(),
                block_size: 512,
                num_blocks: config.size_bytes / 512,
                bdev_type: "lvol".to_string(),
            };
            self.bdevs.lock().unwrap().insert(bdev_name, info.clone());
            Ok(info)
        }
    }

    pub fn delete_bdev(&self, name: &str) -> Result<()> {
        info!("deleting bdev: {}", name);
        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_bdev_unregister") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            self.bdevs.lock().unwrap().remove(name);
            Ok(())
        }
    }

    pub fn list_bdevs(&self) -> Vec<BdevInfo> {
        self.bdevs.lock().unwrap().values().cloned().collect()
    }

    pub fn get_bdev(&self, name: &str) -> Option<BdevInfo> {
        self.bdevs.lock().unwrap().get(name).cloned()
    }
}
