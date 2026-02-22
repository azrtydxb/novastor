//! NVMe-oF target and initiator management.

use crate::config::{NvmfInitiatorConfig, NvmfTargetConfig};
use crate::error::Result;
use log::info;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SubsystemInfo {
    pub nqn: String,
    pub bdev_name: String,
    pub listen_address: String,
    pub listen_port: u16,
    pub ana_group_id: u32,
    pub ana_state: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct InitiatorInfo {
    pub nqn: String,
    pub remote_address: String,
    pub remote_port: u16,
    pub local_bdev_name: String,
}

pub struct NvmfManager {
    subsystems: Mutex<HashMap<String, SubsystemInfo>>,
    initiators: Mutex<HashMap<String, InitiatorInfo>>,
    next_local_port: Mutex<u16>,
}

impl NvmfManager {
    pub fn new(base_port: u16) -> Self {
        Self {
            subsystems: Mutex::new(HashMap::new()),
            initiators: Mutex::new(HashMap::new()),
            next_local_port: Mutex::new(base_port),
        }
    }

    pub fn create_target(&self, config: &NvmfTargetConfig) -> Result<SubsystemInfo> {
        let nqn = config.nqn();
        info!("creating NVMe-oF target: nqn={}, bdev={}, addr={}:{}", nqn, config.bdev_name, config.listen_address, config.listen_port);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_nvmf_subsystem_create + add_ns + add_listener") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            let info = SubsystemInfo {
                nqn: nqn.clone(),
                bdev_name: config.bdev_name.clone(),
                listen_address: config.listen_address.clone(),
                listen_port: config.listen_port,
                ana_group_id: config.ana_group_id,
                ana_state: config.ana_state.clone(),
            };
            self.subsystems.lock().unwrap().insert(nqn, info.clone());
            Ok(info)
        }
    }

    pub fn delete_target(&self, volume_id: &str) -> Result<()> {
        let nqn = format!("nqn.2024-01.io.novastor:volume-{}", volume_id);
        info!("deleting NVMe-oF target: nqn={}", nqn);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_nvmf_subsystem_destroy") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            self.subsystems.lock().unwrap().remove(&nqn);
            Ok(())
        }
    }

    pub fn connect_initiator(&self, config: &NvmfInitiatorConfig) -> Result<InitiatorInfo> {
        info!("connecting NVMe-oF initiator: nqn={}, remote={}:{}", config.nqn, config.remote_address, config.remote_port);

        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_nvme_connect") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            let info = InitiatorInfo {
                nqn: config.nqn.clone(),
                remote_address: config.remote_address.clone(),
                remote_port: config.remote_port,
                local_bdev_name: config.bdev_name.clone(),
            };
            self.initiators.lock().unwrap().insert(config.bdev_name.clone(), info.clone());
            Ok(info)
        }
    }

    pub fn disconnect_initiator(&self, bdev_name: &str) -> Result<()> {
        info!("disconnecting NVMe-oF initiator: bdev={}", bdev_name);
        #[cfg(feature = "spdk-sys")]
        { todo!("SPDK FFI: spdk_nvme_detach") }

        #[cfg(not(feature = "spdk-sys"))]
        {
            self.initiators.lock().unwrap().remove(bdev_name);
            Ok(())
        }
    }

    pub fn allocate_local_port(&self) -> u16 {
        let mut port = self.next_local_port.lock().unwrap();
        let p = *port;
        *port += 1;
        p
    }

    pub fn export_local(&self, bdev_name: &str, volume_id: &str) -> Result<SubsystemInfo> {
        let port = self.allocate_local_port();
        let config = NvmfTargetConfig {
            volume_id: volume_id.to_string(),
            bdev_name: bdev_name.to_string(),
            listen_address: "127.0.0.1".to_string(),
            listen_port: port,
            ana_group_id: 0,
            ana_state: "optimized".to_string(),
        };
        self.create_target(&config)
    }

    pub fn list_subsystems(&self) -> Vec<SubsystemInfo> {
        self.subsystems.lock().unwrap().values().cloned().collect()
    }

    pub fn list_initiators(&self) -> Vec<InitiatorInfo> {
        self.initiators.lock().unwrap().values().cloned().collect()
    }

    pub fn set_ana_state(&self, nqn: &str, ana_group_id: u32, ana_state: &str) -> Result<()> {
        let mut subsystems = self.subsystems.lock().unwrap();
        let info = subsystems.get_mut(nqn).ok_or_else(|| {
            crate::error::DataPlaneError::NvmfTargetError(
                format!("subsystem not found: {}", nqn),
            )
        })?;
        info!("setting ANA state: nqn={}, ana_group_id={}, ana_state={}", nqn, ana_group_id, ana_state);
        info.ana_group_id = ana_group_id;
        info.ana_state = ana_state.to_string();
        Ok(())
    }

    pub fn get_ana_state(&self, nqn: &str) -> Result<(u32, String)> {
        let subsystems = self.subsystems.lock().unwrap();
        let info = subsystems.get(nqn).ok_or_else(|| {
            crate::error::DataPlaneError::NvmfTargetError(
                format!("subsystem not found: {}", nqn),
            )
        })?;
        Ok((info.ana_group_id, info.ana_state.clone()))
    }
}
