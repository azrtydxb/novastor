//! Replica bdev: fans out writes to N replicas with majority quorum,
//! distributes reads via round-robin or local-first policy.

use crate::config::{ReadPolicy, ReplicaBdevConfig, ReplicaTarget};
use crate::error::{DataPlaneError, Result};
use log::{debug, info, warn};
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicaState {
    Healthy = 0,
    Degraded = 1,
    Offline = 2,
}

impl From<u8> for ReplicaState {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Healthy,
            1 => Self::Degraded,
            _ => Self::Offline,
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplicaStats {
    pub reads_completed: AtomicU32,
    pub writes_completed: AtomicU32,
    pub read_errors: AtomicU32,
    pub write_errors: AtomicU32,
}

pub struct Replica {
    pub target: ReplicaTarget,
    pub state: AtomicU8,
    pub stats: ReplicaStats,
    pub bdev_name: String,
}

impl Replica {
    pub fn new(target: ReplicaTarget, bdev_name: String) -> Self {
        Self {
            target,
            state: AtomicU8::new(ReplicaState::Healthy as u8),
            stats: ReplicaStats::default(),
            bdev_name,
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == ReplicaState::Healthy as u8
    }

    pub fn mark_degraded(&self) {
        self.state.store(ReplicaState::Degraded as u8, Ordering::Release);
        warn!("replica {} marked degraded", self.target.address);
    }

    pub fn mark_offline(&self) {
        self.state.store(ReplicaState::Offline as u8, Ordering::Release);
        warn!("replica {} marked offline", self.target.address);
    }

    pub fn mark_healthy(&self) {
        self.state.store(ReplicaState::Healthy as u8, Ordering::Release);
        info!("replica {} marked healthy", self.target.address);
    }
}

pub struct ReplicaBdev {
    pub volume_id: String,
    pub replicas: Vec<Arc<Replica>>,
    pub write_quorum: u32,
    pub read_policy: ReadPolicy,
    read_index: AtomicU32,
    pub bdev_name: String,
}

impl ReplicaBdev {
    pub fn new(config: ReplicaBdevConfig) -> Self {
        let bdev_name = format!("replica_{}", config.volume_id);
        let replicas: Vec<Arc<Replica>> = config
            .replicas
            .iter()
            .enumerate()
            .map(|(i, target)| {
                let initiator_bdev = format!("nvme_{}_{}_n1", config.volume_id, i);
                Arc::new(Replica::new(target.clone(), initiator_bdev))
            })
            .collect();

        Self {
            volume_id: config.volume_id,
            replicas,
            write_quorum: config.write_quorum,
            read_policy: config.read_policy,
            read_index: AtomicU32::new(0),
            bdev_name,
        }
    }

    pub fn healthy_replicas(&self) -> Vec<Arc<Replica>> {
        self.replicas.iter().filter(|r| r.is_healthy()).cloned().collect()
    }

    pub fn select_read_replica(&self) -> Result<Arc<Replica>> {
        let healthy = self.healthy_replicas();
        if healthy.is_empty() {
            return Err(DataPlaneError::ReplicaError("no healthy replicas".to_string()));
        }
        match &self.read_policy {
            ReadPolicy::RoundRobin => {
                let idx = self.read_index.fetch_add(1, Ordering::Relaxed);
                Ok(healthy[idx as usize % healthy.len()].clone())
            }
            ReadPolicy::LocalFirst { local_address } => {
                if let Some(local) = healthy.iter().find(|r| r.target.address == *local_address) {
                    return Ok(local.clone());
                }
                let idx = self.read_index.fetch_add(1, Ordering::Relaxed);
                Ok(healthy[idx as usize % healthy.len()].clone())
            }
        }
    }

    pub fn submit_write(&self, _offset: u64, _data: &[u8]) -> Result<()> {
        let healthy = self.healthy_replicas();
        let healthy_count = healthy.len() as u32;
        if healthy_count < self.write_quorum {
            return Err(DataPlaneError::ReplicaError(format!(
                "insufficient healthy replicas: have {}, need {}",
                healthy_count, self.write_quorum
            )));
        }

        // Production SPDK: spdk_bdev_write() to each replica bdev descriptor,
        // track completions, ACK on quorum.
        debug!("write submitted to {} replicas (quorum={})", healthy_count, self.write_quorum);
        for replica in &healthy {
            replica.stats.writes_completed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    pub fn submit_read(&self, _offset: u64, length: u64) -> Result<Vec<u8>> {
        let replica = self.select_read_replica()?;
        // Production SPDK: spdk_bdev_read() on selected replica bdev descriptor.
        debug!("read submitted to replica {}", replica.target.address);
        replica.stats.reads_completed.fetch_add(1, Ordering::Relaxed);
        Ok(vec![0u8; length as usize])
    }

    pub fn replica_status(&self) -> Vec<ReplicaStatusInfo> {
        self.replicas
            .iter()
            .map(|r| ReplicaStatusInfo {
                address: r.target.address.clone(),
                port: r.target.port,
                state: ReplicaState::from(r.state.load(Ordering::Acquire)),
                reads_completed: r.stats.reads_completed.load(Ordering::Relaxed),
                writes_completed: r.stats.writes_completed.load(Ordering::Relaxed),
                read_errors: r.stats.read_errors.load(Ordering::Relaxed),
                write_errors: r.stats.write_errors.load(Ordering::Relaxed),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaStatusInfo {
    pub address: String,
    pub port: u16,
    pub state: ReplicaState,
    pub reads_completed: u32,
    pub writes_completed: u32,
    pub read_errors: u32,
    pub write_errors: u32,
}
