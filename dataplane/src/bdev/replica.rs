//! Replica bdev: fans out writes to N replicas with majority quorum,
//! distributes reads via round-robin or local-first policy.

use crate::config::{ReadPolicy, ReplicaBdevConfig, ReplicaTarget};
use crate::error::{DataPlaneError, Result};
use log::{debug, info, warn};
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

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

    /// Returns a snapshot of this replica's status information.
    pub fn status_info(&self) -> ReplicaStatusInfo {
        ReplicaStatusInfo {
            address: self.target.address.clone(),
            port: self.target.port,
            state: ReplicaState::from(self.state.load(Ordering::Acquire)),
            reads_completed: self.stats.reads_completed.load(Ordering::Relaxed),
            writes_completed: self.stats.writes_completed.load(Ordering::Relaxed),
            read_errors: self.stats.read_errors.load(Ordering::Relaxed),
            write_errors: self.stats.write_errors.load(Ordering::Relaxed),
        }
    }
}

pub struct ReplicaBdev {
    pub volume_id: String,
    pub replicas: RwLock<Vec<Arc<Replica>>>,
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
            replicas: RwLock::new(replicas),
            write_quorum: config.write_quorum,
            read_policy: config.read_policy,
            read_index: AtomicU32::new(0),
            bdev_name,
        }
    }

    pub fn healthy_replicas(&self) -> Vec<Arc<Replica>> {
        let replicas = self.replicas.read().unwrap();
        replicas.iter().filter(|r| r.is_healthy()).cloned().collect()
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
        let replicas = self.replicas.read().unwrap();
        replicas.iter().map(|r| r.status_info()).collect()
    }

    /// Add a new replica to this bdev at runtime.
    pub fn add_replica(&self, target: ReplicaTarget) -> Result<()> {
        let mut replicas = self.replicas.write().unwrap();
        // Check for duplicate address
        if replicas.iter().any(|r| r.target.address == target.address && r.target.port == target.port) {
            return Err(DataPlaneError::ReplicaError(format!(
                "replica {}:{} already exists",
                target.address, target.port,
            )));
        }
        let idx = replicas.len();
        let bdev_name = format!("nvme_{}_{}_n1", self.volume_id, idx);
        info!("adding replica {}:{} as {}", target.address, target.port, bdev_name);
        replicas.push(Arc::new(Replica::new(target, bdev_name)));
        Ok(())
    }

    /// Remove a replica by address. Fails if it would remove the last replica.
    pub fn remove_replica(&self, addr: &str) -> Result<()> {
        let mut replicas = self.replicas.write().unwrap();
        if replicas.len() <= 1 {
            return Err(DataPlaneError::ReplicaError(
                "cannot remove last replica".to_string(),
            ));
        }
        let before = replicas.len();
        replicas.retain(|r| r.target.address != addr);
        if replicas.len() == before {
            return Err(DataPlaneError::ReplicaError(format!(
                "replica with address {} not found",
                addr,
            )));
        }
        info!("removed replica at {}", addr);
        Ok(())
    }

    /// Returns full status information for this replica bdev.
    pub fn full_status(&self) -> ReplicaBdevStatus {
        let replicas = self.replicas.read().unwrap();
        let replica_infos: Vec<ReplicaStatusInfo> = replicas.iter().map(|r| r.status_info()).collect();
        let healthy_count = replicas.iter().filter(|r| r.is_healthy()).count() as u32;
        ReplicaBdevStatus {
            volume_id: self.volume_id.clone(),
            replicas: replica_infos,
            write_quorum: self.write_quorum,
            healthy_count,
        }
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

/// Full status report for a ReplicaBdev.
#[derive(Debug, Clone)]
pub struct ReplicaBdevStatus {
    pub volume_id: String,
    pub replicas: Vec<ReplicaStatusInfo>,
    pub write_quorum: u32,
    pub healthy_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_targets() -> Vec<ReplicaTarget> {
        vec![
            ReplicaTarget {
                address: "10.0.0.1".into(),
                port: 4420,
                nqn: "nqn-1".into(),
            },
            ReplicaTarget {
                address: "10.0.0.2".into(),
                port: 4420,
                nqn: "nqn-2".into(),
            },
        ]
    }

    fn make_bdev(targets: Vec<ReplicaTarget>) -> ReplicaBdev {
        let config = ReplicaBdevConfig {
            volume_id: "test-vol".into(),
            replicas: targets,
            write_quorum: 1,
            read_policy: ReadPolicy::RoundRobin,
        };
        ReplicaBdev::new(config)
    }

    #[test]
    fn test_add_replica() {
        let bdev = make_bdev(test_targets());
        assert_eq!(bdev.replicas.read().unwrap().len(), 2);

        let new_target = ReplicaTarget {
            address: "10.0.0.3".into(),
            port: 4420,
            nqn: "nqn-3".into(),
        };
        bdev.add_replica(new_target).unwrap();
        assert_eq!(bdev.replicas.read().unwrap().len(), 3);
    }

    #[test]
    fn test_add_duplicate_replica_fails() {
        let bdev = make_bdev(test_targets());
        let dup_target = ReplicaTarget {
            address: "10.0.0.1".into(),
            port: 4420,
            nqn: "nqn-dup".into(),
        };
        let result = bdev.add_replica(dup_target);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_replica() {
        let bdev = make_bdev(test_targets());
        assert_eq!(bdev.replicas.read().unwrap().len(), 2);

        bdev.remove_replica("10.0.0.1").unwrap();
        assert_eq!(bdev.replicas.read().unwrap().len(), 1);
    }

    #[test]
    fn test_remove_last_replica_fails() {
        let single_target = vec![ReplicaTarget {
            address: "10.0.0.1".into(),
            port: 4420,
            nqn: "nqn-1".into(),
        }];
        let bdev = make_bdev(single_target);
        let result = bdev.remove_replica("10.0.0.1");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_nonexistent_replica_fails() {
        let bdev = make_bdev(test_targets());
        let result = bdev.remove_replica("10.0.0.99");
        assert!(result.is_err());
    }

    #[test]
    fn test_full_status() {
        let bdev = make_bdev(test_targets());
        let status = bdev.full_status();
        assert_eq!(status.volume_id, "test-vol");
        assert_eq!(status.replicas.len(), 2);
        assert_eq!(status.write_quorum, 1);
        assert_eq!(status.healthy_count, 2);

        // Mark one replica degraded and verify count changes
        {
            let replicas = bdev.replicas.read().unwrap();
            replicas[0].mark_degraded();
        }
        let status = bdev.full_status();
        assert_eq!(status.healthy_count, 1);
        assert_eq!(status.replicas[0].state, ReplicaState::Degraded);
        assert_eq!(status.replicas[1].state, ReplicaState::Healthy);
    }

    #[test]
    fn test_replica_status_info() {
        let target = ReplicaTarget {
            address: "10.0.0.1".into(),
            port: 4420,
            nqn: "nqn-1".into(),
        };
        let replica = Replica::new(target, "test-bdev".into());
        replica.stats.reads_completed.store(5, Ordering::Relaxed);
        replica.stats.writes_completed.store(3, Ordering::Relaxed);

        let info = replica.status_info();
        assert_eq!(info.address, "10.0.0.1");
        assert_eq!(info.port, 4420);
        assert_eq!(info.state, ReplicaState::Healthy);
        assert_eq!(info.reads_completed, 5);
        assert_eq!(info.writes_completed, 3);
    }
}
