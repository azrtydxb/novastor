//! Replica bdev: fans out writes to N replicas with majority quorum,
//! distributes reads via round-robin, local-first, or latency-aware policy.

use crate::config::{Protection, ReadPolicy, ReplicaBdevConfig, ReplicaTarget};
use crate::error::{DataPlaneError, Result};
use log::{debug, info, warn};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// EMA smoothing factor as fixed-point (alpha = 0.1, stored as 10 out of 100).
const EMA_ALPHA_NUM: u64 = 10;
const EMA_ALPHA_DEN: u64 = 100;

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
    pub read_bytes: AtomicU64,
    pub write_bytes: AtomicU64,
    /// Exponential moving average of read latency in microseconds.
    pub avg_read_latency_us: AtomicU64,
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
        self.state
            .store(ReplicaState::Degraded as u8, Ordering::Release);
        warn!("replica {} marked degraded", self.target.address);
    }

    pub fn mark_offline(&self) {
        self.state
            .store(ReplicaState::Offline as u8, Ordering::Release);
        warn!("replica {} marked offline", self.target.address);
    }

    pub fn mark_healthy(&self) {
        self.state
            .store(ReplicaState::Healthy as u8, Ordering::Release);
        info!("replica {} marked healthy", self.target.address);
    }

    /// Records a read latency sample and updates the EMA.
    pub fn record_read_latency(&self, latency_us: u64) {
        let old = self.stats.avg_read_latency_us.load(Ordering::Relaxed);
        let new_avg = if old == 0 {
            latency_us
        } else {
            // EMA: new = alpha * sample + (1 - alpha) * old
            (EMA_ALPHA_NUM * latency_us + (EMA_ALPHA_DEN - EMA_ALPHA_NUM) * old) / EMA_ALPHA_DEN
        };
        self.stats
            .avg_read_latency_us
            .store(new_avg, Ordering::Relaxed);
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
            read_bytes: self.stats.read_bytes.load(Ordering::Relaxed),
            avg_read_latency_us: self.stats.avg_read_latency_us.load(Ordering::Relaxed),
        }
    }
}

pub struct ReplicaBdev {
    pub volume_id: String,
    pub replicas: RwLock<Vec<Arc<Replica>>>,
    pub write_quorum: u32,
    pub read_policy: ReadPolicy,
    /// Per-volume protection policy describing how data is protected.
    pub protection: RwLock<Protection>,
    read_index: AtomicU32,
    pub bdev_name: String,
    created_at: Instant,
    /// Tracks cumulative write quorum latency in microseconds for averaging.
    write_quorum_latency_sum_us: AtomicU64,
    write_quorum_count: AtomicU64,
}

impl ReplicaBdev {
    pub fn new(config: ReplicaBdevConfig) -> Self {
        let bdev_name = format!("replica_{}", config.volume_id);
        let replica_count = config.replicas.len() as u32;
        let replicas: Vec<Arc<Replica>> = config
            .replicas
            .iter()
            .enumerate()
            .map(|(i, target)| {
                let initiator_bdev = format!("nvme_{}_{}_n1", config.volume_id, i);
                Arc::new(Replica::new(target.clone(), initiator_bdev))
            })
            .collect();

        // Use explicitly provided protection, or derive from replica count.
        let protection = config
            .protection
            .unwrap_or_else(|| Protection::Replication {
                factor: if replica_count == 0 { 1 } else { replica_count },
            });

        Self {
            volume_id: config.volume_id,
            replicas: RwLock::new(replicas),
            write_quorum: config.write_quorum,
            read_policy: config.read_policy,
            protection: RwLock::new(protection),
            read_index: AtomicU32::new(0),
            bdev_name,
            created_at: Instant::now(),
            write_quorum_latency_sum_us: AtomicU64::new(0),
            write_quorum_count: AtomicU64::new(0),
        }
    }

    /// Updates the protection policy for this volume.
    pub fn set_protection(&self, protection: Protection) {
        info!(
            "volume {}: protection updated to {:?}",
            self.volume_id, protection
        );
        *self.protection.write().unwrap() = protection;
    }

    /// Returns a clone of the current protection policy for this volume.
    pub fn get_protection(&self) -> Protection {
        self.protection.read().unwrap().clone()
    }

    pub fn healthy_replicas(&self) -> Vec<Arc<Replica>> {
        let replicas = self.replicas.read().unwrap();
        replicas
            .iter()
            .filter(|r| r.is_healthy())
            .cloned()
            .collect()
    }

    pub fn select_read_replica(&self) -> Result<Arc<Replica>> {
        let healthy = self.healthy_replicas();
        if healthy.is_empty() {
            return Err(DataPlaneError::ReplicaError(
                "no healthy replicas".to_string(),
            ));
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
            ReadPolicy::LatencyAware => self.select_latency_aware(&healthy),
        }
    }

    /// Selects a replica using weighted round-robin based on inverse latency.
    /// Replicas with lower latency get proportionally more reads.
    /// Falls back to simple round-robin if no latency data is available.
    fn select_latency_aware(&self, healthy: &[Arc<Replica>]) -> Result<Arc<Replica>> {
        // Collect latencies; 0 means no data yet
        let latencies: Vec<u64> = healthy
            .iter()
            .map(|r| r.stats.avg_read_latency_us.load(Ordering::Relaxed))
            .collect();

        // If all latencies are 0 (no data), fall back to round-robin
        if latencies.iter().all(|&l| l == 0) {
            let idx = self.read_index.fetch_add(1, Ordering::Relaxed);
            return Ok(healthy[idx as usize % healthy.len()].clone());
        }

        // Compute weights as inverse of latency (use 1 for unknown latencies).
        // To avoid division, use max_latency / latency_i as the weight.
        let max_latency = *latencies.iter().max().unwrap_or(&1);
        let max_latency = max_latency.max(1); // avoid division by zero

        let weights: Vec<u64> = latencies
            .iter()
            .map(|&l| {
                if l == 0 {
                    // No data yet — give it equal weight to max
                    1
                } else {
                    // Higher weight for lower latency
                    max_latency / l.max(1)
                }
            })
            .collect();

        let total_weight: u64 = weights.iter().sum();
        if total_weight == 0 {
            let idx = self.read_index.fetch_add(1, Ordering::Relaxed);
            return Ok(healthy[idx as usize % healthy.len()].clone());
        }

        // Weighted round-robin: use read_index mod total_weight to pick
        let idx = self.read_index.fetch_add(1, Ordering::Relaxed) as u64;
        let slot = idx % total_weight;

        let mut cumulative = 0u64;
        for (i, &w) in weights.iter().enumerate() {
            cumulative += w;
            if slot < cumulative {
                return Ok(healthy[i].clone());
            }
        }

        // Fallback (shouldn't happen)
        Ok(healthy[healthy.len() - 1].clone())
    }

    pub fn submit_write(&self, _offset: u64, data: &[u8]) -> Result<()> {
        let healthy = self.healthy_replicas();
        let healthy_count = healthy.len() as u32;
        if healthy_count < self.write_quorum {
            return Err(DataPlaneError::ReplicaError(format!(
                "insufficient healthy replicas: have {}, need {}",
                healthy_count, self.write_quorum
            )));
        }

        let start = Instant::now();
        let data_len = data.len() as u64;

        // Production SPDK: spdk_bdev_write() to each replica bdev descriptor,
        // track completions, ACK on quorum.
        debug!(
            "write submitted to {} replicas (quorum={})",
            healthy_count, self.write_quorum
        );
        for replica in &healthy {
            replica
                .stats
                .writes_completed
                .fetch_add(1, Ordering::Relaxed);
            replica
                .stats
                .write_bytes
                .fetch_add(data_len, Ordering::Relaxed);
        }

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.write_quorum_latency_sum_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.write_quorum_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    pub fn submit_read(&self, _offset: u64, length: u64) -> Result<Vec<u8>> {
        let start = Instant::now();
        let replica = self.select_read_replica()?;
        // Production SPDK: spdk_bdev_read() on selected replica bdev descriptor.
        debug!("read submitted to replica {}", replica.target.address);
        replica
            .stats
            .reads_completed
            .fetch_add(1, Ordering::Relaxed);
        replica
            .stats
            .read_bytes
            .fetch_add(length, Ordering::Relaxed);

        let result = vec![0u8; length as usize];

        // Record read latency for this replica
        let elapsed_us = start.elapsed().as_micros() as u64;
        replica.record_read_latency(elapsed_us);

        Ok(result)
    }

    pub fn replica_status(&self) -> Vec<ReplicaStatusInfo> {
        let replicas = self.replicas.read().unwrap();
        replicas.iter().map(|r| r.status_info()).collect()
    }

    /// Add a new replica to this bdev at runtime.
    pub fn add_replica(&self, target: ReplicaTarget) -> Result<()> {
        let mut replicas = self.replicas.write().unwrap();
        // Check for duplicate address
        if replicas
            .iter()
            .any(|r| r.target.address == target.address && r.target.port == target.port)
        {
            return Err(DataPlaneError::ReplicaError(format!(
                "replica {}:{} already exists",
                target.address, target.port,
            )));
        }
        let idx = replicas.len();
        let bdev_name = format!("nvme_{}_{}_n1", self.volume_id, idx);
        info!(
            "adding replica {}:{} as {}",
            target.address, target.port, bdev_name
        );
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
        let replica_infos: Vec<ReplicaStatusInfo> =
            replicas.iter().map(|r| r.status_info()).collect();
        let healthy_count = replicas.iter().filter(|r| r.is_healthy()).count() as u32;

        let total_read_iops: u64 = replica_infos.iter().map(|r| r.reads_completed as u64).sum();
        let total_write_iops: u64 = replica_infos
            .iter()
            .map(|r| r.writes_completed as u64)
            .sum();

        let wq_count = self.write_quorum_count.load(Ordering::Relaxed);
        let write_quorum_latency_us = if wq_count > 0 {
            self.write_quorum_latency_sum_us.load(Ordering::Relaxed) / wq_count
        } else {
            0
        };

        ReplicaBdevStatus {
            volume_id: self.volume_id.clone(),
            replicas: replica_infos,
            write_quorum: self.write_quorum,
            healthy_count,
            total_read_iops,
            total_write_iops,
            write_quorum_latency_us,
            protection: self.get_protection(),
        }
    }

    /// Returns I/O statistics for this replica bdev.
    pub fn io_stats(&self) -> IoStats {
        let replicas = self.replicas.read().unwrap();
        let replica_stats: Vec<ReplicaIoStats> = replicas
            .iter()
            .map(|r| ReplicaIoStats {
                address: r.target.address.clone(),
                port: r.target.port,
                reads_completed: r.stats.reads_completed.load(Ordering::Relaxed) as u64,
                read_bytes: r.stats.read_bytes.load(Ordering::Relaxed),
                avg_read_latency_us: r.stats.avg_read_latency_us.load(Ordering::Relaxed),
            })
            .collect();

        let total_read_iops: u64 = replica_stats.iter().map(|r| r.reads_completed).sum();
        let total_write_iops: u64 = replicas
            .iter()
            .map(|r| r.stats.writes_completed.load(Ordering::Relaxed) as u64)
            .sum();

        let wq_count = self.write_quorum_count.load(Ordering::Relaxed);
        let write_quorum_latency_us = if wq_count > 0 {
            self.write_quorum_latency_sum_us.load(Ordering::Relaxed) / wq_count
        } else {
            0
        };

        IoStats {
            volume_id: self.volume_id.clone(),
            replicas: replica_stats,
            total_read_iops,
            total_write_iops,
            write_quorum_latency_us,
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
    pub read_bytes: u64,
    pub avg_read_latency_us: u64,
}

/// Full status report for a ReplicaBdev.
#[derive(Debug, Clone)]
pub struct ReplicaBdevStatus {
    pub volume_id: String,
    pub replicas: Vec<ReplicaStatusInfo>,
    pub write_quorum: u32,
    pub healthy_count: u32,
    pub total_read_iops: u64,
    pub total_write_iops: u64,
    pub write_quorum_latency_us: u64,
    pub protection: Protection,
}

/// Per-replica I/O statistics for the novastor_io_stats RPC.
#[derive(Debug, Clone)]
pub struct ReplicaIoStats {
    pub address: String,
    pub port: u16,
    pub reads_completed: u64,
    pub read_bytes: u64,
    pub avg_read_latency_us: u64,
}

/// Aggregated I/O statistics for a volume.
#[derive(Debug, Clone)]
pub struct IoStats {
    pub volume_id: String,
    pub replicas: Vec<ReplicaIoStats>,
    pub total_read_iops: u64,
    pub total_write_iops: u64,
    pub write_quorum_latency_us: u64,
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
            protection: None,
        };
        ReplicaBdev::new(config)
    }

    fn make_latency_bdev(targets: Vec<ReplicaTarget>) -> ReplicaBdev {
        let config = ReplicaBdevConfig {
            volume_id: "test-vol".into(),
            replicas: targets,
            write_quorum: 1,
            read_policy: ReadPolicy::LatencyAware,
            protection: None,
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

    #[test]
    fn test_ema_latency_tracking() {
        let target = ReplicaTarget {
            address: "10.0.0.1".into(),
            port: 4420,
            nqn: "nqn-1".into(),
        };
        let replica = Replica::new(target, "test-bdev".into());

        // First sample — becomes the initial value
        replica.record_read_latency(1000);
        assert_eq!(
            replica.stats.avg_read_latency_us.load(Ordering::Relaxed),
            1000
        );

        // Second sample — EMA: 0.1 * 2000 + 0.9 * 1000 = 200 + 900 = 1100
        replica.record_read_latency(2000);
        assert_eq!(
            replica.stats.avg_read_latency_us.load(Ordering::Relaxed),
            1100
        );

        // Third sample — EMA: 0.1 * 500 + 0.9 * 1100 = 50 + 990 = 1040
        replica.record_read_latency(500);
        assert_eq!(
            replica.stats.avg_read_latency_us.load(Ordering::Relaxed),
            1040
        );
    }

    #[test]
    fn test_latency_aware_no_data_falls_back_to_round_robin() {
        let bdev = make_latency_bdev(test_targets());

        // No latency data yet — should distribute evenly
        let mut counts = [0u32; 2];
        for _ in 0..100 {
            let r = bdev.select_read_replica().unwrap();
            if r.target.address == "10.0.0.1" {
                counts[0] += 1;
            } else {
                counts[1] += 1;
            }
        }
        // Should be roughly 50/50
        assert_eq!(counts[0], 50);
        assert_eq!(counts[1], 50);
    }

    #[test]
    fn test_latency_aware_prefers_lower_latency() {
        let bdev = make_latency_bdev(test_targets());

        // Set latencies: replica 0 = 100us, replica 1 = 400us
        // Weight: replica 0 = 400/100 = 4, replica 1 = 400/400 = 1
        // Total weight = 5, so replica 0 gets 4/5 = 80%, replica 1 gets 1/5 = 20%
        {
            let replicas = bdev.replicas.read().unwrap();
            replicas[0]
                .stats
                .avg_read_latency_us
                .store(100, Ordering::Relaxed);
            replicas[1]
                .stats
                .avg_read_latency_us
                .store(400, Ordering::Relaxed);
        }

        let mut counts = [0u32; 2];
        for _ in 0..100 {
            let r = bdev.select_read_replica().unwrap();
            if r.target.address == "10.0.0.1" {
                counts[0] += 1;
            } else {
                counts[1] += 1;
            }
        }

        // With weights 4:1, replica 0 should get ~80 reads
        assert_eq!(counts[0], 80);
        assert_eq!(counts[1], 20);
    }

    #[test]
    fn test_latency_aware_equal_latency_distributes_evenly() {
        let bdev = make_latency_bdev(test_targets());

        // Set equal latencies
        {
            let replicas = bdev.replicas.read().unwrap();
            replicas[0]
                .stats
                .avg_read_latency_us
                .store(200, Ordering::Relaxed);
            replicas[1]
                .stats
                .avg_read_latency_us
                .store(200, Ordering::Relaxed);
        }

        let mut counts = [0u32; 2];
        for _ in 0..100 {
            let r = bdev.select_read_replica().unwrap();
            if r.target.address == "10.0.0.1" {
                counts[0] += 1;
            } else {
                counts[1] += 1;
            }
        }

        // Equal latency → equal weight → 50/50
        assert_eq!(counts[0], 50);
        assert_eq!(counts[1], 50);
    }

    #[test]
    fn test_io_stats() {
        let bdev = make_bdev(test_targets());

        // Simulate some I/O
        bdev.submit_write(0, &[0u8; 4096]).unwrap();
        bdev.submit_read(0, 4096).unwrap();
        bdev.submit_read(0, 4096).unwrap();

        let stats = bdev.io_stats();
        assert_eq!(stats.volume_id, "test-vol");
        assert_eq!(stats.replicas.len(), 2);
        assert_eq!(stats.total_write_iops, 2); // both replicas got the write
        assert_eq!(stats.total_read_iops, 2); // round-robin distributed reads
    }

    #[test]
    fn test_read_bytes_tracking() {
        let bdev = make_bdev(test_targets());

        bdev.submit_read(0, 4096).unwrap();
        bdev.submit_read(0, 8192).unwrap();

        let stats = bdev.io_stats();
        let total_read_bytes: u64 = stats.replicas.iter().map(|r| r.read_bytes).sum();
        assert_eq!(total_read_bytes, 4096 + 8192);
    }

    #[test]
    fn test_write_bytes_tracking() {
        let bdev = make_bdev(test_targets());

        bdev.submit_write(0, &[0u8; 4096]).unwrap();

        let status = bdev.full_status();
        // Both replicas get the write, so total_write_iops = 2
        assert_eq!(status.total_write_iops, 2);
    }

    #[test]
    fn test_default_protection_from_target_count() {
        let bdev = make_bdev(test_targets());
        let prot = bdev.get_protection();
        // With 2 targets and no explicit protection, defaults to Replication { factor: 2 }
        assert_eq!(prot, Protection::Replication { factor: 2 });
    }

    #[test]
    fn test_explicit_protection() {
        let config = ReplicaBdevConfig {
            volume_id: "test-vol".into(),
            replicas: test_targets(),
            write_quorum: 1,
            read_policy: ReadPolicy::RoundRobin,
            protection: Some(Protection::Replication { factor: 3 }),
        };
        let bdev = ReplicaBdev::new(config);
        assert_eq!(bdev.get_protection(), Protection::Replication { factor: 3 });
    }

    #[test]
    fn test_set_protection() {
        let bdev = make_bdev(test_targets());
        assert_eq!(bdev.get_protection(), Protection::Replication { factor: 2 });

        bdev.set_protection(Protection::Replication { factor: 3 });
        assert_eq!(bdev.get_protection(), Protection::Replication { factor: 3 });
    }

    #[test]
    fn test_protection_in_status() {
        let config = ReplicaBdevConfig {
            volume_id: "test-vol".into(),
            replicas: test_targets(),
            write_quorum: 1,
            read_policy: ReadPolicy::RoundRobin,
            protection: Some(Protection::Replication { factor: 3 }),
        };
        let bdev = ReplicaBdev::new(config);
        let status = bdev.full_status();
        assert_eq!(status.protection, Protection::Replication { factor: 3 });
    }

    #[test]
    fn test_protection_replication_helper() {
        assert_eq!(
            Protection::replication(0),
            Protection::Replication { factor: 1 }
        );
        assert_eq!(
            Protection::replication(3),
            Protection::Replication { factor: 3 }
        );
    }

    #[test]
    fn test_protection_required_nodes() {
        assert_eq!(Protection::Replication { factor: 3 }.required_nodes(), 3);
        assert_eq!(
            Protection::ErasureCoding {
                data_shards: 4,
                parity_shards: 2
            }
            .required_nodes(),
            6
        );
    }
}
