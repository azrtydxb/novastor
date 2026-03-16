//! Erasure coding bdev — Reed-Solomon encoding across multiple target bdevs.
//!
//! Splits data into `data_shards` pieces and generates `parity_shards`
//! additional parity pieces using Reed-Solomon (via `reed-solomon-simd`).
//! Data can be reconstructed from any `data_shards` of the total shards.
//!
//! This provides 1.5x storage overhead (for 4+2) vs 3x for triple replication,
//! making it suitable for capacity-efficient cold/warm data.

use crate::config::{ErasureBdevConfig, ReplicaTarget};
use crate::error::{DataPlaneError, Result};
use crate::spdk::reactor_dispatch;
use log::{debug, info, warn};
use std::sync::RwLock;

/// An erasure-coded bdev that distributes shards across multiple targets.
pub struct ErasureBdev {
    pub volume_id: String,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub shards: RwLock<Vec<ShardTarget>>,
    pub bdev_name: String,
    /// When true, submit_write/submit_read perform real SPDK bdev I/O.
    use_spdk_io: bool,
}

/// A target that holds one shard of the erasure-coded data.
#[derive(Debug, Clone)]
pub struct ShardTarget {
    pub target: ReplicaTarget,
    pub shard_index: usize,
    pub is_parity: bool,
    pub state: ShardState,
    /// The SPDK bdev name for this shard target.
    pub bdev_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShardState {
    Healthy,
    Degraded,
    Missing,
}

impl ErasureBdev {
    pub fn new(config: ErasureBdevConfig) -> Result<Self> {
        let total_shards = config.data_shards as usize + config.parity_shards as usize;
        if config.shards.len() != total_shards {
            return Err(DataPlaneError::ErasureError(format!(
                "expected {} shard targets ({}+{}), got {}",
                total_shards,
                config.data_shards,
                config.parity_shards,
                config.shards.len()
            )));
        }
        if config.data_shards == 0 {
            return Err(DataPlaneError::ErasureError(
                "data_shards must be > 0".to_string(),
            ));
        }

        let shard_targets: Vec<ShardTarget> = config
            .shards
            .iter()
            .enumerate()
            .map(|(i, target)| {
                let shard_bdev = target
                    .bdev_name
                    .clone()
                    .unwrap_or_else(|| format!("shard_{}_{}", config.volume_id, i));
                ShardTarget {
                    target: target.clone(),
                    shard_index: i,
                    is_parity: i >= config.data_shards as usize,
                    state: ShardState::Healthy,
                    bdev_name: shard_bdev,
                }
            })
            .collect();

        let bdev_name = format!("erasure_{}", config.volume_id);

        info!(
            "erasure bdev created: volume_id={}, data_shards={}, parity_shards={}, total_targets={}",
            config.volume_id,
            config.data_shards,
            config.parity_shards,
            shard_targets.len()
        );

        Ok(Self {
            volume_id: config.volume_id,
            data_shards: config.data_shards as usize,
            parity_shards: config.parity_shards as usize,
            shards: RwLock::new(shard_targets),
            bdev_name,
            use_spdk_io: false,
        })
    }

    /// Encode data into data+parity shards using Reed-Solomon.
    ///
    /// The input data is split into `data_shards` equal pieces (padded if
    /// necessary), then `parity_shards` additional parity pieces are generated.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let shard_size = self.shard_size(data.len());

        // Pad data to be evenly divisible by data_shards.
        let padded_len = shard_size * self.data_shards;
        let mut padded = data.to_vec();
        padded.resize(padded_len, 0);

        // Split into data shards.
        let data_shards: Vec<Vec<u8>> = padded.chunks(shard_size).map(|c| c.to_vec()).collect();

        // Generate parity shards using reed-solomon-simd.
        // encode() returns Vec<Vec<u8>> — one parity shard per recovery index.
        let parity = reed_solomon_simd::encode(
            self.data_shards,
            self.parity_shards,
            data_shards.iter().map(|s| s.as_slice()),
        )
        .map_err(|e| DataPlaneError::ErasureError(format!("Reed-Solomon encode failed: {e}")))?;

        // Combine data + parity shards.
        let mut all_shards = data_shards;
        all_shards.extend(parity);

        debug!(
            "encoded {} bytes into {} shards ({} data + {} parity, shard_size={})",
            data.len(),
            all_shards.len(),
            self.data_shards,
            self.parity_shards,
            shard_size
        );

        Ok(all_shards)
    }

    /// Decode (reconstruct) data from available shards.
    ///
    /// `available_shards` maps shard index to shard data. Indices 0..data_shards
    /// are original data shards, indices data_shards.. are parity (recovery)
    /// shards. At least `data_shards` total shards must be provided.
    pub fn decode(
        &self,
        available_shards: &[(usize, Vec<u8>)],
        original_data_len: usize,
    ) -> Result<Vec<u8>> {
        if available_shards.len() < self.data_shards {
            return Err(DataPlaneError::ErasureError(format!(
                "insufficient shards for decode: have {}, need {}",
                available_shards.len(),
                self.data_shards
            )));
        }

        // Check if all data shards are available — fast path.
        let has_all_data =
            (0..self.data_shards).all(|i| available_shards.iter().any(|(idx, _)| *idx == i));

        if has_all_data {
            // Fast path: just concatenate the data shards in order.
            let mut result = Vec::with_capacity(original_data_len);
            for i in 0..self.data_shards {
                let shard = available_shards.iter().find(|(idx, _)| *idx == i).unwrap();
                result.extend_from_slice(&shard.1);
            }
            result.truncate(original_data_len);
            return Ok(result);
        }

        // Slow path: need to reconstruct missing data shards.
        // Separate into original (data) shards and recovery (parity) shards.
        let mut originals: Vec<(usize, &[u8])> = Vec::new();
        let mut recovery: Vec<(usize, &[u8])> = Vec::new();

        for (idx, data) in available_shards {
            if *idx < self.data_shards {
                originals.push((*idx, data.as_slice()));
            } else {
                // Recovery shards use 0-based index within the parity set.
                let recovery_idx = *idx - self.data_shards;
                recovery.push((recovery_idx, data.as_slice()));
            }
        }

        let recovered =
            reed_solomon_simd::decode(self.data_shards, self.parity_shards, originals, recovery)
                .map_err(|e| {
                    DataPlaneError::ErasureError(format!("Reed-Solomon decode failed: {e}"))
                })?;

        // Reconstruct the data shards in order.
        let mut result = Vec::with_capacity(original_data_len);
        for i in 0..self.data_shards {
            if let Some((_, data)) = available_shards.iter().find(|(idx, _)| *idx == i) {
                result.extend_from_slice(data);
            } else if let Some(shard_data) = recovered.get(&i) {
                result.extend_from_slice(shard_data);
            } else {
                return Err(DataPlaneError::ErasureError(format!(
                    "shard {} missing from both available and recovered",
                    i
                )));
            }
        }

        result.truncate(original_data_len);
        debug!(
            "decoded {} bytes from {} available shards (recovered {} missing)",
            result.len(),
            available_shards.len(),
            recovered.len()
        );
        Ok(result)
    }

    /// Compute the shard size for a given data length.
    /// Reed-Solomon-SIMD requires shard sizes to be even (multiple of 2).
    pub fn shard_size(&self, data_len: usize) -> usize {
        let raw = (data_len + self.data_shards - 1) / self.data_shards;
        // Round up to the next even number.
        (raw + 1) & !1
    }

    /// Return the status of all shard targets.
    pub fn status(&self) -> ErasureBdevStatus {
        let shards = self.shards.read().unwrap();
        let healthy = shards
            .iter()
            .filter(|s| s.state == ShardState::Healthy)
            .count();
        let degraded = shards
            .iter()
            .filter(|s| s.state == ShardState::Degraded)
            .count();
        let missing = shards
            .iter()
            .filter(|s| s.state == ShardState::Missing)
            .count();

        let can_read = healthy + degraded >= self.data_shards;
        let can_write = healthy >= self.data_shards + self.parity_shards;

        ErasureBdevStatus {
            volume_id: self.volume_id.clone(),
            data_shards: self.data_shards as u32,
            parity_shards: self.parity_shards as u32,
            healthy_shards: healthy as u32,
            degraded_shards: degraded as u32,
            missing_shards: missing as u32,
            can_read,
            can_write,
        }
    }

    /// Mark a shard as degraded by its index.
    pub fn mark_shard_degraded(&self, shard_index: usize) -> Result<()> {
        let mut shards = self.shards.write().unwrap();
        let shard = shards.get_mut(shard_index).ok_or_else(|| {
            DataPlaneError::ErasureError(format!("shard index {} out of range", shard_index))
        })?;
        warn!(
            "erasure shard {} (target={}) marked degraded",
            shard_index, shard.target.address
        );
        shard.state = ShardState::Degraded;
        // TODO(gap-17): Notify the PolicyEngine that this shard is degraded so
        // it can trigger a repair operation (re-encode from surviving shards and
        // write to a new node). Currently mark_shard_degraded/missing only updates
        // local state — no downstream effect. The PolicyEngine's reconcile loop
        // should detect degraded EC shards and emit ReconstructShard actions.
        Ok(())
    }

    /// Mark a shard as missing by its index.
    pub fn mark_shard_missing(&self, shard_index: usize) -> Result<()> {
        let mut shards = self.shards.write().unwrap();
        let shard = shards.get_mut(shard_index).ok_or_else(|| {
            DataPlaneError::ErasureError(format!("shard index {} out of range", shard_index))
        })?;
        warn!(
            "erasure shard {} (target={}) marked missing",
            shard_index, shard.target.address
        );
        shard.state = ShardState::Missing;
        Ok(())
    }

    /// Mark a shard as healthy by its index.
    pub fn mark_shard_healthy(&self, shard_index: usize) -> Result<()> {
        let mut shards = self.shards.write().unwrap();
        let shard = shards.get_mut(shard_index).ok_or_else(|| {
            DataPlaneError::ErasureError(format!("shard index {} out of range", shard_index))
        })?;
        info!(
            "erasure shard {} (target={}) marked healthy",
            shard_index, shard.target.address
        );
        shard.state = ShardState::Healthy;
        Ok(())
    }

    /// Enable real SPDK bdev I/O for submit_write/submit_read.
    pub fn enable_spdk_io(&mut self) {
        self.use_spdk_io = true;
    }

    /// Write data to the erasure-coded volume at `offset`.
    ///
    /// Encodes the data into data+parity shards, then writes each shard
    /// to its respective bdev at the given offset.
    pub fn submit_write(&self, offset: u64, data: &[u8]) -> Result<()> {
        let shards_data = self.encode(data)?;
        let shard_targets = self.shards.read().unwrap();

        if shards_data.len() != shard_targets.len() {
            return Err(DataPlaneError::ErasureError(
                "shard count mismatch after encode".into(),
            ));
        }

        if self.use_spdk_io {
            for (shard_target, shard_bytes) in shard_targets.iter().zip(shards_data.iter()) {
                if shard_target.state == ShardState::Missing {
                    continue;
                }
                reactor_dispatch::bdev_write(&shard_target.bdev_name, offset, shard_bytes)
                    .map_err(|e| {
                        DataPlaneError::ErasureError(format!(
                            "write to shard {} ({}) failed: {}",
                            shard_target.shard_index, shard_target.bdev_name, e
                        ))
                    })?;
            }
        }

        debug!(
            "erasure write: {} bytes encoded into {} shards at offset {}",
            data.len(),
            shards_data.len(),
            offset
        );
        Ok(())
    }

    /// Read data from the erasure-coded volume at `offset`.
    ///
    /// Reads each shard from its bdev, then decodes (reconstructing
    /// missing shards if necessary).
    pub fn submit_read(&self, offset: u64, original_data_len: usize) -> Result<Vec<u8>> {
        if !self.use_spdk_io {
            // Stats-only mode for unit tests — return zeros.
            return Ok(vec![0u8; original_data_len]);
        }

        let shard_size = self.shard_size(original_data_len);
        let shard_targets = self.shards.read().unwrap();

        let mut available_shards: Vec<(usize, Vec<u8>)> = Vec::new();
        for shard_target in shard_targets.iter() {
            if shard_target.state == ShardState::Missing {
                continue;
            }
            match reactor_dispatch::bdev_read(&shard_target.bdev_name, offset, shard_size as u64) {
                Ok(data) => {
                    available_shards.push((shard_target.shard_index, data));
                }
                Err(e) => {
                    warn!(
                        "read from shard {} ({}) failed: {}",
                        shard_target.shard_index, shard_target.bdev_name, e
                    );
                }
            }
        }
        drop(shard_targets);

        self.decode(&available_shards, original_data_len)
    }
}

/// Status report for an erasure-coded bdev.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ErasureBdevStatus {
    pub volume_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub healthy_shards: u32,
    pub degraded_shards: u32,
    pub missing_shards: u32,
    pub can_read: bool,
    pub can_write: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_targets(n: usize) -> Vec<ReplicaTarget> {
        (0..n)
            .map(|i| ReplicaTarget {
                address: format!("10.0.0.{}", i + 1),
                port: 4420,
                nqn: format!("nqn-shard-{}", i),
                bdev_name: None,
            })
            .collect()
    }

    fn make_erasure_bdev(data: u32, parity: u32) -> ErasureBdev {
        let total = data + parity;
        let config = ErasureBdevConfig {
            volume_id: "ec-test-vol".to_string(),
            data_shards: data,
            parity_shards: parity,
            shards: make_targets(total as usize),
        };
        ErasureBdev::new(config).unwrap()
    }

    #[test]
    fn test_create_erasure_bdev() {
        let bdev = make_erasure_bdev(4, 2);
        assert_eq!(bdev.data_shards, 4);
        assert_eq!(bdev.parity_shards, 2);
        assert_eq!(bdev.shards.read().unwrap().len(), 6);
        assert_eq!(bdev.bdev_name, "erasure_ec-test-vol");
    }

    #[test]
    fn test_create_wrong_shard_count_fails() {
        let config = ErasureBdevConfig {
            volume_id: "bad".to_string(),
            data_shards: 4,
            parity_shards: 2,
            shards: make_targets(3), // wrong: should be 6
        };
        assert!(ErasureBdev::new(config).is_err());
    }

    #[test]
    fn test_encode_decode_no_loss() {
        let bdev = make_erasure_bdev(4, 2);
        let data =
            b"Hello, erasure coding in NovaStor! This is a test of the Reed-Solomon encoding.";

        let shards = bdev.encode(data).unwrap();
        assert_eq!(shards.len(), 6); // 4 data + 2 parity

        // Decode with all shards available.
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .map(|(i, s)| (i, s.clone()))
            .collect();
        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_encode_decode_with_one_missing_data_shard() {
        let bdev = make_erasure_bdev(4, 2);
        let data = vec![0xABu8; 4096];

        let shards = bdev.encode(&data).unwrap();

        // Remove data shard 1 — should still be recoverable.
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 1)
            .map(|(i, s)| (i, s.clone()))
            .collect();

        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_encode_decode_with_two_missing_shards() {
        let bdev = make_erasure_bdev(4, 2);
        let data = vec![0xCDu8; 8192];

        let shards = bdev.encode(&data).unwrap();

        // Remove data shard 0 and data shard 2 — use parity to recover.
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 0 && *i != 2)
            .map(|(i, s)| (i, s.clone()))
            .collect();

        assert_eq!(available.len(), 4); // 2 data + 2 parity
        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_decode_insufficient_shards_fails() {
        let bdev = make_erasure_bdev(4, 2);
        let data = vec![0xEFu8; 4096];

        let shards = bdev.encode(&data).unwrap();

        // Only 3 shards available (need 4).
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .take(3)
            .map(|(i, s)| (i, s.clone()))
            .collect();

        let result = bdev.decode(&available, data.len());
        assert!(result.is_err());
    }

    #[test]
    fn test_encode_small_data() {
        let bdev = make_erasure_bdev(4, 2);
        let data = b"tiny";
        let shards = bdev.encode(data).unwrap();
        assert_eq!(shards.len(), 6);

        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .map(|(i, s)| (i, s.clone()))
            .collect();
        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_shard_size() {
        let bdev = make_erasure_bdev(4, 2);
        // 100 bytes / 4 data shards = 25, rounded up to 26 (even)
        assert_eq!(bdev.shard_size(100), 26);
        // 101 bytes / 4 = ceil(25.25) = 26, already even
        assert_eq!(bdev.shard_size(101), 26);
        // Exact multiple, already even
        assert_eq!(bdev.shard_size(1024), 256);
    }

    #[test]
    fn test_status_all_healthy() {
        let bdev = make_erasure_bdev(4, 2);
        let status = bdev.status();
        assert_eq!(status.healthy_shards, 6);
        assert_eq!(status.degraded_shards, 0);
        assert_eq!(status.missing_shards, 0);
        assert!(status.can_read);
        assert!(status.can_write);
    }

    #[test]
    fn test_status_with_degraded() {
        let bdev = make_erasure_bdev(4, 2);
        bdev.mark_shard_degraded(0).unwrap();
        bdev.mark_shard_degraded(5).unwrap();

        let status = bdev.status();
        assert_eq!(status.healthy_shards, 4);
        assert_eq!(status.degraded_shards, 2);
        // Can still read (4 healthy + 2 degraded >= 4 data_shards).
        assert!(status.can_read);
    }

    #[test]
    fn test_status_with_missing() {
        let bdev = make_erasure_bdev(4, 2);
        bdev.mark_shard_missing(0).unwrap();
        bdev.mark_shard_missing(1).unwrap();
        bdev.mark_shard_missing(2).unwrap();

        let status = bdev.status();
        assert_eq!(status.missing_shards, 3);
        // 3 healthy + 0 degraded = 3 < 4 data_shards → can't read.
        assert!(!status.can_read);
    }

    #[test]
    fn test_mark_shard_state_transitions() {
        let bdev = make_erasure_bdev(4, 2);
        bdev.mark_shard_degraded(0).unwrap();
        assert_eq!(bdev.shards.read().unwrap()[0].state, ShardState::Degraded);
        bdev.mark_shard_healthy(0).unwrap();
        assert_eq!(bdev.shards.read().unwrap()[0].state, ShardState::Healthy);
        bdev.mark_shard_missing(0).unwrap();
        assert_eq!(bdev.shards.read().unwrap()[0].state, ShardState::Missing);
    }

    #[test]
    fn test_mark_invalid_shard_fails() {
        let bdev = make_erasure_bdev(4, 2);
        assert!(bdev.mark_shard_degraded(10).is_err());
    }

    #[test]
    fn test_shard_target_parity_flag() {
        let bdev = make_erasure_bdev(4, 2);
        let shards = bdev.shards.read().unwrap();
        // First 4 are data shards.
        for i in 0..4 {
            assert!(!shards[i].is_parity, "shard {} should be data", i);
        }
        // Last 2 are parity.
        for i in 4..6 {
            assert!(shards[i].is_parity, "shard {} should be parity", i);
        }
    }

    #[test]
    fn test_encode_decode_2_1() {
        let bdev = make_erasure_bdev(2, 1);
        let data = b"Two data shards, one parity shard test data for RS encoding.";

        let shards = bdev.encode(data).unwrap();
        assert_eq!(shards.len(), 3);

        // Remove one data shard.
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 0)
            .map(|(i, s)| (i, s.clone()))
            .collect();

        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data.as_slice());
    }

    #[test]
    fn test_large_data_encode_decode() {
        let bdev = make_erasure_bdev(4, 2);
        // 4MB — realistic chunk size.
        let data = vec![0x42u8; 4 * 1024 * 1024];

        let shards = bdev.encode(&data).unwrap();
        assert_eq!(shards.len(), 6);

        // Each shard should be 1MB.
        assert_eq!(shards[0].len(), 1024 * 1024);

        // Remove 2 shards and recover.
        let available: Vec<(usize, Vec<u8>)> = shards
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 1 && *i != 3)
            .map(|(i, s)| (i, s.clone()))
            .collect();

        let recovered = bdev.decode(&available, data.len()).unwrap();
        assert_eq!(recovered, data);
    }
}
