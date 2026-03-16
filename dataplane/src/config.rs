//! Data plane configuration types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlaneConfig {
    pub rpc_socket: String,
    pub reactor_mask: String,
    pub mem_size: u32,
    pub transport_type: String,
    pub listen_address: String,
    pub listen_port: u16,
    pub grpc_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalBdevConfig {
    pub name: String,
    pub device_path: String,
    #[serde(default = "default_block_size")]
    pub block_size: u32,
}

fn default_block_size() -> u32 {
    512
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobstoreConfig {
    pub base_bdev: String,
    #[serde(default = "default_cluster_size")]
    pub cluster_size: u32,
}

fn default_cluster_size() -> u32 {
    1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LvolConfig {
    pub volume_id: String,
    pub size_bytes: u64,
    pub lvol_store: String,
    #[serde(default = "default_true")]
    pub thin_provision: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmfTargetConfig {
    pub volume_id: String,
    pub bdev_name: String,
    pub listen_address: String,
    pub listen_port: u16,
    #[serde(default)]
    pub ana_group_id: u32,
    #[serde(default = "default_ana_state")]
    pub ana_state: String,
}

fn default_ana_state() -> String {
    "optimized".to_string()
}

impl NvmfTargetConfig {
    pub fn nqn(&self) -> String {
        format!("nqn.2024-01.io.novastor:volume-{}", self.volume_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmfInitiatorConfig {
    pub nqn: String,
    pub remote_address: String,
    pub remote_port: u16,
    pub bdev_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaBdevConfig {
    pub volume_id: String,
    pub replicas: Vec<ReplicaTarget>,
    pub write_quorum: u32,
    #[serde(default)]
    pub read_policy: ReadPolicy,
    /// Per-volume protection policy. If not set, defaults to
    /// `Replication { factor: <number of replicas> }`.
    #[serde(default)]
    pub protection: Option<Protection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaTarget {
    pub address: String,
    pub port: u16,
    pub nqn: String,
    /// Optional explicit bdev name for this target (used for local malloc bdev testing).
    /// If absent, the bdev name is auto-generated from the volume ID and index.
    #[serde(default)]
    pub bdev_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReadPolicy {
    #[default]
    RoundRobin,
    LocalFirst {
        local_address: String,
    },
    LatencyAware,
}

/// Per-volume data protection policy.
///
/// Specifies how a volume's data is protected across nodes. When no explicit
/// protection is set, the dataplane falls back to `Replication { factor: 1 }`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum Protection {
    /// Synchronous N-way replication.
    Replication {
        /// Number of replicas to maintain (e.g., 3 for 3-way replication).
        factor: u32,
    },
    /// Reed-Solomon erasure coding.
    ErasureCoding {
        /// Number of data shards.
        data_shards: u32,
        /// Number of parity shards.
        parity_shards: u32,
    },
}

impl Default for Protection {
    fn default() -> Self {
        Protection::Replication { factor: 1 }
    }
}

impl Protection {
    /// Returns the total number of nodes required for this protection scheme.
    pub fn required_nodes(&self) -> u32 {
        match self {
            Protection::Replication { factor } => *factor,
            Protection::ErasureCoding {
                data_shards,
                parity_shards,
            } => data_shards + parity_shards,
        }
    }

    /// Creates a replication protection with the given factor.
    /// Falls back to factor=1 if the provided value is 0.
    pub fn replication(factor: u32) -> Self {
        let factor = if factor == 0 { 1 } else { factor };
        Protection::Replication { factor }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NvmeBdevConfig {
    pub name: String,
    pub pcie_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureBdevConfig {
    pub volume_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub shards: Vec<ReplicaTarget>,
}
