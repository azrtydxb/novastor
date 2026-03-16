use serde::{Deserialize, Serialize};

/// Per-volume protection policy.
///
/// A volume may be protected by either replication (N copies) or erasure
/// coding (K data + M parity shards). The evaluator uses these fields to
/// detect under-replication and missing EC shards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumePolicy {
    pub volume_id: String,
    pub desired_replicas: u32,
    /// Don't place on backends above this % full (default 90).
    pub capacity_threshold_pct: u8,
    /// EC protection parameters. When `Some`, the evaluator checks shard
    /// counts instead of replica counts.
    pub ec_params: Option<EcPolicyParams>,
}

/// Erasure coding parameters stored in the volume policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcPolicyParams {
    pub data_shards: u32,
    pub parity_shards: u32,
}

impl VolumePolicy {
    pub fn new(volume_id: String, desired_replicas: u32) -> Self {
        Self {
            volume_id,
            desired_replicas,
            capacity_threshold_pct: 90,
            ec_params: None,
        }
    }

    /// Create a policy for an erasure-coded volume.
    pub fn new_ec(volume_id: String, data_shards: u32, parity_shards: u32) -> Self {
        Self {
            volume_id,
            desired_replicas: 1, // not used for EC
            capacity_threshold_pct: 90,
            ec_params: Some(EcPolicyParams {
                data_shards,
                parity_shards,
            }),
        }
    }
}

/// Tracks where a specific chunk is stored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub chunk_id: String,
    pub node_ids: Vec<String>,
}

impl ChunkLocation {
    pub fn new(chunk_id: String) -> Self {
        Self {
            chunk_id,
            node_ids: Vec::new(),
        }
    }

    pub fn add_node(&mut self, node_id: String) {
        if !self.node_ids.contains(&node_id) {
            self.node_ids.push(node_id);
        }
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.node_ids.retain(|n| n != node_id);
    }

    pub fn replica_count(&self) -> u32 {
        self.node_ids.len() as u32
    }
}

/// Reference: which volumes reference a chunk (for dedup-safe deletion).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub chunk_id: String,
    pub volume_ids: Vec<String>,
}

impl ChunkRef {
    pub fn new(chunk_id: String) -> Self {
        Self {
            chunk_id,
            volume_ids: Vec::new(),
        }
    }

    pub fn add_volume(&mut self, volume_id: String) {
        if !self.volume_ids.contains(&volume_id) {
            self.volume_ids.push(volume_id);
        }
    }

    pub fn remove_volume(&mut self, volume_id: &str) {
        self.volume_ids.retain(|v| v != volume_id);
    }

    pub fn ref_count(&self) -> u32 {
        self.volume_ids.len() as u32
    }
}

/// Actions the policy engine can take.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum PolicyAction {
    Replicate {
        chunk_id: String,
        source_node: String,
        target_node: String,
    },
    RemoveReplica {
        chunk_id: String,
        node_id: String,
    },
    /// Reconstruct a missing EC shard from surviving shards.
    ReconstructShard {
        chunk_id: String,
        shard_index: usize,
        data_shards: u32,
        parity_shards: u32,
        /// Nodes that hold surviving shards (node_id list).
        source_nodes: Vec<String>,
        /// Where to write the reconstructed shard.
        target_node: String,
    },
}

/// Health status of a chunk relative to its policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkHealth {
    Healthy,
    UnderReplicated {
        actual: u32,
        desired: u32,
    },
    OverReplicated {
        actual: u32,
        desired: u32,
    },
    /// EC-protected chunk with missing shards that can be reconstructed.
    ShardDegraded {
        /// Total expected shards (data + parity).
        total_shards: u32,
        /// How many shards are present.
        present_shards: u32,
        /// Indices of missing shards.
        missing_indices: Vec<usize>,
    },
    /// EC-protected chunk with too many missing shards to reconstruct.
    ShardUnrecoverable {
        total_shards: u32,
        present_shards: u32,
        data_shards: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_policy_defaults() {
        let policy = VolumePolicy::new("vol-1".to_string(), 3);
        assert_eq!(policy.volume_id, "vol-1");
        assert_eq!(policy.desired_replicas, 3);
        assert_eq!(policy.capacity_threshold_pct, 90);
    }

    #[test]
    fn chunk_location_add_remove() {
        let mut loc = ChunkLocation::new("chunk-1".to_string());
        loc.add_node("node-a".to_string());
        loc.add_node("node-b".to_string());
        assert_eq!(loc.replica_count(), 2);

        loc.remove_node("node-a");
        assert_eq!(loc.replica_count(), 1);
        assert_eq!(loc.node_ids[0], "node-b");
    }

    #[test]
    fn chunk_location_no_duplicates() {
        let mut loc = ChunkLocation::new("chunk-2".to_string());
        loc.add_node("node-a".to_string());
        loc.add_node("node-a".to_string());
        assert_eq!(loc.replica_count(), 1);
    }

    #[test]
    fn chunk_ref_add_remove() {
        let mut cr = ChunkRef::new("chunk-1".to_string());
        cr.add_volume("vol-a".to_string());
        cr.add_volume("vol-b".to_string());
        assert_eq!(cr.ref_count(), 2);

        cr.remove_volume("vol-a");
        assert_eq!(cr.ref_count(), 1);
        assert_eq!(cr.volume_ids[0], "vol-b");
    }

    #[test]
    fn chunk_ref_no_duplicates() {
        let mut cr = ChunkRef::new("chunk-2".to_string());
        cr.add_volume("vol-a".to_string());
        cr.add_volume("vol-a".to_string());
        assert_eq!(cr.ref_count(), 1);
    }

    #[test]
    fn policy_action_serialization_roundtrip() {
        let replicate = PolicyAction::Replicate {
            chunk_id: "chunk-1".to_string(),
            source_node: "node-a".to_string(),
            target_node: "node-b".to_string(),
        };
        let json = serde_json::to_string(&replicate).unwrap();
        let deserialized: PolicyAction = serde_json::from_str(&json).unwrap();
        assert_eq!(replicate, deserialized);

        let remove = PolicyAction::RemoveReplica {
            chunk_id: "chunk-2".to_string(),
            node_id: "node-c".to_string(),
        };
        let json = serde_json::to_string(&remove).unwrap();
        let deserialized: PolicyAction = serde_json::from_str(&json).unwrap();
        assert_eq!(remove, deserialized);

        let reconstruct = PolicyAction::ReconstructShard {
            chunk_id: "chunk-3".to_string(),
            shard_index: 2,
            data_shards: 4,
            parity_shards: 2,
            source_nodes: vec!["node-a".to_string(), "node-b".to_string()],
            target_node: "node-c".to_string(),
        };
        let json = serde_json::to_string(&reconstruct).unwrap();
        let deserialized: PolicyAction = serde_json::from_str(&json).unwrap();
        assert_eq!(reconstruct, deserialized);
    }

    #[test]
    fn volume_policy_ec() {
        let policy = VolumePolicy::new_ec("vol-ec".to_string(), 4, 2);
        assert_eq!(policy.volume_id, "vol-ec");
        let ec = policy.ec_params.unwrap();
        assert_eq!(ec.data_shards, 4);
        assert_eq!(ec.parity_shards, 2);
    }

    #[test]
    fn chunk_health_variants() {
        let healthy = ChunkHealth::Healthy;
        assert_eq!(healthy, ChunkHealth::Healthy);

        let under = ChunkHealth::UnderReplicated {
            actual: 1,
            desired: 3,
        };
        assert_eq!(
            under,
            ChunkHealth::UnderReplicated {
                actual: 1,
                desired: 3
            }
        );

        let over = ChunkHealth::OverReplicated {
            actual: 5,
            desired: 3,
        };
        assert_eq!(
            over,
            ChunkHealth::OverReplicated {
                actual: 5,
                desired: 3
            }
        );
    }
}
