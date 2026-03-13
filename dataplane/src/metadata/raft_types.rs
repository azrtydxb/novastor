//! openraft type configuration for the NovaStor metadata Raft.

use crate::metadata::types::{MetadataRequest, MetadataResponse};

/// Node information stored in Raft membership.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RaftNode {
    pub address: String,
    pub port: u16,
}

impl std::fmt::Display for RaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.address, self.port)
    }
}

// Use the declare_raft_types! macro to set up TypeConfig.
// Configures: D = MetadataRequest, R = MetadataResponse, Node = RaftNode.
// SnapshotData is std::io::Cursor<Vec<u8>> (the openraft default).
// NodeId defaults to u64, Entry defaults to Entry<Self>, etc.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = MetadataRequest,
        R = MetadataResponse,
        Node = RaftNode,
        SnapshotData = std::io::Cursor<Vec<u8>>,
);

/// Return a validated openraft `Config` with reasonable defaults for NovaStor.
///
/// Timeouts are set conservatively to accommodate cross-zone deployments:
/// - heartbeat: 500 ms
/// - election timeout: 1500–3000 ms
pub fn default_raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_payload_entries: 100,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_node_display() {
        let node = RaftNode {
            address: "10.0.0.1".to_string(),
            port: 7000,
        };
        assert_eq!(node.to_string(), "10.0.0.1:7000");
    }

    #[test]
    fn default_config_valid() {
        let config = default_raft_config();
        config
            .validate()
            .expect("default raft config must be valid");
    }
}
