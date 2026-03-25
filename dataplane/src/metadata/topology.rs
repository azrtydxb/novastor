//! Cluster topology types for CRUSH placement.

use serde::{Deserialize, Serialize};

/// The type of storage backend on a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BackendType {
    /// NVMe block device managed via SPDK bdev layer.
    Bdev,
    /// Regular file-backed storage.
    File,
    /// LVM logical volume managed via SPDK lvol store.
    Lvm,
}

/// A single storage backend (device) on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backend {
    /// Unique identifier for this backend.
    pub id: String,
    /// The node this backend belongs to.
    pub node_id: String,
    /// Total capacity in bytes.
    pub capacity_bytes: u64,
    /// Used capacity in bytes.
    pub used_bytes: u64,
    /// Relative weight for placement decisions (higher = more data placed here).
    pub weight: u32,
    /// Backend storage type.
    pub backend_type: BackendType,
}

impl Backend {
    /// Returns the number of free bytes on this backend.
    pub fn free_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }
}

/// Operational status of a node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node is healthy and accepting reads/writes.
    Online,
    /// Node is unreachable or has failed.
    Offline,
    /// Node is being drained; no new data is placed here.
    Draining,
}

/// A storage node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for this node.
    pub id: String,
    /// Network address (hostname or IP) for data-path connections.
    pub address: String,
    /// Port for data-path connections.
    pub port: u16,
    /// Storage backends attached to this node.
    pub backends: Vec<Backend>,
    /// Current operational status.
    pub status: NodeStatus,
}

impl Node {
    /// Returns the sum of capacity across all backends on this node.
    pub fn total_capacity(&self) -> u64 {
        self.backends.iter().map(|b| b.capacity_bytes).sum()
    }

    /// Returns the sum of weights across all backends on this node.
    pub fn total_weight(&self) -> u32 {
        self.backends.iter().map(|b| b.weight).sum()
    }
}

/// The authoritative view of cluster topology used by the placement engine.
///
/// The generation counter is incremented on every structural change so that
/// consumers can detect stale cached copies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMap {
    /// Monotonically increasing version of the map.
    generation: u64,
    /// All nodes known to the cluster.
    nodes: Vec<Node>,
    /// Volumes tracked in this cluster map snapshot.
    #[serde(default)]
    volumes: Vec<crate::metadata::types::VolumeDefinition>,
}

impl ClusterMap {
    /// Creates a new, empty `ClusterMap` with the given generation.
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            nodes: Vec::new(),
            volumes: Vec::new(),
        }
    }

    /// Returns the current generation of this map.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns a slice of all nodes in the map.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Adds or replaces a node (upsert by `node.id`).
    ///
    /// The generation is incremented on every call regardless of whether the
    /// node already existed, because even an update is a structural change.
    pub fn add_node(&mut self, node: Node) {
        self.generation += 1;
        if let Some(existing) = self.nodes.iter_mut().find(|n| n.id == node.id) {
            *existing = node;
        } else {
            self.nodes.push(node);
        }
    }

    /// Removes the node with the given `id`, incrementing the generation.
    ///
    /// Does nothing (but still increments generation) if the node is not found.
    pub fn remove_node(&mut self, id: &str) {
        self.generation += 1;
        self.nodes.retain(|n| n.id != id);
    }

    /// Returns a reference to the node with the given `id`, if it exists.
    pub fn get_node(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Returns references to all nodes whose status is [`NodeStatus::Online`].
    pub fn online_nodes(&self) -> Vec<&Node> {
        self.nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Online)
            .collect()
    }

    /// Bulk-set nodes without incrementing generation per node.
    ///
    /// Used when constructing from proto — generation is set once from the proto field.
    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }

    /// Replace the volumes list in this map.
    pub fn set_volumes(&mut self, volumes: Vec<crate::metadata::types::VolumeDefinition>) {
        self.volumes = volumes;
    }

    /// Returns a slice of all volumes tracked in this cluster map.
    pub fn volumes(&self) -> &[crate::metadata::types::VolumeDefinition] {
        &self.volumes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend(id: &str, node_id: &str, capacity: u64, used: u64, weight: u32) -> Backend {
        Backend {
            id: id.to_string(),
            node_id: node_id.to_string(),
            capacity_bytes: capacity,
            used_bytes: used,
            weight,
            backend_type: BackendType::Bdev,
        }
    }

    fn make_node(id: &str, status: NodeStatus, backends: Vec<Backend>) -> Node {
        Node {
            id: id.to_string(),
            address: "127.0.0.1".to_string(),
            port: 4420,
            backends,
            status,
        }
    }

    #[test]
    fn node_builder() {
        let backend = make_backend("bdev-0", "node-1", 1_000_000, 200_000, 10);
        let node = make_node("node-1", NodeStatus::Online, vec![backend]);

        assert_eq!(node.total_capacity(), 1_000_000);
        assert_eq!(node.total_weight(), 10);
        // Verify free_bytes via the backend directly
        assert_eq!(node.backends[0].free_bytes(), 800_000);
    }

    #[test]
    fn cluster_map_add_remove_node() {
        let mut map = ClusterMap::new(0);
        let node = make_node("node-1", NodeStatus::Online, vec![]);

        map.add_node(node);
        assert_eq!(map.nodes().len(), 1);
        assert_eq!(map.generation(), 1);

        map.remove_node("node-1");
        assert_eq!(map.nodes().len(), 0);
        assert_eq!(map.generation(), 2);
    }

    #[test]
    fn cluster_map_online_nodes_filters_offline() {
        let mut map = ClusterMap::new(0);
        map.add_node(make_node("node-online", NodeStatus::Online, vec![]));
        map.add_node(make_node("node-offline", NodeStatus::Offline, vec![]));
        map.add_node(make_node("node-draining", NodeStatus::Draining, vec![]));

        let online = map.online_nodes();
        assert_eq!(online.len(), 1);
        assert_eq!(online[0].id, "node-online");
    }

    #[test]
    fn cluster_map_serialization_roundtrip() {
        let backend = make_backend("bdev-0", "node-1", 2_000_000, 500_000, 5);
        let node = make_node("node-1", NodeStatus::Online, vec![backend]);
        let mut original = ClusterMap::new(42);
        original.add_node(node);

        let json = serde_json::to_string(&original).expect("serialization failed");
        let deserialized: ClusterMap = serde_json::from_str(&json).expect("deserialization failed");

        assert_eq!(deserialized.generation(), original.generation());
        assert_eq!(deserialized.nodes().len(), 1);
        assert_eq!(deserialized.nodes()[0].id, "node-1");
        assert_eq!(
            deserialized.nodes()[0].backends[0].capacity_bytes,
            2_000_000
        );
        assert_eq!(deserialized.nodes()[0].backends[0].free_bytes(), 1_500_000);
    }
}
