//! Policy evaluator: compares actual chunk locations against desired policies
//! and generates corrective actions (replicate / remove-replica).
//!
//! This module is pure logic — no I/O, no async, no network calls.

use std::collections::HashMap;

use crate::metadata::crush;
use crate::metadata::topology::ClusterMap;
use crate::policy::types::*;

/// Evaluates chunk health against volume policies and produces corrective
/// [`PolicyAction`]s using the CRUSH placement algorithm.
pub struct PolicyEvaluator<'a> {
    topology: &'a ClusterMap,
}

impl<'a> PolicyEvaluator<'a> {
    /// Create a new evaluator bound to the given cluster topology.
    pub fn new(topology: &'a ClusterMap) -> Self {
        Self { topology }
    }

    /// Evaluate a single chunk's health against its volume policy.
    pub fn evaluate_chunk(&self, location: &ChunkLocation, policy: &VolumePolicy) -> ChunkHealth {
        let actual = location.replica_count();
        let desired = policy.desired_replicas;
        if actual < desired {
            ChunkHealth::UnderReplicated { actual, desired }
        } else if actual > desired {
            ChunkHealth::OverReplicated { actual, desired }
        } else {
            ChunkHealth::Healthy
        }
    }

    /// Generate corrective actions for a chunk based on its health.
    pub fn generate_actions(
        &self,
        location: &ChunkLocation,
        policy: &VolumePolicy,
    ) -> Vec<PolicyAction> {
        let health = self.evaluate_chunk(location, policy);
        match health {
            ChunkHealth::Healthy => Vec::new(),
            ChunkHealth::UnderReplicated { actual, desired } => {
                let needed = (desired - actual) as usize;
                // Use CRUSH to find candidate nodes for this chunk.
                let placements = crush::select(&location.chunk_id, desired as usize, self.topology);
                // Pick a source node (first existing replica).
                let source = location.node_ids.first().cloned().unwrap_or_default();
                let mut actions = Vec::new();
                for (node_id, _backend_id) in placements {
                    if !location.node_ids.contains(&node_id) && actions.len() < needed {
                        actions.push(PolicyAction::Replicate {
                            chunk_id: location.chunk_id.clone(),
                            source_node: source.clone(),
                            target_node: node_id,
                        });
                    }
                }
                actions
            }
            ChunkHealth::OverReplicated { actual, desired } => {
                let excess = (actual - desired) as usize;
                // Remove from the last nodes in the list (least preferred).
                let to_remove = &location.node_ids[location.node_ids.len() - excess..];
                to_remove
                    .iter()
                    .map(|node_id| PolicyAction::RemoveReplica {
                        chunk_id: location.chunk_id.clone(),
                        node_id: node_id.clone(),
                    })
                    .collect()
            }
        }
    }

    /// Scan all chunk locations against their volume policies, returning all
    /// needed corrective actions.
    ///
    /// Uses `refs` to map each chunk to its owning volume(s), then looks up the
    /// corresponding [`VolumePolicy`].  The first matching policy wins.
    pub fn evaluate_all(
        &self,
        locations: &[ChunkLocation],
        policies: &HashMap<String, VolumePolicy>,
        refs: &HashMap<String, ChunkRef>,
    ) -> Vec<PolicyAction> {
        let mut actions = Vec::new();
        for loc in locations {
            if let Some(chunk_ref) = refs.get(&loc.chunk_id) {
                for volume_id in &chunk_ref.volume_ids {
                    if let Some(policy) = policies.get(volume_id) {
                        actions.extend(self.generate_actions(loc, policy));
                        break; // Use first matching policy
                    }
                }
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::topology::*;

    fn test_topology() -> ClusterMap {
        let mut map = ClusterMap::new(1);
        for i in 0..4 {
            map.add_node(Node {
                id: format!("node-{i}"),
                address: format!("10.0.0.{}", i + 1),
                port: 9100,
                backends: vec![Backend {
                    id: format!("bdev-{i}"),
                    node_id: format!("node-{i}"),
                    capacity_bytes: 1_000_000_000_000,
                    used_bytes: 0,
                    weight: 100,
                    backend_type: BackendType::Bdev,
                }],
                status: NodeStatus::Online,
            });
        }
        map
    }

    #[test]
    fn healthy_chunk_no_actions() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        let mut loc = ChunkLocation::new("chunk-1".to_string());
        loc.add_node("node-0".to_string());
        loc.add_node("node-1".to_string());
        loc.add_node("node-2".to_string());

        let policy = VolumePolicy::new("vol-1".to_string(), 3);

        assert_eq!(eval.evaluate_chunk(&loc, &policy), ChunkHealth::Healthy);
        assert!(eval.generate_actions(&loc, &policy).is_empty());
    }

    #[test]
    fn under_replicated_generates_replicate() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        let mut loc = ChunkLocation::new("chunk-2".to_string());
        loc.add_node("node-0".to_string());

        let policy = VolumePolicy::new("vol-1".to_string(), 3);

        let health = eval.evaluate_chunk(&loc, &policy);
        assert_eq!(
            health,
            ChunkHealth::UnderReplicated {
                actual: 1,
                desired: 3
            }
        );

        let actions = eval.generate_actions(&loc, &policy);
        assert_eq!(actions.len(), 2);
        for action in &actions {
            match action {
                PolicyAction::Replicate {
                    chunk_id,
                    source_node,
                    target_node,
                } => {
                    assert_eq!(chunk_id, "chunk-2");
                    assert_eq!(source_node, "node-0");
                    assert_ne!(target_node, "node-0");
                }
                _ => panic!("expected Replicate action"),
            }
        }
    }

    #[test]
    fn over_replicated_generates_remove() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        let mut loc = ChunkLocation::new("chunk-3".to_string());
        loc.add_node("node-0".to_string());
        loc.add_node("node-1".to_string());
        loc.add_node("node-2".to_string());
        loc.add_node("node-3".to_string());

        let policy = VolumePolicy::new("vol-1".to_string(), 3);

        let health = eval.evaluate_chunk(&loc, &policy);
        assert_eq!(
            health,
            ChunkHealth::OverReplicated {
                actual: 4,
                desired: 3
            }
        );

        let actions = eval.generate_actions(&loc, &policy);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PolicyAction::RemoveReplica { chunk_id, node_id } => {
                assert_eq!(chunk_id, "chunk-3");
                assert_eq!(node_id, "node-3"); // last node removed
            }
            _ => panic!("expected RemoveReplica action"),
        }
    }

    #[test]
    fn under_replicated_no_available_nodes() {
        // Only 1 online node — can't replicate to any new node.
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "node-0".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-0".into(),
                node_id: "node-0".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });

        let eval = PolicyEvaluator::new(&map);

        let mut loc = ChunkLocation::new("chunk-4".to_string());
        loc.add_node("node-0".to_string());

        let policy = VolumePolicy::new("vol-1".to_string(), 3);

        let health = eval.evaluate_chunk(&loc, &policy);
        assert_eq!(
            health,
            ChunkHealth::UnderReplicated {
                actual: 1,
                desired: 3
            }
        );

        // CRUSH can only return node-0 (already holds the chunk), so no
        // replicate actions can be generated.
        let actions = eval.generate_actions(&loc, &policy);
        assert!(
            actions.is_empty(),
            "expected no actions when no new nodes available, got {actions:?}"
        );
    }

    #[test]
    fn evaluate_all_mixed() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        // Chunk A: healthy (3 replicas, wants 3)
        let mut loc_a = ChunkLocation::new("chunk-a".to_string());
        loc_a.add_node("node-0".to_string());
        loc_a.add_node("node-1".to_string());
        loc_a.add_node("node-2".to_string());

        // Chunk B: under-replicated (1 replica, wants 3)
        let mut loc_b = ChunkLocation::new("chunk-b".to_string());
        loc_b.add_node("node-0".to_string());

        // Chunk C: over-replicated (4 replicas, wants 3)
        let mut loc_c = ChunkLocation::new("chunk-c".to_string());
        loc_c.add_node("node-0".to_string());
        loc_c.add_node("node-1".to_string());
        loc_c.add_node("node-2".to_string());
        loc_c.add_node("node-3".to_string());

        let locations = vec![loc_a, loc_b, loc_c];

        let mut policies = HashMap::new();
        policies.insert(
            "vol-1".to_string(),
            VolumePolicy::new("vol-1".to_string(), 3),
        );

        let mut refs = HashMap::new();
        for chunk_id in &["chunk-a", "chunk-b", "chunk-c"] {
            let mut cr = ChunkRef::new(chunk_id.to_string());
            cr.add_volume("vol-1".to_string());
            refs.insert(chunk_id.to_string(), cr);
        }

        let actions = eval.evaluate_all(&locations, &policies, &refs);

        // chunk-a: 0 actions (healthy)
        // chunk-b: 2 Replicate actions (under-replicated)
        // chunk-c: 1 RemoveReplica action (over-replicated)
        assert_eq!(actions.len(), 3);

        let replicate_count = actions
            .iter()
            .filter(|a| matches!(a, PolicyAction::Replicate { .. }))
            .count();
        let remove_count = actions
            .iter()
            .filter(|a| matches!(a, PolicyAction::RemoveReplica { .. }))
            .count();
        assert_eq!(replicate_count, 2);
        assert_eq!(remove_count, 1);
    }

    #[test]
    fn evaluate_all_no_matching_policy() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        let mut loc = ChunkLocation::new("chunk-orphan".to_string());
        loc.add_node("node-0".to_string());

        let locations = vec![loc];
        let policies = HashMap::new(); // No policies at all
        let refs = HashMap::new(); // No refs either

        let actions = eval.evaluate_all(&locations, &policies, &refs);
        assert!(actions.is_empty());
    }
}
