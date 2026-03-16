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
            // EC shard health is handled by generate_ec_actions, not here.
            ChunkHealth::Healthy
            | ChunkHealth::ShardDegraded { .. }
            | ChunkHealth::ShardUnrecoverable { .. } => Vec::new(),
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

    // -------------------------------------------------------------------
    // EC shard evaluation
    // -------------------------------------------------------------------

    /// Evaluate EC shard health for a given chunk.
    ///
    /// Scans `all_locations` for shard entries matching `{chunk_id}:shard:N`
    /// and compares against the expected shard count from the policy's EC
    /// parameters. Returns the health status.
    pub fn evaluate_ec_chunk(
        &self,
        chunk_id: &str,
        ec: &crate::policy::types::EcPolicyParams,
        all_locations: &[ChunkLocation],
    ) -> ChunkHealth {
        let total = ec.data_shards + ec.parity_shards;
        let mut present_indices: Vec<usize> = Vec::new();
        let prefix = format!("{chunk_id}:shard:");

        for loc in all_locations {
            if let Some(suffix) = loc.chunk_id.strip_prefix(&prefix) {
                if let Ok(idx) = suffix.parse::<usize>() {
                    if idx < total as usize && !loc.node_ids.is_empty() {
                        present_indices.push(idx);
                    }
                }
            }
        }

        present_indices.sort_unstable();
        present_indices.dedup();

        let present = present_indices.len() as u32;

        if present >= total {
            return ChunkHealth::Healthy;
        }

        // Determine which shard indices are missing.
        let missing: Vec<usize> = (0..total as usize)
            .filter(|i| !present_indices.contains(i))
            .collect();

        if present >= ec.data_shards {
            // Recoverable: we have enough shards to reconstruct.
            ChunkHealth::ShardDegraded {
                total_shards: total,
                present_shards: present,
                missing_indices: missing,
            }
        } else {
            // Unrecoverable: not enough shards.
            ChunkHealth::ShardUnrecoverable {
                total_shards: total,
                present_shards: present,
                data_shards: ec.data_shards,
            }
        }
    }

    /// Generate `ReconstructShard` actions for missing EC shards.
    pub fn generate_ec_actions(
        &self,
        chunk_id: &str,
        ec: &crate::policy::types::EcPolicyParams,
        all_locations: &[ChunkLocation],
    ) -> Vec<PolicyAction> {
        let health = self.evaluate_ec_chunk(chunk_id, ec, all_locations);
        match health {
            ChunkHealth::ShardDegraded {
                missing_indices,
                total_shards,
                ..
            } => {
                // Collect nodes that hold surviving shards.
                let prefix = format!("{chunk_id}:shard:");
                let source_nodes: Vec<String> = all_locations
                    .iter()
                    .filter(|loc| loc.chunk_id.starts_with(&prefix) && !loc.node_ids.is_empty())
                    .flat_map(|loc| loc.node_ids.iter().cloned())
                    .collect::<std::collections::HashSet<String>>()
                    .into_iter()
                    .collect();

                // Use CRUSH to pick target nodes for reconstructed shards.
                let placements = crush::select(chunk_id, total_shards as usize, self.topology);

                let mut actions = Vec::new();
                for missing_idx in &missing_indices {
                    // Pick a target from CRUSH placements. If the shard index
                    // maps to a CRUSH slot, use that node; otherwise use the
                    // first available CRUSH node not already holding a shard.
                    let target = if *missing_idx < placements.len() {
                        placements[*missing_idx].0.clone()
                    } else {
                        placements
                            .iter()
                            .find(|(n, _)| !source_nodes.contains(n))
                            .map(|(n, _)| n.clone())
                            .unwrap_or_else(|| placements[0].0.clone())
                    };

                    actions.push(PolicyAction::ReconstructShard {
                        chunk_id: chunk_id.to_string(),
                        shard_index: *missing_idx,
                        data_shards: ec.data_shards,
                        parity_shards: ec.parity_shards,
                        source_nodes: source_nodes.clone(),
                        target_node: target,
                    });
                }
                actions
            }
            _ => Vec::new(), // Healthy or unrecoverable — no actions.
        }
    }

    /// Scan all chunk locations against their volume policies, returning all
    /// needed corrective actions.
    ///
    /// Uses `refs` to map each chunk to its owning volume(s), then looks up the
    /// corresponding [`VolumePolicy`].  The first matching policy wins.
    ///
    /// For EC-protected volumes, the evaluator checks shard presence for
    /// parent chunk IDs (those NOT containing `:shard:`) and emits
    /// `ReconstructShard` actions for any missing shards.
    pub fn evaluate_all(
        &self,
        locations: &[ChunkLocation],
        policies: &HashMap<String, VolumePolicy>,
        refs: &HashMap<String, ChunkRef>,
    ) -> Vec<PolicyAction> {
        let mut actions = Vec::new();
        // Track parent chunk IDs already evaluated for EC to avoid duplicates.
        let mut ec_evaluated: std::collections::HashSet<String> = std::collections::HashSet::new();

        for loc in locations {
            if let Some(chunk_ref) = refs.get(&loc.chunk_id) {
                for volume_id in &chunk_ref.volume_ids {
                    if let Some(policy) = policies.get(volume_id) {
                        if let Some(ec) = &policy.ec_params {
                            // For EC, we evaluate the parent chunk. If this
                            // entry is a shard (`chunk:shard:N`), extract the
                            // parent. Otherwise, this IS the parent.
                            let parent_id = if let Some(pos) = loc.chunk_id.find(":shard:") {
                                loc.chunk_id[..pos].to_string()
                            } else {
                                loc.chunk_id.clone()
                            };
                            if ec_evaluated.insert(parent_id.clone()) {
                                actions.extend(self.generate_ec_actions(&parent_id, ec, locations));
                            }
                        } else {
                            actions.extend(self.generate_actions(loc, policy));
                        }
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
    fn ec_healthy_all_shards_present() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);
        let ec = crate::policy::types::EcPolicyParams {
            data_shards: 2,
            parity_shards: 1,
        };

        // All 3 shards present on distinct nodes.
        let locations: Vec<ChunkLocation> = (0..3)
            .map(|i| {
                let mut loc = ChunkLocation::new(format!("chunk-ec:shard:{i}"));
                loc.add_node(format!("node-{i}"));
                loc
            })
            .collect();

        let health = eval.evaluate_ec_chunk("chunk-ec", &ec, &locations);
        assert_eq!(health, ChunkHealth::Healthy);
    }

    #[test]
    fn ec_degraded_one_shard_missing() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);
        let ec = crate::policy::types::EcPolicyParams {
            data_shards: 2,
            parity_shards: 1,
        };

        // Shard 0 and 1 present, shard 2 missing.
        let mut loc0 = ChunkLocation::new("chunk-ec:shard:0".to_string());
        loc0.add_node("node-0".to_string());
        let mut loc1 = ChunkLocation::new("chunk-ec:shard:1".to_string());
        loc1.add_node("node-1".to_string());
        let locations = vec![loc0, loc1];

        let health = eval.evaluate_ec_chunk("chunk-ec", &ec, &locations);
        match &health {
            ChunkHealth::ShardDegraded {
                total_shards,
                present_shards,
                missing_indices,
            } => {
                assert_eq!(*total_shards, 3);
                assert_eq!(*present_shards, 2);
                assert_eq!(*missing_indices, vec![2]);
            }
            other => panic!("expected ShardDegraded, got {other:?}"),
        }

        // generate_ec_actions should produce 1 ReconstructShard action.
        let actions = eval.generate_ec_actions("chunk-ec", &ec, &locations);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            PolicyAction::ReconstructShard {
                chunk_id,
                shard_index,
                data_shards,
                parity_shards,
                ..
            } => {
                assert_eq!(chunk_id, "chunk-ec");
                assert_eq!(*shard_index, 2);
                assert_eq!(*data_shards, 2);
                assert_eq!(*parity_shards, 1);
            }
            other => panic!("expected ReconstructShard, got {other:?}"),
        }
    }

    #[test]
    fn ec_unrecoverable_too_many_missing() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);
        let ec = crate::policy::types::EcPolicyParams {
            data_shards: 4,
            parity_shards: 2,
        };

        // Only 3 shards present out of 6, need at least 4 (data_shards).
        let locations: Vec<ChunkLocation> = (0..3)
            .map(|i| {
                let mut loc = ChunkLocation::new(format!("chunk-ec:shard:{i}"));
                loc.add_node(format!("node-{i}"));
                loc
            })
            .collect();

        let health = eval.evaluate_ec_chunk("chunk-ec", &ec, &locations);
        match health {
            ChunkHealth::ShardUnrecoverable { .. } => {}
            other => panic!("expected ShardUnrecoverable, got {other:?}"),
        }

        // No actions for unrecoverable state.
        let actions = eval.generate_ec_actions("chunk-ec", &ec, &locations);
        assert!(actions.is_empty());
    }

    #[test]
    fn evaluate_all_with_ec_policy() {
        let topo = test_topology();
        let eval = PolicyEvaluator::new(&topo);

        // EC 2+1 chunk with shard 2 missing.
        let mut loc0 = ChunkLocation::new("chunk-ec:shard:0".to_string());
        loc0.add_node("node-0".to_string());
        let mut loc1 = ChunkLocation::new("chunk-ec:shard:1".to_string());
        loc1.add_node("node-1".to_string());

        let locations = vec![loc0, loc1];

        let mut policies = HashMap::new();
        policies.insert(
            "vol-ec".to_string(),
            VolumePolicy::new_ec("vol-ec".to_string(), 2, 1),
        );

        // Both shard entries reference vol-ec.
        let mut refs = HashMap::new();
        for shard_id in &["chunk-ec:shard:0", "chunk-ec:shard:1"] {
            let mut cr = ChunkRef::new(shard_id.to_string());
            cr.add_volume("vol-ec".to_string());
            refs.insert(shard_id.to_string(), cr);
        }

        let actions = eval.evaluate_all(&locations, &policies, &refs);
        assert_eq!(actions.len(), 1);
        assert!(matches!(&actions[0], PolicyAction::ReconstructShard { .. }));
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
