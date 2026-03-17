//! CRUSH straw2 placement algorithm.
//!
//! Implements the straw2 bucket type from the CRUSH paper, providing
//! deterministic, pseudo-random placement with failure-domain separation.

use ring::digest;

use crate::metadata::topology::{ClusterMap, Node, NodeStatus};

/// A placement decision: `(node_id, backend_id)`.
pub type Placement = (String, String);

/// Select `count` distinct placements for the given chunk using straw2.
///
/// Returns up to `count` placements (may be fewer if fewer online nodes are
/// available).  Each selected node appears at most once in the result, giving
/// failure-domain separation at the node level.
pub fn select(chunk_id: &str, count: usize, topology: &ClusterMap) -> Vec<Placement> {
    // Collect online nodes that have positive total weight.
    // Nodes with weight=0 (no backends or all backends weight=0) are excluded
    // from CRUSH selection — they cannot store data.
    let mut candidates: Vec<&Node> = topology
        .nodes()
        .iter()
        .filter(|n| n.status == NodeStatus::Online && total_backend_weight(n) > 0)
        .collect();

    let actual_count = count.min(candidates.len());
    let mut placements = Vec::with_capacity(actual_count);

    for replica in 0..actual_count {
        // Compute straw2 draw for every remaining candidate.
        let best = candidates
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                let w = total_backend_weight(node);
                let draw = straw2_draw(chunk_id, &node.id, replica, w);
                (idx, draw)
            })
            .max_by(|(_, a): &(usize, f64), (_, b): &(usize, f64)| {
                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
            });

        if let Some((best_idx, _)) = best {
            let node = candidates.remove(best_idx);
            let backend_id = select_backend(chunk_id, node, replica);
            placements.push((node.id.clone(), backend_id));
        }
    }

    placements
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Sum of weights across all backends on a node.
fn total_backend_weight(node: &Node) -> u64 {
    node.backends.iter().map(|b| b.weight as u64).sum()
}

/// straw2 draw value: `ln(draw) / weight`.
///
/// A higher value wins.  The logarithm combined with division by weight
/// gives correct proportional probability (see Weil et al., CRUSH paper).
fn straw2_draw(chunk_id: &str, candidate_id: &str, replica: usize, weight: u64) -> f64 {
    let hash = crush_hash(chunk_id, candidate_id, replica);
    // Map the u32 hash into (0, 1] to avoid ln(0).
    let draw = (hash as f64 + 0.5) / (u32::MAX as f64 + 1.0);
    draw.ln() / weight as f64
}

/// Select a backend within `node` using straw2 over per-backend weights.
fn select_backend(chunk_id: &str, node: &Node, replica: usize) -> String {
    node.backends
        .iter()
        .map(|b| {
            let draw = straw2_draw(chunk_id, &b.id, replica, b.weight as u64);
            (b.id.clone(), draw)
        })
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(id, _)| id)
        .unwrap_or_default()
}

/// Deterministic hash: SHA-256 of `"chunk_id:candidate_id:replica"`, first 4
/// bytes interpreted as a big-endian u32.
///
/// Uses `ring::digest::Context` to incrementally feed segments, avoiding the
/// heap allocation that `format!()` would incur on every call.
fn crush_hash(chunk_id: &str, candidate_id: &str, replica: usize) -> u32 {
    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(chunk_id.as_bytes());
    ctx.update(b":");
    ctx.update(candidate_id.as_bytes());
    ctx.update(b":");
    // Write the replica number into a small stack buffer to avoid allocation.
    let mut buf = [0u8; 20]; // max u64 decimal digits
    let len = {
        use std::io::Write;
        let mut cursor = std::io::Cursor::new(&mut buf[..]);
        write!(cursor, "{}", replica).unwrap();
        cursor.position() as usize
    };
    ctx.update(&buf[..len]);
    let result = ctx.finish();
    let bytes = result.as_ref();
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn select_returns_requested_count() {
        let topo = test_topology();
        let placements = select("chunk-abc", 3, &topo);
        assert_eq!(placements.len(), 3);
    }

    #[test]
    fn select_is_deterministic() {
        let topo = test_topology();
        let a = select("chunk-xyz", 3, &topo);
        let b = select("chunk-xyz", 3, &topo);
        assert_eq!(a, b);
    }

    #[test]
    fn select_different_chunks_distribute() {
        let topo = test_topology();
        let mut counts = std::collections::HashMap::<String, usize>::new();

        for i in 0..1000 {
            let chunk_id = format!("chunk-{i}");
            let placements = select(&chunk_id, 1, &topo);
            assert_eq!(placements.len(), 1);
            *counts.entry(placements[0].0.clone()).or_insert(0) += 1;
        }

        // With 4 equal-weight nodes each should receive roughly 250 ± 100.
        for (_node, count) in &counts {
            assert!(
                (150..=350).contains(count),
                "node got {count} placements, expected 150-350"
            );
        }
    }

    #[test]
    fn select_no_duplicate_nodes() {
        let topo = test_topology();
        let placements = select("chunk-dup", 3, &topo);
        assert_eq!(placements.len(), 3);

        let mut seen = std::collections::HashSet::new();
        for (node_id, _) in &placements {
            assert!(seen.insert(node_id.clone()), "duplicate node: {node_id}");
        }
    }

    #[test]
    fn select_respects_weight() {
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "heavy".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-heavy".into(),
                node_id: "heavy".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 900,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });
        map.add_node(Node {
            id: "light".into(),
            address: "10.0.0.2".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-light".into(),
                node_id: "light".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });

        let mut heavy_count = 0usize;
        for i in 0..1000 {
            let chunk_id = format!("chunk-{i}");
            let placements = select(&chunk_id, 1, &map);
            assert_eq!(placements.len(), 1);
            if placements[0].0 == "heavy" {
                heavy_count += 1;
            }
        }

        assert!(
            heavy_count > 800,
            "heavy node got {heavy_count}/1000, expected >800"
        );
    }

    #[test]
    fn select_caps_at_available_nodes() {
        let topo = test_topology(); // 4 nodes
        let placements = select("chunk-cap", 10, &topo);
        assert_eq!(placements.len(), 4);
    }

    #[test]
    fn select_skips_offline_nodes() {
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "online-node".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-0".into(),
                node_id: "online-node".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });
        map.add_node(Node {
            id: "offline-node".into(),
            address: "10.0.0.2".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-1".into(),
                node_id: "offline-node".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Offline,
        });

        let placements = select("chunk-offline", 2, &map);
        assert_eq!(
            placements.len(),
            1,
            "should only get 1 placement (online node only)"
        );
        assert_eq!(placements[0].0, "online-node");
    }
}
