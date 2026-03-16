//! Policy engine — orchestrates chunk health evaluation and corrective actions.
//!
//! The `PolicyEngine` ties together the location store (where chunks live),
//! the evaluator (what needs fixing), and chunk operations (how to fix it).

use std::collections::HashMap;
use std::sync::Arc;

use log::{info, warn};
use tokio::sync::RwLock;

use crate::backend::chunk_store::ChunkStore;
use crate::error::Result;
use crate::metadata::topology::ClusterMap;
use crate::policy::evaluator::PolicyEvaluator;
use crate::policy::location_store::ChunkLocationStore;
use crate::policy::operations::ChunkOperations;
use crate::policy::types::*;

/// The policy engine orchestrates chunk health evaluation and corrective actions.
///
/// It holds the cluster topology, per-volume policies, and the persistent
/// location store.  The [`reconcile`](PolicyEngine::reconcile) method runs one
/// pass: evaluate every tracked chunk against its policy and execute any
/// corrective actions (replicate / remove-replica).
pub struct PolicyEngine {
    node_id: String,
    location_store: Arc<ChunkLocationStore>,
    operations: ChunkOperations,
    topology: RwLock<ClusterMap>,
    policies: RwLock<HashMap<String, VolumePolicy>>,
}

impl PolicyEngine {
    /// Create a new policy engine.
    ///
    /// * `node_id` — identifier for this node (used by `ChunkOperations` to
    ///   decide whether I/O is local or remote).
    /// * `location_store` — persistent store tracking chunk-to-node mappings.
    /// * `local_store` — the local chunk store for same-node I/O.
    /// * `topology` — initial cluster topology snapshot.
    pub fn new(
        node_id: String,
        location_store: Arc<ChunkLocationStore>,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
    ) -> Self {
        let operations = ChunkOperations::new(node_id.clone(), local_store);
        Self {
            node_id,
            location_store,
            operations,
            topology: RwLock::new(topology),
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Register or update a volume's replication policy.
    pub async fn set_policy(&self, policy: VolumePolicy) {
        let mut policies = self.policies.write().await;
        info!(
            "policy set for volume {}: desired_replicas={}",
            policy.volume_id, policy.desired_replicas
        );
        policies.insert(policy.volume_id.clone(), policy);
    }

    /// Retrieve the current policy for a volume, if any.
    pub async fn get_policy(&self, volume_id: &str) -> Option<VolumePolicy> {
        let policies = self.policies.read().await;
        policies.get(volume_id).cloned()
    }

    /// Replace the cluster topology with an updated snapshot.
    pub async fn update_topology(&self, topology: ClusterMap) {
        let mut topo = self.topology.write().await;
        info!(
            "topology updated: generation {} -> {}",
            topo.generation(),
            topology.generation()
        );
        *topo = topology;
    }

    /// Record that a chunk is stored on a given node.
    pub fn record_chunk_location(&self, chunk_id: &str, node_id: &str) -> Result<()> {
        self.location_store.add_node_to_chunk(chunk_id, node_id)
    }

    /// Record that a volume references a chunk.
    pub fn record_chunk_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()> {
        self.location_store.add_volume_ref(chunk_id, volume_id)
    }

    /// Returns this engine's node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Run one reconciliation pass: evaluate all chunks, execute corrective
    /// actions.  Returns the number of actions attempted.
    ///
    /// Individual action failures are logged as warnings but do not abort the
    /// pass — the engine processes every action and returns the total count so
    /// the caller knows how many corrections were needed.
    pub async fn reconcile(&self) -> Result<usize> {
        let topology = self.topology.read().await;
        let policies = self.policies.read().await;

        // Load all chunk locations from persistent store.
        let locations = self.location_store.list_locations()?;

        // Build the chunk-ref map (chunk_id -> ChunkRef).
        let mut refs: HashMap<String, ChunkRef> = HashMap::new();
        for loc in &locations {
            if let Ok(Some(chunk_ref)) = self.location_store.get_ref(&loc.chunk_id) {
                refs.insert(loc.chunk_id.clone(), chunk_ref);
            }
        }

        // Evaluate all chunks against their policies.
        let evaluator = PolicyEvaluator::new(&topology);
        let actions = evaluator.evaluate_all(&locations, &policies, &refs);

        let action_count = actions.len();
        if action_count > 0 {
            info!(
                "reconcile: {} corrective action(s) to execute",
                action_count
            );
        }

        // Execute each action.  Failures are logged but do not stop the pass.
        for action in &actions {
            match action {
                PolicyAction::Replicate {
                    chunk_id,
                    source_node,
                    target_node,
                } => {
                    let source_addr = self.node_addr(&topology, source_node);
                    let target_addr = self.node_addr(&topology, target_node);

                    if let (Some(src), Some(tgt)) = (source_addr, target_addr) {
                        match self
                            .operations
                            .replicate_chunk(chunk_id, source_node, &src, target_node, &tgt)
                            .await
                        {
                            Ok(()) => {
                                if let Err(e) =
                                    self.location_store.add_node_to_chunk(chunk_id, target_node)
                                {
                                    warn!(
                                        "failed to record chunk location for {}: {}",
                                        chunk_id, e
                                    );
                                }
                                info!(
                                    "replicated chunk {} from {} to {}",
                                    chunk_id, source_node, target_node
                                );
                            }
                            Err(e) => {
                                warn!("failed to replicate chunk {}: {}", chunk_id, e);
                            }
                        }
                    } else {
                        warn!(
                            "cannot replicate chunk {}: missing address for source={} or target={}",
                            chunk_id, source_node, target_node
                        );
                    }
                }
                PolicyAction::RemoveReplica { chunk_id, node_id } => {
                    let node_addr = self.node_addr(&topology, node_id);
                    if let Some(addr) = node_addr {
                        match self
                            .operations
                            .remove_replica(chunk_id, node_id, &addr)
                            .await
                        {
                            Ok(()) => {
                                if let Err(e) = self
                                    .location_store
                                    .remove_node_from_chunk(chunk_id, node_id)
                                {
                                    warn!(
                                        "failed to remove chunk location for {}: {}",
                                        chunk_id, e
                                    );
                                }
                                info!("removed replica of chunk {} from {}", chunk_id, node_id);
                            }
                            Err(e) => {
                                warn!(
                                    "failed to remove replica of chunk {} from {}: {}",
                                    chunk_id, node_id, e
                                );
                            }
                        }
                    } else {
                        warn!(
                            "cannot remove replica of chunk {}: missing address for node {}",
                            chunk_id, node_id
                        );
                    }
                }
            }
        }

        Ok(action_count)
    }

    /// Resolve a node's data-path address from the topology.
    fn node_addr(&self, topology: &ClusterMap, node_id: &str) -> Option<String> {
        topology
            .get_node(node_id)
            .map(|n| format!("http://{}:{}", n.address, n.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::chunk_store::ChunkHeader;
    use crate::backend::file_store::FileChunkStore;
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

    fn fake_chunk_id() -> String {
        "aabbccdd00112233aabbccdd00112233aabbccdd00112233aabbccdd00112233".to_string()
    }

    fn make_chunk_data(data: &[u8]) -> Vec<u8> {
        let header = ChunkHeader {
            magic: *b"NVAC",
            version: 1,
            flags: 0,
            checksum: crc32c::crc32c(data),
            data_len: data.len() as u32,
            _reserved: [0; 2],
        };
        let mut buf = Vec::with_capacity(ChunkHeader::SIZE + data.len());
        buf.extend_from_slice(&header.to_bytes());
        buf.extend_from_slice(data);
        buf
    }

    async fn make_engine() -> (tempfile::TempDir, tempfile::TempDir, PolicyEngine) {
        let store_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();

        let file_store = FileChunkStore::new(store_dir.path().to_path_buf())
            .await
            .unwrap();
        let location_store =
            Arc::new(ChunkLocationStore::open(db_dir.path().join("locations.redb")).unwrap());
        let topology = test_topology();

        let engine = PolicyEngine::new(
            "node-0".to_string(),
            location_store,
            Arc::new(file_store),
            topology,
        );

        (store_dir, db_dir, engine)
    }

    #[tokio::test]
    async fn set_and_get_policy() {
        let (_s, _d, engine) = make_engine().await;

        // No policy initially.
        assert!(engine.get_policy("vol-1").await.is_none());

        // Set a policy.
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 3))
            .await;
        let policy = engine.get_policy("vol-1").await.unwrap();
        assert_eq!(policy.desired_replicas, 3);

        // Update the same policy.
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 5))
            .await;
        let policy = engine.get_policy("vol-1").await.unwrap();
        assert_eq!(policy.desired_replicas, 5);
    }

    #[tokio::test]
    async fn record_and_query_locations() {
        let (_s, _d, engine) = make_engine().await;
        let chunk_id = fake_chunk_id();

        // Record locations.
        engine.record_chunk_location(&chunk_id, "node-0").unwrap();
        engine.record_chunk_location(&chunk_id, "node-1").unwrap();

        // Verify via the location store.
        let loc = engine
            .location_store
            .get_location(&chunk_id)
            .unwrap()
            .unwrap();
        assert_eq!(loc.node_ids.len(), 2);
        assert!(loc.node_ids.contains(&"node-0".to_string()));
        assert!(loc.node_ids.contains(&"node-1".to_string()));
    }

    #[tokio::test]
    async fn record_and_query_refs() {
        let (_s, _d, engine) = make_engine().await;
        let chunk_id = fake_chunk_id();

        engine.record_chunk_ref(&chunk_id, "vol-1").unwrap();
        engine.record_chunk_ref(&chunk_id, "vol-2").unwrap();

        let cr = engine.location_store.get_ref(&chunk_id).unwrap().unwrap();
        assert_eq!(cr.volume_ids.len(), 2);
    }

    #[tokio::test]
    async fn reconcile_healthy_no_actions() {
        let (_s, _d, engine) = make_engine().await;
        let chunk_id = fake_chunk_id();

        // Set up: chunk on 1 node, policy wants 1 replica.
        engine.record_chunk_location(&chunk_id, "node-0").unwrap();
        engine.record_chunk_ref(&chunk_id, "vol-1").unwrap();
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 1))
            .await;

        let action_count = engine.reconcile().await.unwrap();
        assert_eq!(action_count, 0, "healthy chunk should need 0 actions");
    }

    #[tokio::test]
    async fn reconcile_uses_evaluator_for_under_replicated() {
        // Verify the engine feeds evaluator correctly for under-replicated
        // chunks.  We use a single-node topology so CRUSH cannot find new
        // targets and no gRPC connections are attempted.
        let store_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();

        let file_store = FileChunkStore::new(store_dir.path().to_path_buf())
            .await
            .unwrap();
        let location_store =
            Arc::new(ChunkLocationStore::open(db_dir.path().join("locations.redb")).unwrap());

        // Single-node topology: only node-0.
        let mut topo = ClusterMap::new(1);
        topo.add_node(Node {
            id: "node-0".to_string(),
            address: "10.0.0.1".to_string(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-0".to_string(),
                node_id: "node-0".to_string(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });

        let engine = PolicyEngine::new(
            "node-0".to_string(),
            location_store,
            Arc::new(file_store),
            topo,
        );

        let chunk_id = fake_chunk_id();

        // Chunk on node-0 only, policy wants 3.  CRUSH can only return
        // node-0 (already holds the chunk) so no Replicate actions are
        // generated — but the chunk IS under-replicated.  Verify 0 actions
        // because there are no candidates, confirming the engine correctly
        // evaluates but respects placement constraints.
        engine.record_chunk_location(&chunk_id, "node-0").unwrap();
        engine.record_chunk_ref(&chunk_id, "vol-1").unwrap();
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 3))
            .await;

        let action_count = engine.reconcile().await.unwrap();
        assert_eq!(
            action_count, 0,
            "no actions when CRUSH cannot find new target nodes"
        );
    }

    #[tokio::test]
    async fn reconcile_over_replicated_local_removal() {
        let (store_dir, _d, engine) = make_engine().await;
        let chunk_id = fake_chunk_id();

        // Seed the chunk in local store so local delete succeeds.
        let data = make_chunk_data(b"over-replicated test data");
        let chunk_path = store_dir
            .path()
            .join("chunks")
            .join(&chunk_id[..2])
            .join(&chunk_id[2..4]);
        std::fs::create_dir_all(&chunk_path).unwrap();
        std::fs::write(chunk_path.join(&chunk_id), &data).unwrap();

        // Put chunk on 2 nodes with node-0 LAST so the evaluator's
        // over-replicated logic removes it (removes from end of list).
        // node-0 is local, so remove_replica goes through local store.
        engine
            .location_store
            .add_node_to_chunk(&chunk_id, "node-1")
            .unwrap();
        engine
            .location_store
            .add_node_to_chunk(&chunk_id, "node-0")
            .unwrap();
        engine.record_chunk_ref(&chunk_id, "vol-1").unwrap();
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 1))
            .await;

        let action_count = engine.reconcile().await.unwrap();
        assert_eq!(
            action_count, 1,
            "over-replicated by 1 should generate 1 action"
        );

        // Verify the chunk was removed from the local store.
        let loc = engine
            .location_store
            .get_location(&chunk_id)
            .unwrap()
            .unwrap();
        assert_eq!(loc.node_ids.len(), 1);
        assert_eq!(loc.node_ids[0], "node-1");
    }

    #[tokio::test]
    async fn reconcile_no_refs_no_actions() {
        let (_s, _d, engine) = make_engine().await;
        let chunk_id = fake_chunk_id();

        // Chunk exists in the location store but has no volume ref — the
        // evaluator skips chunks without refs.
        engine.record_chunk_location(&chunk_id, "node-0").unwrap();
        engine
            .set_policy(VolumePolicy::new("vol-1".to_string(), 3))
            .await;

        let action_count = engine.reconcile().await.unwrap();
        assert_eq!(action_count, 0);
    }

    #[tokio::test]
    async fn update_topology() {
        let (_s, _d, engine) = make_engine().await;

        let mut new_topo = ClusterMap::new(100);
        new_topo.add_node(Node {
            id: "node-new".to_string(),
            address: "192.168.1.1".to_string(),
            port: 9200,
            backends: vec![],
            status: NodeStatus::Online,
        });

        engine.update_topology(new_topo).await;

        // Verify the topology was replaced by checking generation.
        let topo = engine.topology.read().await;
        // new(100) + add_node increments to 101
        assert_eq!(topo.generation(), 101);
        assert!(topo.get_node("node-new").is_some());
        // Old nodes should be gone.
        assert!(topo.get_node("node-0").is_none());
    }

    #[tokio::test]
    async fn node_id_accessor() {
        let (_s, _d, engine) = make_engine().await;
        assert_eq!(engine.node_id(), "node-0");
    }
}
