# Chunk Engine Redesign — Plan 2: CRUSH Placement + Metadata Store

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the CRUSH-like placement algorithm and the local metadata store (redb-backed) with openraft integration, so that chunk locations are computed deterministically and volume/policy metadata can be persisted and replicated via Raft consensus.

**Architecture:** Two subsystems: (1) CRUSH placement computes chunk → node/backend mappings deterministically from a shared cluster topology — no per-chunk metadata writes on the hot path. (2) A sharded Raft consensus layer (256 shards keyed by volume_id prefix) stores volume definitions, chunk maps, and policies. Each shard is an independent openraft group with its own redb database file. This plan implements single-node Raft (no networking) — gRPC transport for multi-node replication is deferred to Plan 3.

**Tech Stack:** Rust, openraft, redb, serde, sha2, crc32c

**Spec:** `docs/specs/2026-03-13-chunk-engine-redesign.md` (sections 2.1 and 2.2)

**Plan series:** This is Plan 2 of 4. Plan 1 (Backend Engine) is complete. Plan 3 will add gRPC transport + chunk engine. Plan 4 will add policy engine + cleanup.

---

## File Structure

### New files

| File | Responsibility |
|------|---------------|
| `src/metadata/mod.rs` | Module declarations |
| `src/metadata/topology.rs` | `Node`, `Backend`, `ClusterMap` types + serialization (inline tests) |
| `src/metadata/crush.rs` | CRUSH straw2 placement algorithm (inline tests) |
| `src/metadata/types.rs` | `VolumeDefinition`, `ChunkMapEntry`, `VolumePolicy` types (inline tests) |
| `src/metadata/store.rs` | redb-based local metadata store — tables for volumes, chunk maps, policies (inline tests) |
| `src/metadata/raft_types.rs` | openraft type config, request/response types |
| `src/metadata/raft_store.rs` | openraft `RaftLogStorage` + `RaftStateMachine` backed by redb (inline tests) |
| `src/metadata/shard.rs` | Shard manager — routes volume_id to shard, manages 256 openraft instances (inline tests) |

### Modified files

| File | Change |
|------|--------|
| `Cargo.toml` | Add `openraft`, `redb`, `rand` dependencies |
| `src/main.rs` | Add `mod metadata;` declaration |
| `src/error.rs` | Add `MetadataError` and `RaftError` variants |

---

## Chunk 1: Foundation — Dependencies, Topology, CRUSH

### Task 1: Add dependencies

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add openraft, redb, and rand to Cargo.toml**

Add to `[dependencies]`:
```toml
openraft = { version = "0.9", features = ["serde"] }
redb = "3"
rand = { version = "0.8", features = ["small_rng"] }
```

- [ ] **Step 2: Add metadata module declaration**

In `src/main.rs`, add `mod metadata;` after the existing module declarations.

- [ ] **Step 3: Add error variants**

In `src/error.rs`, add:
```rust
#[error("metadata error: {0}")]
MetadataError(String),
#[error("raft error: {0}")]
RaftError(String),
```

- [ ] **Step 4: Create metadata module skeleton**

Create `src/metadata/mod.rs`:
```rust
//! Metadata store — CRUSH placement and sharded Raft consensus.

pub mod topology;
pub mod crush;
pub mod types;
pub mod store;
pub mod raft_types;
pub mod raft_store;
pub mod shard;
```

Create empty placeholder files for each submodule (just `//! <description>` comment) so the project compiles.

- [ ] **Step 5: Verify compilation**

Run: `cargo check` (locally, no SPDK needed since metadata has no SPDK dependency)
Expected: Compiles with warnings about unused modules.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "[Chore] Add openraft, redb, rand dependencies and metadata module skeleton"
```

---

### Task 2: Cluster topology types

**Files:**
- Create: `src/metadata/topology.rs`

Cluster topology defines nodes, backends, and the cluster map used by CRUSH placement. These are pure data types with serialization — no I/O.

- [ ] **Step 1: Write tests for topology types**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_builder() {
        let node = Node {
            id: "node-1".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "bdev-nvme0".into(),
                node_id: "node-1".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        };
        assert_eq!(node.id, "node-1");
        assert_eq!(node.backends.len(), 1);
        assert_eq!(node.total_capacity(), 1_000_000_000_000);
    }

    #[test]
    fn cluster_map_add_remove_node() {
        let mut map = ClusterMap::new(1);
        let node = Node {
            id: "node-1".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![],
            status: NodeStatus::Online,
        };
        map.add_node(node.clone());
        assert_eq!(map.nodes().len(), 1);
        assert_eq!(map.generation(), 1);

        map.remove_node("node-1");
        assert_eq!(map.nodes().len(), 0);
        assert_eq!(map.generation(), 2);
    }

    #[test]
    fn cluster_map_online_nodes_filters_offline() {
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "node-1".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![],
            status: NodeStatus::Online,
        });
        map.add_node(Node {
            id: "node-2".into(),
            address: "10.0.0.2".into(),
            port: 9100,
            backends: vec![],
            status: NodeStatus::Offline,
        });
        assert_eq!(map.online_nodes().len(), 1);
    }

    #[test]
    fn cluster_map_serialization_roundtrip() {
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "n1".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "b1".into(),
                node_id: "n1".into(),
                capacity_bytes: 500_000_000_000,
                used_bytes: 100_000_000_000,
                weight: 50,
                backend_type: BackendType::File,
            }],
            status: NodeStatus::Online,
        });
        let json = serde_json::to_string(&map).unwrap();
        let parsed: ClusterMap = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.nodes().len(), 1);
        assert_eq!(parsed.generation(), map.generation());
    }
}
```

- [ ] **Step 2: Implement topology types**

```rust
//! Cluster topology types for CRUSH placement.

use serde::{Deserialize, Serialize};

/// Type of storage backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendType {
    /// SPDK bdev (raw disk or LVM lvol).
    Bdev,
    /// POSIX file store on existing filesystem.
    File,
}

/// A storage backend on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Backend {
    pub id: String,
    pub node_id: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    /// CRUSH weight (proportional to capacity, 0-100 scale).
    pub weight: u32,
    pub backend_type: BackendType,
}

impl Backend {
    pub fn free_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }
}

/// Node operational status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Online,
    Offline,
    Draining,
}

/// A storage node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub backends: Vec<Backend>,
    pub status: NodeStatus,
}

impl Node {
    pub fn total_capacity(&self) -> u64 {
        self.backends.iter().map(|b| b.capacity_bytes).sum()
    }

    pub fn total_weight(&self) -> u32 {
        self.backends.iter().map(|b| b.weight).sum()
    }
}

/// The cluster map — shared topology known to all nodes.
/// Changes go through Raft consensus (rare events).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMap {
    generation: u64,
    nodes: Vec<Node>,
}

impl ClusterMap {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            nodes: Vec::new(),
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.retain(|n| n.id != node.id);
        self.nodes.push(node);
    }

    pub fn remove_node(&mut self, node_id: &str) {
        self.nodes.retain(|n| n.id != node_id);
        self.generation += 1;
    }

    pub fn get_node(&self, node_id: &str) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == node_id)
    }

    pub fn online_nodes(&self) -> Vec<&Node> {
        self.nodes
            .iter()
            .filter(|n| n.status == NodeStatus::Online)
            .collect()
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib metadata::topology`
Expected: All 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/topology.rs
git commit -m "[Feature] Add cluster topology types (Node, Backend, ClusterMap)"
```

---

### Task 3: CRUSH straw2 placement algorithm

**Files:**
- Create: `src/metadata/crush.rs`

Pure deterministic algorithm. Given a chunk_id and cluster topology, computes which nodes/backends should store the chunk. Uses straw2 bucket selection: hash(chunk_id, candidate_id) weighted by capacity.

- [ ] **Step 1: Write tests for CRUSH placement**

```rust
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
        let result = select("abcdef0123456789", 3, &topo);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn select_is_deterministic() {
        let topo = test_topology();
        let a = select("chunk-abc-123", 3, &topo);
        let b = select("chunk-abc-123", 3, &topo);
        assert_eq!(a, b);
    }

    #[test]
    fn select_different_chunks_distribute() {
        let topo = test_topology();
        let mut node_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        // Place 1000 chunks with replication=1, check distribution.
        for i in 0..1000 {
            let chunk_id = format!("{:064x}", i);
            let placements = select(&chunk_id, 1, &topo);
            *node_counts.entry(placements[0].0.clone()).or_default() += 1;
        }
        // With 4 equal-weight nodes, each should get ~250 ± 50.
        for (_node, count) in &node_counts {
            assert!(
                *count > 150 && *count < 350,
                "uneven distribution: {node_counts:?}"
            );
        }
    }

    #[test]
    fn select_no_duplicate_nodes() {
        let topo = test_topology();
        let result = select("test-chunk-id-1234", 3, &topo);
        let node_ids: Vec<&str> = result.iter().map(|(n, _)| n.as_str()).collect();
        let unique: std::collections::HashSet<&str> = node_ids.iter().copied().collect();
        assert_eq!(node_ids.len(), unique.len(), "duplicate nodes in placement");
    }

    #[test]
    fn select_respects_weight() {
        let mut map = ClusterMap::new(1);
        // Node 0: weight 900, Node 1: weight 100
        map.add_node(Node {
            id: "heavy".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "b0".into(),
                node_id: "heavy".into(),
                capacity_bytes: 9_000_000_000_000,
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
                id: "b1".into(),
                node_id: "light".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });

        let mut heavy_count = 0;
        for i in 0..1000 {
            let chunk_id = format!("{:064x}", i);
            let placements = select(&chunk_id, 1, &map);
            if placements[0].0 == "heavy" {
                heavy_count += 1;
            }
        }
        // Heavy node (weight 900) should get ~900 of 1000.
        assert!(
            heavy_count > 800,
            "heavy node got only {heavy_count}/1000, expected ~900"
        );
    }

    #[test]
    fn select_caps_at_available_nodes() {
        let topo = test_topology(); // 4 nodes
        let result = select("chunk-123", 10, &topo);
        assert_eq!(result.len(), 4); // capped at 4
    }

    #[test]
    fn select_skips_offline_nodes() {
        let mut map = ClusterMap::new(1);
        map.add_node(Node {
            id: "online".into(),
            address: "10.0.0.1".into(),
            port: 9100,
            backends: vec![Backend {
                id: "b0".into(),
                node_id: "online".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Online,
        });
        map.add_node(Node {
            id: "offline".into(),
            address: "10.0.0.2".into(),
            port: 9100,
            backends: vec![Backend {
                id: "b1".into(),
                node_id: "offline".into(),
                capacity_bytes: 1_000_000_000_000,
                used_bytes: 0,
                weight: 100,
                backend_type: BackendType::Bdev,
            }],
            status: NodeStatus::Offline,
        });
        let result = select("chunk-123", 2, &map);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "online");
    }
}
```

- [ ] **Step 2: Implement CRUSH straw2 placement**

```rust
//! CRUSH-like placement algorithm (straw2 bucket type).
//!
//! Given a chunk_id, replication count, and cluster topology, deterministically
//! selects which nodes and backends should store the chunk. Uses the straw2
//! algorithm: each candidate draws a "straw" (hash-based pseudo-random value
//! weighted by capacity), and the longest straws win.
//!
//! Reference: Weil et al., "CRUSH: Controlled, Scalable, Decentralized
//! Placement of Replicated Data" (SC '06).

use crate::metadata::topology::{ClusterMap, Node, NodeStatus};
use sha2::{Digest, Sha256};

/// Placement result: (node_id, backend_id).
pub type Placement = (String, String);

/// Select `count` (node, backend) pairs for storing a chunk.
///
/// Guarantees:
/// - Deterministic: same inputs → same output on any node.
/// - Failure domain separation: no two placements on the same node.
/// - Weighted: nodes with higher total weight get proportionally more chunks.
/// - Only online nodes are considered.
///
/// Returns fewer than `count` if insufficient online nodes.
pub fn select(chunk_id: &str, count: usize, topology: &ClusterMap) -> Vec<Placement> {
    let online: Vec<&Node> = topology
        .nodes()
        .iter()
        .filter(|n| n.status == NodeStatus::Online && n.total_weight() > 0)
        .collect();

    let count = count.min(online.len());
    let mut selected: Vec<Placement> = Vec::with_capacity(count);
    let mut excluded_nodes: Vec<&str> = Vec::new();

    for replica in 0..count {
        let mut best_straw: f64 = f64::NEG_INFINITY;
        let mut best_node: Option<&Node> = None;

        for node in &online {
            if excluded_nodes.contains(&node.id.as_str()) {
                continue;
            }
            let straw = straw2_draw(chunk_id, &node.id, replica, node.total_weight());
            if straw > best_straw {
                best_straw = straw;
                best_node = Some(node);
            }
        }

        if let Some(node) = best_node {
            // Select backend within node using straw2 among backends.
            let backend_id = select_backend(chunk_id, node, replica);
            selected.push((node.id.clone(), backend_id));
            excluded_nodes.push(&node.id);
        }
    }

    selected
}

/// Straw2 draw for a candidate.
/// Returns ln(hash / 0xFFFF) / weight — higher weight = higher (less negative) value.
fn straw2_draw(chunk_id: &str, candidate_id: &str, replica: usize, weight: u32) -> f64 {
    let hash = crush_hash(chunk_id, candidate_id, replica);
    // Normalize hash to (0, 1) range, avoiding 0.
    let draw = (hash as f64 + 1.0) / (u32::MAX as f64 + 1.0);
    draw.ln() / (weight as f64)
}

/// Select the best backend within a node using straw2.
fn select_backend(chunk_id: &str, node: &Node, replica: usize) -> String {
    if node.backends.len() == 1 {
        return node.backends[0].id.clone();
    }

    let mut best_straw = f64::NEG_INFINITY;
    let mut best_id = node.backends[0].id.clone();

    for backend in &node.backends {
        if backend.weight == 0 {
            continue;
        }
        let straw = straw2_draw(chunk_id, &backend.id, replica, backend.weight);
        if straw > best_straw {
            best_straw = straw;
            best_id = backend.id.clone();
        }
    }

    best_id
}

/// Hash function for CRUSH: SHA-256 of (chunk_id || candidate_id || replica)
/// truncated to u32. Deterministic and well-distributed.
fn crush_hash(chunk_id: &str, candidate_id: &str, replica: usize) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(chunk_id.as_bytes());
    hasher.update(b":");
    hasher.update(candidate_id.as_bytes());
    hasher.update(b":");
    hasher.update(replica.to_le_bytes());
    let result = hasher.finalize();
    u32::from_le_bytes([result[0], result[1], result[2], result[3]])
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib metadata::crush`
Expected: All 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/crush.rs
git commit -m "[Feature] Add CRUSH straw2 placement algorithm"
```

---

## Chunk 2: Metadata Types + Local Store

### Task 4: Metadata types (volumes, chunk maps, policies)

**Files:**
- Create: `src/metadata/types.rs`

Pure data types representing what the Raft state machine stores. No I/O.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_definition_defaults() {
        let vol = VolumeDefinition {
            id: "vol-001".into(),
            name: "test-volume".into(),
            size_bytes: 10 * 1024 * 1024 * 1024, // 10 GiB
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1000,
            chunk_count: 0,
        };
        assert_eq!(vol.id, "vol-001");
        assert!(matches!(vol.protection, Protection::Replication { factor: 3 }));
    }

    #[test]
    fn chunk_map_entry_roundtrip() {
        let entry = ChunkMapEntry {
            chunk_index: 42,
            chunk_id: "abcdef1234567890".into(),
            ec_params: None,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: ChunkMapEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.chunk_index, 42);
        assert_eq!(parsed.chunk_id, "abcdef1234567890");
        assert!(parsed.ec_params.is_none());
    }

    #[test]
    fn ec_params_serialization() {
        let entry = ChunkMapEntry {
            chunk_index: 0,
            chunk_id: "abc123".into(),
            ec_params: Some(ErasureParams {
                data_shards: 4,
                parity_shards: 2,
            }),
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("data_shards"));
        let parsed: ChunkMapEntry = serde_json::from_str(&json).unwrap();
        let ec = parsed.ec_params.unwrap();
        assert_eq!(ec.data_shards, 4);
        assert_eq!(ec.parity_shards, 2);
    }

    #[test]
    fn shard_id_from_volume_id() {
        assert_eq!(shard_for_volume("ab12cd34"), 0xab);
        assert_eq!(shard_for_volume("ff001122"), 0xff);
        assert_eq!(shard_for_volume("00aabbcc"), 0x00);
    }

    #[test]
    fn metadata_request_variants() {
        let req = MetadataRequest::PutVolume(VolumeDefinition {
            id: "v1".into(),
            name: "test".into(),
            size_bytes: 1024,
            protection: Protection::ErasureCoding {
                data_shards: 4,
                parity_shards: 2,
            },
            status: VolumeStatus::Available,
            created_at: 0,
            chunk_count: 0,
        });
        let json = serde_json::to_string(&req).unwrap();
        let parsed: MetadataRequest = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, MetadataRequest::PutVolume(_)));
    }
}
```

- [ ] **Step 2: Implement metadata types**

```rust
//! Metadata types stored in the sharded Raft state machine.

use serde::{Deserialize, Serialize};

/// Data protection mode for a volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum Protection {
    Replication { factor: u32 },
    ErasureCoding { data_shards: u32, parity_shards: u32 },
}

/// Volume lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeStatus {
    Creating,
    Available,
    Deleting,
    Error,
}

/// A volume definition stored in the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeDefinition {
    pub id: String,
    pub name: String,
    pub size_bytes: u64,
    pub protection: Protection,
    pub status: VolumeStatus,
    pub created_at: u64,
    pub chunk_count: u64,
}

/// Erasure coding parameters for a chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureParams {
    pub data_shards: u32,
    pub parity_shards: u32,
}

/// Mapping of a volume's chunk index to its content-addressed chunk ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapEntry {
    pub chunk_index: u64,
    pub chunk_id: String,
    /// Set for erasure-coded chunks.
    pub ec_params: Option<ErasureParams>,
}

/// Requests that can be applied to the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MetadataRequest {
    PutVolume(VolumeDefinition),
    DeleteVolume { volume_id: String },
    PutChunkMap {
        volume_id: String,
        entry: ChunkMapEntry,
    },
    DeleteChunkMap {
        volume_id: String,
        chunk_index: u64,
    },
}

/// Responses from the Raft state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MetadataResponse {
    Ok,
    Volume(VolumeDefinition),
    NotFound,
    Error { message: String },
}

/// Compute which shard a volume belongs to.
/// Uses the first 2 hex characters of the volume_id → 0..255.
pub fn shard_for_volume(volume_id: &str) -> u8 {
    let hex = &volume_id[..2.min(volume_id.len())];
    u8::from_str_radix(hex, 16).unwrap_or(0)
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib metadata::types`
Expected: All 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/types.rs
git commit -m "[Feature] Add metadata types (VolumeDefinition, ChunkMapEntry, MetadataRequest)"
```

---

### Task 5: redb-based local metadata store

**Files:**
- Create: `src/metadata/store.rs`

Local key-value store backed by redb. This is the data layer that the Raft state machine writes to. Provides typed get/put/delete/list for volumes and chunk maps.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::*;

    fn temp_store() -> MetadataStore {
        let dir = tempfile::tempdir().unwrap();
        MetadataStore::open(dir.path().join("test.redb")).unwrap()
    }

    fn sample_volume() -> VolumeDefinition {
        VolumeDefinition {
            id: "aabb0011".into(),
            name: "test-vol".into(),
            size_bytes: 10 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1000,
            chunk_count: 0,
        }
    }

    #[test]
    fn put_and_get_volume() {
        let store = temp_store();
        let vol = sample_volume();
        store.put_volume(&vol).unwrap();
        let got = store.get_volume("aabb0011").unwrap().unwrap();
        assert_eq!(got.name, "test-vol");
    }

    #[test]
    fn get_nonexistent_volume_returns_none() {
        let store = temp_store();
        assert!(store.get_volume("nonexistent").unwrap().is_none());
    }

    #[test]
    fn delete_volume() {
        let store = temp_store();
        store.put_volume(&sample_volume()).unwrap();
        store.delete_volume("aabb0011").unwrap();
        assert!(store.get_volume("aabb0011").unwrap().is_none());
    }

    #[test]
    fn list_volumes() {
        let store = temp_store();
        store.put_volume(&sample_volume()).unwrap();
        let mut vol2 = sample_volume();
        vol2.id = "ccdd0022".into();
        vol2.name = "second-vol".into();
        store.put_volume(&vol2).unwrap();

        let vols = store.list_volumes().unwrap();
        assert_eq!(vols.len(), 2);
    }

    #[test]
    fn put_and_get_chunk_map_entry() {
        let store = temp_store();
        let entry = ChunkMapEntry {
            chunk_index: 0,
            chunk_id: "deadbeef".into(),
            ec_params: None,
        };
        store.put_chunk_map("vol-1", &entry).unwrap();
        let got = store.get_chunk_map("vol-1", 0).unwrap().unwrap();
        assert_eq!(got.chunk_id, "deadbeef");
    }

    #[test]
    fn list_chunk_map_for_volume() {
        let store = temp_store();
        for i in 0..5 {
            store
                .put_chunk_map(
                    "vol-1",
                    &ChunkMapEntry {
                        chunk_index: i,
                        chunk_id: format!("chunk-{i}"),
                        ec_params: None,
                    },
                )
                .unwrap();
        }
        let entries = store.list_chunk_map("vol-1").unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn delete_chunk_map_entry() {
        let store = temp_store();
        store
            .put_chunk_map(
                "vol-1",
                &ChunkMapEntry {
                    chunk_index: 0,
                    chunk_id: "abc".into(),
                    ec_params: None,
                },
            )
            .unwrap();
        store.delete_chunk_map("vol-1", 0).unwrap();
        assert!(store.get_chunk_map("vol-1", 0).unwrap().is_none());
    }
}
```

- [ ] **Step 2: Implement MetadataStore**

```rust
//! redb-backed local metadata store.
//!
//! Provides typed get/put/delete/list for volumes and chunk maps.
//! This is the persistence layer used by the Raft state machine.

use crate::error::{DataPlaneError, Result};
use crate::metadata::types::{ChunkMapEntry, VolumeDefinition};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;

/// Table: volume_id → JSON-encoded VolumeDefinition.
const VOLUMES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("volumes");

/// Table: "{volume_id}:{chunk_index:016x}" → JSON-encoded ChunkMapEntry.
const CHUNK_MAP_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("chunk_map");

/// Local metadata store backed by redb.
pub struct MetadataStore {
    db: Database,
}

impl MetadataStore {
    /// Open or create a metadata store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path.as_ref())
            .map_err(|e| DataPlaneError::MetadataError(format!("redb open: {e}")))?;

        // Create tables if they don't exist.
        {
            let txn = db
                .begin_write()
                .map_err(|e| DataPlaneError::MetadataError(format!("begin_write: {e}")))?;
            let _ = txn.open_table(VOLUMES_TABLE);
            let _ = txn.open_table(CHUNK_MAP_TABLE);
            txn.commit()
                .map_err(|e| DataPlaneError::MetadataError(format!("commit: {e}")))?;
        }

        Ok(Self { db })
    }

    pub fn put_volume(&self, vol: &VolumeDefinition) -> Result<()> {
        let data = serde_json::to_vec(vol)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
            table
                .insert(vol.id.as_str(), data.as_slice())
                .map_err(|e| DataPlaneError::MetadataError(format!("insert: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn get_volume(&self, volume_id: &str) -> Result<Option<VolumeDefinition>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_read: {e}")))?;
        let table = txn
            .open_table(VOLUMES_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
        match table.get(volume_id) {
            Ok(Some(value)) => {
                let vol: VolumeDefinition = serde_json::from_slice(value.value())?;
                Ok(Some(vol))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(DataPlaneError::MetadataError(format!("get: {e}"))),
        }
    }

    pub fn delete_volume(&self, volume_id: &str) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(VOLUMES_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
            let _ = table.remove(volume_id);
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn list_volumes(&self) -> Result<Vec<VolumeDefinition>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_read: {e}")))?;
        let table = txn
            .open_table(VOLUMES_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
        let mut volumes = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| DataPlaneError::MetadataError(format!("iter: {e}")))?;
        for entry in iter {
            let (_, value) = entry
                .map_err(|e| DataPlaneError::MetadataError(format!("iter entry: {e}")))?;
            let vol: VolumeDefinition = serde_json::from_slice(value.value())?;
            volumes.push(vol);
        }
        Ok(volumes)
    }

    /// Chunk map key: "{volume_id}:{chunk_index:016x}"
    fn chunk_map_key(volume_id: &str, chunk_index: u64) -> String {
        format!("{volume_id}:{chunk_index:016x}")
    }

    /// Chunk map prefix for range scans: "{volume_id}:"
    fn chunk_map_prefix(volume_id: &str) -> String {
        format!("{volume_id}:")
    }

    pub fn put_chunk_map(&self, volume_id: &str, entry: &ChunkMapEntry) -> Result<()> {
        let key = Self::chunk_map_key(volume_id, entry.chunk_index);
        let data = serde_json::to_vec(entry)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
            table
                .insert(key.as_str(), data.as_slice())
                .map_err(|e| DataPlaneError::MetadataError(format!("insert: {e}")))?;
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn get_chunk_map(
        &self,
        volume_id: &str,
        chunk_index: u64,
    ) -> Result<Option<ChunkMapEntry>> {
        let key = Self::chunk_map_key(volume_id, chunk_index);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_read: {e}")))?;
        let table = txn
            .open_table(CHUNK_MAP_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
        match table.get(key.as_str()) {
            Ok(Some(value)) => {
                let entry: ChunkMapEntry = serde_json::from_slice(value.value())?;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(DataPlaneError::MetadataError(format!("get: {e}"))),
        }
    }

    pub fn delete_chunk_map(&self, volume_id: &str, chunk_index: u64) -> Result<()> {
        let key = Self::chunk_map_key(volume_id, chunk_index);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_write: {e}")))?;
        {
            let mut table = txn
                .open_table(CHUNK_MAP_TABLE)
                .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;
            let _ = table.remove(key.as_str());
        }
        txn.commit()
            .map_err(|e| DataPlaneError::MetadataError(format!("commit: {e}")))?;
        Ok(())
    }

    pub fn list_chunk_map(&self, volume_id: &str) -> Result<Vec<ChunkMapEntry>> {
        let prefix = Self::chunk_map_prefix(volume_id);
        let txn = self
            .db
            .begin_read()
            .map_err(|e| DataPlaneError::MetadataError(format!("begin_read: {e}")))?;
        let table = txn
            .open_table(CHUNK_MAP_TABLE)
            .map_err(|e| DataPlaneError::MetadataError(format!("open table: {e}")))?;

        let mut entries = Vec::new();
        // redb range scan: all keys starting with "{volume_id}:"
        let prefix_end = {
            let mut p = prefix.clone();
            // Increment last char for exclusive upper bound.
            if let Some(last) = p.pop() {
                p.push((last as u8 + 1) as char);
            }
            p
        };
        let range = table
            .range(prefix.as_str()..prefix_end.as_str())
            .map_err(|e| DataPlaneError::MetadataError(format!("range: {e}")))?;
        for item in range {
            let (_, value) = item
                .map_err(|e| DataPlaneError::MetadataError(format!("range entry: {e}")))?;
            let entry: ChunkMapEntry = serde_json::from_slice(value.value())?;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Apply a MetadataRequest to the store. Used by the Raft state machine.
    pub fn apply(&self, req: &crate::metadata::types::MetadataRequest) -> Result<crate::metadata::types::MetadataResponse> {
        use crate::metadata::types::{MetadataRequest, MetadataResponse};
        match req {
            MetadataRequest::PutVolume(vol) => {
                self.put_volume(vol)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::DeleteVolume { volume_id } => {
                self.delete_volume(volume_id)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::PutChunkMap { volume_id, entry } => {
                self.put_chunk_map(volume_id, entry)?;
                Ok(MetadataResponse::Ok)
            }
            MetadataRequest::DeleteChunkMap {
                volume_id,
                chunk_index,
            } => {
                self.delete_chunk_map(volume_id, *chunk_index)?;
                Ok(MetadataResponse::Ok)
            }
        }
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib metadata::store`
Expected: All 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/store.rs
git commit -m "[Feature] Add redb-backed local metadata store"
```

---

## Chunk 3: Raft Integration

### Task 6: openraft type config and Raft state machine

**Files:**
- Create: `src/metadata/raft_types.rs`
- Create: `src/metadata/raft_store.rs`

openraft requires a type config (node ID type, log entry type, etc.) and implementations of `RaftLogStorage` and `RaftStateMachine`. The state machine delegates to our `MetadataStore`. The log storage uses a separate redb database.

- [ ] **Step 1: Implement openraft type config**

Create `src/metadata/raft_types.rs`:

```rust
//! openraft type configuration for the NovaStor metadata Raft.

use crate::metadata::types::{MetadataRequest, MetadataResponse};
use openraft::Config;

/// openraft type configuration.
///
/// Defines the concrete types used by our Raft instances:
/// - Node ID: u64
/// - Node: basic node info (address)
/// - Log entry: MetadataRequest
/// - State machine response: MetadataResponse
openraft::declare_raft_types!(
    pub TypeConfig:
        D = MetadataRequest,
        R = MetadataResponse,
        Node = RaftNode,
);

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

/// Create a default openraft config for metadata shards.
pub fn default_raft_config() -> Config {
    Config {
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
            address: "10.0.0.1".into(),
            port: 9100,
        };
        assert_eq!(format!("{node}"), "10.0.0.1:9100");
    }

    #[test]
    fn default_config_valid() {
        let config = default_raft_config();
        config.validate().expect("config should be valid");
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --lib metadata::raft_types`
Expected: 2 tests pass.

- [ ] **Step 3: Implement Raft log storage and state machine**

Create `src/metadata/raft_store.rs`. This is the most complex file — it implements openraft's storage traits using redb.

**Note to implementer:** The openraft API has evolved significantly. The exact trait methods depend on the version installed. After adding the `openraft` dependency, check the trait requirements by examining compiler errors and the openraft documentation. The implementation pattern is:

- **Log storage**: Stores Raft log entries, vote state, and committed index in a redb database. Tables: `raft_log` (log_index → serialized Entry), `raft_meta` (key → value for vote and commit state).
- **State machine**: Wraps our `MetadataStore`. On `apply()`, deserializes the log entry as `MetadataRequest` and delegates to `MetadataStore::apply()`. Snapshot = serialized dump of all volumes and chunk maps.

```rust
//! openraft storage implementations backed by redb.
//!
//! Two components:
//! - `LogStore`: Raft log entries + vote state persisted in redb.
//! - `StateMachine`: Wraps MetadataStore, applies committed entries.

use crate::error::{DataPlaneError, Result};
use crate::metadata::store::MetadataStore;
use crate::metadata::raft_types::TypeConfig;
use std::path::Path;
use std::sync::Arc;

/// Combined Raft storage for a single shard.
pub struct ShardStorage {
    /// The redb-backed metadata store (state machine data).
    pub state: Arc<MetadataStore>,
    /// Path to the shard's data directory.
    pub data_dir: std::path::PathBuf,
}

impl ShardStorage {
    /// Open or create storage for a shard.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| DataPlaneError::MetadataError(format!("create shard dir: {e}")))?;

        let state_path = data_dir.join("state.redb");
        let state = Arc::new(MetadataStore::open(state_path)?);

        Ok(Self { state, data_dir })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::*;

    #[test]
    fn shard_storage_open_creates_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard_00");
        let storage = ShardStorage::open(&shard_dir).unwrap();
        assert!(shard_dir.exists());

        // State machine should work.
        let vol = VolumeDefinition {
            id: "test".into(),
            name: "test".into(),
            size_bytes: 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 0,
            chunk_count: 0,
        };
        storage.state.put_volume(&vol).unwrap();
        assert!(storage.state.get_volume("test").unwrap().is_some());
    }

    #[test]
    fn shard_storage_apply_request() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ShardStorage::open(tmp.path().join("shard")).unwrap();

        let req = MetadataRequest::PutVolume(VolumeDefinition {
            id: "vol-1".into(),
            name: "volume one".into(),
            size_bytes: 10 * 1024 * 1024 * 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 1000,
            chunk_count: 0,
        });

        let resp = storage.state.apply(&req).unwrap();
        assert!(matches!(resp, MetadataResponse::Ok));

        let vol = storage.state.get_volume("vol-1").unwrap().unwrap();
        assert_eq!(vol.name, "volume one");
    }

    #[test]
    fn shard_storage_apply_chunk_map() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ShardStorage::open(tmp.path().join("shard")).unwrap();

        let req = MetadataRequest::PutChunkMap {
            volume_id: "vol-1".into(),
            entry: ChunkMapEntry {
                chunk_index: 42,
                chunk_id: "deadbeef".into(),
                ec_params: None,
            },
        };
        storage.state.apply(&req).unwrap();

        let entry = storage.state.get_chunk_map("vol-1", 42).unwrap().unwrap();
        assert_eq!(entry.chunk_id, "deadbeef");
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib metadata::raft_store`
Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/metadata/raft_types.rs src/metadata/raft_store.rs
git commit -m "[Feature] Add openraft type config and shard storage"
```

---

### Task 7: Shard manager

**Files:**
- Create: `src/metadata/shard.rs`

The shard manager routes operations to the correct shard (0-255) based on volume_id prefix, and manages shard storage instances.

- [ ] **Step 1: Write tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::*;

    #[test]
    fn shard_manager_routes_by_volume_id() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(tmp.path()).unwrap();

        // Volume "aa..." should go to shard 0xaa = 170.
        let vol = VolumeDefinition {
            id: "aa112233".into(),
            name: "test".into(),
            size_bytes: 1024,
            protection: Protection::Replication { factor: 3 },
            status: VolumeStatus::Available,
            created_at: 0,
            chunk_count: 0,
        };
        mgr.put_volume(&vol).unwrap();
        let got = mgr.get_volume("aa112233").unwrap().unwrap();
        assert_eq!(got.name, "test");
    }

    #[test]
    fn shard_manager_different_volumes_different_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(tmp.path()).unwrap();

        for prefix in &["00", "11", "ff"] {
            let vol = VolumeDefinition {
                id: format!("{prefix}aabbcc"),
                name: format!("vol-{prefix}"),
                size_bytes: 1024,
                protection: Protection::Replication { factor: 3 },
                status: VolumeStatus::Available,
                created_at: 0,
                chunk_count: 0,
            };
            mgr.put_volume(&vol).unwrap();
        }

        assert_eq!(
            mgr.get_volume("00aabbcc").unwrap().unwrap().name,
            "vol-00"
        );
        assert_eq!(
            mgr.get_volume("11aabbcc").unwrap().unwrap().name,
            "vol-11"
        );
        assert_eq!(
            mgr.get_volume("ffaabbcc").unwrap().unwrap().name,
            "vol-ff"
        );
    }

    #[test]
    fn shard_manager_chunk_map_operations() {
        let tmp = tempfile::tempdir().unwrap();
        let mgr = ShardManager::open(tmp.path()).unwrap();

        mgr.put_chunk_map(
            "ab001122",
            &ChunkMapEntry {
                chunk_index: 0,
                chunk_id: "chunk-0".into(),
                ec_params: None,
            },
        )
        .unwrap();

        let entry = mgr.get_chunk_map("ab001122", 0).unwrap().unwrap();
        assert_eq!(entry.chunk_id, "chunk-0");

        let list = mgr.list_chunk_map("ab001122").unwrap();
        assert_eq!(list.len(), 1);

        mgr.delete_chunk_map("ab001122", 0).unwrap();
        assert!(mgr.get_chunk_map("ab001122", 0).unwrap().is_none());
    }

    #[test]
    fn shard_count_is_256() {
        assert_eq!(SHARD_COUNT, 256);
    }
}
```

- [ ] **Step 2: Implement ShardManager**

```rust
//! Shard manager — routes metadata operations to the correct shard.
//!
//! 256 shards (first 2 hex chars of volume_id). Each shard has its own
//! redb database for isolation and lock-free parallel access.

use crate::error::Result;
use crate::metadata::raft_store::ShardStorage;
use crate::metadata::types::{self, ChunkMapEntry, VolumeDefinition};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const SHARD_COUNT: usize = 256;

/// Manages 256 metadata shards, each backed by its own redb store.
///
/// Shards are lazily initialized on first access to avoid creating 256
/// redb databases at startup when most may never be used.
pub struct ShardManager {
    base_dir: PathBuf,
    shards: Vec<Mutex<Option<ShardStorage>>>,
}

impl ShardManager {
    /// Open the shard manager. Creates the base directory if needed.
    /// Shards are initialized lazily on first access.
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_dir)
            .map_err(|e| crate::error::DataPlaneError::MetadataError(
                format!("create shard base dir: {e}")
            ))?;

        let shards = (0..SHARD_COUNT)
            .map(|_| Mutex::new(None))
            .collect();

        Ok(Self { base_dir, shards })
    }

    /// Get or initialize the shard for a volume_id.
    fn shard_for(&self, volume_id: &str) -> Result<std::sync::MutexGuard<'_, Option<ShardStorage>>> {
        let shard_id = types::shard_for_volume(volume_id) as usize;
        let mut guard = self.shards[shard_id]
            .lock()
            .map_err(|e| crate::error::DataPlaneError::MetadataError(
                format!("shard lock poisoned: {e}")
            ))?;
        if guard.is_none() {
            let shard_dir = self.base_dir.join(format!("shard_{shard_id:02x}"));
            *guard = Some(ShardStorage::open(shard_dir)?);
        }
        Ok(guard)
    }

    pub fn put_volume(&self, vol: &VolumeDefinition) -> Result<()> {
        let guard = self.shard_for(&vol.id)?;
        guard.as_ref().unwrap().state.put_volume(vol)
    }

    pub fn get_volume(&self, volume_id: &str) -> Result<Option<VolumeDefinition>> {
        let guard = self.shard_for(volume_id)?;
        guard.as_ref().unwrap().state.get_volume(volume_id)
    }

    pub fn delete_volume(&self, volume_id: &str) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard.as_ref().unwrap().state.delete_volume(volume_id)
    }

    pub fn put_chunk_map(&self, volume_id: &str, entry: &ChunkMapEntry) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard.as_ref().unwrap().state.put_chunk_map(volume_id, entry)
    }

    pub fn get_chunk_map(
        &self,
        volume_id: &str,
        chunk_index: u64,
    ) -> Result<Option<ChunkMapEntry>> {
        let guard = self.shard_for(volume_id)?;
        guard
            .as_ref()
            .unwrap()
            .state
            .get_chunk_map(volume_id, chunk_index)
    }

    pub fn delete_chunk_map(&self, volume_id: &str, chunk_index: u64) -> Result<()> {
        let guard = self.shard_for(volume_id)?;
        guard
            .as_ref()
            .unwrap()
            .state
            .delete_chunk_map(volume_id, chunk_index)
    }

    pub fn list_chunk_map(&self, volume_id: &str) -> Result<Vec<ChunkMapEntry>> {
        let guard = self.shard_for(volume_id)?;
        guard.as_ref().unwrap().state.list_chunk_map(volume_id)
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test --lib metadata::shard`
Expected: All 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/metadata/shard.rs
git commit -m "[Feature] Add shard manager with lazy 256-shard routing"
```

---

### Task 8: Docker verification

- [ ] **Step 1: Run all metadata tests locally**

Run: `cargo test --lib metadata`
Expected: All tests across all metadata modules pass.

- [ ] **Step 2: Run Docker build + test**

Run: `cd dataplane && DOCKER=podman make test`
Expected: All tests pass (53 from Plan 1 + new metadata tests).

- [ ] **Step 3: Fix any compilation or test issues**

If Docker build reveals issues (e.g., dependency resolution, platform-specific behavior), fix and re-run.

- [ ] **Step 4: Commit any fixes**

```bash
git add -A
git commit -m "[Fix] Address Docker build issues for metadata store"
```
