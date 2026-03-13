# Policy Engine Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Policy Engine layer that sits on top of chunks — tracking chunk locations, enforcing replication factors, and providing chunk operations (replicate/remove/move).

**Architecture:** The Policy Engine monitors chunk health by comparing actual locations against desired replication policy per-volume, then generates and executes corrective actions (replicate under-replicated chunks, remove excess replicas). It uses the existing CRUSH placement, gRPC transport, and metadata store.

**Tech Stack:** Rust, serde, tokio, existing ChunkStore/ChunkClient/MetadataStore

---

## Chunk 1: Types and Location Tracking

### Task 1: Policy types and chunk location metadata

**Files:**
- Create: `dataplane/src/policy/mod.rs`
- Create: `dataplane/src/policy/types.rs`
- Modify: `dataplane/src/main.rs` — add `mod policy;`
- Modify: `dataplane/src/error.rs` — add `PolicyError` variant

**Context:** The policy engine needs types to represent per-volume replication policy, chunk locations (which nodes hold each chunk), and reference counts (which volumes use each chunk). These are pure data types with serde serialization.

- [ ] **Step 1: Add PolicyError variant to error.rs**

Add `PolicyError(String)` to `DataPlaneError` enum.

- [ ] **Step 2: Create policy/types.rs with core types**

```rust
/// Per-volume replication/protection policy.
pub struct VolumePolicy {
    pub volume_id: String,
    pub desired_replicas: u32,         // target replication factor
    pub capacity_threshold_pct: u8,    // don't place on backends above this % full (default 90)
}

/// Tracks where a specific chunk is stored.
pub struct ChunkLocation {
    pub chunk_id: String,
    pub node_ids: Vec<String>,         // nodes currently holding this chunk
}

/// Reference: which volumes reference a chunk (for dedup-safe deletion).
pub struct ChunkRef {
    pub chunk_id: String,
    pub volume_ids: Vec<String>,       // volumes referencing this chunk
}

/// Actions the policy engine can take.
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
}

/// Health status of a chunk relative to its policy.
pub enum ChunkHealth {
    Healthy,
    UnderReplicated { actual: u32, desired: u32 },
    OverReplicated { actual: u32, desired: u32 },
}
```

All types must derive `Debug, Clone, Serialize, Deserialize`. Add comprehensive tests for serialization roundtrips.

- [ ] **Step 3: Create policy/mod.rs declaring submodules**

```rust
pub mod types;
```

- [ ] **Step 4: Add `mod policy;` to main.rs**

- [ ] **Step 5: Run tests, verify compilation**

Run: `cargo test`

- [ ] **Step 6: Commit**

```
[Feature] Add policy engine types and chunk location tracking
```

---

### Task 2: Chunk location store — persist and query chunk locations

**Files:**
- Create: `dataplane/src/policy/location_store.rs`
- Modify: `dataplane/src/policy/mod.rs` — add `pub mod location_store;`

**Context:** We need persistent storage for chunk locations and reference counts. Use the same redb pattern as `metadata/store.rs`. Two tables: `chunk_locations` (chunk_id → serialized ChunkLocation) and `chunk_refs` (chunk_id → serialized ChunkRef).

- [ ] **Step 1: Write tests for ChunkLocationStore**

Test put/get/delete/list for both chunk locations and chunk refs. Test updating an existing location (add/remove node). Test reference counting (add/remove volume ref).

- [ ] **Step 2: Implement ChunkLocationStore**

```rust
pub struct ChunkLocationStore {
    db: redb::Database,
}

impl ChunkLocationStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self>;
    pub fn put_location(&self, loc: &ChunkLocation) -> Result<()>;
    pub fn get_location(&self, chunk_id: &str) -> Result<Option<ChunkLocation>>;
    pub fn add_node_to_chunk(&self, chunk_id: &str, node_id: &str) -> Result<()>;
    pub fn remove_node_from_chunk(&self, chunk_id: &str, node_id: &str) -> Result<()>;
    pub fn list_locations(&self) -> Result<Vec<ChunkLocation>>;
    pub fn put_ref(&self, chunk_ref: &ChunkRef) -> Result<()>;
    pub fn get_ref(&self, chunk_id: &str) -> Result<Option<ChunkRef>>;
    pub fn add_volume_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()>;
    pub fn remove_volume_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()>;
    pub fn ref_count(&self, chunk_id: &str) -> Result<u32>;
}
```

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

```
[Feature] Add persistent chunk location and reference store
```

---

## Chunk 2: Policy Evaluation and Chunk Operations

### Task 3: Policy evaluator — scan chunks and generate corrective actions

**Files:**
- Create: `dataplane/src/policy/evaluator.rs`
- Modify: `dataplane/src/policy/mod.rs` — add `pub mod evaluator;`

**Context:** The evaluator takes a set of volume policies and chunk locations, compares actual vs desired state, and generates a list of PolicyActions. It uses CRUSH to determine target nodes for new replicas. Pure logic, no I/O.

- [ ] **Step 1: Write tests for PolicyEvaluator**

Test cases:
- Chunk with 1 replica, policy wants 3 → generates 2 Replicate actions
- Chunk with 4 replicas, policy wants 3 → generates 1 RemoveReplica
- Chunk with 3 replicas, policy wants 3 → no actions (Healthy)
- Multiple chunks, mixed health → correct aggregate actions
- No online nodes available for replication → no action generated (can't replicate)
- Capacity threshold: skip full backends

- [ ] **Step 2: Implement PolicyEvaluator**

```rust
pub struct PolicyEvaluator<'a> {
    topology: &'a ClusterMap,
}

impl<'a> PolicyEvaluator<'a> {
    pub fn new(topology: &'a ClusterMap) -> Self;

    /// Evaluate a single chunk's health against its volume policy.
    pub fn evaluate_chunk(
        &self,
        chunk_id: &str,
        location: &ChunkLocation,
        policy: &VolumePolicy,
    ) -> ChunkHealth;

    /// Generate corrective actions for a chunk based on its health.
    pub fn generate_actions(
        &self,
        chunk_id: &str,
        location: &ChunkLocation,
        policy: &VolumePolicy,
    ) -> Vec<PolicyAction>;

    /// Scan all chunk locations against their volume policies, returning all needed actions.
    pub fn evaluate_all(
        &self,
        locations: &[ChunkLocation],
        policies: &HashMap<String, VolumePolicy>,
        refs: &HashMap<String, ChunkRef>,
    ) -> Vec<PolicyAction>;
}
```

For `generate_actions`:
- Under-replicated: call `crush::select(chunk_id, desired, topology)` to get target nodes, filter out nodes that already hold the chunk, generate Replicate actions with source = first existing node.
- Over-replicated: pick nodes to remove (prefer nodes with highest utilization), generate RemoveReplica actions.

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

```
[Feature] Add policy evaluator for replication health checks
```

---

### Task 4: Chunk operations — replicate and remove replicas via gRPC

**Files:**
- Create: `dataplane/src/policy/operations.rs`
- Modify: `dataplane/src/policy/mod.rs` — add `pub mod operations;`

**Context:** Execute PolicyActions by reading chunks from source nodes and writing to target nodes via the existing gRPC ChunkClient, or deleting from target nodes. Uses the ChunkStore for local operations and ChunkClient for remote.

- [ ] **Step 1: Write tests for chunk operations**

Test with local file stores (no network needed):
- replicate_chunk: read from source store, write to target store, verify data matches
- remove_replica: delete from store, verify gone
- replicate_chunk with CRC verification: verify chunk integrity after copy
- Error: source chunk not found → proper error

- [ ] **Step 2: Implement ChunkOperations**

```rust
pub struct ChunkOperations {
    local_store: Arc<dyn ChunkStore>,
    connections: Mutex<HashMap<String, ChunkClient>>,
}

impl ChunkOperations {
    pub fn new(local_store: Arc<dyn ChunkStore>) -> Self;

    /// Replicate a chunk from source to target node.
    pub async fn replicate_chunk(
        &self,
        chunk_id: &str,
        source_addr: &str,
        target_addr: &str,
        local_node_id: &str,
    ) -> Result<()>;

    /// Remove a chunk replica from a node.
    pub async fn remove_replica(
        &self,
        chunk_id: &str,
        node_addr: &str,
        local_node_id: &str,
    ) -> Result<()>;
}
```

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

```
[Feature] Add chunk replication and removal operations
```

---

### Task 5: Policy engine — orchestrate evaluation and execution

**Files:**
- Create: `dataplane/src/policy/engine.rs`
- Modify: `dataplane/src/policy/mod.rs` — add `pub mod engine;`

**Context:** The PolicyEngine ties everything together: it holds the location store, evaluator, and operations, and provides a `reconcile()` method that scans all chunks and executes corrective actions. No background loop yet — just the reconcile method that can be called on demand (e.g., from JSON-RPC or a timer).

- [ ] **Step 1: Write tests for PolicyEngine**

Test full reconciliation cycle:
- Write chunks to local store, register locations with only 1 replica
- Set policy to 3 replicas
- Call reconcile()
- Verify correct Replicate actions were generated
- Test with already-healthy chunks → no actions
- Test with over-replicated chunks → RemoveReplica actions

- [ ] **Step 2: Implement PolicyEngine**

```rust
pub struct PolicyEngine {
    node_id: String,
    location_store: Arc<ChunkLocationStore>,
    operations: ChunkOperations,
    topology: RwLock<ClusterMap>,
    policies: RwLock<HashMap<String, VolumePolicy>>,
}

impl PolicyEngine {
    pub fn new(
        node_id: String,
        location_store: Arc<ChunkLocationStore>,
        local_store: Arc<dyn ChunkStore>,
        topology: ClusterMap,
    ) -> Self;

    /// Register or update a volume's replication policy.
    pub async fn set_policy(&self, policy: VolumePolicy);

    /// Update the cluster topology.
    pub async fn update_topology(&self, topology: ClusterMap);

    /// Record that a chunk is stored on a node.
    pub fn record_chunk_location(&self, chunk_id: &str, node_id: &str) -> Result<()>;

    /// Record that a volume references a chunk.
    pub fn record_chunk_ref(&self, chunk_id: &str, volume_id: &str) -> Result<()>;

    /// Run one reconciliation pass: evaluate all chunks, execute corrective actions.
    /// Returns the number of actions taken.
    pub async fn reconcile(&self) -> Result<usize>;
}
```

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

```
[Feature] Add policy engine with reconciliation loop
```

---

### Task 6: Wire ChunkEngine writes into policy tracking

**Files:**
- Modify: `dataplane/src/chunk/engine.rs` — record locations and refs after writes

**Context:** After ChunkEngine::write() places a chunk, it should record the chunk's location and volume reference in the policy engine's stores. This wires the chunk engine into the policy layer so the policy engine has accurate data to evaluate.

- [ ] **Step 1: Add optional PolicyEngine reference to ChunkEngine**

Add `policy: Option<Arc<PolicyEngine>>` field. When present, write() records locations and refs.

- [ ] **Step 2: Update write() to record location and ref**

After successful put (local or remote), call:
```rust
if let Some(policy) = &self.policy {
    policy.record_chunk_location(&chunk_id, target_node)?;
    policy.record_chunk_ref(&chunk_id, volume_id)?;
}
```

- [ ] **Step 3: Update tests**

Existing tests should still pass (policy=None). Add one test with policy=Some showing that locations and refs are recorded.

- [ ] **Step 4: Run all tests**

- [ ] **Step 5: Commit**

```
[Feature] Wire chunk engine writes into policy location tracking
```

---

### Task 7: Docker build verification and final cleanup

**Files:**
- Verify: `dataplane/Dockerfile.build` — no changes needed (redb already in Cargo.lock)

- [ ] **Step 1: Run cargo fmt**

- [ ] **Step 2: Run local tests**

Run: `cargo test`

- [ ] **Step 3: Run Docker tests**

Run: `cd /Users/pascal/Development/Nova/novastor/.worktrees/policy-engine/dataplane && DOCKER=podman make test`

- [ ] **Step 4: Commit any formatting fixes**

```
[Chore] Format policy engine code
```
