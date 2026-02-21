# SPDK Active-Passive Failover Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable sub-500ms HA failover for replicated block volumes using kernel ANA multipath with SPDK NVMe-oF targets on every replica agent.

**Architecture:** All replica agents export NVMe-oF targets with ANA states. The write-owner agent runs an active replica bdev for write fan-out; other agents run standby bdevs. On write-owner failure, the kernel retries I/O on a surviving path within ~200ms. Metadata service manages ownership with quorum-fenced promotion. Background ANA state updates formalize the new preferred path.

**Tech Stack:** Go 1.24+, Rust (SPDK data plane), gRPC/protobuf, Kubernetes, NVMe-oF/TCP, kernel NVMe multipath

**Issue:** #147

---

## Task 1: Extend NVMe Proto with ANA Fields

**Files:**
- Modify: `api/proto/nvme/nvme.proto`
- Regenerate: `internal/proto/gen/nvme/`

**Step 1: Add ANA fields to proto**

In `api/proto/nvme/nvme.proto`, add ANA fields to `CreateTargetRequest` and new `SetANAState` RPC:

```protobuf
service NVMeTargetService {
  rpc CreateTarget(CreateTargetRequest) returns (CreateTargetResponse);
  rpc DeleteTarget(DeleteTargetRequest) returns (DeleteTargetResponse);
  rpc SetANAState(SetANAStateRequest) returns (SetANAStateResponse);      // NEW
}

message CreateTargetRequest {
  string volume_id = 1;
  int64 size_bytes = 2;
  string ana_state = 3;      // NEW: "optimized" or "non_optimized"
  uint32 ana_group_id = 4;   // NEW: consistent per-volume across agents
}

message SetANAStateRequest {
  string volume_id = 1;
  string ana_state = 2;      // "optimized", "non_optimized", "inaccessible"
  uint32 ana_group_id = 3;
}

message SetANAStateResponse {}
```

**Step 2: Regenerate protobuf Go code**

Run: `make generate-proto`
Expected: Files in `internal/proto/gen/nvme/` regenerated with new fields and RPC.

**Step 3: Verify compilation**

Run: `go build ./internal/proto/...`
Expected: PASS

**Step 4: Commit**

```bash
git add api/proto/nvme/nvme.proto internal/proto/gen/
git commit -m "[Feature] Extend NVMe proto with ANA state fields and SetANAState RPC"
```

---

## Task 2: Rust Data Plane — ANA State on NVMe-oF Targets

**Files:**
- Modify: `dataplane/src/spdk/nvmf_manager.rs`
- Modify: `dataplane/src/jsonrpc/methods.rs`
- Test: `cargo test` in `dataplane/`

**Step 1: Add ANA fields to NvmfManager**

In `dataplane/src/spdk/nvmf_manager.rs`, add `ana_group_id` and `ana_state` fields to `SubsystemInfo` (line 10):

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemInfo {
    pub nqn: String,
    pub bdev_name: String,
    pub listen_address: String,
    pub listen_port: u16,
    pub ana_group_id: u32,      // NEW
    pub ana_state: String,      // NEW: "optimized", "non_optimized", "inaccessible"
}
```

Update `create_target()` to accept and store ANA state. Add a `set_ana_state()` method and a `get_ana_state()` method:

```rust
pub fn create_target(&self, config: &NvmfTargetConfig) -> Result<SubsystemInfo> {
    // ... existing logic ...
    // Store ana_group_id and ana_state from config
    // In SPDK FFI mode: call spdk_nvmf_subsystem_set_ana_state()
}

pub fn set_ana_state(&self, nqn: &str, ana_group_id: u32, ana_state: &str) -> Result<()> {
    let mut subs = self.subsystems.lock().unwrap();
    if let Some(info) = subs.get_mut(nqn) {
        info.ana_group_id = ana_group_id;
        info.ana_state = ana_state.to_string();
        // In SPDK FFI mode: call spdk_nvmf_subsystem_set_ana_state()
        Ok(())
    } else {
        Err(DataPlaneError::NvmfTargetError(format!("subsystem not found: {}", nqn)))
    }
}

pub fn get_ana_state(&self, nqn: &str) -> Result<(u32, String)> {
    let subs = self.subsystems.lock().unwrap();
    if let Some(info) = subs.get(nqn) {
        Ok((info.ana_group_id, info.ana_state.clone()))
    } else {
        Err(DataPlaneError::NvmfTargetError(format!("subsystem not found: {}", nqn)))
    }
}
```

Add `ana_group_id` and `ana_state` fields to `NvmfTargetConfig` as well (in `config.rs` or inline).

**Step 2: Register JSON-RPC methods**

In `dataplane/src/jsonrpc/methods.rs`, add handlers and register in `register_all()`:

```rust
// Add to register_all():
router.register("nvmf_set_ana_state", wrap(handle_nvmf_set_ana_state));
router.register("nvmf_get_ana_state", wrap(handle_nvmf_get_ana_state));
```

Implement handlers:

```rust
#[derive(Deserialize)]
struct SetAnaStateParams {
    nqn: String,
    ana_group_id: u32,
    ana_state: String,
}

fn handle_nvmf_set_ana_state(params: SetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or_else(|| DataPlaneError::SpdkInit("not initialized".into()))?;
    mgr.set_ana_state(&params.nqn, params.ana_group_id, &params.ana_state)?;
    Ok(json!({}))
}

#[derive(Deserialize)]
struct GetAnaStateParams {
    nqn: String,
}

fn handle_nvmf_get_ana_state(params: GetAnaStateParams) -> Result<serde_json::Value, DataPlaneError> {
    let mgr = NVMF_MANAGER.get().ok_or_else(|| DataPlaneError::SpdkInit("not initialized".into()))?;
    let (group_id, state) = mgr.get_ana_state(&params.nqn)?;
    Ok(json!({"ana_group_id": group_id, "ana_state": state}))
}
```

Also update `handle_nvmf_create_target` to pass ANA fields through.

**Step 3: Run tests**

Run: `cd dataplane && cargo test`
Expected: PASS (existing tests still pass, new code compiles)

**Step 4: Commit**

```bash
git add dataplane/src/
git commit -m "[Feature] Add ANA state management to SPDK NVMe-oF targets"
```

---

## Task 3: Rust Data Plane — Dynamic Replica Bdev Add/Remove

**Files:**
- Modify: `dataplane/src/bdev/replica.rs`
- Modify: `dataplane/src/jsonrpc/methods.rs`

**Step 1: Write tests for add/remove replica**

In `dataplane/src/bdev/replica.rs`, add tests at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_targets() -> Vec<ReplicaTarget> {
        vec![
            ReplicaTarget { address: "10.0.0.1".into(), port: 4420, nqn: "nqn-1".into() },
            ReplicaTarget { address: "10.0.0.2".into(), port: 4420, nqn: "nqn-2".into() },
        ]
    }

    #[test]
    fn test_add_replica() {
        let bdev = ReplicaBdev::new("vol-1", test_targets(), 2, ReadPolicy::RoundRobin);
        assert_eq!(bdev.replicas.len(), 2);

        let new_target = ReplicaTarget { address: "10.0.0.3".into(), port: 4420, nqn: "nqn-3".into() };
        bdev.add_replica(new_target).unwrap();
        assert_eq!(bdev.replicas.len(), 3);
    }

    #[test]
    fn test_remove_replica() {
        let bdev = ReplicaBdev::new("vol-1", test_targets(), 2, ReadPolicy::RoundRobin);
        bdev.remove_replica("10.0.0.2").unwrap();
        assert_eq!(bdev.replicas.len(), 1);
    }

    #[test]
    fn test_remove_last_replica_fails() {
        let targets = vec![ReplicaTarget { address: "10.0.0.1".into(), port: 4420, nqn: "nqn-1".into() }];
        let bdev = ReplicaBdev::new("vol-1", targets, 1, ReadPolicy::RoundRobin);
        assert!(bdev.remove_replica("10.0.0.1").is_err());
    }

    #[test]
    fn test_replica_status_enhanced() {
        let bdev = ReplicaBdev::new("vol-1", test_targets(), 2, ReadPolicy::RoundRobin);
        let status = bdev.full_status();
        assert_eq!(status.healthy_count, 2);
        assert_eq!(status.write_quorum, 2);
        assert_eq!(status.replicas.len(), 2);
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `cd dataplane && cargo test`
Expected: FAIL — `add_replica`, `remove_replica`, `full_status` not defined

**Step 3: Implement add/remove/status on ReplicaBdev**

In `dataplane/src/bdev/replica.rs`, change `replicas` from `Vec<Arc<Replica>>` to `RwLock<Vec<Arc<Replica>>>` and add methods:

```rust
pub struct ReplicaBdev {
    pub volume_id: String,
    pub replicas: RwLock<Vec<Arc<Replica>>>,  // CHANGED: RwLock for dynamic add/remove
    pub write_quorum: u32,
    pub read_policy: ReadPolicy,
    read_index: AtomicU32,
    pub bdev_name: String,
}

impl ReplicaBdev {
    pub fn add_replica(&self, target: ReplicaTarget) -> Result<(), DataPlaneError> {
        let replica = Arc::new(Replica::new(target, format!("nvme-{}", self.replicas.read().unwrap().len())));
        self.replicas.write().unwrap().push(replica);
        Ok(())
    }

    pub fn remove_replica(&self, addr: &str) -> Result<(), DataPlaneError> {
        let mut replicas = self.replicas.write().unwrap();
        if replicas.len() <= 1 {
            return Err(DataPlaneError::ReplicaError("cannot remove last replica".into()));
        }
        replicas.retain(|r| r.target.address != addr);
        Ok(())
    }

    pub fn full_status(&self) -> ReplicaBdevStatus {
        let replicas = self.replicas.read().unwrap();
        let healthy_count = replicas.iter().filter(|r| r.is_healthy()).count() as u32;
        ReplicaBdevStatus {
            volume_id: self.volume_id.clone(),
            replicas: replicas.iter().map(|r| r.status_info()).collect(),
            write_quorum: self.write_quorum,
            healthy_count,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ReplicaBdevStatus {
    pub volume_id: String,
    pub replicas: Vec<ReplicaStatusInfo>,
    pub write_quorum: u32,
    pub healthy_count: u32,
}
```

Update all existing methods that read `self.replicas` to use `.read().unwrap()`.

**Step 4: Register JSON-RPC handlers**

In `methods.rs`, add `replica_bdev_add_replica`, `replica_bdev_remove_replica`, and enhance `replica_bdev_status`. This requires a global registry of active replica bdevs:

```rust
static REPLICA_BDEVS: OnceLock<Mutex<HashMap<String, Arc<ReplicaBdev>>>> = OnceLock::new();
```

Register in `register_all()`:
```rust
router.register("replica_bdev_add_replica", wrap(handle_replica_add));
router.register("replica_bdev_remove_replica", wrap(handle_replica_remove));
// Update existing replica_bdev_status to use full_status()
```

**Step 5: Run tests**

Run: `cd dataplane && cargo test`
Expected: PASS

**Step 6: Commit**

```bash
git add dataplane/src/
git commit -m "[Feature] Add dynamic replica add/remove and enhanced status to replica bdev"
```

---

## Task 4: Go SPDK Client — New ANA and Replica Methods

**Files:**
- Modify: `internal/spdk/client.go`
- Create: `internal/spdk/client_test.go` (if not exists, or add tests)

**Step 1: Add new types and methods to client.go**

After existing `ReplicaTarget` struct (~line 243), add:

```go
// ReplicaBdevStatus represents the status of a replica bdev.
type ReplicaBdevStatus struct {
    VolumeID     string              `json:"volume_id"`
    Replicas     []ReplicaStatusInfo `json:"replicas"`
    WriteQuorum  uint32              `json:"write_quorum"`
    HealthyCount uint32              `json:"healthy_count"`
}

type ReplicaStatusInfo struct {
    Address         string `json:"address"`
    Port            uint16 `json:"port"`
    State           string `json:"state"`
    ReadsCompleted  uint64 `json:"reads_completed"`
    WritesCompleted uint64 `json:"writes_completed"`
    ReadErrors      uint64 `json:"read_errors"`
    WriteErrors     uint64 `json:"write_errors"`
}
```

After existing `GetVersion()` method (~line 265), add:

```go
func (c *Client) SetANAState(nqn string, anaGroupID uint32, state string) error {
    return c.call("nvmf_set_ana_state", map[string]interface{}{
        "nqn":          nqn,
        "ana_group_id": anaGroupID,
        "ana_state":    state,
    }, nil)
}

func (c *Client) GetANAState(nqn string) (uint32, string, error) {
    var result struct {
        GroupID uint32 `json:"ana_group_id"`
        State   string `json:"ana_state"`
    }
    if err := c.call("nvmf_get_ana_state", map[string]interface{}{"nqn": nqn}, &result); err != nil {
        return 0, "", err
    }
    return result.GroupID, result.State, nil
}

func (c *Client) AddReplica(bdevName string, target ReplicaTarget) error {
    return c.call("replica_bdev_add_replica", map[string]interface{}{
        "name":   bdevName,
        "target": target,
    }, nil)
}

func (c *Client) RemoveReplica(bdevName, targetAddr string) error {
    return c.call("replica_bdev_remove_replica", map[string]interface{}{
        "name":        bdevName,
        "target_addr": targetAddr,
    }, nil)
}

func (c *Client) GetReplicaBdevStatus(bdevName string) (*ReplicaBdevStatus, error) {
    var result ReplicaBdevStatus
    if err := c.call("replica_bdev_status", map[string]interface{}{"name": bdevName}, &result); err != nil {
        return nil, err
    }
    return &result, nil
}
```

**Step 2: Verify compilation**

Run: `go build ./internal/spdk/...`
Expected: PASS

**Step 3: Commit**

```bash
git add internal/spdk/client.go
git commit -m "[Feature] Add ANA state and dynamic replica methods to SPDK client"
```

---

## Task 5: Metadata Service — Volume Ownership

**Files:**
- Create: `internal/metadata/ownership.go`
- Create: `internal/metadata/ownership_test.go`
- Modify: `internal/metadata/store.go` (add FSM bucket + operations)
- Modify: `internal/metadata/grpc_server.go` (add handlers)
- Modify: `internal/metadata/grpc_client.go` (add client methods)

**Step 1: Write failing tests for ownership operations**

Create `internal/metadata/ownership_test.go`:

```go
package metadata

import (
    "testing"
    "time"
)

func TestSetAndGetVolumeOwner(t *testing.T) {
    store := setupTestStore(t)
    defer store.Close()

    owner := &VolumeOwnership{
        VolumeID:   "vol-1",
        OwnerAddr:  "10.0.0.1:9100",
        OwnerSince: time.Now().Unix(),
        Generation: 1,
    }
    if err := store.SetVolumeOwner(owner); err != nil {
        t.Fatalf("SetVolumeOwner: %v", err)
    }

    got, err := store.GetVolumeOwner("vol-1")
    if err != nil {
        t.Fatalf("GetVolumeOwner: %v", err)
    }
    if got.OwnerAddr != "10.0.0.1:9100" {
        t.Fatalf("expected owner 10.0.0.1:9100, got %s", got.OwnerAddr)
    }
    if got.Generation != 1 {
        t.Fatalf("expected generation 1, got %d", got.Generation)
    }
}

func TestRequestOwnership_GrantsWhenOwnerDead(t *testing.T) {
    store := setupTestStore(t)
    defer store.Close()

    // Set initial owner
    store.SetVolumeOwner(&VolumeOwnership{
        VolumeID: "vol-1", OwnerAddr: "10.0.0.1:9100", Generation: 1,
    })

    // Mark owner as dead via stale heartbeat
    store.PutNodeMeta(&NodeMeta{
        NodeID: "node-1", Address: "10.0.0.1:9100",
        LastHeartbeat: time.Now().Add(-10 * time.Second).Unix(),
        Status: "offline",
    })

    // Mark requester as alive
    store.PutNodeMeta(&NodeMeta{
        NodeID: "node-2", Address: "10.0.0.2:9100",
        LastHeartbeat: time.Now().Unix(),
        Status: "ready",
    })

    granted, gen, err := store.RequestOwnership("vol-1", "10.0.0.2:9100")
    if err != nil {
        t.Fatalf("RequestOwnership: %v", err)
    }
    if !granted {
        t.Fatal("expected ownership granted")
    }
    if gen != 2 {
        t.Fatalf("expected generation 2, got %d", gen)
    }
}

func TestRequestOwnership_DeniedWhenOwnerAlive(t *testing.T) {
    store := setupTestStore(t)
    defer store.Close()

    store.SetVolumeOwner(&VolumeOwnership{
        VolumeID: "vol-1", OwnerAddr: "10.0.0.1:9100", Generation: 1,
    })
    store.PutNodeMeta(&NodeMeta{
        NodeID: "node-1", Address: "10.0.0.1:9100",
        LastHeartbeat: time.Now().Unix(), Status: "ready",
    })

    granted, _, err := store.RequestOwnership("vol-1", "10.0.0.2:9100")
    if err != nil {
        t.Fatalf("RequestOwnership: %v", err)
    }
    if granted {
        t.Fatal("expected ownership denied (current owner alive)")
    }
}
```

**Step 2: Run tests to verify they fail**

Run: `go test ./internal/metadata/ -run TestSetAndGetVolumeOwner -v`
Expected: FAIL — `VolumeOwnership` type and methods not defined

**Step 3: Implement ownership types and store operations**

Create `internal/metadata/ownership.go`:

```go
package metadata

import (
    "encoding/json"
    "fmt"
    "time"
)

const (
    bucketVolumeOwners = "volume-owners"
    deadThreshold      = 2 * time.Second // Agent dead if no heartbeat for 2s
)

type VolumeOwnership struct {
    VolumeID   string `json:"volume_id"`
    OwnerAddr  string `json:"owner_addr"`
    OwnerSince int64  `json:"owner_since"`
    Generation uint64 `json:"generation"`
}

func (s *RaftStore) SetVolumeOwner(ownership *VolumeOwnership) error {
    data, err := json.Marshal(ownership)
    if err != nil {
        return fmt.Errorf("marshal ownership: %w", err)
    }
    return s.apply(fsmOp{
        Op:     "put",
        Bucket: bucketVolumeOwners,
        Key:    ownership.VolumeID,
        Value:  data,
    })
}

func (s *RaftStore) GetVolumeOwner(volumeID string) (*VolumeOwnership, error) {
    data, err := s.fsm.Get(bucketVolumeOwners, volumeID)
    if err != nil {
        return nil, fmt.Errorf("get volume owner: %w", err)
    }
    if data == nil {
        return nil, nil
    }
    var ownership VolumeOwnership
    if err := json.Unmarshal(data, &ownership); err != nil {
        return nil, fmt.Errorf("unmarshal ownership: %w", err)
    }
    return &ownership, nil
}

func (s *RaftStore) RequestOwnership(volumeID, requesterAddr string) (granted bool, generation uint64, err error) {
    current, err := s.GetVolumeOwner(volumeID)
    if err != nil {
        return false, 0, fmt.Errorf("get current owner: %w", err)
    }

    // If there's a current owner, check if it's dead
    if current != nil && current.OwnerAddr != "" {
        ownerNode, err := s.findNodeByAddr(current.OwnerAddr)
        if err == nil && ownerNode != nil {
            lastHB := time.Unix(ownerNode.LastHeartbeat, 0)
            if time.Since(lastHB) < deadThreshold && ownerNode.Status != "offline" {
                return false, current.Generation, nil // Owner still alive
            }
        }
    }

    // Grant ownership
    newGen := uint64(1)
    if current != nil {
        newGen = current.Generation + 1
    }

    ownership := &VolumeOwnership{
        VolumeID:   volumeID,
        OwnerAddr:  requesterAddr,
        OwnerSince: time.Now().Unix(),
        Generation: newGen,
    }
    if err := s.SetVolumeOwner(ownership); err != nil {
        return false, 0, fmt.Errorf("set new owner: %w", err)
    }
    return true, newGen, nil
}

func (s *RaftStore) findNodeByAddr(addr string) (*NodeMeta, error) {
    nodes, err := s.ListNodeMetas()
    if err != nil {
        return nil, err
    }
    for _, n := range nodes {
        if n.Address == addr {
            return n, nil
        }
    }
    return nil, nil
}
```

**Step 4: Add FSM bucket initialization**

In `internal/metadata/store.go`, add `bucketVolumeOwners` to the bucket initialization list (wherever `bucketNodes` etc. are created in the FSM).

**Step 5: Run tests**

Run: `go test ./internal/metadata/ -run TestSetAndGetVolumeOwner -v`
Run: `go test ./internal/metadata/ -run TestRequestOwnership -v`
Expected: PASS

**Step 6: Add gRPC server handlers**

In `internal/metadata/grpc_server.go`, add cases to the `Execute` switch:

```go
case "SetVolumeOwner":
    return s.handleSetVolumeOwner(ctx, req.Payload)
case "GetVolumeOwner":
    return s.handleGetVolumeOwner(ctx, req.Payload)
case "RequestOwnership":
    return s.handleRequestOwnership(ctx, req.Payload)
```

Implement each handler following the existing pattern (unmarshal payload → call store method → return result).

**Step 7: Add gRPC client methods**

In `internal/metadata/grpc_client.go`, add:

```go
func (c *GRPCClient) SetVolumeOwner(ctx context.Context, ownership *VolumeOwnership) error {
    _, err := c.exec(ctx, "SetVolumeOwner", ownership)
    return err
}

func (c *GRPCClient) GetVolumeOwner(ctx context.Context, volumeID string) (*VolumeOwnership, error) {
    data, err := c.exec(ctx, "GetVolumeOwner", map[string]string{"volume_id": volumeID})
    if err != nil {
        return nil, err
    }
    if data == nil {
        return nil, nil
    }
    var ownership VolumeOwnership
    if err := json.Unmarshal(data, &ownership); err != nil {
        return nil, fmt.Errorf("unmarshal ownership: %w", err)
    }
    return &ownership, nil
}

func (c *GRPCClient) RequestOwnership(ctx context.Context, volumeID, requesterAddr string) (bool, uint64, error) {
    data, err := c.exec(ctx, "RequestOwnership", map[string]string{
        "volume_id":      volumeID,
        "requester_addr": requesterAddr,
    })
    if err != nil {
        return false, 0, err
    }
    var result struct {
        Granted    bool   `json:"granted"`
        Generation uint64 `json:"generation"`
    }
    if err := json.Unmarshal(data, &result); err != nil {
        return false, 0, err
    }
    return result.Granted, result.Generation, nil
}
```

**Step 8: Run full metadata tests**

Run: `go test ./internal/metadata/ -v`
Expected: All PASS

**Step 9: Commit**

```bash
git add internal/metadata/
git commit -m "[Feature] Add volume ownership management to metadata service"
```

---

## Task 6: Agent Target Server — ANA-Aware CreateTarget and SetANAState

**Files:**
- Modify: `internal/agent/spdk_target_server.go`
- Modify: `internal/agent/spdk_target_client.go`
- Create: `internal/agent/spdk_target_server_test.go` (if not exists, add ANA tests)

**Step 1: Update SPDKTargetServer.CreateTarget for ANA**

In `internal/agent/spdk_target_server.go`, update `CreateTarget` (~line 61) to:
1. Read `ana_state` and `ana_group_id` from the request
2. Pass them to `spdkClient.CreateNvmfTarget()` — update the call to include ANA params
3. After target creation, call `spdkClient.SetANAState(nqn, anaGroupID, anaState)`

```go
func (s *SPDKTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
    // ... existing validation ...

    // Create logical volume (unchanged)
    lvolName, err := s.spdkClient.CreateLvol(spdkLvsName, volumeID, sizeMB)
    // ...

    // Create NVMe-oF target (unchanged)
    if err := s.spdkClient.CreateNvmfTarget(nqn, "0.0.0.0", spdkTargetPort, lvolName); err != nil {
        // ... cleanup ...
    }

    // NEW: Set ANA state if specified
    anaState := req.GetAnaState()
    anaGroupID := req.GetAnaGroupId()
    if anaState != "" && anaGroupID > 0 {
        if err := s.spdkClient.SetANAState(nqn, anaGroupID, anaState); err != nil {
            logger.Warn("failed to set ANA state", zap.Error(err))
            // Non-fatal: target works without ANA
        }
    }

    return &pb.CreateTargetResponse{
        SubsystemNqn:  nqn,
        TargetAddress: s.hostIP,
        TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
    }, nil
}
```

**Step 2: Implement SetANAState RPC**

Add to `SPDKTargetServer`:

```go
func (s *SPDKTargetServer) SetANAState(ctx context.Context, req *pb.SetANAStateRequest) (*pb.SetANAStateResponse, error) {
    volumeID := req.GetVolumeId()
    if volumeID == "" {
        return nil, status.Error(codes.InvalidArgument, "volume_id required")
    }

    nqn := spdkNQNPrefix + volumeID
    if err := s.spdkClient.SetANAState(nqn, req.GetAnaGroupId(), req.GetAnaState()); err != nil {
        return nil, status.Errorf(codes.Internal, "set ANA state: %v", err)
    }

    return &pb.SetANAStateResponse{}, nil
}
```

**Step 3: Update client**

In `internal/agent/spdk_target_client.go`, add `SetANAState` method and update `CreateTarget` to accept ANA params:

```go
func (c *SPDKTargetClient) CreateTarget(ctx context.Context, agentAddr, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32) (string, string, string, error) {
    // ... existing dial logic ...
    resp, err := client.CreateTarget(ctx, &pb.CreateTargetRequest{
        VolumeId:   volumeID,
        SizeBytes:  sizeBytes,
        AnaState:   anaState,
        AnaGroupId: anaGroupID,
    })
    // ...
}

func (c *SPDKTargetClient) SetANAState(ctx context.Context, agentAddr, volumeID, anaState string, anaGroupID uint32) error {
    conn, err := grpc.NewClient(agentAddr, c.dialOpts...)
    if err != nil {
        return fmt.Errorf("dial agent %s: %w", agentAddr, err)
    }
    defer conn.Close()

    client := pb.NewNVMeTargetServiceClient(conn)
    _, err = client.SetANAState(ctx, &pb.SetANAStateRequest{
        VolumeId:   volumeID,
        AnaState:   anaState,
        AnaGroupId: anaGroupID,
    })
    return err
}
```

**Step 4: Verify compilation**

Run: `go build ./internal/agent/... ./cmd/agent/...`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/agent/ internal/proto/
git commit -m "[Feature] Add ANA state support to agent target server and client"
```

---

## Task 7: CSI Controller — Multi-Target Provisioning

**Files:**
- Modify: `internal/csi/controller.go`
- Modify: `internal/csi/controller_test.go`

**Step 1: Write test for multi-target creation**

In `internal/csi/controller_test.go`, add a test that verifies CreateVolume creates targets on all replica agents and stores all addresses in the volume context.

**Step 2: Update AgentTargetClient interface**

In `internal/csi/controller.go`, the `AgentTargetClient` interface (~line 78) needs to pass ANA params:

```go
type AgentTargetClient interface {
    CreateTarget(ctx context.Context, agentAddr, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32) (subsystemNQN, targetAddress, targetPort string, err error)
    DeleteTarget(ctx context.Context, agentAddr, volumeID string) error
    SetANAState(ctx context.Context, agentAddr, volumeID, anaState string, anaGroupID uint32) error
}
```

**Step 3: Update CreateVolume for multi-target**

In `CreateVolume` (~line 483), replace the single-target creation with multi-target:

```go
// Compute ANA group ID from volume ID (consistent across all agents)
anaGroupID := crc32.ChecksumIEEE([]byte(volumeID)) & 0xFF

// Create targets on ALL replica agents in parallel
type targetResult struct {
    nqn  string
    addr string
    port string
    err  error
}
results := make([]targetResult, len(nodeIDs))
g, gCtx := errgroup.WithContext(ctx)
for i, nodeAddr := range nodeIDs {
    i, nodeAddr := i, nodeAddr
    anaState := "non_optimized"
    if i == 0 {
        anaState = "optimized" // First node is write-owner
    }
    g.Go(func() error {
        nqn, addr, port, err := cs.agentTarget.CreateTarget(
            gCtx, nodeAddr, volumeID, int64(requiredBytes), anaState, anaGroupID,
        )
        results[i] = targetResult{nqn, addr, port, err}
        return err
    })
}
if err := g.Wait(); err != nil {
    return nil, status.Errorf(codes.Internal, "create targets: %v", err)
}

// Build target addresses JSON for volume context
type targetInfo struct {
    Addr    string `json:"addr"`
    Port    string `json:"port"`
    NQN     string `json:"nqn"`
    IsOwner bool   `json:"is_owner"`
}
targets := make([]targetInfo, len(results))
for i, r := range results {
    targets[i] = targetInfo{Addr: r.addr, Port: r.port, NQN: r.nqn, IsOwner: i == 0}
}
targetsJSON, _ := json.Marshal(targets)

volContext := map[string]string{
    "targetAddresses": string(targetsJSON),
    "writeOwner":      results[0].addr,
    "subsystemNQN":    results[0].nqn,
    "targetAddress":   results[0].addr,  // Backward compat
    "targetPort":      results[0].port,  // Backward compat
}
```

Also register the write-owner in metadata:

```go
if cs.meta != nil {
    cs.meta.SetVolumeOwner(ctx, &metadata.VolumeOwnership{
        VolumeID:   volumeID,
        OwnerAddr:  nodeIDs[0],
        OwnerSince: time.Now().Unix(),
        Generation: 1,
    })
}
```

**Step 4: Update DeleteVolume for multi-target cleanup**

In `DeleteVolume`, call `DeleteTarget` on all agents that hold replicas (look up from placement maps), not just the target node.

**Step 5: Run tests**

Run: `go test ./internal/csi/ -v`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/csi/
git commit -m "[Feature] CSI controller creates targets on all replica agents with ANA states"
```

---

## Task 8: CSI Node — Multi-Path Attachment

**Files:**
- Modify: `internal/csi/spdk_initiator.go`
- Modify: `internal/csi/node.go`
- Modify: `internal/csi/nvme.go`

**Step 1: Add ConnectMultipath to SPDKInitiator**

In `internal/csi/spdk_initiator.go`, add:

```go
type TargetInfo struct {
    Addr    string `json:"addr"`
    Port    string `json:"port"`
    NQN     string `json:"nqn"`
    IsOwner bool   `json:"is_owner"`
}

func (s *SPDKInitiator) ConnectMultipath(ctx context.Context, targets []TargetInfo) (string, error) {
    if len(targets) == 0 {
        return "", fmt.Errorf("no targets provided")
    }

    // Connect to each target, each gets its own loopback port
    basePort := uint16(localLoopbackPort)
    var connectedNQNs []string

    for i, t := range targets {
        port, _ := strconv.ParseUint(t.Port, 10, 16)
        bdevName, err := s.client.ConnectInitiator(t.Addr, uint16(port), t.NQN)
        if err != nil {
            // Cleanup already-connected
            for _, nqn := range connectedNQNs {
                s.client.DisconnectInitiator(nqn)
            }
            return "", fmt.Errorf("connect target %s: %w", t.Addr, err)
        }
        connectedNQNs = append(connectedNQNs, t.NQN)

        // Export each as loopback on sequential ports
        localNQN := fmt.Sprintf("nqn.2024-01.io.novastor:local-%s-path%d", t.NQN, i)
        loopPort := basePort + uint16(i)
        if err := s.client.ExportLocal(localNQN, localLoopbackAddr, loopPort, bdevName); err != nil {
            logging.L.Warn("export local failed", zap.Error(err))
        }
    }

    // Discover multipath device
    devicePath, err := discoverMultipathDevice(targets[0].NQN)
    if err != nil {
        return "", fmt.Errorf("discover multipath device: %w", err)
    }

    return devicePath, nil
}

func (s *SPDKInitiator) DisconnectMultipath(ctx context.Context, targets []TargetInfo) error {
    var lastErr error
    for i, t := range targets {
        localNQN := fmt.Sprintf("nqn.2024-01.io.novastor:local-%s-path%d", t.NQN, i)
        if err := s.client.DeleteNvmfTarget(localNQN); err != nil {
            lastErr = err
        }
        if err := s.client.DisconnectInitiator(t.NQN); err != nil {
            lastErr = err
        }
    }
    return lastErr
}
```

**Step 2: Add multipath device discovery**

In `internal/csi/nvme.go`, add:

```go
func discoverMultipathDevice(nqn string) (string, error) {
    const (
        pollInterval = 200 * time.Millisecond
        timeout      = 15 * time.Second
    )
    deadline := time.Now().Add(timeout)
    for {
        // Look for multipath device via /sys/class/nvme-subsystem
        path, err := findMultipathDeviceByNQN(nqn)
        if err == nil {
            return path, nil
        }
        if time.Now().After(deadline) {
            return "", fmt.Errorf("timed out waiting for multipath device with nqn %s", nqn)
        }
        time.Sleep(pollInterval)
    }
}

func findMultipathDeviceByNQN(nqn string) (string, error) {
    // Check /sys/class/nvme-subsystem/*/subsysnqn for matching NQN
    // Return /dev/nvmeXnY (the multipath namespace device)
    entries, err := os.ReadDir("/sys/class/nvme-subsystem")
    if err != nil {
        return "", err
    }
    for _, entry := range entries {
        subsysnqnPath := filepath.Join("/sys/class/nvme-subsystem", entry.Name(), "subsysnqn")
        data, err := os.ReadFile(subsysnqnPath)
        if err != nil {
            continue
        }
        if strings.TrimSpace(string(data)) != nqn {
            continue
        }
        // Found matching subsystem — find its namespace block device
        nsEntries, _ := filepath.Glob(filepath.Join("/sys/class/nvme-subsystem", entry.Name(), "nvme*n*"))
        for _, nsPath := range nsEntries {
            nsName := filepath.Base(nsPath)
            devPath := "/dev/" + nsName
            if _, err := os.Stat(devPath); err == nil {
                return devPath, nil
            }
        }
    }
    return "", fmt.Errorf("no multipath device found for nqn %s", nqn)
}
```

**Step 3: Update NodeStageVolume for multipath**

In `internal/csi/node.go` NodeStageVolume (~line 150), add multipath path:

```go
// Check for multi-target volume context
targetAddressesJSON := volCtx["targetAddresses"]
if targetAddressesJSON != "" && ns.initiator != nil {
    // Multi-path attachment (new)
    var targets []spdk_initiator.TargetInfo
    if err := json.Unmarshal([]byte(targetAddressesJSON), &targets); err != nil {
        return nil, status.Errorf(codes.Internal, "parse target addresses: %v", err)
    }

    if spdkInit, ok := ns.initiator.(*SPDKInitiator); ok {
        devicePath, err = spdkInit.ConnectMultipath(ctx, targets)
    } else {
        // Fallback: connect to first target only (legacy)
        devicePath, err = ns.initiator.Connect(ctx, targets[0].Addr, targets[0].Port, targets[0].NQN)
    }
} else if targetAddr != "" && subsystemNQN != "" && ns.initiator != nil {
    // Single-target attachment (backward compat)
    devicePath, err = ns.initiator.Connect(ctx, targetAddr, targetPort, subsystemNQN)
}
```

**Step 4: Update NodeUnstageVolume for multipath**

Store the full targets JSON in the marker file instead of just the NQN. On unstage, parse it and disconnect all paths.

**Step 5: Verify compilation**

Run: `go build ./internal/csi/... ./cmd/csi/...`
Expected: PASS

**Step 6: Commit**

```bash
git add internal/csi/
git commit -m "[Feature] CSI node attaches volumes via kernel NVMe multipath with multiple targets"
```

---

## Task 9: Agent Failover Controller

**Files:**
- Create: `internal/agent/failover/controller.go`
- Create: `internal/agent/failover/controller_test.go`
- Modify: `cmd/agent/main.go` (start failover controller)

**Step 1: Write failing tests**

Create `internal/agent/failover/controller_test.go`:

```go
package failover

import (
    "testing"
)

func TestController_PromotesToOwner(t *testing.T) {
    // Create controller with mock metadata client
    // Simulate: volume ownership becomes available (owner died)
    // Assert: controller claims ownership and sets ANA Optimized
}

func TestController_DemotesOnRecovery(t *testing.T) {
    // Create controller that is current owner
    // Simulate: original owner recovers and reclaims
    // Assert: controller sets ANA Non-Optimized
}

func TestController_SkipsIfNotNextInCRUSH(t *testing.T) {
    // Create controller that is NOT next in CRUSH order
    // Simulate: ownership available
    // Assert: controller does NOT claim ownership
}
```

**Step 2: Implement Controller**

Create `internal/agent/failover/controller.go`:

```go
package failover

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/piwi3910/novastor/internal/metadata"
    "github.com/piwi3910/novastor/internal/spdk"
    "go.uber.org/zap"
)

type VolumeState struct {
    VolumeID      string
    IsOwner       bool
    OwnerAddr     string
    Generation    uint64
    ANAState      string
    ReplicaNodes  []string // Ordered by CRUSH priority
    ReplicaBdev   string
}

type Controller struct {
    nodeID     string
    nodeAddr   string
    metaClient *metadata.GRPCClient
    spdkClient *spdk.Client
    hostIP     string
    logger     *zap.Logger

    mu             sync.RWMutex
    managedVolumes map[string]*VolumeState
    stopCh         chan struct{}
}

func New(nodeID, nodeAddr, hostIP string, metaClient *metadata.GRPCClient, spdkClient *spdk.Client, logger *zap.Logger) *Controller {
    return &Controller{
        nodeID:         nodeID,
        nodeAddr:       nodeAddr,
        hostIP:         hostIP,
        metaClient:     metaClient,
        spdkClient:     spdkClient,
        logger:         logger,
        managedVolumes: make(map[string]*VolumeState),
        stopCh:         make(chan struct{}),
    }
}

func (c *Controller) Start(ctx context.Context) error {
    // Discover volumes this agent holds replicas for
    // Set initial ANA states based on current ownership
    // Start watching for ownership changes
    go c.watchLoop(ctx)
    return nil
}

func (c *Controller) Stop() {
    close(c.stopCh)
}

func (c *Controller) watchLoop(ctx context.Context) {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-c.stopCh:
            return
        case <-ticker.C:
            c.checkOwnership(ctx)
        }
    }
}

func (c *Controller) checkOwnership(ctx context.Context) {
    c.mu.RLock()
    volumes := make(map[string]*VolumeState, len(c.managedVolumes))
    for k, v := range c.managedVolumes {
        volumes[k] = v
    }
    c.mu.RUnlock()

    for volumeID, vs := range volumes {
        ownership, err := c.metaClient.GetVolumeOwner(ctx, volumeID)
        if err != nil {
            c.logger.Error("get volume owner", zap.String("volume", volumeID), zap.Error(err))
            continue
        }
        if ownership == nil {
            continue
        }

        if ownership.OwnerAddr == c.nodeAddr && !vs.IsOwner {
            c.promote(ctx, vs, ownership.Generation)
        } else if ownership.OwnerAddr != c.nodeAddr && vs.IsOwner {
            c.demote(ctx, vs)
        }
    }
}

func (c *Controller) promote(ctx context.Context, vs *VolumeState, generation uint64) {
    c.logger.Info("promoting to write-owner",
        zap.String("volume", vs.VolumeID),
        zap.Uint64("generation", generation))

    // Set ANA state to optimized
    nqn := "novastor-" + vs.VolumeID
    anaGroupID := uint32(crc32IEEE(vs.VolumeID) & 0xFF)
    if err := c.spdkClient.SetANAState(nqn, anaGroupID, "optimized"); err != nil {
        c.logger.Error("set ANA optimized", zap.Error(err))
    }

    c.mu.Lock()
    vs.IsOwner = true
    vs.Generation = generation
    vs.ANAState = "optimized"
    c.mu.Unlock()
}

func (c *Controller) demote(ctx context.Context, vs *VolumeState) {
    c.logger.Info("demoting from write-owner", zap.String("volume", vs.VolumeID))

    nqn := "novastor-" + vs.VolumeID
    anaGroupID := uint32(crc32IEEE(vs.VolumeID) & 0xFF)
    if err := c.spdkClient.SetANAState(nqn, anaGroupID, "non_optimized"); err != nil {
        c.logger.Error("set ANA non-optimized", zap.Error(err))
    }

    c.mu.Lock()
    vs.IsOwner = false
    vs.ANAState = "non_optimized"
    c.mu.Unlock()
}

func (c *Controller) RegisterVolume(volumeID string, replicaNodes []string, isOwner bool) {
    anaState := "non_optimized"
    if isOwner {
        anaState = "optimized"
    }
    c.mu.Lock()
    c.managedVolumes[volumeID] = &VolumeState{
        VolumeID:     volumeID,
        IsOwner:      isOwner,
        ReplicaNodes: replicaNodes,
        ANAState:     anaState,
    }
    c.mu.Unlock()
}
```

**Step 3: Wire into agent main**

In `cmd/agent/main.go`, after SPDK initialization, start the failover controller:

```go
if *dataPlane == "spdk" {
    // ... existing SPDK init ...

    // Start failover controller
    fc := failover.New(*nodeID, registrationAddr, *hostIP, metaClient, spdkClient, logger)
    if err := fc.Start(ctx); err != nil {
        logger.Fatal("failover controller start", zap.Error(err))
    }
    defer fc.Stop()
}
```

**Step 4: Run tests**

Run: `go test ./internal/agent/failover/ -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/agent/failover/ cmd/agent/main.go
git commit -m "[Feature] Add agent failover controller for volume ownership and ANA management"
```

---

## Task 10: Helm Chart Updates

**Files:**
- Modify: `deploy/helm/novastor/values.yaml`
- Modify: `deploy/helm/novastor/templates/agent-daemonset.yaml`

**Step 1: Add configurable heartbeat and failover settings**

In `values.yaml`, under `agent:` section:

```yaml
agent:
  heartbeatInterval: "500ms"    # Fast heartbeat for failover detection
  failover:
    enabled: true
    katoTimeout: "200ms"        # SPDK keep-alive timeout
```

In `values.yaml`, under `csi.provisioner:`:

```yaml
  provisioner:
    timeout: "120s"             # Increased for multi-target creation
```

**Step 2: Update agent DaemonSet template**

Pass `--heartbeat-interval` flag from Helm values.

**Step 3: Lint chart**

Run: `helm lint deploy/helm/novastor/`
Expected: PASS

**Step 4: Commit**

```bash
git add deploy/helm/
git commit -m "[Feature] Add failover and heartbeat configuration to Helm chart"
```

---

## Task 11: Integration Test — Multi-Target Volume Lifecycle

**Files:**
- Create: `test/integration/failover_test.go`

**Step 1: Write integration test**

Test the full flow: create volume with 3 targets → verify all targets have correct ANA states → simulate ownership transfer → verify ANA states update.

This test uses the metadata store and mock SPDK clients to verify the control plane flow end-to-end without requiring a real cluster.

**Step 2: Run test**

Run: `go test ./test/integration/ -run TestFailoverLifecycle -v`
Expected: PASS

**Step 3: Commit**

```bash
git add test/integration/
git commit -m "[Test] Add integration test for multi-target failover lifecycle"
```

---

## Task 12: E2E Test — Kubernetes Failover

**Files:**
- Create: `test/e2e/failover_test.go`

**Step 1: Write E2E test**

Test on a real cluster:
1. Create PVC with `novastor-block-replicated` StorageClass
2. Create pod that writes data via fio
3. Verify PVC is Bound with multipath device
4. Kill write-owner agent pod
5. Verify fio continues without errors
6. Verify new write-owner ANA state
7. Recover agent pod
8. Verify data integrity

**Step 2: Commit**

```bash
git add test/e2e/
git commit -m "[Test] Add E2E test for SPDK failover with live agent kill"
```

---

## Task 13: Documentation

**Files:**
- Modify: `docs/` (add failover documentation)

**Step 1: Update architecture docs**

Add section on ANA multipath failover to existing architecture documentation. Include the failover sequence diagram and configuration options.

**Step 2: Build docs**

Run: `mkdocs build --strict`
Expected: PASS

**Step 3: Commit**

```bash
git add docs/
git commit -m "[Docs] Add SPDK ANA multipath failover documentation"
```

---

## Summary

| Task | Component | Estimated Complexity |
|------|-----------|---------------------|
| 1 | Proto extensions | Low |
| 2 | Rust ANA on targets | Medium |
| 3 | Rust dynamic replica bdev | Medium |
| 4 | Go SPDK client methods | Low |
| 5 | Metadata ownership | Medium |
| 6 | Agent target ANA | Medium |
| 7 | CSI controller multi-target | High |
| 8 | CSI node multipath | High |
| 9 | Agent failover controller | High |
| 10 | Helm chart | Low |
| 11 | Integration test | Medium |
| 12 | E2E test | Medium |
| 13 | Documentation | Low |

**Dependency order:** 1 → 2,3 (parallel) → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13
