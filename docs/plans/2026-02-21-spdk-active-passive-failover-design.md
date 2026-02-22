# SPDK Active-Passive Failover with ANA Multipath

**Issue**: #147
**Date**: 2026-02-21
**Status**: Design Approved

## Summary

Enable sub-500ms high-availability failover for replicated volumes on the SPDK data plane. All replica agents export NVMe-oF targets with ANA (Asymmetric Namespace Access) states. The kernel NVMe multipath layer handles path selection and instant failover. Each agent maintains a standby replica bdev for immediate write acceptance on promotion.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Failover scope | Kernel ANA multipath | Native NVMe multipath gives kernel-level path visibility and sub-second failover |
| Replica bdev role | Hybrid: writes via owner's replica bdev, reads from any path | Write fan-out needs coordination; reads are local to each replica |
| Failure detection | SPDK KATO (100-200ms) | Transport-level detection is faster than application heartbeats |
| Consistency on promotion | Quorum fence via metadata | Synchronous replication guarantees data consistency; metadata confirms with majority |
| Failover target | ~200ms I/O disruption | Standby bdevs eliminate setup delay; kernel multipath retries immediately |

## Architecture

### How It Works Today (Single Target)

```
Pod → kernel NVMe → loopback → SPDK initiator → NVMe-oF/TCP → Agent1 (only target)
```

One agent exports one target. If that agent dies, the volume is unavailable.

### New Design (Multi-Target with ANA)

```
                          ┌─ NVMe-oF/TCP ─→ Agent1 [ANA: Optimized]  (write-owner)
Pod → kernel NVMe ───────┼─ NVMe-oF/TCP ─→ Agent2 [ANA: Non-Optimized]
      multipath           └─ NVMe-oF/TCP ─→ Agent3 [ANA: Non-Optimized]
```

All 3 replica agents export targets. The kernel sees 3 paths with ANA states. Writes go to the Optimized path (write-owner). On failure, kernel retries on surviving paths within ~100ms.

### Standby Replica Bdev

Every agent that holds a replica has a **standby replica bdev** pre-configured at volume creation time. This eliminates setup delay during failover:

- **Write-owner (Agent1)**: Active replica bdev fans out writes to Agent2 + Agent3
- **Non-owner (Agent2)**: Standby replica bdev pre-configured to fan out to Agent1 + Agent3
- **Non-owner (Agent3)**: Standby replica bdev pre-configured to fan out to Agent1 + Agent2

When Agent1 dies, Agent2's standby bdev is already wired up. It accepts the kernel's retried write and fans out to Agent3. No bdev creation on the critical path.

## Failover Sequence

### Fast Path (~200ms I/O disruption)

```
T+0ms:    Write-owner Agent1 dies (TCP connection drops)
T+100ms:  KATO expires → kernel marks Agent1 path as failed
T+100ms:  Kernel retries I/O on next path (Agent2, ANA Non-Optimized)
T+150ms:  Agent2's standby bdev accepts write, fans out to Agent2+Agent3
T+200ms:  Write completes (quorum met: 2 of 3)
```

### Background Ownership Transfer (~500-1000ms)

```
T+500ms:  Agent1 misses fast heartbeat → metadata marks Dead
T+500ms:  Metadata revokes Agent1 ownership, notifies watchers
T+600ms:  Agent2 (next in CRUSH order) claims ownership
T+700ms:  Agent2 sets ANA Optimized; Agent3 stays Non-Optimized
T+700ms:  ANA Change Notice sent to kernel (formalizes new preferred path)
```

### Agent Recovery

```
T+Ns:     Agent1 restarts, heartbeats to metadata → marked Alive
          Agent1 sets local ANA state to Non-Optimized
          Agent2's replica bdev adds Agent1 as replica target
          Background rebuild: sync any chunks written during Agent1 downtime
```

## Component Changes

### 1. CSI Controller — Multi-Target Provisioning

**File**: `internal/csi/controller.go`

Currently CreateVolume creates a target on one agent. Changed to:

1. Call `CreateTarget` on **all 3 replica agents** (parallel)
2. First agent in CRUSH order is designated write-owner
3. Store all target addresses in volume context:
   - `targetAddresses` = JSON array of `{addr, port, nqn, isOwner}` for all replicas
   - `writeOwner` = address of initial write-owner
4. Register write-owner in metadata service

### 2. CSI Node — Multi-Path Attachment

**File**: `internal/csi/node.go`, `internal/csi/spdk_initiator.go`

NodeStageVolume changes:

1. Parse all target addresses from volume context
2. Connect to each target via SPDK initiator (3 separate ConnectInitiator calls)
3. Export each as separate loopback targets (ports 4421, 4422, 4423)
4. Kernel NVMe multipath aggregates into one `/dev/nvmeXnY` device
5. Verify multipath device has 3 paths before formatting/mounting

New SPDKInitiator method:
```go
func (s *SPDKInitiator) ConnectMultipath(targets []TargetInfo) (devicePath string, err error)
```

NodeUnstageVolume disconnects all 3 paths.

### 3. Agent Target Server — ANA Support

**File**: `internal/agent/spdk_target_server.go`

CreateTarget enhanced:
- Accepts `ana_state` parameter (optimized / non_optimized)
- Creates SPDK NVMe-oF target with ANA group enabled
- ANA group ID = hash(volumeID) % 256

New RPC:
```go
func (s *SPDKTargetServer) SetANAState(ctx, req) (*SetANAStateResponse, error)
```

### 4. Agent Failover Controller

**New package**: `internal/agent/failover/`

```go
type Controller struct {
    nodeID         string
    metaClient     *metadata.GRPCClient
    spdkClient     *spdk.Client
    hostIP         string
    managedVolumes map[string]*VolumeState
}
```

Responsibilities:
- On startup: query metadata for volumes this agent holds replicas for, set ANA states
- Watch metadata for ownership changes (WatchVolumeOwner streaming RPC)
- On orphaned volume: claim ownership if next in CRUSH order
- On promotion: activate standby replica bdev, set ANA Optimized
- On demotion (recovery case): deactivate replica bdev, set ANA Non-Optimized

### 5. SPDK Data Plane (Rust) — New JSON-RPC Methods

**Files**: `dataplane/src/jsonrpc/methods.rs`, `dataplane/src/spdk/nvmf_manager.rs`, `dataplane/src/bdev/replica.rs`

New methods:
```
nvmf_set_ana_state      { nqn, ana_group_id, ana_state }
nvmf_get_ana_state      { nqn } → { ana_group_id, ana_state }
replica_bdev_add_replica    { name, target: {addr, port, nqn} }
replica_bdev_remove_replica { name, target_addr }
replica_bdev_status         { name } → { replicas: [...], write_quorum, healthy_count }
```

Replica bdev enhancements:
- Dynamic add/remove replica targets on running bdev
- Health check thread: KATO-based failure detection (100-200ms)
- State transitions: Healthy → Degraded → Offline with atomic state tracking

NVMe-oF target enhancements:
- ANA group support on target creation
- Runtime ANA state updates via `spdk_nvmf_subsystem_set_ana_state()`
- ANA Change Notices propagated to connected initiators

### 6. Metadata Service — Ownership Management

**Files**: `internal/metadata/`, `api/proto/metadata/`

New proto RPCs:
```protobuf
rpc GetVolumeOwner(GetVolumeOwnerRequest) returns (GetVolumeOwnerResponse);
rpc SetVolumeOwner(SetVolumeOwnerRequest) returns (SetVolumeOwnerResponse);
rpc WatchVolumeOwner(WatchVolumeOwnerRequest) returns (stream VolumeOwnerEvent);
rpc RequestOwnership(RequestOwnershipRequest) returns (RequestOwnershipResponse);
rpc GetAgentHealth(GetAgentHealthRequest) returns (GetAgentHealthResponse);
```

Ownership model:
- Key: `volume-owner/<volumeID>` → `{owner_addr, owner_since, generation}`
- Generation counter increments on each ownership change
- RequestOwnership: checks current owner is Dead, requester is Alive, requester holds replica

Fast heartbeat:
- Reduce interval to 500ms (configurable)
- Dead threshold: 2 missed heartbeats (1s)
- On agent death: revoke all volume ownerships, notify watchers

### 7. Go SPDK Client — New Methods

**File**: `internal/spdk/client.go`

```go
func (c *Client) SetANAState(nqn string, anaGroupID uint32, state string) error
func (c *Client) GetANAState(nqn string) (uint32, string, error)
func (c *Client) AddReplica(bdevName string, target ReplicaTarget) error
func (c *Client) RemoveReplica(bdevName, targetAddr string) error
func (c *Client) GetReplicaBdevStatus(bdevName string) (*ReplicaBdevStatus, error)
```

### 8. Proto — NVMe Target Service Extensions

**File**: `api/proto/nvme/nvme.proto`

```protobuf
message CreateTargetRequest {
    string volume_id = 1;
    int64 size_bytes = 2;
    string ana_state = 3;          // NEW: "optimized" or "non_optimized"
    uint32 ana_group_id = 4;       // NEW: consistent across replicas
}

rpc SetANAState(SetANAStateRequest) returns (SetANAStateResponse);  // NEW

message SetANAStateRequest {
    string volume_id = 1;
    string ana_state = 2;
    uint32 ana_group_id = 3;
}
```

## Testing Strategy

### Unit Tests
- Replica bdev add/remove replica operations
- ANA state transitions (Optimized ↔ Non-Optimized ↔ Inaccessible)
- Ownership request quorum logic in metadata service
- Failover controller state machine transitions

### Integration Tests
- Multi-target creation: all 3 agents export targets with correct ANA states
- Ownership transfer via metadata → ANA states update on all agents
- Replica bdev dynamic add/remove → write fan-out adjusts
- Full failover sequence: kill agent → new owner claims → ANA states flip

### E2E Test (Kubernetes)
- Deploy 3-replica volume with multipath
- Run fio workload on consuming pod
- Kill write-owner agent pod
- Verify: I/O continues after ~200ms pause (latency spike, no errors)
- Verify: new write-owner ANA state is Optimized
- Recover killed agent → verify it rejoins as Non-Optimized
- Run fio verify for data integrity

### Performance Targets
| Metric | Target |
|--------|--------|
| I/O disruption on failover | < 500ms |
| KATO detection | 100-200ms |
| Kernel path retry | < 10ms |
| Ownership transfer (background) | < 1s |
| Agent recovery + rejoin | < 10s |

## Dependencies

- #146 (Replica bdev) — CLOSED, provides the foundation
- Kernel NVMe multipath support (CONFIG_NVME_MULTIPATH=y)
- SPDK ANA support in NVMe-oF target (`spdk_nvmf_subsystem_set_ana_state`)

## Blocks

- #148 (Active-active read scaling — builds on ANA infrastructure)
