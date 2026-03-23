# NovaStor Stability, HA & Volume Lifecycle Design

## Goal

Make NovaStor production-grade by addressing 10 critical gaps in stability, high availability, and volume lifecycle management. Ensure volumes survive pod crashes, dataplane restarts, node reboots, and network partitions without data loss or hanging I/O.

## Scope

Three phases, ordered by impact:

- **Phase 1 — Critical Recovery**: Prevent hanging I/O and data loss on component restart
- **Phase 2 — Graceful Lifecycle**: Prevent issues during planned maintenance
- **Phase 3 — Full HA**: Prevent issues during unplanned failures

---

## Phase 1: Critical Recovery

### 1.1 Volume Recovery After Dataplane Restart

**Problem**: When the dataplane pod restarts, all in-memory SPDK state is lost (bdevs, NVMe-oF targets, chunk stores). Connected kernel initiators see the target disappear, keep-alive fires (10s), I/O errors. The agent doesn't re-create the targets.

**Solution**: Volume reconciliation loop in the agent.

**Reconciliation loop** runs:
1. On startup (after `InitChunkStore` succeeds)
2. After any dataplane reconnection (connection state transitions TRANSIENT_FAILURE → READY)
3. Periodically every 60s as a safety net

**Loop logic**:
1. Query metadata service for all volumes placed on this node (`TargetNodeID == self`)
2. Query dataplane for currently active NVMe-oF targets (new `ListTargets` gRPC call)
3. For each volume in metadata NOT in the dataplane → `CreateTarget` (re-create)
4. For each target in the dataplane NOT in metadata → `DeleteTarget` (orphan cleanup)
5. Log summary: "reconciled: N created, M deleted, K already present"

**Agent-dataplane connection resilience**: Replace single `Dial()` at startup with `grpc.WithKeepaliveParams` + `grpc.WithConnectParams` for automatic reconnection. On connection state transition to READY after a failure, trigger the reconciliation loop.

**New gRPC call**: `ListTargets()` on the dataplane service — returns all active NVMe-oF subsystem NQNs and their volume IDs.

### 1.2 Stale NVMe Connection Cleanup

**Problem**: Stale kernel NVMe subsystem entries accumulate from crashed pods, failed mounts, and node reboots. Prevent new `nvme connect` calls ("already connected" error). Currently `cleanStaleNVMeSubsystems()` only runs during `ConnectMultipath`.

**Solution**: Run cleanup on CSI node startup + every `NodeStageVolume`.

1. **CSI node pod startup**: Run `cleanStaleNVMeSubsystems()` once before registering the CSI driver. Catches stale connections from node reboot or previous CSI crash.

2. **Every `NodeStageVolume`**: Run cleanup before `nvme connect`, for both single-path (`LinuxInitiator.Connect()`) and multipath (`ConnectMultipath`). Already done for multipath — extend to single-path.

3. **Extended cleanup logic**: In addition to disconnecting subsystems with zero live controllers, also disconnect controllers stuck in `connecting` or `reconnecting` state for >30s (stuck reconnects from crashed targets).

4. **Idempotent connect**: Before calling `nvme connect`, check sysfs for an existing live connection to the same NQN. If found with a live controller, skip connect and discover the device path directly. "Already connected" becomes a no-op, not an error.

### 1.3 Chunk Store Init Retry & Capacity Reporting

**Problem**: Agent calls `InitChunkStore` once with a 3-second delay. If dataplane isn't ready (SPDK init takes 10-15s), it fails and never retries. Node reports 0 capacity, scheduler won't place volumes.

**Solution**: Retry loop + capacity guard + readiness gate.

1. **Init retry loop**: Exponential backoff: 2s, 4s, 8s, 16s, 30s, then 30s repeating. Runs in a background goroutine. Sets an atomic flag on success. Retries indefinitely (agent is useless without chunk store).

2. **Capacity reporting guard**: Heartbeat loop reports capacity as `-1` (not `0`) while chunk store is not initialized. Metadata service interprets `-1` as "node initializing — don't place new volumes but don't evict existing ones." Real capacity reported once init succeeds.

3. **Readiness gate**: Agent readiness probe returns `NOT_READY` until chunk store init succeeds. Prevents Kubernetes from routing CSI RPCs to an unready agent. Liveness probe unchanged (TCP socket — agent is alive but not ready).

---

## Phase 2: Graceful Lifecycle

### 2.1 Graceful Shutdown

**Problem**: No preStop hooks, no termination grace periods, no drain coordination. SIGTERM during active RPCs leaves stale mounts, half-connected NVMe devices, unfinished Raft writes.

**Solution**: Per-component shutdown choreography.

**Agent DaemonSet** (`terminationGracePeriodSeconds: 120`):
- `preStop`: `sleep 5` — lets Kubernetes remove pod from endpoints
- SIGTERM handler: marks node offline in metadata, calls `GracefulStop()` on gRPC server
- New: `NotifyShutdown` RPC to dataplane — dataplane flushes in-flight I/O, drains write cache, stops accepting new requests. Agent waits up to 10s for response.

**Dataplane DaemonSet** (`terminationGracePeriodSeconds: 120`):
- `preStop`: `sleep 5`
- SIGTERM handler: calls `destage_all_bitmaps()`, flushes write cache, closes NVMe-oF listeners
- Sets "shutting down" flag — all new bdev I/Os return `SPDK_BDEV_IO_STATUS_ABORTED`. Connected initiators see errors, kernel triggers NVMe reconnection to another path (rep3/multipath).

**Metadata StatefulSet** (`terminationGracePeriodSeconds: 60`):
- `preStop`: `sleep 5`
- Raft graceful leave: `raft.Shutdown()` transfers leadership before exit

**Controller Deployment** (`terminationGracePeriodSeconds: 60`):
- `preStop`: `sleep 5`
- `GracefulStop()` on gRPC server

**CSI Node DaemonSet** (`terminationGracePeriodSeconds: 120`):
- `preStop`: `sleep 5`
- No special drain — CSI RPCs are short-lived, Kubernetes handles retry

### 2.2 Health Checks

**Problem**: TCP socket probes only check if port is listening. An agent with a dead dataplane connection still reports "ready."

**Solution**: gRPC health service on all components.

Implement `grpc.health.v1.Health` on every gRPC server:

- **Agent**: `SERVING` only if chunk store initialized AND dataplane connection alive (last successful RPC < 30s ago). `NOT_SERVING` otherwise.
- **Controller**: `SERVING` if metadata connection alive
- **Metadata**: `SERVING` if Raft state is leader or follower (not candidate or shutdown)
- **CSI**: `SERVING` if node registered

Replace Helm `tcpSocket` probes with native gRPC probes (Kubernetes 1.24+):
```yaml
livenessProbe:
  grpc:
    port: 9100
  initialDelaySeconds: 15
  periodSeconds: 20
readinessProbe:
  grpc:
    port: 9100
  initialDelaySeconds: 5
  periodSeconds: 10
```

### 2.3 Pod Disruption Budgets

**Problem**: No PDBs on critical components. Node drain can evict all agents simultaneously.

**Solution**: PDBs for all DaemonSets and Deployments.

| Component | PDB Rule |
|-----------|----------|
| Metadata StatefulSet | `minAvailable: 2` (already exists) |
| Controller Deployment | `minAvailable: 1` |
| Agent DaemonSet | `maxUnavailable: 1` |
| CSI Node DaemonSet | `maxUnavailable: 1` |
| Dataplane DaemonSet | `maxUnavailable: 1` |

**Controller anti-affinity**: Add `podAntiAffinity: preferredDuringSchedulingIgnoredDuringExecution` to spread controller replicas across nodes.

---

## Phase 3: Full HA

### 3.1 Split-Brain Detection with Generation Counter

**Problem**: After a network partition heals, two agents may both have active NVMe-oF targets for the same volume. Without fencing, both serve I/O → data corruption.

**Solution**: Generation counter in VolumeMeta.

**Metadata change**:
- Add `Generation uint64` to `VolumeMeta`
- Starts at 1 on `CreateVolume`
- Incremented on every ownership change (failover, migration, reassignment)
- Metadata service is the single source of truth (Raft-replicated)

**Agent enforcement**:
- On `CreateTarget`, agent reads current generation from metadata and stores locally: `volumeID → generation`
- Reconciliation loop (every 60s) compares local generation against metadata:
  - **Match**: target valid, keep serving
  - **Local < metadata**: another agent took ownership → **fence** (destroy local target). Initiators disconnect, kernel multipath fails over.
  - **Local > metadata**: should never happen → log CRITICAL, fence as precaution

**Failover flow**:
1. Agent A is owner (generation=5), Agent B is replica
2. Agent A crashes, metadata marks offline after heartbeat TTL
3. Failover controller promotes Agent B → generation incremented to 6
4. Agent B creates target with generation=6
5. Agent A comes back, reconciliation sees local gen 5 < metadata gen 6 → fences
6. No split-brain: only Agent B serves I/O

### 3.2 Backend Storage Deallocation with Async GC

**Problem**: Deleted volumes leave chunks allocated on backend NVMe. After many create/delete cycles, chunk store reports full with no real data.

**Solution**: Async garbage collector with "pending_delete" state.

**Delete flow**:
1. `DeleteVolume` marks volume's chunks as `state: pending_delete` in metadata
2. Removes volume entry from metadata
3. Returns success to Kubernetes — fast delete

**Chunk GC** (per-agent background goroutine, every 5 minutes):
1. Query metadata for chunks with `state: pending_delete` on this node
2. For each: call `DeallocateChunk(bdev_name, offset, size)` on dataplane
3. Dataplane marks offset range as free in free-space bitmap, optionally issues UNMAP to underlying NVMe
4. On success, remove chunk entry from metadata
5. Rate-limited to 10 chunks per cycle to avoid I/O spikes
6. Log: "GC: freed N chunks (X MB)"

**New dataplane gRPC call**: `DeallocateChunk(bdev_name, offset, size)` — marks backend range as available for reuse.

**Free space reporting**: Capacity refresh subtracts `pending_delete` space from `used_bytes`. Scheduler sees accurate available space — "about to be freed" counts as available since GC runs within 5 minutes.

### 3.3 Lock Timeout & Orphan Cleanup

**Problem**: Write-in-progress locks dropped silently on cluster reboot. Orphan targets and placement maps accumulate.

**Solution**: Lock expiry + reconciliation-based cleanup.

**Lock timeout**:
- Add `LockedAt time.Time` and `LockedBy string` (node ID) to lock entries in PlacementMap
- Locks expire after 5 minutes
- Metadata Raft leader runs lock-expiry sweep every 60s — expired locks released with log entry
- On cluster reboot: Raft replays log, timestamps are preserved, sweep releases locks > 5 minutes old

**Orphan target cleanup**:
- Handled by the reconciliation loop (Section 1.1) — targets in dataplane but not in metadata are destroyed
- No separate garbage collector needed

**Orphan placement map cleanup** (controller, every 10 minutes):
- Query all placement maps from metadata
- For each, check if corresponding volume exists
- If volume gone → remove orphan placement map
- Log: "orphan cleanup: removed N placement maps"

**Quota reconciliation** (controller, same 10-minute sweep):
- Check quota reservations against existing volumes
- If reservation exists for deleted volume → release quota
- Handles crash-during-delete case

---

## Files Changed

### Phase 1
| File | Changes |
|------|---------|
| `cmd/agent/main.go` | Reconciliation loop, init retry, resilient gRPC connection, capacity guard |
| `internal/agent/reconciler.go` | New file: volume reconciliation logic |
| `internal/agent/spdk_target_server.go` | `ListTargets` implementation |
| `internal/csi/spdk_initiator.go` | Startup cleanup, idempotent connect |
| `internal/csi/nvme.go` | Stale cleanup on `LinuxInitiator.Connect()` |
| `internal/csi/node.go` | Call cleanup before connect |
| `api/proto/nvme/nvme.proto` | `ListTargets` RPC definition |
| `dataplane/src/transport/dataplane_service.rs` | `ListTargets` gRPC handler |

### Phase 2
| File | Changes |
|------|---------|
| `deploy/helm/novastor/templates/agent-daemonset.yaml` | preStop, terminationGrace, gRPC probes |
| `deploy/helm/novastor/templates/dataplane-daemonset.yaml` | preStop, terminationGrace |
| `deploy/helm/novastor/templates/meta-statefulset.yaml` | preStop, terminationGrace |
| `deploy/helm/novastor/templates/controller-deployment.yaml` | preStop, terminationGrace, anti-affinity |
| `deploy/helm/novastor/templates/csi-node-daemonset.yaml` | preStop, terminationGrace |
| `deploy/helm/novastor/templates/agent-pdb.yaml` | New: PDB for agent |
| `deploy/helm/novastor/templates/dataplane-pdb.yaml` | New: PDB for dataplane |
| `deploy/helm/novastor/templates/controller-pdb.yaml` | New: PDB for controller |
| `deploy/helm/novastor/templates/csi-node-pdb.yaml` | New: PDB for CSI node |
| `cmd/agent/main.go` | gRPC health service, NotifyShutdown |
| `internal/agent/health.go` | New: health check logic |
| `dataplane/src/main.rs` | Shutdown flag, I/O abort on SIGTERM |

### Phase 3
| File | Changes |
|------|---------|
| `internal/metadata/store.go` | `Generation` field, `LockedAt`/`LockedBy` fields, lock expiry sweep |
| `internal/csi/controller.go` | Pending-delete marking, orphan/quota sweep |
| `internal/agent/reconciler.go` | Generation check, fencing logic |
| `internal/agent/gc.go` | New: chunk garbage collector |
| `api/proto/chunk/chunk.proto` | `DeallocateChunk` RPC |
| `dataplane/src/transport/dataplane_service.rs` | `DeallocateChunk` handler |
| `dataplane/src/bdev/novastor_bdev.rs` | Free-space bitmap deallocation |

## Testing

### Phase 1
- Kill dataplane pod while fio is running → verify volumes recover within 15s
- Kill CSI node pod → verify next `NodeStageVolume` succeeds (stale cleanup)
- Start agent before dataplane → verify chunk store init retries and succeeds
- Kill agent → verify node reports initializing (not 0 capacity)

### Phase 2
- `kubectl drain` a node → verify PDB prevents simultaneous eviction
- Rolling restart of agents → verify zero volume downtime
- Kill metadata pod → verify Raft leadership transfer

### Phase 3
- Simulate network partition → verify generation counter prevents split-brain
- Create and delete 100 volumes → verify GC frees backend space
- Kill controller during DeleteVolume → verify quota reconciliation releases stuck quota
- Full cluster reboot → verify lock expiry releases stale locks within 5 minutes
