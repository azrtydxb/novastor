# Stability Phase 1: Critical Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent hanging I/O and volume loss when dataplane, agent, or CSI pods restart — the three most critical production failures.

**Architecture:** Agent-driven volume reconciliation loop detects missing NVMe-oF targets after dataplane restart and re-creates them. Resilient gRPC connection auto-reconnects. Stale NVMe cleanup runs on CSI startup and every NodeStageVolume. Chunk store init retries indefinitely with backoff.

**Tech Stack:** Go 1.25+, gRPC, Kubernetes CSI, NVMe-oF kernel initiator, sysfs

**Spec:** `docs/superpowers/specs/2026-03-23-stability-ha-design.md` (Phase 1 sections 1.1–1.3)

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `api/proto/nvme/nvme.proto` | Add `ListTargets` RPC definition | 1 |
| `internal/agent/spdk_target_server.go` | Implement `ListTargets` handler | 1 |
| `internal/agent/reconciler.go` | **New**: Volume reconciliation loop | 2 |
| `cmd/agent/main.go` | Resilient gRPC, init retry loop, wire reconciler | 3, 4 |
| `internal/csi/spdk_initiator.go` | Extend stale cleanup, idempotent connect | 5 |
| `internal/csi/nvme.go` | Add stale cleanup to LinuxInitiator | 5 |
| `internal/csi/node.go` | Call cleanup on startup, idempotent stage | 5 |

---

## Task 1: ListTargets RPC

**Files:**
- Modify: `api/proto/nvme/nvme.proto`
- Modify: `internal/agent/spdk_target_server.go`

- [ ] **Step 1: Add ListTargets to proto**

In `api/proto/nvme/nvme.proto`, add to the `NVMeTargetService`:

```protobuf
  rpc ListTargets(ListTargetsRequest) returns (ListTargetsResponse);
```

Add message types after `SetANAStateResponse`:

```protobuf
message ListTargetsRequest {}

message ListTargetsResponse {
  repeated TargetEntry targets = 1;
}

message TargetEntry {
  string volume_id = 1;
  string subsystem_nqn = 2;
  string target_address = 3;
  string target_port = 4;
}
```

- [ ] **Step 2: Generate Go code**

```bash
make generate-proto
```

- [ ] **Step 3: Implement ListTargets handler**

In `internal/agent/spdk_target_server.go`, add a `volumes` map field to `SPDKTargetServer`:

```go
// Track created volumes for reconciliation.
volumesMu sync.RWMutex
volumes   map[string]*pb.CreateTargetResponse // volumeID → response
```

Initialize in constructor. Update `CreateTarget` to store in the map, `DeleteTarget` to remove.

Implement `ListTargets`:

```go
func (s *SPDKTargetServer) ListTargets(
    ctx context.Context,
    req *pb.ListTargetsRequest,
) (*pb.ListTargetsResponse, error) {
    s.volumesMu.RLock()
    defer s.volumesMu.RUnlock()

    var entries []*pb.TargetEntry
    for volID, resp := range s.volumes {
        entries = append(entries, &pb.TargetEntry{
            VolumeId:      volID,
            SubsystemNqn:  resp.SubsystemNqn,
            TargetAddress: resp.TargetAddress,
            TargetPort:    resp.TargetPort,
        })
    }
    return &pb.ListTargetsResponse{Targets: entries}, nil
}
```

- [ ] **Step 4: Verify compilation**

```bash
go build ./...
```

- [ ] **Step 5: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add api/proto/ internal/agent/spdk_target_server.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Add ListTargets RPC for volume reconciliation

Returns all active NVMe-oF targets on this agent. Used by the
reconciliation loop to detect missing targets after dataplane restart."
```

---

## Task 2: Volume Reconciliation Loop

**Files:**
- Create: `internal/agent/reconciler.go`

- [ ] **Step 1: Create reconciler**

```go
package agent

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

const reconcileInterval = 60 * time.Second

// VolumeReconciler ensures NVMe-oF targets on this node match metadata.
// After a dataplane restart, targets are lost — this loop re-creates them.
type VolumeReconciler struct {
	nodeID     string
	targetSrv  *SPDKTargetServer
	metaClient *metadata.GRPCClient
	triggerCh  chan struct{} // non-blocking trigger for immediate reconciliation
}

func NewVolumeReconciler(
	nodeID string,
	targetSrv *SPDKTargetServer,
	metaClient *metadata.GRPCClient,
) *VolumeReconciler {
	return &VolumeReconciler{
		nodeID:     nodeID,
		targetSrv:  targetSrv,
		metaClient: metaClient,
		triggerCh:  make(chan struct{}, 1),
	}
}

// Trigger requests an immediate reconciliation (non-blocking).
// Used when the dataplane connection recovers.
func (r *VolumeReconciler) Trigger() {
	select {
	case r.triggerCh <- struct{}{}:
	default: // already triggered
	}
}

// Run starts the reconciliation loop. Blocks until ctx is cancelled.
func (r *VolumeReconciler) Run(ctx context.Context) {
	// Run once immediately on startup.
	r.reconcile(ctx)

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx)
		case <-r.triggerCh:
			r.reconcile(ctx)
		}
	}
}

func (r *VolumeReconciler) reconcile(ctx context.Context) {
	// 1. Get volumes that should be on this node from metadata.
	expected, err := r.metaClient.ListVolumesForNode(ctx, r.nodeID)
	if err != nil {
		logging.L.Warn("reconciler: failed to list volumes from metadata", zap.Error(err))
		return
	}

	// 2. Get volumes actually present on the agent.
	actual, err := r.targetSrv.ListTargets(ctx, nil)
	if err != nil {
		logging.L.Warn("reconciler: failed to list active targets", zap.Error(err))
		return
	}
	activeSet := make(map[string]bool)
	for _, t := range actual.GetTargets() {
		activeSet[t.VolumeId] = true
	}

	// 3. Create missing targets.
	created := 0
	for _, vol := range expected {
		if activeSet[vol.VolumeID] {
			delete(activeSet, vol.VolumeID)
			continue
		}
		logging.L.Info("reconciler: re-creating missing target",
			zap.String("volumeID", vol.VolumeID))
		_, createErr := r.targetSrv.CreateTarget(ctx, vol.ToCreateTargetRequest())
		if createErr != nil {
			logging.L.Warn("reconciler: failed to re-create target",
				zap.String("volumeID", vol.VolumeID),
				zap.Error(createErr))
		} else {
			created++
		}
	}

	// 4. Delete orphan targets (in agent but not in metadata).
	deleted := 0
	for orphanID := range activeSet {
		logging.L.Info("reconciler: deleting orphan target",
			zap.String("volumeID", orphanID))
		_, delErr := r.targetSrv.DeleteTarget(ctx, &pb.DeleteTargetRequest{
			VolumeId: orphanID,
		})
		if delErr != nil {
			logging.L.Warn("reconciler: failed to delete orphan",
				zap.String("volumeID", orphanID),
				zap.Error(delErr))
		} else {
			deleted++
		}
	}

	if created > 0 || deleted > 0 {
		logging.L.Info("reconciler: completed",
			zap.Int("created", created),
			zap.Int("deleted", deleted),
			zap.Int("unchanged", len(expected)-created))
	}
}
```

Note: `ListVolumesForNode` and `vol.ToCreateTargetRequest()` need to be added to the metadata client and volume types respectively. These are thin wrappers over existing metadata queries.

- [ ] **Step 2: Add ListVolumesForNode to metadata client**

In `internal/metadata/client.go`, add:

```go
// ListVolumesForNode returns all volumes with a target on the given node.
func (c *GRPCClient) ListVolumesForNode(ctx context.Context, nodeID string) ([]VolumePlacement, error) {
    // Query all volume metadata and filter by target node.
    volumes, err := c.ListVolumes(ctx)
    if err != nil {
        return nil, err
    }
    var result []VolumePlacement
    for _, v := range volumes {
        if v.TargetNodeID == nodeID || containsNode(v.ReplicaNodeIDs, nodeID) {
            result = append(result, VolumePlacement{
                VolumeID:   v.VolumeID,
                SizeBytes:  v.SizeBytes,
                Protection: v.DataProtection,
                IsOwner:    v.TargetNodeID == nodeID,
            })
        }
    }
    return result, nil
}
```

Add `VolumePlacement` struct and `ToCreateTargetRequest()` method.

- [ ] **Step 3: Verify compilation**

```bash
go build ./...
```

- [ ] **Step 4: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add internal/agent/reconciler.go internal/metadata/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Volume reconciliation loop

Detects missing NVMe-oF targets after dataplane restart and
re-creates them from metadata. Cleans up orphan targets.
Runs on startup, every 60s, and on dataplane reconnection."
```

---

## Task 3: Resilient Dataplane Connection

**Files:**
- Modify: `cmd/agent/main.go`

- [ ] **Step 1: Replace single Dial with resilient connection**

Replace the dataplane connection block (lines ~383-395) with:

```go
// Connect to dataplane with auto-reconnection.
dpDialOpts = append(dpDialOpts,
    grpc.WithKeepaliveParams(keepalive.ClientParameters{
        Time:                10 * time.Second,
        Timeout:             5 * time.Second,
        PermitWithoutStream: true,
    }),
    grpc.WithConnectParams(grpc.ConnectParams{
        Backoff: backoff.Config{
            BaseDelay:  1 * time.Second,
            Multiplier: 1.6,
            MaxDelay:   30 * time.Second,
        },
        MinConnectTimeout: 5 * time.Second,
    }),
)
```

Add import for `google.golang.org/grpc/backoff` and `google.golang.org/grpc/keepalive`.

- [ ] **Step 2: Add connection state watcher**

After creating `dpClient`, start a goroutine that watches connection state and triggers reconciliation on recovery:

```go
// Watch dataplane connection state — trigger reconciliation on recovery.
go func() {
    var wasReady bool
    for {
        conn := dpClient.GetConnection()
        if conn == nil {
            time.Sleep(time.Second)
            continue
        }
        state := conn.GetState()
        isReady := state == connectivity.Ready

        if isReady && !wasReady {
            logging.L.Info("dataplane connection recovered, triggering reconciliation")
            // Re-init chunk store after dataplane restart.
            if _, err := dpClient.InitChunkStore(*spdkBaseBdev, *nodeID); err != nil {
                if !strings.Contains(err.Error(), "already") {
                    logging.L.Warn("re-init chunk store failed", zap.Error(err))
                }
            }
            reconciler.Trigger()
        }
        wasReady = isReady

        if !conn.WaitForStateChange(ctx, state) {
            return // context cancelled
        }
    }
}()
```

- [ ] **Step 3: Wire reconciler into main**

After creating `SPDKTargetServer` and metadata client, instantiate and start the reconciler:

```go
reconciler := agent.NewVolumeReconciler(*nodeID, spdkTargetSrv, metaClient)
go reconciler.Run(ctx)
```

- [ ] **Step 4: Verify compilation**

```bash
go build ./cmd/agent/...
```

- [ ] **Step 5: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add cmd/agent/main.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Resilient dataplane connection + reconciler wiring

Auto-reconnects to dataplane with keepalive + backoff. Watches
connection state and triggers volume reconciliation on recovery.
Re-initializes chunk store after dataplane restart."
```

---

## Task 4: Chunk Store Init Retry Loop

**Files:**
- Modify: `cmd/agent/main.go`

- [ ] **Step 1: Replace single-shot init with retry loop**

Replace the init goroutine (lines ~403-425) with:

```go
// Chunk store init with exponential backoff — retries indefinitely.
var chunkStoreReady atomic.Bool
go func() {
    delays := []time.Duration{2, 4, 8, 16, 30}
    attempt := 0
    for {
        select {
        case <-ctx.Done():
            return
        default:
        }

        delay := 30 * time.Second
        if attempt < len(delays) {
            delay = delays[attempt] * time.Second
        }
        time.Sleep(delay)

        if _, err := dpClient.InitChunkStore(*spdkBaseBdev, *nodeID); err != nil {
            if strings.Contains(err.Error(), "already") {
                logging.L.Info("chunk store already initialized")
            } else {
                logging.L.Warn("chunk store init failed, retrying",
                    zap.Int("attempt", attempt+1),
                    zap.Duration("nextRetry", delay),
                    zap.Error(err))
                attempt++
                continue
            }
        }

        // Query capacity after successful init.
        refreshCapacity(dpClient, *spdkBaseBdev)
        chunkStoreReady.Store(true)
        logging.L.Info("chunk store ready",
            zap.Int64("capacity", localCapacity.Load()),
            zap.Int64("used", localUsedBytes.Load()))
        return
    }
}()
```

- [ ] **Step 2: Guard capacity reporting**

In `refreshCapacity`, return `-1` if chunk store not ready:

```go
func refreshCapacity(dpClient *dataplane.Client, bdevName string) {
    stats, err := dpClient.ChunkStoreStats(bdevName)
    if err != nil {
        logging.L.Debug("capacity refresh: ChunkStoreStats failed", zap.Error(err))
        // Report -1 (initializing) instead of 0 (no capacity).
        localCapacity.Store(-1)
        localUsedBytes.Store(0)
        return
    }
    totalBytes := int64(stats.GetTotalSlots()) * int64(stats.GetChunkSize())
    usedBytes := int64(stats.GetUsedSlots()) * int64(stats.GetChunkSize())
    localCapacity.Store(totalBytes)
    localUsedBytes.Store(usedBytes)
}
```

- [ ] **Step 3: Readiness gate**

Update the agent's readiness check (if using gRPC health — Phase 2 adds this, for now use the atomic flag):

The `chunkStoreReady` flag will be used by Phase 2's health check. For now, add a log line to the heartbeat loop:

```go
if !chunkStoreReady.Load() {
    logging.L.Debug("heartbeat: chunk store not ready yet, reporting initializing")
}
```

- [ ] **Step 4: Verify compilation**

```bash
go build ./cmd/agent/...
```

- [ ] **Step 5: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add cmd/agent/main.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Chunk store init retry with exponential backoff

Retries indefinitely (2s, 4s, 8s, 16s, 30s...) instead of single
attempt. Reports capacity as -1 (initializing) while retrying.
Sets chunkStoreReady flag for readiness gating."
```

---

## Task 5: Stale NVMe Cleanup & Idempotent Connect

**Files:**
- Modify: `internal/csi/spdk_initiator.go`
- Modify: `internal/csi/nvme.go`
- Modify: `internal/csi/node.go`

- [ ] **Step 1: Extend cleanStaleNVMeSubsystems for stuck controllers**

In `internal/csi/spdk_initiator.go`, update `cleanStaleNVMeSubsystems()`:

```go
func cleanStaleNVMeSubsystems() {
    entries, err := os.ReadDir("/sys/class/nvme-subsystem")
    if err != nil {
        return
    }
    for _, entry := range entries {
        subsysDir := "/sys/class/nvme-subsystem/" + entry.Name()

        nqnData, err := os.ReadFile(subsysDir + "/subsysnqn")
        if err != nil {
            continue
        }
        nqn := strings.TrimSpace(string(nqnData))
        if !strings.Contains(nqn, "novastor") {
            continue
        }

        ctrls, _ := filepath.Glob(subsysDir + "/nvme*")
        hasLive := false
        for _, ctrl := range ctrls {
            state, err := os.ReadFile(ctrl + "/state")
            if err != nil {
                continue
            }
            s := strings.TrimSpace(string(state))
            if s == "live" {
                hasLive = true
                break
            }
            // Disconnect controllers stuck in connecting/reconnecting > 30s.
            if s == "connecting" || s == "reconnecting" {
                // Check controller age via sysfs ctime (approximate).
                info, statErr := os.Stat(ctrl)
                if statErr == nil && time.Since(info.ModTime()) > 30*time.Second {
                    logging.L.Warn("disconnecting stuck NVMe controller",
                        zap.String("nqn", nqn),
                        zap.String("state", s))
                    _ = nvmeDisconnect(nqn)
                    break
                }
            }
        }

        if !hasLive && len(ctrls) > 0 {
            _ = nvmeDisconnect(nqn)
        }
    }
}
```

- [ ] **Step 2: Add idempotent connect**

Add a helper that checks for existing live connections before calling `nvme connect`:

```go
// nvmeConnectIdempotent connects only if no live controller exists for this NQN.
func nvmeConnectIdempotent(addr, port, nqn string) error {
    // Check if already connected with a live controller.
    if isNVMeConnected(nqn) {
        return nil
    }
    return nvmeConnect(addr, port, nqn)
}

// isNVMeConnected checks sysfs for a live controller to the given NQN.
func isNVMeConnected(nqn string) bool {
    entries, err := os.ReadDir("/sys/class/nvme-subsystem")
    if err != nil {
        return false
    }
    for _, entry := range entries {
        subsysDir := "/sys/class/nvme-subsystem/" + entry.Name()
        nqnData, _ := os.ReadFile(subsysDir + "/subsysnqn")
        if strings.TrimSpace(string(nqnData)) != nqn {
            continue
        }
        ctrls, _ := filepath.Glob(subsysDir + "/nvme*")
        for _, ctrl := range ctrls {
            state, _ := os.ReadFile(ctrl + "/state")
            if strings.TrimSpace(string(state)) == "live" {
                return true
            }
        }
    }
    return false
}
```

Update `ConnectMultipath` and `nvmeConnect` callers to use `nvmeConnectIdempotent`.

- [ ] **Step 3: Add stale cleanup to LinuxInitiator.Connect**

In `internal/csi/nvme.go`, update `LinuxInitiator.Connect()`:

```go
func (l *LinuxInitiator) Connect(ctx context.Context, addr, port, nqn string) (string, error) {
    // Clean stale NVMe-oF subsystems before connecting.
    cleanStaleNVMeSubsystems()

    // existing connect logic...
}
```

- [ ] **Step 4: Run cleanup on CSI node startup**

In `internal/csi/node.go`, add cleanup call in `NewNodeServer` or at the start of the first `NodeStageVolume`:

```go
func NewNodeServer(...) *NodeServer {
    // Clean stale NVMe connections from previous CSI pod or node reboot.
    cleanStaleNVMeSubsystems()
    // ... existing init ...
}
```

- [ ] **Step 5: Verify compilation**

```bash
go build ./internal/csi/...
```

- [ ] **Step 6: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add internal/csi/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Stale NVMe cleanup on startup + idempotent connect

Run cleanStaleNVMeSubsystems on CSI node startup and every
NodeStageVolume. Extended to detect stuck connecting/reconnecting
controllers (>30s). Idempotent connect skips nvme connect if
a live controller already exists for the NQN."
```

---

## Task 6: Integration Test — Dataplane Restart Recovery

**Files:**
- No source changes — manual test procedure

- [ ] **Step 1: Deploy and create a volume**

```bash
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: stability-test
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: novastor-rep1
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: stability-pod
spec:
  containers:
  - name: test
    image: alpine:latest
    command: ["sh", "-c", "while true; do dd if=/dev/urandom of=/data/test bs=4k count=1 conv=fsync 2>/dev/null && sleep 1; done"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: stability-test
EOF
```

- [ ] **Step 2: Verify I/O is running**

```bash
kubectl exec stability-pod -- ls -la /data/test
```

- [ ] **Step 3: Kill the dataplane pod**

```bash
NODE=$(kubectl get pod stability-pod -o jsonpath='{.spec.nodeName}')
DP=$(kubectl get pods -n novastor-system -l app.kubernetes.io/component=dataplane \
  -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
kubectl delete pod -n novastor-system $DP --force --grace-period=0
```

- [ ] **Step 4: Wait for recovery and verify**

```bash
# Wait for dataplane to restart
sleep 30
# Check agent logs for reconciliation
AG=$(kubectl get pods -n novastor-system -l app.kubernetes.io/component=agent \
  -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
kubectl logs -n novastor-system $AG -c agent --tail=20 | grep -i 'reconcil\|re-creat\|recovered'
# Verify I/O resumes
kubectl exec stability-pod -- ls -la /data/test
```

Expected: Agent detects dataplane reconnection, reconciler re-creates the NVMe-oF target, I/O resumes within ~15 seconds (10s keep-alive + 5s reconnect + reconciliation).

- [ ] **Step 5: Cleanup**

```bash
kubectl delete pod stability-pod --force --grace-period=0
kubectl delete pvc stability-test
```
