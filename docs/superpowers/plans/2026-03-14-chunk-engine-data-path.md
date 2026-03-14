# Chunk Engine Data Path Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the chunk engine into the active NVMe-oF data path so that block volumes are stored as content-addressed 4MB chunks through the Rust dataplane, matching the documented architecture.

**Architecture:** The Go agent currently bypasses the Rust dataplane and talks directly to SPDK native RPC for volume lifecycle (lvol create/delete). This plan reroutes volume operations through the Rust dataplane's chunk backend: Go agent → Rust JSON-RPC (`backend.init_chunk`, `backend.create_volume`) → ChunkBackend → novastor_bdev (SPDK bdev bridge) → NVMe-oF target. NVMe-oF transport setup (subsystem/listener creation) stays on SPDK native RPC since it's transport-level configuration that must run on the reactor thread.

**Tech Stack:** Go 1.25+, Rust (SPDK dataplane), SPDK v26.01 native RPC, JSON-RPC 2.0

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `dataplane/src/bdev/mod.rs` | Modify | Export `novastor_bdev` module |
| `dataplane/src/jsonrpc/methods.rs` | Modify | Wire `novastor_bdev::set_chunk_backend` in `backend_init_chunk`, auto-register bdev in `backend_create_volume`, auto-destroy bdev in `backend_delete_volume` |
| `internal/spdk/client.go` | Modify | Update `CreateChunkVolume` to return bdev name, add `DeleteChunkVolumeBdev`, remove dead lvol methods |
| `internal/agent/spdk_target_server.go` | Modify | Replace `ensureLvolStore`/`NativeCreateLvol`/`NativeDeleteBdev` with chunk backend calls |
| `cmd/agent/main.go` | Modify | Rename `--spdk-base-bdev` flag semantics (now means chunk backend bdev, not lvol store base) |

---

## Chunk 1: Rust Dataplane — Wire novastor_bdev Into Chunk Backend

### Task 1: Export novastor_bdev module

**Files:**
- Modify: `dataplane/src/bdev/mod.rs`

- [ ] **Step 1: Add novastor_bdev to module exports**

```rust
//! Custom SPDK bdev modules for NovaStor.
pub mod chunk_io;
pub mod erasure;
pub mod novastor_bdev;
pub mod replica;
```

- [ ] **Step 2: Verify it compiles**

Run (on Linux build host or Docker):
```bash
cd dataplane && cargo check 2>&1 | head -20
```
Expected: No errors related to `novastor_bdev` module visibility.

- [ ] **Step 3: Commit**

```bash
git add dataplane/src/bdev/mod.rs
git commit -m "[Refactor] Export novastor_bdev module from bdev"
```

---

### Task 2: Wire set_chunk_backend into backend_init_chunk

When `backend.init_chunk` is called, the Rust dataplane creates a `ChunkBackend` but never tells `novastor_bdev` about it. The bdev bridge needs the backend reference to handle I/O.

**Files:**
- Modify: `dataplane/src/jsonrpc/methods.rs`

- [ ] **Step 1: Add import for novastor_bdev**

At the top of `methods.rs`, add:

```rust
use crate::bdev::novastor_bdev;
```

- [ ] **Step 2: Call set_chunk_backend in backend_init_chunk**

Replace the `backend_init_chunk` function body:

```rust
fn backend_init_chunk(p: BackendInitChunkParams) -> Result<serde_json::Value, DataPlaneError> {
    let store = Arc::new(ChunkStore::new(&p.bdev_name, p.capacity_bytes));
    let backend: Arc<dyn StorageBackend> = Arc::new(ChunkBackend::new(store));

    // Register the chunk backend with the novastor_bdev module so it can
    // bridge SPDK block I/O to ChunkBackend::read/write.
    novastor_bdev::set_chunk_backend(backend.clone());

    backends()
        .lock()
        .unwrap()
        .insert("chunk".to_string(), backend);
    Ok(serde_json::json!({"backend": "chunk", "bdev_name": p.bdev_name, "status": "initialised"}))
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd dataplane && cargo check 2>&1 | head -20
```

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/jsonrpc/methods.rs
git commit -m "[Feature] Wire chunk backend into novastor_bdev on init"
```

---

### Task 3: Auto-register SPDK bdev on backend.create_volume (chunk backend only)

When a chunk volume is created, it must also be registered as an SPDK bdev (`novastor_<name>`) so NVMe-oF can expose it. The bdev name must be returned to the Go agent.

**Files:**
- Modify: `dataplane/src/jsonrpc/methods.rs`

- [ ] **Step 1: Modify backend_create_volume to auto-register novastor bdev**

Replace the `backend_create_volume` function:

```rust
fn backend_create_volume(
    p: BackendCreateVolumeParams,
) -> Result<serde_json::Value, DataPlaneError> {
    let backend = get_backend(&p.backend)?;
    let info = backend.create_volume(&p.name, p.size_bytes, p.thin)?;

    // For chunk backend volumes, automatically register an SPDK bdev so
    // the volume can be exposed via NVMe-oF without a separate RPC call.
    let mut result = serde_json::to_value(&info)
        .map_err(|e| DataPlaneError::JsonRpcError(format!("serialization: {e}")))?;

    if p.backend == "chunk" {
        let bdev_name = novastor_bdev::create(&p.name, p.size_bytes)?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert("bdev_name".to_string(), serde_json::Value::String(bdev_name));
        }
    }

    Ok(result)
}
```

- [ ] **Step 2: Modify backend_delete_volume to auto-destroy novastor bdev**

Replace the `backend_delete_volume` function:

```rust
fn backend_delete_volume(p: BackendVolumeNameParams) -> Result<serde_json::Value, DataPlaneError> {
    // For chunk backend, destroy the SPDK bdev first (before deleting the
    // volume data) so that any NVMe-oF subsystem referencing it is cleaned up.
    if p.backend == "chunk" {
        // Best-effort: bdev may not exist if creation failed partway.
        let _ = novastor_bdev::destroy(&p.name);
    }

    let backend = get_backend(&p.backend)?;
    backend.delete_volume(&p.name)?;
    Ok(serde_json::json!(true))
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd dataplane && cargo check 2>&1 | head -20
```

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/jsonrpc/methods.rs
git commit -m "[Feature] Auto-register/destroy SPDK bdev for chunk volumes"
```

---

## Chunk 2: Go Agent — Route Volume Lifecycle Through Chunk Backend

### Task 4: Update Go SPDK client to return bdev name from CreateChunkVolume

**Files:**
- Modify: `internal/spdk/client.go`

- [ ] **Step 1: Change CreateChunkVolume to return bdev name**

Replace the `CreateChunkVolume` method:

```go
// CreateChunkVolume creates a volume in the chunk backend.
// Returns the SPDK bdev name (e.g. "novastor_<name>") that can be used
// for NVMe-oF target creation.
func (c *Client) CreateChunkVolume(name string, sizeBytes uint64, thin bool) (string, error) {
	params := map[string]interface{}{
		"backend":    "chunk",
		"name":       name,
		"size_bytes": sizeBytes,
		"thin":       thin,
	}
	var result struct {
		BdevName string `json:"bdev_name"`
	}
	if err := c.call("backend.create_volume", params, &result); err != nil {
		return "", err
	}
	return result.BdevName, nil
}
```

- [ ] **Step 2: Remove dead lvol methods**

Delete these methods that are no longer used (superseded by chunk backend path):

- `CreateLvolStore` (lines 236-249) — was Rust-dataplane-routed lvol store creation
- `CreateLvol` (lines 252-265) — was Rust-dataplane-routed lvol creation
- `DeleteBdev` (lines 268-270) — was Rust-dataplane-routed bdev deletion
- `CreateMallocBdev` (lines 227-234) — was Rust-dataplane-routed malloc bdev creation

Keep:
- `NativeCreateMallocBdev` — still needed to create the base SPDK bdev before chunk init
- `NativeCreateLvolStore`, `NativeGetLvolStores`, `NativeCreateLvol`, `NativeDeleteBdev` — remove these too (no longer needed since we're not using lvols)

- [ ] **Step 3: Add InitChunkBackend capacity calculation helper**

The `InitChunkBackend` method already exists (line 575). It's correct as-is. No change needed.

- [ ] **Step 4: Verify Go code compiles**

```bash
go build ./...
```

- [ ] **Step 5: Commit**

```bash
git add internal/spdk/client.go
git commit -m "[Refactor] Route volume operations through chunk backend, remove dead lvol methods"
```

---

### Task 5: Rewrite SPDKTargetServer to use chunk backend

This is the core change. Replace `ensureLvolStore()` + `NativeCreateLvol()` with `InitChunkBackend()` + `CreateChunkVolume()`.

**Files:**
- Modify: `internal/agent/spdk_target_server.go`

- [ ] **Step 1: Replace ensureLvolStore with ensureChunkBackend**

Replace the `mallocBdevSizeMB` constant, `ensureLvolStore` method, and related fields:

```go
const (
	// spdkTargetPort is the TCP port used by SPDK NVMe-oF targets on this node.
	spdkTargetPort = 4430

	// spdkNQNPrefix matches the NQN format generated by the Rust data-plane.
	spdkNQNPrefix = "nqn.2024-01.io.novastor:volume-"

	// mallocBdevSizeMB is the size of the auto-created Malloc bdev (256MB).
	// Must fit within the SPDK memory allocation (1024MB) minus SPDK/DPDK overhead (~700MB).
	mallocBdevSizeMB = 256
)

// SPDKTargetServer implements the NVMeTargetService gRPC server using the SPDK
// data-plane process. Volumes are created as chunk-backed volumes in the Rust
// dataplane and exposed as NVMe-oF/TCP targets. All data-path I/O flows through
// the Rust SPDK data-plane's chunk engine; the Go agent never touches data.
type SPDKTargetServer struct {
	pb.UnimplementedNVMeTargetServiceServer

	hostIP     string
	baseBdev   string
	spdkClient *spdk.Client
	metaClient *metadata.GRPCClient

	// chunkInit tracks whether the chunk backend has been initialised.
	initOnce sync.Once
	initErr  error
}
```

- [ ] **Step 2: Implement ensureChunkBackend**

```go
// ensureChunkBackend initialises the chunk storage backend on the SPDK
// data-plane. The underlying bdev is determined by baseBdev (e.g. "NVMe0n1"
// for real NVMe drives, "Malloc0" for testing). This is called lazily so the
// agent can start even if the storage bdev isn't immediately available.
//
// The chunk backend stores data as content-addressed 4MB chunks on the bdev,
// matching the NovaStor architecture: Backend → Chunk Engine → NVMe-oF.
func (s *SPDKTargetServer) ensureChunkBackend() error {
	s.initOnce.Do(func() {
		// Auto-create a Malloc bdev when configured for testing (no real NVMe).
		if strings.HasPrefix(s.baseBdev, "Malloc") {
			logging.L.Info("spdk target: creating malloc bdev for testing",
				zap.String("name", s.baseBdev),
				zap.Uint64("sizeMB", mallocBdevSizeMB),
			)
			if err := s.spdkClient.NativeCreateMallocBdev(s.baseBdev, mallocBdevSizeMB, 512); err != nil {
				// EEXIST — the bdev persists across agent restarts.
				if !strings.Contains(err.Error(), "already exists") {
					s.initErr = fmt.Errorf("creating malloc bdev %s: %w", s.baseBdev, err)
					logging.L.Error("spdk target: failed to create malloc bdev", zap.Error(s.initErr))
					return
				}
				logging.L.Info("spdk target: malloc bdev already exists, reusing",
					zap.String("name", s.baseBdev),
				)
			}
		}

		// Initialise the chunk backend on the Rust data-plane.
		// capacity = bdev size in bytes (for Malloc: mallocBdevSizeMB * 1024 * 1024).
		capacityBytes := uint64(mallocBdevSizeMB) * 1024 * 1024
		if err := s.spdkClient.InitChunkBackend(s.baseBdev, capacityBytes); err != nil {
			// The chunk backend may already be initialised if the agent restarted
			// while the dataplane kept running.
			if !strings.Contains(err.Error(), "already") {
				s.initErr = fmt.Errorf("initialising chunk backend on %s: %w", s.baseBdev, err)
				logging.L.Error("spdk target: failed to init chunk backend", zap.Error(s.initErr))
				return
			}
			logging.L.Info("spdk target: chunk backend already initialised, reusing",
				zap.String("baseBdev", s.baseBdev),
			)
		}

		logging.L.Info("spdk target: chunk backend initialised",
			zap.String("baseBdev", s.baseBdev),
			zap.Uint64("capacityBytes", capacityBytes),
		)
	})
	return s.initErr
}
```

- [ ] **Step 3: Rewrite CreateTarget to use chunk backend**

```go
// CreateTarget creates a chunk-backed volume and exposes it as an NVMe-oF/TCP
// target via the data-plane process. The volume is stored as content-addressed
// 4MB chunks in the Rust dataplane's chunk engine.
func (s *SPDKTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}
	sizeBytes := req.GetSizeBytes()
	if sizeBytes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "size_bytes must be positive")
	}

	// Ensure chunk backend is ready.
	if err := s.ensureChunkBackend(); err != nil {
		return nil, status.Errorf(codes.Internal, "chunk backend init: %v", err)
	}

	// Create chunk volume via Rust data-plane. This also registers an SPDK
	// bdev named "novastor_<volumeID>" that bridges block I/O to the chunk engine.
	bdevName, err := s.spdkClient.CreateChunkVolume(volumeID, uint64(sizeBytes), true)
	if err != nil {
		logging.L.Error("spdk target: failed to create chunk volume",
			zap.String("volumeID", volumeID),
			zap.Int64("sizeBytes", sizeBytes),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating chunk volume for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: chunk volume created",
		zap.String("volumeID", volumeID),
		zap.String("bdevName", bdevName),
		zap.Int64("sizeBytes", sizeBytes),
	)

	// Expose the chunk bdev as an NVMe-oF/TCP target.
	// Use the specific host IP (not 0.0.0.0) because SPDK's listener ACL
	// performs a literal address comparison: the subsystem listener address
	// must match the address the client connects to.
	nqn, err := s.spdkClient.CreateNvmfTarget(volumeID, s.hostIP, spdkTargetPort, bdevName)
	if err != nil {
		// Cleanup chunk volume on failure.
		if delErr := s.spdkClient.DeleteChunkVolume(volumeID); delErr != nil {
			logging.L.Warn("spdk target: failed to clean up chunk volume",
				zap.String("volumeID", volumeID),
				zap.Error(delErr),
			)
		}
		logging.L.Error("spdk target: failed to create NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "creating NVMe-oF target for %s: %v", volumeID, err)
	}

	logging.L.Info("spdk target: target created",
		zap.String("volumeID", volumeID),
		zap.String("nqn", nqn),
		zap.String("address", s.hostIP),
		zap.Int("port", spdkTargetPort),
		zap.String("bdevName", bdevName),
	)

	// Set ANA state if specified.
	anaState := req.GetAnaState()
	anaGroupID := req.GetAnaGroupId()
	if anaState != "" && anaGroupID > 0 {
		if err := s.spdkClient.SetANAState(nqn, anaGroupID, anaState); err != nil {
			logging.L.Warn("spdk target: failed to set ANA state",
				zap.String("volumeID", volumeID),
				zap.String("anaState", anaState),
				zap.Uint32("anaGroupID", anaGroupID),
				zap.Error(err),
			)
			// Non-fatal: target works without ANA.
		} else {
			logging.L.Info("spdk target: ANA state set",
				zap.String("volumeID", volumeID),
				zap.String("anaState", anaState),
				zap.Uint32("anaGroupID", anaGroupID),
			)
		}
	}

	return &pb.CreateTargetResponse{
		SubsystemNqn:  nqn,
		TargetAddress: s.hostIP,
		TargetPort:    fmt.Sprintf("%d", spdkTargetPort),
	}, nil
}
```

- [ ] **Step 4: Rewrite DeleteTarget to use chunk backend**

```go
// DeleteTarget removes the NVMe-oF target and the chunk volume via the data-plane.
func (s *SPDKTargetServer) DeleteTarget(ctx context.Context, req *pb.DeleteTargetRequest) (*pb.DeleteTargetResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id is required")
	}

	// Remove the NVMe-oF target (best-effort; target may not exist on retry).
	if err := s.spdkClient.DeleteNvmfTarget(volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete NVMe-oF target",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
	}

	// Remove the chunk volume via Rust data-plane (best-effort).
	// This also destroys the novastor_<volumeID> SPDK bdev.
	if err := s.spdkClient.DeleteChunkVolume(volumeID); err != nil {
		logging.L.Warn("spdk target: failed to delete chunk volume",
			zap.String("volumeID", volumeID),
			zap.Error(err),
		)
	}

	logging.L.Info("spdk target: target deleted", zap.String("volumeID", volumeID))
	return &pb.DeleteTargetResponse{}, nil
}
```

- [ ] **Step 5: Remove lvsName field from struct and NewSPDKTargetServer**

Update `NewSPDKTargetServer` — the `lvsName` and `lvsInit` fields are gone since we no longer use lvol stores:

```go
func NewSPDKTargetServer(hostIP, baseBdev string, spdkClient *spdk.Client, metaClient *metadata.GRPCClient) *SPDKTargetServer {
	logging.L.Info("spdk target: initialized SPDK target server",
		zap.String("hostIP", hostIP),
		zap.String("baseBdev", baseBdev),
	)
	return &SPDKTargetServer{
		hostIP:     hostIP,
		baseBdev:   baseBdev,
		spdkClient: spdkClient,
		metaClient: metaClient,
	}
}
```

- [ ] **Step 6: Verify Go code compiles**

```bash
go build ./...
```

- [ ] **Step 7: Commit**

```bash
git add internal/agent/spdk_target_server.go
git commit -m "[Feature] Route volume creation through chunk backend instead of SPDK lvols"
```

---

### Task 6: Clean up dead SPDK native RPC methods

Now that volume lifecycle goes through the chunk backend, remove the unused SPDK native RPC methods for lvol operations.

**Files:**
- Modify: `internal/spdk/client.go`

- [ ] **Step 1: Remove unused Native* lvol methods**

Delete these methods:
- `NativeCreateLvolStore` — no longer creating lvol stores
- `NativeGetLvolStores` — no longer querying lvol stores
- `NativeCreateLvol` — no longer creating lvols directly
- `NativeDeleteBdev` — no longer deleting lvols directly (chunk backend handles it)
- `LvolStoreInfo` struct — only used by the above

Keep:
- `NativeCreateMallocBdev` — still needed to create the base SPDK bdev for testing
- `nativeCall` — still needed for NVMe-oF transport operations and Malloc bdev
- `ensureNativeTransport` — still needed for NVMe-oF
- `CreateNvmfTarget`, `DeleteNvmfTarget` — still needed for transport
- `nativeMu`, `nativeID`, `transportInit` fields — still needed

- [ ] **Step 2: Verify Go code compiles**

```bash
go build ./...
```

- [ ] **Step 3: Run existing tests**

```bash
go test ./internal/spdk/... ./internal/agent/... ./internal/csi/... -v -count=1
```

- [ ] **Step 4: Commit**

```bash
git add internal/spdk/client.go
git commit -m "[Refactor] Remove dead SPDK native lvol methods, volume lifecycle uses chunk backend"
```

---

## Chunk 3: Build, Deploy, Verify End-to-End

### Task 7: Build and deploy updated images

- [ ] **Step 1: Build dataplane image**

```bash
docker build -f deploy/docker/Dockerfile.dataplane --platform linux/arm64 -t ghcr.io/azrtydxb/novastor-dataplane:latest dataplane/
```

- [ ] **Step 2: Build agent image**

```bash
docker build -f build/Dockerfile.agent --platform linux/arm64 --build-arg TARGETARCH=arm64 -t ghcr.io/azrtydxb/novastor-agent:latest .
```

- [ ] **Step 3: Push images and redeploy**

```bash
docker push ghcr.io/azrtydxb/novastor-dataplane:latest
docker push ghcr.io/azrtydxb/novastor-agent:latest
kubectl --context default -n novastor-system rollout restart daemonset/novastor-dataplane
kubectl --context default -n novastor-system rollout restart daemonset/novastor-agent
```

- [ ] **Step 4: Wait for pods to be ready**

```bash
kubectl --context default -n novastor-system rollout status daemonset/novastor-dataplane --timeout=120s
kubectl --context default -n novastor-system rollout status daemonset/novastor-agent --timeout=120s
```

### Task 8: End-to-end verification

- [ ] **Step 1: Create test PVC**

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test-chunk-block
  namespace: novastor-system
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: novastor-block-replicated
  resources:
    requests:
      storage: 64Mi
```

- [ ] **Step 2: Create test pod that writes data**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: test-chunk-writer
  namespace: novastor-system
spec:
  containers:
    - name: writer
      image: alpine:3.20
      command: ["/bin/sh", "-c"]
      args:
        - |
          echo "NovaStor chunk-backed volume works!" > /data/test.txt
          cat /data/test.txt
          sleep 3600
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: test-chunk-block
```

- [ ] **Step 3: Verify PVC binds and pod runs**

```bash
kubectl --context default -n novastor-system get pvc test-chunk-block
kubectl --context default -n novastor-system get pod test-chunk-writer
```

Expected: PVC status `Bound`, pod status `Running`.

- [ ] **Step 4: Verify data was written**

```bash
kubectl --context default -n novastor-system logs test-chunk-writer
kubectl --context default -n novastor-system exec test-chunk-writer -- cat /data/test.txt
```

Expected: `NovaStor chunk-backed volume works!`

- [ ] **Step 5: Verify chunk backend is in the data path**

Check agent logs for "chunk volume created" (not "lvol created"):

```bash
kubectl --context default -n novastor-system logs -l app.kubernetes.io/component=agent --tail=50 | grep -E "chunk volume|chunk backend"
```

Expected: Lines containing "chunk volume created" and "chunk backend initialised".

- [ ] **Step 6: Clean up test resources**

```bash
kubectl --context default -n novastor-system delete pod test-chunk-writer
kubectl --context default -n novastor-system delete pvc test-chunk-block
```

- [ ] **Step 7: Commit all changes**

```bash
git add -A
git commit -m "[Feature] Wire chunk engine into NVMe-oF data path — end-to-end verified"
```
