# Move DataProtection from StoragePool to Volume Level

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove DataProtection from StoragePool and BackendAssignment (they are pure resource pools / backend provisioning), and make it a required field on BlockVolume, SharedFilesystem, and ObjectStore (data protection is per-volume).

**Architecture:** StoragePool defines which nodes/devices form the resource pool and the backend type (raw/lvm/file). DataProtection belongs at the volume level because different volumes in the same pool can have different protection schemes (e.g., one volume replicated 3x, another erasure-coded 4+2). The policy engine must read protection config from the volume's metadata, not from the pool.

**Tech Stack:** Go 1.25+, controller-runtime, kubebuilder CRD markers, Helm chart

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `api/v1alpha1/types.go` | Modify | Remove DP from StoragePoolSpec/Status/BackendAssignmentSpec, add to BlockVolumeSpec/SharedFilesystemSpec/ObjectStoreSpec |
| `api/v1alpha1/zz_generated_deepcopy_manual.go` | Modify | Update deepcopy for changed types |
| `internal/controller/storagepool_controller.go` | Modify | Remove DP validation, remove DP from buildAssignmentSpec, remove DP from status |
| `internal/policy/compliance.go` | Modify | Change GetChecker/CheckVolumeCompliance/CheckPoolCompliance to read DP from volume |
| `internal/policy/replication_checker.go` | Modify | Change to accept DataProtectionSpec directly, not pool |
| `internal/policy/erasure_checker.go` | Modify | Change to accept DataProtectionSpec directly, not pool |
| `internal/policy/reconciler.go` | Modify | repairErasureChunk reads DP from volume not pool |
| `internal/controller/controller_test.go` | Modify | Remove DP from pool test fixtures, add DP to volume test fixtures |
| `internal/policy/compliance_test.go` | Modify | Move DP from pool fixtures to volume fixtures |
| `config/crd/novastor.io_storagepools.yaml` | Regenerate | Remove DP fields |
| `config/crd/novastor.io_backendassignments.yaml` | Regenerate | Remove DP fields |
| `config/crd/novastor.io_blockvolumes.yaml` | Regenerate | Add DP fields |
| `config/crd/novastor.io_objectstores.yaml` | Regenerate | Add DP fields |
| `config/crd/novastor.io_sharedfilesystems.yaml` | Regenerate | Add DP fields |
| `deploy/crds/*.yaml` | Sync | Copy from config/crd |
| `deploy/helm/novastor/templates/crds.yaml` | Regenerate | Updated CRDs in Helm |
| `config/samples/storagepool-*.yaml` | Modify | Remove DP from pool samples |
| `config/samples/blockvolume.yaml` | Modify | Add DP |
| `config/samples/sharedfilesystem.yaml` | Modify | Add DP |
| `config/samples/objectstore.yaml` | Modify | Add DP |
| `test/e2e/test-storagepool.yaml` | Modify | Remove DP |
| `test/e2e/test-blockvolume.yaml` | Modify | Add DP |

---

## Chunk 1: API Types and DeepCopy

### Task 1: Remove DataProtection from StoragePoolSpec

**Files:**
- Modify: `api/v1alpha1/types.go:10-48`

- [ ] **Step 1: Update StoragePool kubebuilder markers**

Remove the `Mode` print column from StoragePool (it no longer has a DP mode):

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backendType`
// +kubebuilder:printcolumn:name="Nodes",type=integer,JSONPath=`.status.nodeCount`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.status.totalCapacity`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// StoragePool defines a set of storage nodes and their backend configuration.
type StoragePool struct {
```

- [ ] **Step 2: Remove DataProtection from StoragePoolSpec**

Remove the `DataProtection` field from `StoragePoolSpec`:

```go
type StoragePoolSpec struct {
	// NodeSelector selects which nodes belong to this pool.
	NodeSelector *metav1.LabelSelector `json:"nodeSelector,omitempty"`

	// BackendType selects the storage backend for this pool.
	// - "file": File on existing mounted filesystem → SPDK AIO bdev
	// - "lvm": SPDK lvol store on unbound NVMe device (thin provisioning, snapshots)
	// - "raw": Direct SPDK NVMe bdev on unbound NVMe device (highest performance)
	// All backends produce SPDK bdevs consumed by the chunk engine.
	// +kubebuilder:validation:Enum=file;lvm;raw
	// +kubebuilder:default=lvm
	BackendType string `json:"backendType"`

	// DeviceFilter specifies which devices on selected nodes to use.
	// For "raw" and "lvm" backends, this selects NVMe devices to unbind and use.
	// For "file" backend, this is ignored (use FileBackend instead).
	DeviceFilter *DeviceFilter `json:"deviceFilter,omitempty"`

	// FileBackend configures the file backend. Only used when backendType is "file".
	FileBackend *FileBackendSpec `json:"fileBackend,omitempty"`
}
```

- [ ] **Step 3: Remove DataProtection from StoragePoolStatus**

```go
type StoragePoolStatus struct {
	Phase         string             `json:"phase,omitempty"`
	NodeCount     int                `json:"nodeCount,omitempty"`
	TotalCapacity string             `json:"totalCapacity,omitempty"`
	UsedCapacity  string             `json:"usedCapacity,omitempty"`
	Conditions    []metav1.Condition `json:"conditions,omitempty"`
}
```

- [ ] **Step 4: Remove DataProtection from BackendAssignmentSpec**

```go
type BackendAssignmentSpec struct {
	// PoolRef is the name of the StoragePool this assignment belongs to.
	PoolRef string `json:"poolRef"`

	// NodeName is the node this assignment targets.
	NodeName string `json:"nodeName"`

	// BackendType is the backend type (copied from StoragePool).
	// +kubebuilder:validation:Enum=file;lvm;raw
	BackendType string `json:"backendType"`

	// DeviceFilter specifies which devices to use (copied from StoragePool).
	// For raw/lvm backends only.
	DeviceFilter *DeviceFilter `json:"deviceFilter,omitempty"`

	// FileBackend configures the file backend (copied from StoragePool).
	// For file backend only.
	FileBackend *FileBackendSpec `json:"fileBackend,omitempty"`
}
```

### Task 2: Add DataProtection to Volume Types

**Files:**
- Modify: `api/v1alpha1/types.go:108-233`

- [ ] **Step 1: Add DataProtection to BlockVolumeSpec**

Add `DataProtection` as required field and add print column:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Size",type=string,JSONPath=`.spec.size`
// +kubebuilder:printcolumn:name="Protection",type=string,JSONPath=`.spec.dataProtection.mode`
// +kubebuilder:printcolumn:name="Access",type=string,JSONPath=`.spec.accessMode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
```

```go
type BlockVolumeSpec struct {
	Pool string `json:"pool"`
	Size string `json:"size"`
	// +kubebuilder:validation:Enum=ReadWriteOnce;ReadOnlyMany
	AccessMode string `json:"accessMode"`
	// DataProtection configures how this volume's data is protected.
	DataProtection DataProtectionSpec `json:"dataProtection"`
	// Quota specifies an optional per-volume quota limit.
	// When set, the volume cannot grow beyond this size.
	// +optional
	Quota *int64 `json:"quota,omitempty"`
}
```

- [ ] **Step 2: Add DataProtection to SharedFilesystemSpec**

Add print column for Protection:

```go
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Capacity",type=string,JSONPath=`.spec.capacity`
// +kubebuilder:printcolumn:name="Protection",type=string,JSONPath=`.spec.dataProtection.mode`
// +kubebuilder:printcolumn:name="Protocol",type=string,JSONPath=`.spec.export.protocol`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
```

```go
type SharedFilesystemSpec struct {
	Pool     string `json:"pool"`
	Capacity string `json:"capacity"`
	// +kubebuilder:validation:Enum=ReadWriteMany;ReadOnlyMany
	AccessMode string `json:"accessMode"`
	// DataProtection configures how this filesystem's data is protected.
	DataProtection DataProtectionSpec `json:"dataProtection"`
	Export         *ExportSpec        `json:"export,omitempty"`
	// Image overrides the NFS filer container image. Defaults to novastor/novastor-filer:v0.1.0.
	// +optional
	Image string `json:"image,omitempty"`
}
```

- [ ] **Step 3: Add DataProtection to ObjectStoreSpec**

Add print column for Protection:

```go
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.spec.pool`
// +kubebuilder:printcolumn:name="Protection",type=string,JSONPath=`.spec.dataProtection.mode`
// +kubebuilder:printcolumn:name="Port",type=integer,JSONPath=`.spec.endpoint.service.port`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
```

```go
type ObjectStoreSpec struct {
	Pool string `json:"pool"`
	// DataProtection configures how this object store's data is protected.
	DataProtection DataProtectionSpec `json:"dataProtection"`
	Endpoint       ObjectEndpointSpec `json:"endpoint"`
	BucketPolicy   *BucketPolicySpec  `json:"bucketPolicy,omitempty"`
	// Image overrides the S3 gateway container image. Defaults to novastor/novastor-s3gw:v0.1.0.
	// +optional
	Image string `json:"image,omitempty"`
}
```

### Task 3: Update DeepCopy

**Files:**
- Modify: `api/v1alpha1/zz_generated_deepcopy_manual.go`

- [ ] **Step 1: Remove DataProtection from StoragePoolSpec DeepCopy**

Remove line `in.DataProtection.DeepCopyInto(&out.DataProtection)` from `StoragePoolSpec.DeepCopyInto`.

- [ ] **Step 2: Remove DataProtection from BackendAssignmentSpec DeepCopy**

Remove line `in.DataProtection.DeepCopyInto(&out.DataProtection)` from `BackendAssignmentSpec.DeepCopyInto`.

- [ ] **Step 3: Add DataProtection to BlockVolumeSpec DeepCopy**

```go
func (in *BlockVolumeSpec) DeepCopyInto(out *BlockVolumeSpec) {
	*out = *in
	in.DataProtection.DeepCopyInto(&out.DataProtection)
	if in.Quota != nil {
		in, out := &in.Quota, &out.Quota
		*out = new(int64)
		**out = **in
	}
}
```

- [ ] **Step 4: Add DataProtection to SharedFilesystemSpec DeepCopy**

```go
func (in *SharedFilesystemSpec) DeepCopyInto(out *SharedFilesystemSpec) {
	*out = *in
	in.DataProtection.DeepCopyInto(&out.DataProtection)
	if in.Export != nil {
		in, out := &in.Export, &out.Export
		*out = new(ExportSpec)
		**out = **in
	}
}
```

- [ ] **Step 5: Add DataProtection to ObjectStoreSpec DeepCopy**

```go
func (in *ObjectStoreSpec) DeepCopyInto(out *ObjectStoreSpec) {
	*out = *in
	in.DataProtection.DeepCopyInto(&out.DataProtection)
	out.Endpoint = in.Endpoint
	if in.BucketPolicy != nil {
		in, out := &in.BucketPolicy, &out.BucketPolicy
		*out = new(BucketPolicySpec)
		**out = **in
	}
}
```

- [ ] **Step 6: Verify compilation**

Run: `cd /Users/pascal/Development/Nova/novastor && go build ./api/...`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add api/v1alpha1/types.go api/v1alpha1/zz_generated_deepcopy_manual.go
git commit -m "[Refactor] Move DataProtection from StoragePool to volume types

DataProtection is per-volume, not per-pool. A StoragePool is a pure
resource pool (nodes + backend type + devices). Different volumes in
the same pool can have different protection schemes.

- Remove DataProtection from StoragePoolSpec, StoragePoolStatus, BackendAssignmentSpec
- Add DataProtection as required field on BlockVolumeSpec, SharedFilesystemSpec, ObjectStoreSpec
- Update deepcopy methods accordingly"
```

---

## Chunk 2: StoragePool Controller

### Task 4: Update StoragePool Controller

**Files:**
- Modify: `internal/controller/storagepool_controller.go`

- [ ] **Step 1: Remove DataProtection validation from Reconcile**

Remove lines 44-89 (the three DP validation blocks). The Reconcile function should go directly from getting the pool to counting matching nodes. Also remove `pool.Status.DataProtection = pool.Spec.DataProtection.Mode` (line 141).

After the change, the Reconcile method should flow:
1. Get pool
2. Count matching nodes
3. Handle zero nodes
4. Reconcile BackendAssignments
5. Set status (Phase, NodeCount, TotalCapacity — no DataProtection)
6. Update status

- [ ] **Step 2: Remove DataProtection from buildAssignmentSpec**

Remove `DataProtection: pool.Spec.DataProtection,` from the `buildAssignmentSpec` method (line 268).

- [ ] **Step 3: Verify compilation**

Run: `cd /Users/pascal/Development/Nova/novastor && go build ./internal/controller/...`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add internal/controller/storagepool_controller.go
git commit -m "[Refactor] Remove DataProtection from StoragePool controller

StoragePool is a pure resource pool. DataProtection validation and
status tracking belong at the volume level, not the pool level."
```

---

## Chunk 3: Policy Engine

### Task 5: Refactor Policy Engine to Read DP from Volume

The policy engine currently reads DataProtection from the pool spec. It must read from the volume instead. The `VolumeMeta` in `internal/policy/replication_checker.go` already has a `DataProtection` field via `internal/metadata/store.go`, but the policy engine's local `VolumeMeta` struct (line 36-41 in `replication_checker.go`) doesn't include it.

**Files:**
- Modify: `internal/policy/replication_checker.go`
- Modify: `internal/policy/erasure_checker.go`
- Modify: `internal/policy/compliance.go`
- Modify: `internal/policy/reconciler.go`

- [ ] **Step 1: Add DataProtection to policy VolumeMeta**

In `internal/policy/replication_checker.go`, add `DataProtection` to the `VolumeMeta` struct:

```go
// VolumeMeta represents metadata about a volume.
type VolumeMeta struct {
	VolumeID       string              `json:"volumeID"`
	Pool           string              `json:"pool"`
	SizeBytes      uint64              `json:"sizeBytes"`
	ChunkIDs       []string            `json:"chunkIDs"`
	DataProtection *DataProtectionSpec `json:"dataProtection,omitempty"`
}
```

Add the import and type at the top of the file (or define locally):

```go
// DataProtectionSpec mirrors the CRD type for use within the policy engine.
// This avoids importing the full CRD API package in the policy engine's domain types.
type DataProtectionSpec = v1alpha1.DataProtectionSpec
```

Actually, `v1alpha1` is already imported. Just use `*v1alpha1.DataProtectionSpec`.

- [ ] **Step 2: Refactor ReplicationChecker to use volume DP**

Remove the `pool *v1alpha1.StoragePool` field from `ReplicationChecker`. Change `CheckChunk` to read DP from the volume:

```go
type ReplicationChecker struct {
	metaClient  MetadataClient
	nodeChecker NodeAvailabilityChecker
}
```

```go
func (c *ReplicationChecker) RequiredReplicas(volume *VolumeMeta) int {
	if volume == nil || volume.DataProtection == nil || volume.DataProtection.Replication == nil {
		return 3 // Default fallback
	}
	factor := volume.DataProtection.Replication.Factor
	if factor == 0 {
		return 3
	}
	return factor
}
```

Update `CheckChunk` signature — remove `pool` parameter, read DP from volume:

```go
func (c *ReplicationChecker) CheckChunk(ctx context.Context, chunkID string, volume *VolumeMeta) (*ChunkComplianceResult, error) {
	if volume.DataProtection == nil || volume.DataProtection.Mode != "replication" {
		return nil, fmt.Errorf("volume %s is not in replication mode", volume.VolumeID)
	}

	replicationSpec := volume.DataProtection.Replication
	if replicationSpec == nil {
		return nil, fmt.Errorf("volume %s has nil replication spec", volume.VolumeID)
	}

	expectedCount := replicationSpec.Factor
	if expectedCount == 0 {
		expectedCount = 3
	}

	placement, err := c.metaClient.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return nil, fmt.Errorf("getting placement map for chunk %s: %w", chunkID, err)
	}

	result := &ChunkComplianceResult{
		ChunkID:        chunkID,
		VolumeID:       volume.VolumeID,
		Pool:           volume.Pool,
		ProtectionMode: "replication",
		ExpectedCount:  expectedCount,
	}

	for _, nodeID := range placement.Nodes {
		if c.nodeChecker.IsNodeAvailable(ctx, nodeID) {
			result.AvailableNodes = append(result.AvailableNodes, nodeID)
		} else {
			result.FailedNodes = append(result.FailedNodes, nodeID)
		}
	}

	result.ActualCount = len(result.AvailableNodes)

	if result.ActualCount == 0 {
		result.Status = StatusUnavailable
	} else if result.ActualCount < expectedCount {
		result.Status = StatusUnderReplicated
	} else {
		result.Status = StatusCompliant
	}

	return result, nil
}
```

Also update `CheckChunkWithChecksum` and `NewReplicationChecker` (remove mutex, pool field).

- [ ] **Step 3: Refactor ErasureCodingChecker to use volume DP**

Same pattern — remove `pool` field and mutex, read DP from volume in `CheckChunk`, `RequiredReplicas`, `CheckChunkWithIntegrity`.

- [ ] **Step 4: Update ComplianceChecker interface**

In `compliance.go`, update the interface:

```go
type ComplianceChecker interface {
	CheckChunk(ctx context.Context, chunkID string, volume *VolumeMeta) (*ChunkComplianceResult, error)
	RequiredReplicas(volume *VolumeMeta) int
}
```

- [ ] **Step 5: Update PolicyEngine methods**

`GetChecker` should take a volume, not a pool:

```go
func (e *PolicyEngine) GetChecker(volume *VolumeMeta) (ComplianceChecker, error) {
	if volume.DataProtection == nil {
		return nil, fmt.Errorf("volume %s has no data protection config", volume.VolumeID)
	}
	switch volume.DataProtection.Mode {
	case "replication":
		return e.replicationChecker, nil
	case "erasureCoding":
		return e.erasureChecker, nil
	default:
		return nil, fmt.Errorf("unknown data protection mode: %s", volume.DataProtection.Mode)
	}
}
```

`CheckVolumeCompliance` — remove pool parameter:

```go
func (e *PolicyEngine) CheckVolumeCompliance(ctx context.Context, volume *VolumeMeta) (*VolumeComplianceReport, error) {
	report := &VolumeComplianceReport{
		VolumeID: volume.VolumeID,
		Pool:     volume.Pool,
	}

	checker, err := e.GetChecker(volume)
	if err != nil {
		return nil, fmt.Errorf("getting compliance checker for volume %s: %w", volume.VolumeID, err)
	}

	for _, chunkID := range volume.ChunkIDs {
		result, err := checker.CheckChunk(ctx, chunkID, volume)
		if err != nil {
			return nil, fmt.Errorf("checking chunk %s: %w", chunkID, err)
		}

		report.TotalChunks++
		switch result.Status {
		case StatusCompliant:
			report.CompliantChunks++
		case StatusUnderReplicated:
			report.UnderReplicatedChunks++
		case StatusUnavailable:
			report.UnavailableChunks++
		case StatusCorrupted:
			report.CorruptedChunks++
		}
		report.ChunkResults = append(report.ChunkResults, result)
	}

	return report, nil
}
```

`CheckPoolCompliance` — no longer reads DP from pool, aggregates from volumes:

```go
func (e *PolicyEngine) CheckPoolCompliance(ctx context.Context, pool *v1alpha1.StoragePool) (*PoolComplianceReport, error) {
	report := &PoolComplianceReport{
		PoolName: pool.Name,
	}

	volumes, err := e.metaClient.ListVolumesMeta(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}

	for _, volume := range volumes {
		if volume.Pool != pool.Name {
			continue
		}
		report.TotalVolumes++

		// Set protection mode from first volume (for display)
		if report.ProtectionMode == "" && volume.DataProtection != nil {
			report.ProtectionMode = volume.DataProtection.Mode
			if volume.DataProtection.Replication != nil {
				report.ReplicationFactor = volume.DataProtection.Replication.Factor
			}
			if volume.DataProtection.ErasureCoding != nil {
				report.DataShards = volume.DataProtection.ErasureCoding.DataShards
				report.ParityShards = volume.DataProtection.ErasureCoding.ParityShards
			}
		}

		volumeReport, err := e.CheckVolumeCompliance(ctx, volume)
		if err != nil {
			return nil, fmt.Errorf("checking volume %s: %w", volume.VolumeID, err)
		}

		report.TotalChunks += volumeReport.TotalChunks
		report.CompliantChunks += volumeReport.CompliantChunks
		report.UnderReplicatedChunks += volumeReport.UnderReplicatedChunks
		report.UnavailableChunks += volumeReport.UnavailableChunks
		report.CorruptedChunks += volumeReport.CorruptedChunks
		report.VolumeReports = append(report.VolumeReports, volumeReport)
	}

	report.IsCompliant = report.UnderReplicatedChunks == 0 &&
		report.UnavailableChunks == 0 &&
		report.CorruptedChunks == 0

	return report, nil
}
```

- [ ] **Step 6: Update Reconciler.repairErasureChunk**

In `reconciler.go`, change `repairErasureChunk` to get DP from volume metadata instead of pool:

```go
func (r *Reconciler) repairErasureChunk(ctx context.Context, task RepairTask, _ *v1alpha1.StoragePool, healthyNodes []string) error {
	if len(task.AvailableNodes) == 0 {
		return fmt.Errorf("no available shards for chunk %s", task.ChunkID)
	}

	if r.shardReplicator == nil {
		return fmt.Errorf("shard replicator not configured")
	}

	// Get volume metadata for DP config
	volume, err := r.metaClient.GetVolumeMeta(ctx, task.VolumeID)
	if err != nil {
		return fmt.Errorf("getting volume %s metadata: %w", task.VolumeID, err)
	}
	if volume == nil || volume.DataProtection == nil || volume.DataProtection.ErasureCoding == nil {
		return fmt.Errorf("volume %s has no erasure coding config", task.VolumeID)
	}

	ecSpec := volume.DataProtection.ErasureCoding
	dataShards := ecSpec.DataShards
	if dataShards == 0 {
		dataShards = 4
	}
	parityShards := ecSpec.ParityShards
	if parityShards == 0 {
		parityShards = 2
	}

	destNode := healthyNodes[0]

	if err := r.shardReplicator.RegenerateShard(ctx, task.ChunkID, task.AvailableNodes, destNode, dataShards, parityShards); err != nil {
		return fmt.Errorf("regenerating shard: %w", err)
	}

	// Update placement map (same logic as before)
	placement, err := r.metaClient.GetPlacementMap(ctx, task.ChunkID)
	if err != nil {
		return fmt.Errorf("getting placement map: %w", err)
	}

	updated := false
	for i, node := range placement.Nodes {
		isFailed := false
		for _, failed := range task.FailedNodes {
			if node == failed {
				isFailed = true
				break
			}
		}
		if isFailed && !updated {
			placement.Nodes[i] = destNode
			updated = true
			break
		}
	}
	if !updated {
		placement.Nodes = append(placement.Nodes, destNode)
	}

	if err := r.metaClient.PutPlacementMap(ctx, placement); err != nil {
		return fmt.Errorf("updating placement map: %w", err)
	}

	return nil
}
```

- [ ] **Step 7: Verify compilation**

Run: `cd /Users/pascal/Development/Nova/novastor && go build ./internal/policy/...`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add internal/policy/compliance.go internal/policy/replication_checker.go internal/policy/erasure_checker.go internal/policy/reconciler.go
git commit -m "[Refactor] Policy engine reads DataProtection from volume, not pool

The policy engine now reads data protection configuration from the
volume's metadata instead of the pool spec. This aligns with the
per-volume data protection architecture."
```

---

## Chunk 4: Tests

### Task 6: Update Controller Tests

**Files:**
- Modify: `internal/controller/controller_test.go`

- [ ] **Step 1: Update TestStoragePoolReconciler_ValidPool**

Remove `DataProtection` from pool spec. Remove assertion for `updated.Status.DataProtection`. Add `BackendType: "raw"` to pool spec since it's now required.

- [ ] **Step 2: Update TestStoragePoolReconciler_InvalidSpec**

This test validated empty DP mode. Since DP is removed from pool, this test should validate a different invalid condition (e.g., missing backendType). Or simply test that a pool with no matching nodes gets Pending status.

- [ ] **Step 3: Add DataProtection to BlockVolume test fixtures**

In `TestBlockVolumeReconciler_CreatesPV`, add `DataProtection` to the volume spec:

```go
Spec: novastorev1alpha1.BlockVolumeSpec{
	Pool:       "block-pool",
	Size:       "10Gi",
	AccessMode: "ReadWriteOnce",
	DataProtection: novastorev1alpha1.DataProtectionSpec{
		Mode: "replication",
		Replication: &novastorev1alpha1.ReplicationSpec{
			Factor: 3,
		},
	},
},
```

- [ ] **Step 4: Run controller tests**

Run: `cd /Users/pascal/Development/Nova/novastor && go test ./internal/controller/... -v -count=1`
Expected: PASS

### Task 7: Update Policy Tests

**Files:**
- Modify: `internal/policy/compliance_test.go`

- [ ] **Step 1: Add DataProtection to VolumeMeta in test fixtures**

In all test cases, move `DataProtection` from the pool's `Spec` to the `VolumeMeta`. Remove `DataProtection` from pool specs in test fixtures. Add `DataProtection` field to volume fixtures:

```go
volume := &VolumeMeta{
	VolumeID: "vol1",
	Pool:     "test-pool",
	ChunkIDs: []string{"chunk1"},
	DataProtection: &v1alpha1.DataProtectionSpec{
		Mode: "replication",
		Replication: &v1alpha1.ReplicationSpec{
			Factor: 3,
		},
	},
}
```

- [ ] **Step 2: Update CheckChunk calls**

Change `checker.CheckChunk(ctx, chunkID, volume, pool)` to `checker.CheckChunk(ctx, chunkID, volume)` throughout.

- [ ] **Step 3: Update CheckVolumeCompliance calls**

Change `engine.CheckVolumeCompliance(ctx, volume, pool)` to `engine.CheckVolumeCompliance(ctx, volume)`.

- [ ] **Step 4: Update pool compliance test**

In `TestPolicyEngine_CheckPoolCompliance` and `TestReconciler_ScanAndEnqueue`, remove `DataProtection` from pool specs. Add `DataProtection` to `VolumeMeta` entries.

- [ ] **Step 5: Run policy tests**

Run: `cd /Users/pascal/Development/Nova/novastor && go test ./internal/policy/... -v -count=1`
Expected: PASS

- [ ] **Step 6: Commit all test changes**

```bash
git add internal/controller/controller_test.go internal/policy/compliance_test.go
git commit -m "[Test] Update tests for per-volume DataProtection

Move DataProtection from pool fixtures to volume fixtures in all
controller and policy engine tests."
```

---

## Chunk 5: CRDs, Samples, Helm, E2E

### Task 8: Regenerate CRDs

**Files:**
- Regenerate: `config/crd/*.yaml`
- Sync: `deploy/crds/*.yaml`

- [ ] **Step 1: Run CRD generation**

Run: `cd /Users/pascal/Development/Nova/novastor && make manifests`

If `make manifests` is not available or fails, manually update the CRD YAML files:
- `config/crd/novastor.io_storagepools.yaml`: Remove `dataProtection` from spec schema, remove `required: ["dataProtection"]`, remove Mode print column
- `config/crd/novastor.io_backendassignments.yaml`: Remove `dataProtection` from spec schema, update required list
- `config/crd/novastor.io_blockvolumes.yaml`: Add `dataProtection` to spec schema with full sub-schema
- `config/crd/novastor.io_sharedfilesystems.yaml`: Add `dataProtection` to spec schema
- `config/crd/novastor.io_objectstores.yaml`: Add `dataProtection` to spec schema

- [ ] **Step 2: Sync deploy/crds**

Copy updated CRDs from `config/crd/` to `deploy/crds/`:

```bash
cp config/crd/novastor.io_storagepools.yaml deploy/crds/
cp config/crd/novastor.io_blockvolumes.yaml deploy/crds/
cp config/crd/novastor.io_sharedfilesystems.yaml deploy/crds/
cp config/crd/novastor.io_objectstores.yaml deploy/crds/
```

- [ ] **Step 3: Update Helm CRDs template**

Update `deploy/helm/novastor/templates/crds.yaml` to match the regenerated CRDs. This file embeds all CRDs inline for Helm installation.

### Task 9: Update Samples

**Files:**
- Modify: `config/samples/storagepool-replicated.yaml`
- Modify: `config/samples/storagepool-replication.yaml`
- Modify: `config/samples/storagepool-erasurecoded.yaml`
- Modify: `config/samples/storagepool-erasurecoding.yaml`
- Modify: `config/samples/blockvolume.yaml`
- Modify: `config/samples/sharedfilesystem.yaml`
- Modify: `config/samples/objectstore.yaml`
- Modify: `test/e2e/test-storagepool.yaml`
- Modify: `test/e2e/test-blockvolume.yaml`

- [ ] **Step 1: Update StoragePool samples — remove dataProtection**

`config/samples/storagepool-replicated.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: nvme-replicated
spec:
  backendType: raw
  nodeSelector:
    matchLabels:
      storage-tier: nvme
  deviceFilter:
    type: nvme
    minSize: "100Gi"
```

Same pattern for all storagepool-*.yaml samples — remove `dataProtection` section, add `backendType`.

- [ ] **Step 2: Update BlockVolume sample — add dataProtection**

`config/samples/blockvolume.yaml`:
```yaml
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
metadata:
  name: example-block
  namespace: default
spec:
  pool: nvme-replicated
  size: "10Gi"
  accessMode: ReadWriteOnce
  dataProtection:
    mode: replication
    replication:
      factor: 3
      writeQuorum: 2
```

- [ ] **Step 3: Update SharedFilesystem sample — add dataProtection**

- [ ] **Step 4: Update ObjectStore sample — add dataProtection**

- [ ] **Step 5: Update e2e test-storagepool.yaml — remove dataProtection, add backendType**

- [ ] **Step 6: Update e2e test-blockvolume.yaml — add dataProtection**

- [ ] **Step 7: Verify build**

Run: `cd /Users/pascal/Development/Nova/novastor && go build ./... && go test ./... -count=1`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add config/crd/ deploy/crds/ deploy/helm/novastor/templates/crds.yaml config/samples/ test/e2e/
git commit -m "[Refactor] Update CRDs, samples, and Helm chart for per-volume DataProtection

- Regenerate CRDs to reflect API type changes
- Update all sample manifests
- Update Helm chart CRD template
- Update e2e test fixtures"
```

---

## Chunk 6: Helm Values and StorageClass Cleanup

### Task 10: Update Helm Values

**Files:**
- Modify: `deploy/helm/novastor/values.yaml`

- [ ] **Step 1: Remove dataProtection from storageClass values**

The `storageClass` section in `values.yaml` has `dataProtection: replication` — this is a Helm-level default for the StorageClass, which correctly uses CSI parameters (not CRD fields). The StorageClass template already uses `protection: replication` in parameters. No change needed to the StorageClass template itself.

Remove the `dataProtection` key from `storageClass` in values.yaml if it references the pool-level concept:

```yaml
storageClass:
  name: novastor-block
  isDefault: false
  pool: nvme-raw
  reclaimPolicy: Delete
  volumeBindingMode: WaitForFirstConsumer
```

- [ ] **Step 2: Commit**

```bash
git add deploy/helm/novastor/values.yaml
git commit -m "[Chore] Clean up Helm values for per-volume DataProtection"
```

---

## Post-Implementation Verification

After all tasks are complete:

1. Run full test suite: `cd /Users/pascal/Development/Nova/novastor && go test ./... -count=1 -race`
2. Run linter: `cd /Users/pascal/Development/Nova/novastor && make lint` (or `golangci-lint run`)
3. Build all binaries: `cd /Users/pascal/Development/Nova/novastor && make build-all` (or `go build ./cmd/...`)
4. Verify CRDs apply cleanly: `kubectl apply -f config/crd/ --dry-run=client`
5. Deploy to cluster and verify StoragePool works without DataProtection
