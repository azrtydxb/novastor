# Phase 2: Block Storage — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the CSI driver providing block storage to Kubernetes pods, with NVMe-oF/TCP transport for high-performance block I/O.

**Architecture:** CSI controller plugin handles volume lifecycle (create/delete/snapshot). CSI node plugin handles attach/mount on the pod's node. NVMe-oF/TCP target on the agent exposes chunks as block devices. The metadata service tracks volume-to-chunk mappings.

**Tech Stack:** container-storage-interface/spec, NVMe-oF/TCP (Linux nvmet kernel subsystem), gRPC.

---

### Task 1: CSI Identity and Controller Service

**Files:**
- Create: `internal/csi/identity.go`
- Create: `internal/csi/controller.go`
- Create: `internal/csi/controller_test.go`

Implement CSI Identity service (GetPluginInfo, GetPluginCapabilities, Probe) and Controller service (CreateVolume, DeleteVolume, ValidateVolumeCapabilities, ListVolumes, ControllerGetCapabilities). CreateVolume allocates chunks via placement engine and records mapping in metadata service. DeleteVolume cleans up chunks and metadata.

### Task 2: CSI Node Service

**Files:**
- Create: `internal/csi/node.go`
- Create: `internal/csi/node_test.go`

Implement NodeStageVolume (connect to remote chunk storage, assemble block device), NodeUnstageVolume, NodePublishVolume (bind-mount to pod path), NodeUnpublishVolume, NodeGetCapabilities, NodeGetInfo.

### Task 3: NVMe-oF/TCP Target on Agent

**Files:**
- Create: `internal/nvmeof/target.go`
- Create: `internal/nvmeof/target_test.go`

Expose chunk sequences as NVMe-oF/TCP targets using the Linux nvmet configfs interface. Create subsystem, namespace, port, and link them. Support for creating and tearing down targets per volume.

### Task 4: CSI Driver Binary

**Files:**
- Modify: `cmd/csi/main.go`

Wire up CSI identity, controller, and node services into the CSI driver binary. Register with gRPC server, handle socket-based communication per CSI spec.

### Task 5: StorageClass and PVC Integration

**Files:**
- Create: `deploy/helm/novastor/templates/storageclass.yaml`
- Create: `config/samples/pvc-block.yaml`
- Create: `test/e2e/block_storage_test.go`

Create default StorageClass, sample PVC, and end-to-end test that provisions a volume, writes data, reads it back, and deletes the volume.

### Task 6: Volume Snapshots

**Files:**
- Create: `internal/csi/snapshot.go`
- Create: `internal/csi/snapshot_test.go`

Implement CreateSnapshot, DeleteSnapshot, ListSnapshots. Snapshots are metadata-only (record chunk IDs at point in time) since chunks are immutable.

### Task 7: Volume Expansion

**Files:**
- Modify: `internal/csi/controller.go`
- Create: `internal/csi/expand_test.go`

Implement ControllerExpandVolume and NodeExpandVolume. Allocate additional chunks, update metadata, resize filesystem.

### Task 8: Final Verification

Run full test suite, lint, build all binaries, verify CSI driver passes CSI sanity tests.
