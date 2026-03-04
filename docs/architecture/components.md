# Component Details

This page provides in-depth documentation for each NovaStor component: its responsibilities, configuration, and inter-component communication.

## Chunk Storage Engine

**Package**: `internal/chunk/`

The Chunk Storage Engine is the foundational library that all access layers (block, file, object) build upon. It is not a standalone binary -- it is linked into the Node Agent.

### Responsibilities

- Store and retrieve fixed-size (4 MiB) immutable data chunks
- Verify CRC-32C checksums on every read
- Content-addressed chunk IDs for deduplication potential
- Periodic integrity scrubbing via the `Scrubber` subsystem

### Scrubber

The Scrubber runs as a background goroutine on each Node Agent. It periodically scans all local chunks, verifies their CRC-32C checksums, and reports any corrupt chunks to the metadata service for re-replication.

```go
// Configuration
scrubber := chunk.NewScrubber(store, reporter, 24*time.Hour)
scrubber.SetRateLimit(50 * 1024 * 1024) // 50 MB/s I/O rate limit
scrubber.Start(ctx)
```

| Parameter | Default | Description |
|---|---|---|
| `--scrub-interval` | `24h` | Time between full scrub passes |
| Rate limit | Unlimited | Configurable bytes/sec to limit I/O impact |

### Key Interfaces

```go
// Store is the core chunk storage interface
type Store interface {
    Put(ctx context.Context, chunk *Chunk) error
    Get(ctx context.Context, id ChunkID) (*Chunk, error)
    Delete(ctx context.Context, id ChunkID) error
    List(ctx context.Context) ([]ChunkID, error)
}
```

### Storage Backends

The Chunk Storage Engine supports pluggable storage backends through the `Store` interface:

| Backend | Package | Description |
|---|---|---|
| **LocalStore** | `internal/chunk/localstore.go` | File-based storage using a local directory hierarchy |
| **LVMStore** | `internal/chunk/lvmstore.go` | LVM thin volumes for efficient snapshot and copy operations |

#### LVM Backend

The LVM backend stores each chunk as a separate thin logical volume. This enables:

- **Efficient snapshots**: Copy-on-write snapshots for fast chunk duplication
- **Thin provisioning**: Over-allocate storage with actual allocation on write
- **Volume resize**: Dynamic resizing of chunk volumes
- **Capacity tracking**: Query thin pool usage via LVM commands

```go
// Create an LVM-backed store
store, err := chunk.NewLVMStore("novastor", "chunks")
if err != nil {
    log.Fatalf("creating LVM store: %v", err)
}
```

**Requirements**:

- LVM2 tools installed (`lvcreate`, `lvremove`, `lvs`, `vgs`)
- A pre-configured volume group with a thin pool

**Configuration**:

| Parameter | Default | Description |
|---|---|---|
| Volume Group | `novastor` | LVM volume group name |
| Thin Pool | `chunks` | Thin pool within the VG |
| Chunk LV Prefix | `chunk-` | Prefix for chunk logical volumes |

**LVM Commands Used**:

| Operation | Command |
|---|---|
| Create volume | `lvcreate -V 4M --thin --name chunk-<id> novastor/chunks` |
| Delete volume | `lvremove -f novastor/chunk-<id>` |
| List volumes | `lvs --noheadings -o lv_name novastor` |
| Create snapshot | `lvcreate -s --name snap-<id> novastor/chunk-<id>` |
| Query capacity | `lvs --noheadings --units=b --nosuffix -o lv_size,data_percent` |

**Snapshot Support**:

The LVM backend supports efficient chunk snapshots via the `Snapshot` method:

```go
// Create a snapshot of an existing chunk
err := lvmStore.Snapshot(ctx, sourceChunkID, snapshotChunkID)
```

Snapshots are implemented as LVM thin snapshots, which share data blocks with the source until modified (copy-on-write).

**Capacity Tracking**:

The `Capacity` method returns current thin pool usage:

```go
total, free, err := lvmStore.Capacity(ctx)
// total: total thin pool size in bytes
// free: available space in bytes
```

---

## Metadata Service

**Binary**: `novastor-meta` | **Package**: `internal/metadata/` | **K8s Resource**: StatefulSet

The Metadata Service is the single source of truth for all storage metadata. It uses HashiCorp Raft for consensus and supports BoltDB (default) or BadgerDB as the storage backend.

### Architecture

```mermaid
graph TB
    subgraph "Metadata Service (3 replicas)"
        L["Leader<br/>Writes go here"]
        F1["Follower 1<br/>Read replicas"]
        F2["Follower 2<br/>Read replicas"]
        L <-->|Raft consensus| F1
        L <-->|Raft consensus| F2
    end

    CLIENT[gRPC Client] -->|writes| L
    CLIENT -->|reads (eventual)| F1
    CLIENT -->|reads (eventual)| F2

    style L fill:#e8f5e9
    style F1 fill:#e1f5ff
    style F2 fill:#e1f5ff
```

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--node-id` | hostname | Unique Raft node identifier |
| `--data-dir` | `/var/lib/novastor/meta` | Persistent storage directory |
| `--raft-addr` | `:7000` | Raft consensus transport address |
| `--grpc-addr` | `:7001` | gRPC client API address |
| `--metrics-addr` | `:7002` | Prometheus metrics endpoint |
| `--join` | (empty) | Comma-separated peer addresses to join existing cluster |
| `--tls-ca` | (empty) | CA certificate path for mTLS |
| `--tls-cert` | (empty) | Server certificate path for mTLS |
| `--tls-key` | (empty) | Server key path for mTLS |
| `--tls-rotation-interval` | `5m` | Certificate reload check interval |

### Metadata Categories

The FSM stores data in logical buckets:

| Bucket | Contents |
|---|---|
| `volumes` | Volume metadata (ID, pool, size, chunk IDs) |
| `placements` | Chunk placement maps (chunk ID to node list) |
| `objects` | S3 object metadata (bucket, key, ETag, chunk IDs) |
| `buckets` | S3 bucket metadata (name, creation date, versioning) |
| `multipart` | Multipart upload state |
| `snapshots` | Volume snapshot metadata |

### Raft Configuration

| Parameter | Value |
|---|---|
| Snapshot interval | 30 seconds |
| Snapshot threshold | 1024 log entries |
| Max snapshots retained | 2 |
| TCP transport connections | 3 |
| Transport timeout | 10 seconds |
| Join retry attempts | 30 |
| Join retry delay | 2 seconds |

### Cluster Bootstrapping

When `--join` is empty, the node bootstraps as a single-node cluster. When `--join` contains peer addresses, the node attempts to join an existing cluster by calling `AddVoter` on each peer until one accepts.

---

## Node Agent

**Binary**: `novastor-agent` | **Package**: `cmd/agent/`, `internal/agent/` | **K8s Resource**: DaemonSet

The Node Agent runs on every storage node and manages local chunk storage, disk devices, and health reporting.

### Responsibilities

- Serve chunk read/write/delete operations over gRPC
- Run periodic chunk integrity scrubs
- Expose Prometheus metrics (chunk counts, disk usage, scrub errors)
- Send heartbeats to the Metadata Service for health tracking
- Register and deregister with the Metadata Service on startup/shutdown

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--listen` | `:9100` | gRPC server listen address |
| `--data-dir` | `/var/lib/novastor/chunks` | Chunk storage directory |
| `--meta-addr` | `localhost:7001` | Metadata service address |
| `--metrics-addr` | `:9101` | Prometheus metrics endpoint |
| `--scrub-interval` | `24h` | Interval between scrub runs |
| `--heartbeat-interval` | `30s` | Interval between metadata heartbeats |
| `--node-id` | hostname | Unique node identifier |
| `--tls-ca` | (empty) | CA certificate for mTLS |
| `--tls-cert` | (empty) | Server certificate for mTLS |
| `--tls-key` | (empty) | Server key for mTLS |
| `--tls-rotation-interval` | `5m` | Certificate reload interval |

### Host Access

The Agent DaemonSet requires privileged access to the host:

- `hostPID: true` for process namespace access
- `/dev` and `/sys` mounted for device discovery
- `/var/lib/novastor/chunks` as a `DirectoryOrCreate` hostPath volume

---

## CSI Driver

**Binary**: `novastor-csi` | **Package**: `internal/csi/` | **K8s Resources**: Deployment (controller) + DaemonSet (node)

The CSI driver implements the [Container Storage Interface](https://github.com/container-storage-interface/spec) specification, enabling Kubernetes to provision, attach, and mount NovaStor block volumes.

### Controller Capabilities

| Capability | Status |
|---|---|
| `CREATE_DELETE_VOLUME` | Supported |
| `CREATE_DELETE_SNAPSHOT` | Supported |
| `EXPAND_VOLUME` | Supported |
| `PUBLISH_UNPUBLISH_VOLUME` | Supported |
| `GET_CAPACITY` | Supported |

### Node Capabilities

| Capability | Status |
|---|---|
| `STAGE_UNSTAGE_VOLUME` | Supported |
| `GET_VOLUME_STATS` | Supported |
| `EXPAND_VOLUME` | Supported |

### Volume Provisioning Flow

1. **CreateVolume**: Calculates chunk count (`size / 4MiB`), consults the Placement Engine for node assignments, generates chunk IDs, and persists volume metadata
2. **NodeStageVolume**: Connects the NVMe-oF target (if available) or creates a staging marker
3. **NodePublishVolume**: Bind-mounts the staging path to the pod's target path, or NFS-mounts for RWX volumes
4. **NodeUnpublishVolume**: Unmounts the target path
5. **NodeUnstageVolume**: Disconnects the NVMe-oF target and cleans up staging

### Access Modes

| Mode | CSI Mode | Transport |
|---|---|---|
| ReadWriteOnce (RWO) | `SINGLE_NODE_WRITER` | NVMe-oF/TCP or bind mount |
| ReadWriteMany (RWX) | `MULTI_NODE_MULTI_WRITER` | NFS mount |
| ReadOnlyMany (ROX) | `MULTI_NODE_READER_ONLY` | Not yet supported |

### Sidecar Containers

The CSI controller deployment includes standard Kubernetes CSI sidecar containers:

| Sidecar | Image | Purpose |
|---|---|---|
| csi-provisioner | `registry.k8s.io/sig-storage/csi-provisioner:v5.0.1` | Dynamic provisioning |
| csi-snapshotter | `registry.k8s.io/sig-storage/csi-snapshotter:v8.0.1` | Volume snapshots |
| csi-resizer | `registry.k8s.io/sig-storage/csi-resizer:v1.11.1` | Volume expansion |
| csi-node-driver-registrar | `registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.11.1` | Node registration |

### NVMe-oF Transport

The CSI node service supports NVMe-oF/TCP for near-local-disk latency block I/O. On Linux hosts, the `LinuxInitiator` uses the `nvme-cli` tool to connect and disconnect targets:

```
nvme connect -t tcp -a <address> -s <port> -n <subsystemNQN>
nvme disconnect -n <subsystemNQN>
```

Device discovery polls `/sys/class/nvme/*/subsysnqn` to find the attached controller and derive the block device path (e.g., `/dev/nvme0n1`).

---

## File Gateway (NFS)

**Binary**: `novastor-filer` | **Package**: `internal/filer/` | **K8s Resource**: Deployment

The File Gateway provides shared filesystem access via NFS v3, enabling ReadWriteMany (RWX) persistent volumes.

### Supported Operations

| Operation | Description |
|---|---|
| `Stat` | Get inode attributes |
| `Lookup` | Resolve name in directory |
| `Mkdir` | Create directory |
| `Create` | Create file |
| `Unlink` | Delete file |
| `Rmdir` | Delete directory |
| `ReadDir` | List directory entries |
| `Read` | Read file data |
| `Write` | Write file data |
| `Rename` | Rename/move file or directory |
| `Symlink` | Create symbolic link |
| `Readlink` | Read symbolic link target |

### Architecture

The NFS Server accepts TCP connections and processes requests using the ONC RPC (Sun RPC) protocol with XDR encoding. Each connection is handled in its own goroutine. The server includes a `LockManager` for NFS file locking semantics.

### Configuration

| Flag | Default | Description |
|---|---|---|
| `replicas` | 1 | Number of filer pods |
| Resources (requests) | 200m CPU / 256Mi memory | Minimum resources |
| Resources (limits) | 1 CPU / 1Gi memory | Maximum resources |

---

## S3 Gateway

**Binary**: `novastor-s3gw` | **Package**: `internal/s3/` | **K8s Resource**: Deployment

The S3 Gateway provides an S3-compatible HTTP API for object storage operations.

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--listen` | `:9000` | HTTP listen address |
| `--access-key` | (required) | S3 access key |
| `--secret-key` | (required) | S3 secret key |
| `--meta-addr` | `localhost:7001` | Metadata service address |
| `--agent-addr` | `localhost:9100` | Chunk agent address |
| `--tls-ca` | (empty) | CA certificate for mTLS to backend services |
| `--tls-cert` | (empty) | Client certificate for mTLS |
| `--tls-key` | (empty) | Client key for mTLS |

### HTTP Timeouts

| Timeout | Value |
|---|---|
| Read | 60 seconds |
| Write | 60 seconds |
| Idle | 120 seconds |
| Shutdown grace | 15 seconds |

For detailed S3 API compatibility, see the [S3 API Reference](../api/s3.md).

---

## Controller / Operator

**Binary**: `novastor-controller` | **Package**: `internal/controller/`, `internal/operator/` | **K8s Resource**: Deployment

The Controller runs Kubernetes reconciliation loops for all NovaStor CRDs and manages the automatic recovery subsystem.

### Reconcilers

| Controller | CRD | Responsibilities |
|---|---|---|
| `StoragePoolReconciler` | StoragePool | Validate node selection, aggregate capacity |
| `BlockVolumeReconciler` | BlockVolume | Create PersistentVolumes, bind to pools |
| `SharedFilesystemReconciler` | SharedFilesystem | Deploy NFS gateway pods, create services |
| `ObjectStoreReconciler` | ObjectStore | Deploy S3 gateway pods, create services |

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--metrics-bind-address` | `:8080` | Prometheus metrics endpoint |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe endpoint |
| `--leader-elect` | `false` | Enable leader election for HA |
| `--meta-addr` | `:7000` | Metadata service gRPC endpoint |
| `--recovery-enabled` | `true` | Enable automatic node failure recovery |
| `--heartbeat-timeout` | `60s` | Duration before a node is considered down |
| `--recovery-concurrency` | `4` | Max concurrent chunk recovery operations |

### Leader Election

When `--leader-elect` is enabled, the controller uses Kubernetes lease-based leader election with the ID `novastor-controller-leader-election`. Only the leader runs reconciliation loops and the recovery subsystem.

---

## Placement Engine

**Package**: `internal/placement/`

The Placement Engine determines which nodes should store a given set of chunks. It uses a CRUSH-like algorithm to distribute data across the cluster while respecting failure domains.

### Interface

```go
type PlacementEngine interface {
    Place(count int) []string
}
```

The `Place` method returns a list of node IDs for chunk placement. The Placement Engine is consulted during volume creation and recovery operations to select destination nodes for new or re-replicated chunks.

### Placement Metadata

Placement decisions are persisted in the Metadata Service as `PlacementMap` entries:

```go
type PlacementMap struct {
    ChunkID string   `json:"chunkID"`
    Nodes   []string `json:"nodes"`
}
```

Each entry maps a chunk ID to the list of nodes that hold replicas of that chunk. During recovery, the placement map is updated to reflect the new node assignments after re-replication.

---

## Scheduler Webhook

**Binary**: `novastor-webhook` | **Package**: `internal/webhook/` | **K8s Resource**: Deployment

The Scheduler Webhook is a mutating admission webhook that automatically injects the NovaStor custom scheduler name into pods that use NovaStor PVCs. This enables data-locality scheduling without requiring users to manually set `spec.schedulerName`.

### Responsibilities

- Intercepts pod creation requests via the Kubernetes MutatingAdmissionWebhook API
- Inspects pod volumes for PVC references
- Looks up each PVC's storage class to determine if it uses NovaStor provisioner
- Injects `schedulerName: novastor-scheduler` when NovaStor PVCs are detected
- Respects opt-out annotation for pods that should not use the custom scheduler

### Configuration

| Flag | Default | Description |
|---|---|---|
| `--tls-cert` | `/etc/webhook-cert/tls.crt` | TLS certificate file path |
| `--tls-key` | `/etc/webhook-cert/tls.key` | TLS private key file path |
| `--port` | `9443` | Webhook HTTPS server port |
| `--metrics-bind-address` | `:8080` | Prometheus metrics endpoint |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe endpoint |

### Helm Values

| Value | Default | Description |
|---|---|---|
| `scheduler.webhook.enabled` | `true` | Enable the webhook deployment |
| `scheduler.webhook.replicas` | `2` | Number of webhook pods |
| `scheduler.webhook.failurePolicy` | `Ignore` | Webhook failure policy (Ignore/Fail) |
| `scheduler.webhook.timeoutSeconds` | `5` | Webhook admission timeout |
| `scheduler.webhook.cert.existingSecret` | `""` | Existing secret with TLS cert/key |

### Annotations

| Annotation | Value | Description |
|---|---|---|
| `novastor.io/skip-scheduler-injection` | `"true"` | Skip scheduler injection for this pod |

### Detection Logic

The webhook determines if a pod uses NovaStor storage by:

1. Iterating through `spec.volumes` for PVC references
2. Fetching each PVC's `spec.storageClassName`
3. Fetching the StorageClass resource
4. Checking if `provisioner` equals `novastor.csi.novastor.io`

If any PVC uses a NovaStor storage class, the scheduler name is injected.

### Failure Policy

The default `failurePolicy: Ignore` ensures that pod creation proceeds even if the webhook is temporarily unavailable. Pods will fall back to the default Kubernetes scheduler in this case.

### TLS Certificates

The webhook requires TLS certificates for HTTPS communication with the Kubernetes API server. When `scheduler.webhook.cert.existingSecret` is empty, Helm automatically generates self-signed certificates using a post-install hook job.

For production deployments, provide certificates via `scheduler.webhook.cert.existingSecret` or use cert-manager.

