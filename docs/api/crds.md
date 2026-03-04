# CRD Reference

NovaStor defines five Custom Resource Definitions (CRDs) in the `novastor.io/v1alpha1` API group. This page documents every field in their spec and status.

## StoragePool

A StoragePool defines a set of storage nodes, the devices they contribute, and the data protection policy.

```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: nvme-replicated
spec:
  nodeSelector:
    matchLabels:
      novastor.io/storage-node: "true"
  deviceFilter:
    type: nvme
    minSize: "100Gi"
  dataProtection:
    mode: replication
    replication:
      factor: 3
      writeQuorum: 2
```

### StoragePoolSpec

| Field | Type | Required | Description |
|---|---|---|---|
| `nodeSelector` | `LabelSelector` | No | Selects which Kubernetes nodes belong to this pool. If omitted, all nodes with a NovaStor agent are included. |
| `deviceFilter` | `DeviceFilter` | No | Filters which devices on selected nodes to use. |
| `dataProtection` | `DataProtectionSpec` | Yes | Configures data protection mode and parameters. |

#### DeviceFilter

| Field | Type | Validation | Description |
|---|---|---|---|
| `type` | `string` | Enum: `nvme`, `ssd`, `hdd` | Device type filter. |
| `minSize` | `string` | Kubernetes quantity | Minimum device size (e.g., `"100Gi"`). |

#### DataProtectionSpec

| Field | Type | Validation | Description |
|---|---|---|---|
| `mode` | `string` | Enum: `replication`, `erasureCoding` | Data protection mode. |
| `replication` | `ReplicationSpec` | Required when mode=replication | Replication parameters. |
| `erasureCoding` | `ErasureCodingSpec` | Required when mode=erasureCoding | Erasure coding parameters. |

#### ReplicationSpec

| Field | Type | Default | Validation | Description |
|---|---|---|---|---|
| `factor` | `int` | `3` | Min: 1, Max: 5 | Number of replicas per chunk. |
| `writeQuorum` | `int` | majority | -- | Number of replicas that must acknowledge a write. |

#### ErasureCodingSpec

| Field | Type | Default | Validation | Description |
|---|---|---|---|---|
| `dataShards` | `int` | `4` | Min: 2 | Number of data shards. |
| `parityShards` | `int` | `2` | Min: 1 | Number of parity shards. |

### StoragePoolStatus

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Current phase: `Pending`, `Ready`, `Degraded`, `Error` |
| `nodeCount` | `int` | Number of nodes currently in the pool |
| `totalCapacity` | `string` | Aggregated total capacity (e.g., `"1.5Ti"`) |
| `usedCapacity` | `string` | Aggregated used capacity |
| `dataProtection` | `string` | Active data protection mode description |
| `conditions` | `[]Condition` | Standard Kubernetes conditions |

### kubectl Columns

```
NAME               MODE          NODES   CAPACITY   AGE
nvme-replicated    replication   3       1.5Ti      5d
```

---

## BlockVolume

A BlockVolume represents a block storage volume backed by a StoragePool.

```yaml
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
metadata:
  name: my-database-volume
  namespace: default
spec:
  pool: nvme-replicated
  size: "50Gi"
  accessMode: ReadWriteOnce
```

### BlockVolumeSpec

| Field | Type | Required | Validation | Description |
|---|---|---|---|---|
| `pool` | `string` | Yes | -- | Name of the StoragePool to allocate from. |
| `size` | `string` | Yes | Kubernetes quantity | Volume size (e.g., `"50Gi"`, `"1Ti"`). |
| `accessMode` | `string` | Yes | Enum: `ReadWriteOnce`, `ReadOnlyMany` | Volume access mode. |
| `quota` | `*int64` | No | -- | Optional per-volume quota limit in bytes. |

### BlockVolumeStatus

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Current phase: `Pending`, `Bound`, `Released`, `Error` |
| `pool` | `string` | Resolved storage pool name |
| `accessMode` | `string` | Resolved access mode |
| `nodeID` | `string` | Node where the volume is currently attached |
| `chunkCount` | `int` | Number of chunks composing this volume |
| `conditions` | `[]Condition` | Standard Kubernetes conditions |

### Conditions

| Type | Reason | Description |
|---|---|---|
| `Ready` | `VolumeBound` | PersistentVolume created and bound |
| `Ready` | `PoolNotFound` | Referenced StoragePool does not exist |
| `Ready` | `PoolNotReady` | Referenced StoragePool is not in `Ready` phase |
| `Ready` | `PVCreateError` | Failed to create PersistentVolume |

### kubectl Columns

```
NAME                  POOL              SIZE   ACCESS          PHASE
my-database-volume    nvme-replicated   50Gi   ReadWriteOnce   Bound
```

### Reconciliation Behavior

When a BlockVolume is created, the controller:

1. Validates that the referenced StoragePool exists and is `Ready`
2. Creates a PersistentVolume with the CSI driver `novastor.csi.novastor.io`
3. Sets labels `novastor.io/blockvolume` and `novastor.io/namespace` on the PV for ownership tracking
4. Updates the BlockVolume status to `Bound`
5. If the pool is not found or not ready, the volume remains in `Pending` and is requeued after 30 seconds

---

## SharedFilesystem

A SharedFilesystem provides a shared, multi-writer filesystem exported via NFS.

```yaml
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem
metadata:
  name: shared-data
  namespace: default
spec:
  pool: nvme-replicated
  capacity: "100Gi"
  accessMode: ReadWriteMany
  export:
    protocol: nfs
```

### SharedFilesystemSpec

| Field | Type | Required | Validation | Description |
|---|---|---|---|---|
| `pool` | `string` | Yes | -- | Name of the StoragePool. |
| `capacity` | `string` | Yes | Kubernetes quantity | Filesystem capacity. |
| `accessMode` | `string` | Yes | Enum: `ReadWriteMany`, `ReadOnlyMany` | Access mode. |
| `export` | `ExportSpec` | No | -- | Export configuration. |
| `image` | `string` | No | -- | Override the NFS filer container image. Defaults to `novastor/novastor-filer:v0.1.0`. |

#### ExportSpec

| Field | Type | Validation | Description |
|---|---|---|---|
| `protocol` | `string` | Enum: `nfs` | Export protocol. Currently only NFS is supported. |

### SharedFilesystemStatus

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Current phase: `Pending`, `Ready`, `Error` |
| `endpoint` | `string` | NFS endpoint address (e.g., `10.96.0.15:/exports/shared-data`) |
| `conditions` | `[]Condition` | Standard Kubernetes conditions |

### kubectl Columns

```
NAME          POOL              CAPACITY   PROTOCOL   PHASE
shared-data   nvme-replicated   100Gi      nfs        Ready
```

### Reconciliation Behavior

When a SharedFilesystem is created, the controller:

1. Validates the referenced StoragePool
2. Creates a Deployment running the NFS filer image
3. Creates a Service exposing the NFS port
4. Updates the SharedFilesystem status with the NFS endpoint

---

## ObjectStore

An ObjectStore provisions an S3-compatible gateway backed by a StoragePool.

```yaml
apiVersion: novastor.io/v1alpha1
kind: ObjectStore
metadata:
  name: my-object-store
  namespace: default
spec:
  pool: nvme-replicated
  endpoint:
    service:
      port: 9000
  bucketPolicy:
    maxBuckets: 100
    versioning: enabled
```

### ObjectStoreSpec

| Field | Type | Required | Description |
|---|---|---|---|
| `pool` | `string` | Yes | Name of the StoragePool. |
| `endpoint` | `ObjectEndpointSpec` | Yes | Endpoint configuration. |
| `bucketPolicy` | `BucketPolicySpec` | No | Bucket policy defaults. |
| `image` | `string` | No | Override the S3 gateway container image. Defaults to `novastor/novastor-s3gw:v0.1.0`. |

#### ObjectEndpointSpec

| Field | Type | Description |
|---|---|---|
| `service` | `ObjectServiceSpec` | Service configuration. |

#### ObjectServiceSpec

| Field | Type | Description |
|---|---|---|
| `port` | `int32` | TCP port for the S3 HTTP endpoint. |

#### BucketPolicySpec

| Field | Type | Validation | Description |
|---|---|---|---|
| `maxBuckets` | `int` | -- | Maximum number of buckets allowed. |
| `versioning` | `string` | Enum: `enabled`, `disabled`, `suspended` | Default versioning state for new buckets. |
| `maxBucketSize` | `int64` | -- | Per-bucket storage quota in bytes. When set, individual buckets cannot exceed this size. |

### ObjectStoreStatus

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Current phase: `Pending`, `Ready`, `Error` |
| `endpoint` | `string` | S3 endpoint URL (e.g., `http://10.96.0.20:9000`) |
| `conditions` | `[]Condition` | Standard Kubernetes conditions |

### kubectl Columns

```
NAME               POOL              PORT   PHASE
my-object-store    nvme-replicated   9000   Ready
```

### Reconciliation Behavior

When an ObjectStore is created, the controller:

1. Validates the referenced StoragePool
2. Creates a Deployment running the S3 gateway image
3. Creates a Service exposing the configured port
4. Updates the ObjectStore status with the endpoint URL

---

## StorageQuota

A StorageQuota defines storage consumption limits for a scope (namespace, storage pool, bucket, or volume).

```yaml
apiVersion: novastor.io/v1alpha1
kind: StorageQuota
metadata:
  name: team-quota
  namespace: production
spec:
  scope:
    kind: Namespace
    name: production
  storage:
    hard: 1099511627776
    soft: 858993459200
  objectCount:
    hard: 10000
```

### StorageQuotaSpec

| Field | Type | Required | Description |
|---|---|---|---|
| `scope` | `QuotaScope` | Yes | Target of this quota. |
| `storage` | `StorageQuotaSpecStorage` | No | Storage limits. |
| `objectCount` | `ObjectCountQuotaSpec` | No | Object/bucket count limits. |

#### QuotaScope

| Field | Type | Validation | Description |
|---|---|---|---|
| `kind` | `string` | Enum: `Namespace`, `StoragePool`, `Bucket`, `Volume` | Scope type. |
| `name` | `string` | -- | Name of the scoped resource. |

#### StorageQuotaSpecStorage

| Field | Type | Required | Description |
|---|---|---|---|
| `hard` | `int64` | Yes | Hard limit in bytes. Requests exceeding this are rejected. |
| `soft` | `int64` | No | Soft limit in bytes. Warnings are issued but requests are allowed. |

#### ObjectCountQuotaSpec

| Field | Type | Required | Description |
|---|---|---|---|
| `hard` | `int64` | Yes | Hard limit for object/bucket count. |

### StorageQuotaStatus

| Field | Type | Description |
|---|---|---|
| `phase` | `string` | Quota enforcement state: `Active`, `Exceeded`, `Warning` |
| `storage` | `StorageQuotaStatusStorage` | Storage usage report. |
| `objectCount` | `ObjectCountQuotaStatus` | Object/bucket count usage report. |
| `conditions` | `[]Condition` | Standard Kubernetes conditions |

### kubectl Columns

```
NAME          SCOPE       SCOPE NAME    STORAGE LIMIT    USED    PHASE
team-quota    Namespace   production    1099511627776    0       Active
```

---

## Common Patterns

### Conditions

All NovaStor CRDs use standard Kubernetes conditions:

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
      reason: VolumeBound
      message: "PersistentVolume novastor-default-my-vol created and bound"
      lastTransitionTime: "2025-01-15T10:30:00Z"
      observedGeneration: 1
```

### Finalizers

NovaStor controllers use finalizers to ensure clean deletion of dependent resources (PersistentVolumes, Deployments, Services) when a CRD resource is deleted.

### Requeue Strategy

All reconcilers use a 30-second requeue interval for resources that are waiting on dependencies (e.g., a pool that is not yet ready). Once the dependency is satisfied, the resource transitions to its final state without further requeuing.
