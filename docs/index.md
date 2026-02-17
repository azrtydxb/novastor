# NovaStor

**Unified Kubernetes-Native Storage — Block, File, and Object**

NovaStor is a distributed storage system built for Kubernetes that provides block (CSI), file (NFS), and object (S3-compatible) storage through a single, shared chunk storage engine.

## Key Features

- **Block Storage** — CSI driver with NVMe-oF/TCP transport for near-local-disk latency
- **File Storage** — NFS v4.1 gateway with ReadWriteMany (RWX) support
- **Object Storage** — S3-compatible API for application assets and backups
- **Unified Engine** — Single chunk-based storage engine powering all three access layers
- **Flexible Data Protection** — Per-pool choice of replication or erasure coding for any access layer
- **Kubernetes Native** — CRD-driven, operator-managed, Helm-deployable
- **Zero External Dependencies** — No etcd, ZooKeeper, or Ceph required

## Architecture

```mermaid
graph TB
    CSI[CSI Driver<br/>Block Storage] --> ME[Metadata Service]
    NFS[NFS Gateway<br/>File Storage] --> ME
    S3[S3 Gateway<br/>Object Storage] --> ME
    CSI --> CE[Chunk Engine]
    NFS --> CE
    S3 --> CE
    ME --> PE[Placement Engine]
    CE --> NA[Node Agent<br/>DaemonSet]
    PE --> NA

    style CSI fill:#e1f5ff
    style NFS fill:#e1f5ff
    style S3 fill:#e1f5ff
    style ME fill:#f3e5f5
    style PE fill:#f3e5f5
    style CE fill:#e8f5e9
    style NA fill:#fff4e6
```

## Quick Start

See the [Quick Start Guide](getting-started/quickstart.md) to get NovaStor running on your cluster.
