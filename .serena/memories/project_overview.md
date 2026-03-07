# NovaStor Project Overview

## Purpose
NovaStor is a unified Kubernetes-native storage system providing **block** (CSI/NVMe-oF), **file** (NFS/FUSE), and **object** (S3-compatible) storage through a shared chunk storage engine.

## Core Principle
**Everything is chunks.** A block volume is an ordered sequence of 4MB chunks. A file is chunks + inode metadata. An object is chunks + object metadata. One engine, three access layers.

## Tech Stack
- **Language**: Go 1.24+
- **Module**: `github.com/azrtydxb/novastor`
- **Registry**: `ghcr.io/azrtydxb/novastor`
- **Container Storage Interface**: CSI spec
- **Consensus**: hashicorp/raft
- **Metadata store**: dgraph-io/badger/v4
- **RPC**: gRPC with protobuf
- **Logging**: go.uber.org/zap (structured logging)
- **Metrics**: prometheus/client_golang
- **Erasure coding**: klauspost/reedsolomon

## Components
| Binary | K8s Resource | Package | Purpose |
|--------|-------------|---------|---------|
| novastor-agent | DaemonSet | internal/agent/ | Node agent for chunk storage |
| novastor-meta | StatefulSet | internal/metadata/ | Raft-based metadata service |
| novastor-controller | Deployment | internal/controller/ | Kubernetes operator |
| novastor-csi | DaemonSet + Deployment | internal/csi/ | CSI driver for block volumes |
| novastor-filer | Deployment | internal/filer/ | NFS/FUSE file gateway |
| novastor-s3gw | Deployment | internal/s3/ | S3-compatible object gateway |
| novastorctl | CLI | internal/cli/ | Management CLI tool |

## Data Protection
- **Replication**: Synchronous N-way (default factor=3, write quorum=majority)
- **Erasure Coding**: Reed-Solomon (default 4+2). 1.5x overhead vs 3x for replication

## Chunk Engine (internal/chunk/)
- Default chunk size: **4MB** (constant)
- Chunks are immutable once written
- Every chunk has a **CRC-32C checksum** verified on read
- Chunk IDs are content-addressed (hash of content)
- The chunk engine is **access-layer agnostic**

## Key Files to Know
- `internal/chunk/reedsolomon.go` - Reed-Solomon erasure coding implementation
- `internal/chunk/replication.go` - Replication implementation
- `internal/csi/controller.go` - CSI CreateVolume/DeleteVolume
- `internal/metadata/volume_meta.go` - Volume metadata structures
- `api/v1alpha1/` - CRD type definitions

## Architecture Notes
- Zero external runtime dependencies (no etcd, ZooKeeper, Ceph)
- gRPC for inter-component communication (Phase 1)
- NVMe-oF/TCP for block device path (Phase 2)
