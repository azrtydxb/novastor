# NovaStor — Project Guidelines

## Project Overview

NovaStor is a unified Kubernetes-native storage system providing **block** (CSI/NVMe-oF), **file** (NFS/FUSE), and **object** (S3-compatible) storage through a shared chunk storage engine.

- **Module**: `github.com/piwi3910/novastor`
- **Language**: Go 1.24+
- **Registry**: `ghcr.io/piwi3910/novastor`

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  CSI Driver  │  │  NFS/FUSE   │  │  S3 Gateway  │
│   (Block)    │  │   (File)    │  │   (Object)   │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       └────────┬────────┴────────┬────────┘
         ┌──────▼───────┐  ┌─────▼──────┐
         │  Metadata     │  │  Placement │
         │  Service      │  │  Engine    │
         └──────┬───────┘  └─────┬──────┘
         ┌──────▼────────────────▼──────┐
         │     Chunk Storage Engine      │
         └──────────────┬───────────────┘
              ┌─────────▼─────────┐
              │  Node Agent        │
              │  (DaemonSet)       │
              └───────────────────┘
```

### Core Principle

**Everything is chunks.** A block volume is an ordered sequence of 4MB chunks. A file is chunks + inode metadata. An object is chunks + object metadata. One engine, three access layers.

### Components

| Component | Binary | K8s Resource | Package |
|-----------|--------|-------------|---------|
| Chunk Storage Engine | (library) | — | `internal/chunk/` |
| Node Agent | `novastor-agent` | DaemonSet | `cmd/agent/`, `internal/agent/` |
| Metadata Service | `novastor-meta` | StatefulSet | `cmd/meta/`, `internal/metadata/` |
| Controller/Operator | `novastor-controller` | Deployment | `cmd/controller/`, `internal/operator/` |
| CSI Driver | `novastor-csi` | DaemonSet + Deployment | `cmd/csi/`, `internal/csi/` |
| File Gateway | `novastor-filer` | Deployment | `cmd/filer/`, `internal/filer/` |
| S3 Gateway | `novastor-s3gw` | Deployment | `cmd/s3gw/`, `internal/s3/` |
| CLI | `novactl` | — | `cmd/cli/` |

### Data Protection

Both modes are available for **all three access layers** (block, file, object). The choice is per-StoragePool:

- **Replication**: Synchronous N-way (default factor=3, write quorum=majority). For latency-sensitive workloads.
- **Erasure Coding**: Reed-Solomon (default 4+2). For capacity-efficient workloads. 1.5x overhead vs 3x.

## Code Standards

### Critical Rules

- **NEVER create version tags or push tags unless explicitly requested.** Tagging triggers the release workflow.
- **NEVER disable linting rules** via directive comments. Fix the code, not the rules.
- **NEVER use mock data or placeholder implementations.** All code must work with real data.
- **NEVER hardcode secrets, passwords, or API keys.**
- **ALL commits are authored by Pascal Watteel only.** No AI attribution.

### Go Conventions

- **Error handling**: Always wrap errors with context: `fmt.Errorf("operation failed: %w", err)`
- **Logging**: Use `go.uber.org/zap` structured logging everywhere
- **Concurrency**: Use `context.Context` for cancellation, `errgroup` for goroutine management, channels for communication
- **Naming**: Follow Go conventions — `MixedCaps`, not `snake_case`. Acronyms are all-caps (`CSI`, `NVMe`, `gRPC`)
- **Interfaces**: Define interfaces where they're consumed, not where they're implemented
- **Testing**: Table-driven tests with `t.Run()` subtests. Test files alongside source (`foo_test.go` next to `foo.go`)

### Kubernetes Patterns

- Use **informers** with shared informer factories (never poll the API directly)
- Implement **reconciliation loops** with exponential backoff via workqueues
- Use **leader election** for HA controllers
- Cache via **listers**, not direct API calls
- All CRD types live in `api/v1alpha1/`

### Code Generation

```bash
# After modifying CRD types in api/v1alpha1/
make generate       # Generate deepcopy methods
make manifests      # Update CRD manifests
```

### Chunk Engine Guidelines

- Default chunk size: **4MB** (constant, not configurable in v1)
- Chunks are immutable once written — updates create new chunks
- Every chunk has a **CRC-32C checksum** verified on read
- Chunk IDs are content-addressed (hash of content) for deduplication potential
- The chunk engine is **access-layer agnostic** — it does not know about volumes, files, or objects

### Metadata Service Guidelines

- Built on `hashicorp/raft` with BadgerDB backend
- 3 or 5 replicas (odd number required for quorum)
- All metadata operations go through the Raft leader
- Reads can be served from followers for eventual consistency (configurable)
- Metadata categories: volume mappings, file inodes, object mappings, placement maps

### Transport

- **gRPC** for all inter-component communication (Phase 1)
- **NVMe-oF/TCP** added in Phase 2 for block device path
- Proto definitions live in `api/proto/`
- Generated code goes to `internal/proto/gen/`

## Project Structure

```
novastor/
├── cmd/
│   ├── controller/          # Kubernetes operator
│   ├── agent/               # Node DaemonSet agent
│   ├── meta/                # Metadata service
│   ├── csi/                 # CSI driver
│   ├── filer/               # NFS/file gateway
│   ├── s3gw/                # S3-compatible gateway
│   └── cli/                 # novactl CLI tool
├── api/
│   ├── v1alpha1/            # CRD type definitions
│   └── proto/               # Protobuf definitions
├── internal/
│   ├── chunk/               # Chunk storage engine (THE core)
│   ├── placement/           # CRUSH-like placement algorithm
│   ├── metadata/            # Raft-based metadata store
│   ├── transport/           # gRPC transport layer
│   ├── disk/                # Disk discovery & management
│   ├── nvmeof/              # NVMe-oF target management (Phase 2)
│   ├── csi/                 # CSI plugin logic
│   ├── filer/               # File gateway logic
│   ├── s3/                  # S3 gateway logic
│   ├── operator/            # Controller reconcile loops
│   ├── agent/               # Node agent logic
│   └── proto/               # Generated protobuf code
├── deploy/
│   ├── helm/                # Helm chart
│   └── manifests/           # Raw YAML manifests
├── config/
│   ├── crd/                 # Generated CRD manifests
│   ├── rbac/                # RBAC definitions
│   └── samples/             # Example CRs
├── test/
│   ├── e2e/                 # End-to-end tests
│   ├── integration/         # Integration tests
│   └── benchmark/           # Performance benchmarks
├── docs/                    # MkDocs documentation
├── hack/                    # Build tooling
├── .github/workflows/       # CI/CD pipelines
├── Makefile                 # Build automation
├── .golangci.yml            # Linting configuration
├── mkdocs.yml               # Documentation config
└── .gitleaks.toml           # Secret scanning config
```

## CRD Types

```yaml
# Storage pool — defines available storage and data protection
apiVersion: novastor.io/v1alpha1
kind: StoragePool

# Block volume
apiVersion: novastor.io/v1alpha1
kind: BlockVolume

# Shared filesystem
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem

# Object store
apiVersion: novastor.io/v1alpha1
kind: ObjectStore
```

## Build & Development

### Quick Reference

```bash
make build-all          # Build all 7 binaries
make test               # Run tests with race detection
make lint               # Run golangci-lint (16 linters)
make check              # Run fmt + vet + lint
make generate           # Generate deepcopy methods
make manifests          # Generate CRD manifests
make generate-proto     # Generate protobuf Go code
make docs-build         # Build documentation (strict mode)
```

### Git Workflow

**ALWAYS use git worktrees for feature branches:**

```bash
# From the main worktree (stays on main always)
git worktree add ../novastor-worktrees/issue-NUM-description -b issue-NUM-description origin/main

# Work inside the worktree
cd ../novastor-worktrees/issue-NUM-description

# Cleanup after merge
git worktree remove ../novastor-worktrees/issue-NUM-description
git branch -d issue-NUM-description
```

**Branch naming**: `issue-NUM-brief-description`

**Commit format**:
```
[Type] Short summary under 50 chars

Detailed explanation of what and why.

Resolves #IssueNumber
```

Types: `[Fix]`, `[Feature]`, `[Refactor]`, `[Docs]`, `[Test]`, `[Chore]`

### CI Pipeline

Every PR runs:
- **Lint**: gofmt, golangci-lint, `go mod tidy` check
- **Security**: govulncheck, gitleaks secret scanning
- **Test**: All tests with `-race` flag and coverage
- **Build**: All 7 binaries must compile
- **Helm**: Chart linting and CRD sync verification
- **Docs**: `mkdocs build --strict`

### Documentation

Every code change affecting behavior, APIs, or CRDs **must** include documentation updates in the same PR. CI enforces this with `mkdocs build --strict`.

Docs use MkDocs Material theme with Mermaid diagram support.

## Dependencies

### Core Stack

| Purpose | Package |
|---------|---------|
| Kubernetes | `k8s.io/client-go`, `sigs.k8s.io/controller-runtime` |
| Consensus | `github.com/hashicorp/raft` |
| Metadata store | `github.com/dgraph-io/badger/v4` |
| RPC | `google.golang.org/grpc`, `google.golang.org/protobuf` |
| Logging | `go.uber.org/zap` |
| Metrics | `github.com/prometheus/client_golang` |
| Erasure coding | `github.com/klauspost/reedsolomon` |
| CLI | `github.com/spf13/cobra` |
| CSI | `github.com/container-storage-interface/spec` |

### No External Runtime Dependencies

NovaStor has **zero external runtime dependencies** — no etcd, no ZooKeeper, no Ceph. Everything is self-contained.

## Testing Requirements

- **Unit tests**: Every package, table-driven, test success AND error paths
- **Integration tests**: Component interactions (agent ↔ metadata, CSI ↔ agent)
- **E2E tests**: Full storage workflows (provision → write → read → delete)
- **Benchmarks**: Chunk engine throughput, metadata latency, replication overhead
- **Race detection**: All tests run with `-race` in CI

## Security

- Input validation on all external boundaries (gRPC, S3 API, CSI calls)
- Secrets managed via Kubernetes Secrets or environment variables
- Internal errors logged, generic errors returned to clients
- Encryption at rest planned for Phase 5
- mTLS between components planned for Phase 3
