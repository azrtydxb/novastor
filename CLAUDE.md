# NovaStor — Project Guidelines

## Project Overview

NovaStor is a unified Kubernetes-native storage system providing **block** (CSI/NVMe-oF), **file** (NFS/FUSE), and **object** (S3-compatible) storage through a shared chunk storage engine.

- **Module**: `github.com/azrtydxb/novastor`
- **Language**: Go 1.25+
- **Registry**: `ghcr.io/azrtydxb/novastor`

## Architecture

> **AUTHORITATIVE SPEC**: [`docs/superpowers/specs/2026-03-15-layered-architecture-design.md`](docs/superpowers/specs/2026-03-15-layered-architecture-design.md)
> **This spec MUST be read and followed for ALL implementation work.** The summary below is for quick reference only. When in doubt, the spec is the source of truth.

### Four-Layer Architecture (STRICT — layers MUST NOT be collapsed, bypassed, or combined)

```
┌─────────────────────────────────────────────────────────┐
│  Presentation Layer                                      │
│  NVMe-oF bdev  │  NFS server  │  S3 gateway             │
│  Thin protocol translation only — no replication logic   │
└──────────────────────────┬──────────────────────────────┘
                           │ read/write chunks
┌──────────────────────────▼──────────────────────────────┐
│  Chunk Engine                                            │
│  Content-addressed 4MB immutable chunks                  │
│  CRUSH placement + protection scheme                     │
│  Replication fan-out OR Reed-Solomon encoding             │
│  Owner fans out to replicas via gRPC (Rust-to-Rust)      │
└──────────────────────────┬──────────────────────────────┘
                           │ SPDK bdev I/O
┌──────────────────────────▼──────────────────────────────┐
│  Backend Engine                                          │
│  File: file on mounted FS → SPDK AIO bdev                │
│  LVM:  SPDK lvol store on unbound NVMe                   │
│  Raw:  SPDK NVMe bdev on unbound NVMe (no filesystem)    │
│  All produce SPDK bdevs — uniform interface to chunks     │
└─────────────────────────────────────────────────────────┘

  Policy Engine (control plane — NOT in data path)
  Monitors actual vs desired state
  Triggers repairs, rebalancing, rebuilds
```

**Hot data path:** Presentation → Chunk Engine → Backend. Three layers. Policy Engine is NEVER in the data path.

### Core Principle

**Everything is chunks.** A block volume is an ordered sequence of 4MB chunks. A file is chunks + inode metadata. An object is chunks + object metadata. One engine, three access layers.

### Key Invariants (see spec for full list of 10)

1. **Layer separation is absolute.** Presentation MUST NOT replicate. Chunk engine MUST NOT manage devices. Backend MUST NOT know about chunks. Policy engine MUST NOT be in the data path.
2. **All data I/O flows through SPDK.** Every backend produces SPDK bdevs.
3. **Go agent never touches data.** Management and configuration only, via gRPC to Rust dataplane.
4. **CRUSH map is the single source of truth** for placement and protection scheme.
5. **gRPC is the only communication protocol.** Go→Rust, Rust→Rust — all gRPC. No JSON-RPC.
6. **No Malloc bdevs in production.** Only with explicit `--test-mode`.
7. **Fencing on quorum loss.** Dataplane fences itself if it loses contact with Go agent.

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
| Scheduler | `novastor-scheduler` | Deployment | `cmd/scheduler/`, `internal/scheduler/` |
| Scheduler Webhook | `novastor-webhook` | Deployment | `cmd/webhook/`, `internal/webhook/` |
| CLI | `novastorctl` | — | `cmd/cli/` |

### Data Protection (per-volume, not per-pool)

Both modes are available for **all three access layers** (block, file, object). The choice is per-volume (pool provides default):

- **Replication**: Synchronous N-way (default factor=3, write quorum=majority). Owner fans out full chunks to replica nodes via gRPC.
- **Erasure Coding**: Reed-Solomon (default 4+2). Owner RS-encodes into K data + M parity shards, distributes via gRPC. 1.5x overhead vs 3x.

## Custom Agents (MANDATORY)

Two custom agents enforce architecture and code quality. **These are not optional.**

### `architecture-compliance` — Architecture Auditor
- **MUST be run** after implementing any feature, fixing any bug, or making any code change that touches Go agent, Rust dataplane, Helm charts, proto definitions, or any layer boundary code
- **MUST be run** before creating any PR or claiming work is complete
- Audits code against the 10 invariants in the architecture spec
- Invoke with: "Use the architecture-compliance agent to audit the changes"

### `novastor-reviewer` — Code Reviewer
- **MUST be run** before creating any PR
- **SHOULD be run** after completing a logical unit of work (a task, a feature, a fix)
- Full code review covering architecture compliance + Go/Rust code quality + Helm/proto review
- Invoke with: "Use the novastor-reviewer agent to review the changes"

### When to Use Which
| Situation | Agent |
|-----------|-------|
| Quick check during implementation | `architecture-compliance` |
| Before committing a feature | `architecture-compliance` |
| Before creating a PR | Both: `architecture-compliance` first, then `novastor-reviewer` |
| After a large refactor | Both |
| Fixing a bug in one file | `architecture-compliance` (minimum) |

**DO NOT skip these agents.** Architecture drift is the #1 risk in this project. Every shortcut taken in the past led to layers being collapsed, wrong communication protocols, and months of rework.

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
- Generated code goes alongside `.proto` sources: `api/proto/chunk/`, `api/proto/metadata/`, `api/proto/nvme/`

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
│   ├── scheduler/           # Data-locality scheduler
│   ├── webhook/             # Mutating admission webhook
│   └── cli/                 # novastorctl CLI tool
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
│   ├── datamover/           # Data migration and rebalancing
│   ├── policy/              # Storage policy engine
│   ├── webhook/             # Mutating admission webhook logic
│   ├── scheduler/           # Data-locality scheduler logic
│   └── cli/                 # novastorctl command implementations
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

# Storage quota
apiVersion: novastor.io/v1alpha1
kind: StorageQuota
```

## Build & Development

### Quick Reference

```bash
make build-all          # Build all 9 binaries
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
- **Build**: All 9 binaries must compile
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
