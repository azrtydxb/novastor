# Development Guide

This guide covers building NovaStor from source, running tests, understanding the code organization, and contributing changes.

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Go | 1.25+ | Build and test |
| Docker | 20+ | Container image builds |
| Helm | 3.x | Chart linting and testing |
| kubectl | 1.28+ | Kubernetes interaction |
| golangci-lint | latest | Code linting |
| protoc | 3.x | Protobuf code generation |
| mkdocs | latest | Documentation builds |

## Building from Source

### All Binaries

```bash
make build-all
```

This builds all nine binaries:

| Binary | Source |
|---|---|
| `novastor-controller` | `cmd/controller/` |
| `novastor-agent` | `cmd/agent/` |
| `novastor-meta` | `cmd/meta/` |
| `novastor-csi` | `cmd/csi/` |
| `novastor-filer` | `cmd/filer/` |
| `novastor-s3gw` | `cmd/s3gw/` |
| `novastor-scheduler` | `cmd/scheduler/` |
| `novastor-webhook` | `cmd/webhook/` |
| `novastorctl` | `cmd/cli/` |

### Individual Binaries

```bash
go build -o bin/novastor-agent ./cmd/agent/
go build -o bin/novastor-meta ./cmd/meta/
go build -o bin/novastor-controller ./cmd/controller/
```

### Container Images

```bash
# Build all images
make docker-build

# Build a specific image
docker build -t ghcr.io/piwi3910/novastor/novastor-agent:dev -f build/Dockerfile.agent .
```

## Running Tests

### Unit Tests

```bash
# All tests with race detection
make test

# Specific package
go test -race ./internal/chunk/...
go test -race ./internal/metadata/...
go test -race ./internal/csi/...
go test -race ./internal/s3/...
```

### Benchmarks

```bash
# Run all benchmarks
go test -bench=. -benchmem ./test/benchmark/...

# Run a specific benchmark
go test -bench=BenchmarkMetadata -benchmem ./test/benchmark/
```

### End-to-End Tests

E2E tests require a running Kubernetes cluster with NovaStor deployed:

```bash
# Run E2E tests
go test -v -timeout 30m ./test/e2e/...
```

### Test Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

## Code Quality

### Linting

```bash
# Run all linters (16 enabled linters)
make lint

# Full quality check (fmt + vet + lint)
make check
```

The project uses `golangci-lint` with 16 linters enabled. Configuration is in `.golangci.yml`. Never disable linting rules via directive comments -- fix the underlying code.

### Formatting

```bash
# Check formatting
gofmt -l .

# Fix formatting
gofmt -w .
```

### Module Tidying

```bash
go mod tidy
```

CI verifies that `go.mod` and `go.sum` are tidy on every PR.

## Code Generation

### CRD Types

After modifying types in `api/v1alpha1/types.go`:

```bash
# Generate deepcopy methods
make generate

# Generate CRD manifests
make manifests
```

### Protobuf

After modifying `.proto` files in `api/proto/`:

```bash
make generate-proto
```

Generated Go code is written alongside `.proto` sources: `api/proto/chunk/`, `api/proto/metadata/`, `api/proto/nvme/`.

## Code Organization

### Package Structure

```
internal/
  chunk/          # Core chunk storage engine (Store, Scrubber, LocalStore)
  placement/      # CRUSH-like placement algorithm
  metadata/       # Raft-backed metadata store (RaftStore, FSM, GRPCServer/Client)
  transport/      # gRPC transport layer, TLS, certificate rotation
  disk/           # Disk discovery and management
  csi/            # CSI driver (ControllerServer, NodeService, NVMe initiator)
  filer/          # NFS gateway (NFSServer, NFSHandler, LockManager)
  s3/             # S3 gateway (Gateway, auth, buckets, objects, multipart, presign)
  operator/       # Recovery manager, placement adapter
  controller/     # Kubernetes reconcilers (StoragePool, BlockVolume, etc.)
  agent/          # Node agent gRPC server (ChunkServer)
  datamover/      # Data migration and rebalancing
  policy/         # Storage policy engine
  webhook/        # Mutating admission webhook logic
  scheduler/      # Data-locality scheduler logic
  cli/            # novastorctl command implementations
  metrics/        # Prometheus metric definitions and collectors
  logging/        # Structured logging setup (zap)
```

### Key Design Patterns

**Interface-driven design**: Interfaces are defined where they are consumed, not where they are implemented. This enables clean dependency injection and testability.

```go
// In internal/csi/controller.go (consumer)
type MetadataStore interface {
    PutVolumeMeta(ctx context.Context, meta *metadata.VolumeMeta) error
    GetVolumeMeta(ctx context.Context, volumeID string) (*metadata.VolumeMeta, error)
    // ...
}

// In internal/metadata/store.go (implementation)
func (s *RaftStore) PutVolumeMeta(_ context.Context, meta *VolumeMeta) error { ... }
```

**Table-driven tests**: All tests use table-driven patterns with `t.Run()` subtests:

```go
func TestSomething(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        want    string
        wantErr bool
    }{
        {"valid input", "foo", "bar", false},
        {"empty input", "", "", true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := DoSomething(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("unexpected error: %v", err)
            }
            if got != tt.want {
                t.Errorf("got %q, want %q", got, tt.want)
            }
        })
    }
}
```

**Context propagation**: All operations accept `context.Context` for cancellation and timeout support. Use `errgroup` for managing concurrent goroutines.

**Structured logging**: Use `go.uber.org/zap` throughout. Include relevant context fields:

```go
logging.L.Info("chunk replicated",
    zap.String("chunkID", chunkID),
    zap.String("source", sourceNode),
    zap.String("dest", destNode),
    zap.Duration("elapsed", time.Since(start)),
)
```

## Coding Standards

### Error Handling

Always wrap errors with context:

```go
// Good
return fmt.Errorf("creating chunk store: %w", err)

// Bad
return err
```

### Naming

- Use `MixedCaps` (Go convention), never `snake_case`
- Acronyms are all-caps: `CSI`, `NVMe`, `gRPC`, `NFS`, `S3`
- Interfaces are named without `I` prefix: `Store`, not `IStore`

### Concurrency

- Use `context.Context` for cancellation
- Use `errgroup.Group` for goroutine management
- Use channels for communication between goroutines
- Protect shared state with `sync.Mutex` or `sync.RWMutex`

## CI Pipeline

Every pull request runs through the CI pipeline defined in `.github/workflows/ci.yml`:

| Stage | Check | Description |
|---|---|---|
| Lint | `gofmt` | Code formatting |
| Lint | `golangci-lint` | 16 enabled linters |
| Lint | `go mod tidy` | Module tidiness |
| Security | `govulncheck` | Go vulnerability scanner |
| Security | `gitleaks` | Secret scanning |
| Test | `go test -race` | All tests with race detection |
| Build | `go build` | All 9 binaries compile |
| Helm | `helm lint` | Chart validation |
| Helm | CRD sync | CRD manifests match types |
| Docs | `mkdocs build --strict` | Documentation builds without errors |

## Documentation

Documentation is built with MkDocs Material:

```bash
# Build documentation (strict mode, same as CI)
make docs-build

# Serve locally with live reload
mkdocs serve

# Open http://localhost:8000
```

Every code change affecting behavior, APIs, or CRDs must include documentation updates in the same PR. The CI enforces this with `mkdocs build --strict`.

### Mermaid Diagrams

Use Mermaid diagrams (not ASCII art) for architecture and flow diagrams. The MkDocs configuration includes the `pymdownx.superfences` extension with Mermaid support.

```markdown
    ```mermaid
    graph TB
        A[Component A] --> B[Component B]
    ```
```

## Release Process

!!! warning "Tag Policy"
    Never create version tags or push tags unless explicitly requested. Tagging triggers the release workflow and publishes container images.

Releases follow semantic versioning. The release process:

1. Update version references in `values.yaml` and documentation
2. Create a signed tag: `git tag -s v0.2.0 -m "Release v0.2.0"`
3. Push the tag: `git push origin v0.2.0`
4. CI builds and publishes container images to `ghcr.io/piwi3910`
5. Helm chart is published to the GitHub Pages site
