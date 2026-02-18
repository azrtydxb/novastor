# Testing

This guide covers testing NovaStor at various levels.

## Prerequisites

```bash
# Install dependencies
go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.62.0
```

## Unit Tests

Run unit tests for all packages:

```bash
make test
```

Run tests for a specific package:

```bash
go test -v -race ./internal/controller/...
```

## Linting

Run all linters:

```bash
make lint
```

Run individual checks:

```bash
make fmt   # go fmt
make vet   # go vet
```

## E2E Testing

### Controller Validation Script

The `test/e2e/validate-controller.sh` script validates the controller deployment by creating test resources and verifying their reconciliation.

```bash
# Run the validation script
./test/e2e/validate-controller.sh
```

This script validates:

1. **CRD Installation**: Verifies all 4 CRDs are installed
2. **Controller Deployment**: Checks controller pods are running
3. **Leader Election**: Verifies HA with multiple replicas
4. **StoragePool**: Creates a test pool and verifies node discovery
5. **BlockVolume**: Creates a test volume and verifies PV creation
6. **SharedFilesystem**: Creates an NFS filer and verifies Deployment/Service
7. **ObjectStore**: Creates an S3 gateway and verifies Secret/Deployment/Service

### Running E2E Tests with Go

```bash
# Build the e2e test binary
go test -c ./test/e2e/...

# Run e2e tests
KUBECONFIG=~/.kube/config go test -tags=e2e -v ./test/e2e/...
```

## Integration Tests

Integration tests require a running cluster:

```bash
make test-integration
```

## Benchmarks

Run performance benchmarks:

```bash
# Chunk and placement benchmarks
make test-bench

# All benchmarks
make test-bench-all
```

## Code Coverage

Generate and view coverage report:

```bash
make test-coverage
open coverage.html
```

## CRD Validation

Generate CRD manifests from Go types:

```bash
make manifests
```

Verify CRDs are up-to-date:

```bash
make generate-crds
diff -r config/crd/ deploy/helm/novastor/templates/
```

## Helm Chart Testing

Lint the Helm chart:

```bash
make helm-lint
```

Template render (for debugging):

```bash
helm template novastor deploy/helm/novastor/ -n novastor-system
```

## Documentation Build

Verify documentation builds without errors:

```bash
make docs-build
```

## Pre-commit Checklist

Before committing:

1. [ ] `make test` passes
2. [ ] `make lint` passes with no errors
3. [ ] `make check` passes
4. [ ] `make docs-build` passes
5. [ ] New code has tests
6. [ ] Documentation is updated
