# NovaStor Suggested Commands

## Development Workflow

### Building
```bash
make build-all          # Build all 7 binaries
make build-csi          # Build only CSI driver
make build-controller   # Build only controller
```

### Testing
```bash
make test               # Run all tests with race detection
make test-coverage      # Run tests and open coverage report
make test-integration   # Run integration tests
make test-bench         # Run chunk and placement benchmarks
```

### Code Quality
```bash
make fmt                # Run go fmt
make vet                # Run go vet
make lint               # Run golangci-lint (16 linters)
make check              # Run fmt + vet + lint (run before committing)
```

### Code Generation
```bash
make generate           # Generate deepcopy methods for CRD types
make manifests          # Generate CRD manifests
make generate-proto     # Generate protobuf Go code
```

### Deployment
```bash
make install-crds       # Install CRDs into the K8s cluster
make helm-install       # Install NovaStor using Helm
make helm-upgrade       # Upgrade NovaStor using Helm
make helm-lint          # Lint Helm chart
```

### Documentation
```bash
make docs-build         # Build documentation (strict mode)
make docs-serve         # Serve documentation locally
```

### Git Workflow (CRITICAL - Always use worktrees)
```bash
# From main worktree (stays on main always)
git worktree add ../novastor-worktrees/issue-NUM-description -b issue-NUM-description origin/main

# Work inside the worktree
cd ../novastor-worktrees/issue-NUM-description

# Cleanup after merge (from main worktree)
git worktree remove ../novastor-worktrees/issue-NUM-description
git branch -d issue-NUM-description
```

### Darwin/BSD Specific Commands
The system is Darwin (macOS). Some differences from Linux:
- `sed -i ''` instead of `sed -i` (in-place editing without backup)
- No `stat -c` - use `stat -f` instead
- `awk` behaves differently - use GNU awk (`gawk`) if needed
- `find` syntax is mostly compatible
