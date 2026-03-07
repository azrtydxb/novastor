# Issues Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resolve all 17 open GitHub issues, organized by priority and dependencies, using parallel execution with worktrees and pull requests.

**Architecture:** Issues are grouped into three priority tiers:
1. **Critical (Bugs + Quick Wins)**: #92 (duplicate files - already resolved), #89 (placeholders), #98 (event recorder), #94 (NFS stubs)
2. **Test Infrastructure**: #100 (LVM CI), #99 (E2E CI), #101 (short mode), #91 (multi-device tests), #90 (CLI cert tests)
3. **Feature Enhancements**: #88 (EC shard replication), #93 (StorageQuota controller), #95 (S3 audit), #96 (CSI audit), #97 (NFS audit)

**Tech Stack:** Go 1.24, Kubernetes kind/k3d, GitHub Actions, golangci-lint

**Execution Strategy:**
- Use parallel agents (max 3 at a time)
- Each issue gets its own git worktree: `../novastor-worktrees/issue-N-description`
- Create PR for each completed issue
- Issues with no dependencies can be worked in parallel

---

## Issue Status Summary

| Issue | Title | Type | Status | Notes |
|-------|-------|------|--------|-------|
| #92 | Clean up duplicate files | Bug | **RESOLVED** | No "2.go" files exist |
| #89 | Remove placeholder values | Enhancement | Ready | Quick fix |
| #98 | Add event recorder | Enhancement | Ready | Medium complexity |
| #94 | Implement NFS test stubs | Enhancement | Ready | Simple implementation |
| #100 | Enable LVM tests in CI | Enhancement | Ready | CI config change |
| #99 | Enable E2E tests in CI | Enhancement | Ready | CI config change |
| #101 | Review short mode tests | Enhancement | Ready | Test refactoring |
| #91 | Fix multi-device tests | Bug | Needs investigation | Root cause analysis |
| #90 | Implement CLI cert tests | Enhancement | Ready | Test implementation |
| #88 | Implement EC shard replication | Enhancement | Complex | New feature |
| #93 | StorageQuota controller | Enhancement | Complex | New controller |
| #95 | Audit S3 API | Enhancement | Audit | Documentation + code |
| #96 | Audit CSI spec | Enhancement | Audit | Documentation + code |
| #97 | Audit NFS v4.1 | Enhancement | Audit | Documentation + code |
| #102 | Codebase analysis | Documentation | **METADATA** | Parent issue |

---

## Phase 1: Quick Wins (Parallel)

### Issue #92: Clean up duplicate files with '2.go' suffix

**Status:** ALREADY RESOLVED - No duplicate files found in codebase.

**Step 1: Verify no duplicate files exist**

Run:
```bash
find /Users/pascal/Documents/git/novastor -name "* 2.go" 2>/dev/null
```

Expected: No output (no files found)

**Step 2: Close issue #92**

Run:
```bash
gh issue close 92 --comment "Verified no duplicate files with '2.go' suffix exist in the codebase. This issue appears to have been resolved previously."
```

---

### Issue #89: Remove placeholder values from RequiredReplicas() functions

**Files:**
- Modify: `internal/policy/replication_checker.go:57-59`
- Modify: `internal/policy/erasure_checker.go:25-27`
- Test: `internal/policy/replication_checker_test.go`
- Test: `internal/policy/erasure_checker_test.go`

**Step 1: Read current implementations**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-89-required-replicas -b issue-89-required-replicas origin/main
cd ../novastor-worktrees/issue-89-required-replicas
```

**Step 2: Write failing tests for dynamic RequiredReplicas**

Create test expectations:
- ReplicationChecker should return pool.Spec.DataProtection.Replication.Factor
- ErasureCodingChecker should return dataShards + parityShards

**Step 3: Modify ReplicationChecker**

Modify `internal/policy/replication_checker.go`:

```go
// RequiredReplicas returns the replication factor from the pool spec.
func (c *ReplicationChecker) RequiredReplicas() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if c.pool == nil {
        return 3 // default fallback
    }
    return int(c.pool.Spec.DataProtection.Replication.Factor)
}
```

**Step 4: Modify ErasureCodingChecker**

Modify `internal/policy/erasure_checker.go`:

```go
// RequiredReplicas returns the total number of shards (data + parity).
func (c *ErasureCodingChecker) RequiredReplicas() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if c.pool == nil {
        return 6 // default 4 data + 2 parity
    }
    ec := c.pool.Spec.DataProtection.ErasureCoding
    return int(ec.DataShards + ec.ParityShards)
}
```

**Step 5: Run tests**

Run:
```bash
go test ./internal/policy/... -v -run TestReplicationChecker
go test ./internal/policy/... -v -run TestErasureCodingChecker
```

Expected: PASS

**Step 6: Commit**

```bash
git add internal/policy/replication_checker.go internal/policy/erasure_checker.go
git commit -m "[Fix #89] Remove placeholder values from RequiredReplicas() functions

- Store pool reference in checkers for dynamic replica count lookup
- Return actual replication factor from pool spec
- Return actual data+parity shard count from EC spec
- Default fallback values when pool not yet set

Resolves #89"
```

**Step 7: Create PR**

```bash
git push -u origin issue-89-required-replicas
gh pr create --title "[Fix #89] Remove placeholder values from RequiredReplicas() functions" \
  --body "Removes hardcoded placeholder values and returns actual replica counts from pool specifications."
```

---

### Issue #98: Add Kubernetes event recorder to policy engine

**Files:**
- Modify: `cmd/controller/main.go:320-330`
- Modify: `internal/policy/runnable.go`
- Test: `internal/policy/runnable_test.go`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-98-event-recorder -b issue-98-event-recorder origin/main
cd ../novastor-worktrees/issue-98-event-recorder
```

**Step 2: Modify controller main to create event recorder**

In `cmd/controller/main.go`, after the manager is created:

```go
// Create event recorder for policy engine
eventRecorder := mgr.GetEventRecorderFor("policy-engine")
```

**Step 3: Pass event recorder to policy engine**

Modify the policyRunnable creation:

```go
policyRunnable := policy.NewPolicyEngineRunnable(
    metaAdapter,
    poolLookup,
    nodeChecker,
    chunkReplicator,
    nil, // shardReplicator - TODO: implement erasure coding shard replication
    eventRecorder, // eventRecorder - now enabled
    policyScanInterval,
    policyRepairEnabled,
)
```

**Step 4: Modify PolicyEngineRunnable to accept and use recorder**

In `internal/policy/runnable.go`:

```go
type PolicyEngineRunnable struct {
    // ... existing fields ...
    recorder record.EventRecorder
}

func NewPolicyEngineRunnable(/* ... */, recorder record.EventRecorder, /* ... */) *PolicyEngineRunnable {
    return &PolicyEngineRunnable{
        // ...
        recorder: recorder,
    }
}
```

**Step 5: Emit events for key actions**

Add event emission in reconciler:
- Compliance check failure: `recorder.Event(pool, corev1.EventTypeWarning, "ComplianceCheckFailed", message)`
- Healing task created: `recorder.Event(volume, corev1.EventTypeNormal, "HealingTaskCreated", message)`
- Healing completed: `recorder.Event(volume, corev1.EventTypeNormal, "HealingCompleted", message)`
- Node down: `recorder.Event(node, corev1.EventTypeWarning, "NodeUnavailable", message)`

**Step 6: Run tests**

Run:
```bash
go test ./internal/policy/... -v
```

**Step 7: Commit**

```bash
git add cmd/controller/main.go internal/policy/runnable.go
git commit -m "[Feature #98] Add Kubernetes event recorder to policy engine

- Create event recorder in controller startup
- Pass recorder to policy engine runnable
- Emit events for compliance failures, healing operations, node status
- Improves observability for self-healing operations

Resolves #98"
```

**Step 8: Create PR**

```bash
git push -u origin issue-98-event-recorder
gh pr create --title "[Feature #98] Add Kubernetes event recorder to policy engine"
```

---

### Issue #94: Implement NFS test handler stubs

**Files:**
- Modify: `internal/filer/nfs_test.go:1867-1869, 1948-1950, 1952-1954`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-94-nfs-stubs -b issue-94-nfs-stubs origin/main
cd ../novastor-worktrees/issue-94-nfs-stubs
```

**Step 2: Implement Mkdir handler**

```go
func (h *truncationTestHandler) Mkdir(_ context.Context, parent uint64, name string, mode uint32) (*InodeMeta, error) {
    h.mu.Lock()
    defer h.mu.Unlock()

    // Check if parent exists
    if _, ok := h.files[parent]; !ok && parent != rootInode {
        return nil, fmt.Errorf("parent directory does not exist")
    }

    ino := h.nextIno
    h.nextIno++
    h.dirs[ino] = &DirMeta{
        Ino:     ino,
        Parent: parent,
        Name:    name,
        Mode:    mode,
    }

    now := time.Now().UnixNano()
    return &InodeMeta{Ino: ino, Type: TypeDirectory, Mode: mode, Size: 0, LinkCount: 2, ATime: now, MTime: now, CTime: now}, nil
}
```

**Step 3: Implement Symlink handler**

```go
func (h *truncationTestHandler) Symlink(_ context.Context, parent uint64, name, target string) (*InodeMeta, error) {
    h.mu.Lock()
    defer h.mu.Unlock()

    ino := h.nextIno
    h.nextIno++
    h.symlinks[ino] = target

    now := time.Now().UnixNano()
    return &InodeMeta{Ino: ino, Type: TypeSymlink, Mode: 0777, Size: int64(len(target)), LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}
```

**Step 4: Implement Readlink handler**

```go
func (h *truncationTestHandler) Readlink(_ context.Context, ino uint64) (string, error) {
    h.mu.RLock()
    defer h.mu.RUnlock()

    target, ok := h.symlinks[ino]
    if !ok {
        return "", fmt.Errorf("symlink not found")
    }
    return target, nil
}
```

**Step 5: Add symlinks and dirs fields to handler struct**

```go
type truncationTestHandler struct {
    mu        sync.Mutex
    nextIno   uint64
    files     map[uint64][]byte
    dirs      map[uint64]*DirMeta
    symlinks  map[uint64]string
}
```

**Step 6: Run tests**

Run:
```bash
go test ./internal/filer/... -v -run TestNFS
```

**Step 7: Commit**

```bash
git add internal/filer/nfs_test.go
git commit -m "[Feature #94] Implement NFS test handler stubs (Mkdir, Symlink, Readlink)

- Implement Mkdir with proper directory tracking
- Implement Symlink with target storage
- Implement Readlink to retrieve symlink targets
- Adds missing fields to test handler struct

Resolves #94"
```

**Step 8: Create PR**

```bash
git push -u origin issue-94-nfs-stubs
gh pr create --title "[Feature #94] Implement NFS test handler stubs"
```

---

## Phase 2: Test Infrastructure (Parallel)

### Issue #100: Enable LVM store tests in CI

**Files:**
- Modify: `.github/workflows/test.yaml`
- Modify: `internal/chunk/lvmstore_test.go`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-100-lvm-ci -b issue-100-lvm-ci origin/main
cd ../novastor-worktrees/issue-100-lvm-ci
```

**Step 2: Create LVM-specific CI job**

Add to `.github/workflows/test.yaml`:

```yaml
jobs:
  # ... existing jobs ...

  lvm-tests:
    name: LVM Store Tests
    runs-on: ubuntu-latest
    container:
      image: golang:1.24
    steps:
      - name: Install dependencies
        run: |
          apt-get update
          apt-get install -y lvm2 thin-provisioning-tools

      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup loop devices
        run: |
          truncate -s 1G /tmp/loop1.img
          truncate -s 1G /tmp/loop2.img
          losetup /dev/loop0 /tmp/loop1.img
          losetup /dev/loop1 /tmp/loop2.img

      - name: Setup LVM
        run: |
          pvcreate /dev/loop0 /dev/loop1
          vgcreate novastor-test /dev/loop0 /dev/loop1
          lvcreate -L 512M -T novastor-test/thinpool

      - name: Run LVM tests
        run: go test -v ./internal/chunk/... -run LVM
        env:
          LVM_VG_NAME: novastor-test
          LVM_THIN_POOL: thinpool
```

**Step 3: Update LVM tests to use env config**

Modify `internal/chunk/lvmstore_test.go` to respect environment variables for VG name and thin pool.

**Step 4: Commit and PR**

```bash
git add .github/workflows/test.yaml internal/chunk/lvmstore_test.go
git commit -m "[Feature #100] Enable LVM store tests in CI

- Add dedicated LVM test job with root privileges
- Install LVM tools in CI environment
- Setup loop devices and test volume group
- Configure tests via environment variables

Resolves #100"
```

---

### Issue #99: Enable E2E controller tests in CI with kind cluster

**Files:**
- Modify: `.github/workflows/e2e.yaml`
- Modify: `test/e2e/controller_test.go`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-99-e2e-ci -b issue-99-e2e-ci origin/main
cd ../novastor-worktrees/issue-99-e2e-ci
```

**Step 2: Create kind-based E2E workflow**

Create `.github/workflows/e2e.yaml`:

```yaml
name: E2E Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]

jobs:
  e2e:
    name: E2E Controller Tests
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.24'

      - name: Setup kind
        uses: engineerd/setup-kind@v0.5.0
        with:
          version: "v0.20.0"
          image: "kindest/node:v1.29.0"

      - name: Build container images
        run: |
          make docker-build-controller
          make kind-load-images

      - name: Install CRDs
        run: |
          kubectl apply -f config/crd/bases/

      - name: Deploy controller
        run: |
          kubectl apply -f deploy/manifests/controller.yaml
          kubectl wait --for=condition=available --timeout=60s deployment/novastor-controller -n novastor-system

      - name: Run E2E tests
        run: |
          go test -v ./test/e2e/... -timeout 30m
        env:
          KUBECONFIG: /root/.kube/config
          CI: "false"  # Enable E2E tests in CI
```

**Step 3: Remove CI skip from E2E test**

Modify `test/e2e/controller_test.go:66-67`:

```go
// Remove the CI skip - kind cluster is now available
// if os.Getenv("CI") != "" {
//     t.Skip("Skipping controller deployment test in CI (no Kubernetes cluster available)")
// }
```

**Step 4: Commit and PR**

```bash
git add .github/workflows/e2e.yaml test/e2e/controller_test.go
git commit -m "[Feature #99] Enable E2E controller tests in CI with kind

- Add kind-based E2E test workflow
- Build and load container images to kind cluster
- Deploy CRDs and controller in test environment
- Remove CI skip from E2E controller tests

Resolves #99"
```

---

### Issue #101: Review and improve short mode test coverage

**Files:**
- Modify: `internal/chunk/blockstore_test.go`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-101-short-mode -b issue-101-short-mode origin/main
cd ../novastor-worktrees/issue-101-short-mode
```

**Step 2: Identify which tests truly need long mode**

Analyze each skipped test:
- `TestBlockStore_PutGet`: Can be fast with small data
- `TestBlockStore_Delete`: Can be fast
- `TestBlockStore_Has`: Can be fast
- `TestBlockStore_List`: Can be fast
- `TestBlockStore_MultiDevice`: Needs investigation (see #91)
- `TestBlockStore_Recovery`: Needs investigation (see #91)
- `TestBlockStore_OutOfSpace`: Needs actual disk space
- `TestBlockStore_Factory`: Can be fast
- `TestBlockStore_Dedup`: Can be fast

**Step 3: Create fast variants for short mode**

For tests that can run quickly, remove the `testing.Short()` check and limit data size:

```go
func TestBlockStore_PutGet(t *testing.T) {
    dataSize := 10 * 1024 * 1024 // 10MB for short mode
    if !testing.Short() {
        dataSize = 50 * 1024 * 1024 // 50MB for full mode
    }
    // ... rest of test with dataSize variable
}
```

**Step 4: Keep truly slow tests under short mode skip**

```go
func TestBlockStore_OutOfSpace(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping out-of-space test in short mode (requires large allocation)")
    }
    // ... full test
}
```

**Step 5: Run tests in both modes**

```bash
go test ./internal/chunk/... -v -short
go test ./internal/chunk/... -v
```

**Step 6: Commit and PR**

```bash
git add internal/chunk/blockstore_test.go
git commit -m "[Feature #101] Review and improve short mode test coverage

- Replace testing.Short() skips with adaptive data sizing
- Enable fast variants of core blockstore tests in short mode
- Keep only truly expensive operations under short mode skip
- Improves CI coverage without sacrificing speed

Resolves #101"
```

---

### Issue #91: Investigate and fix skipped blockstore multi-device tests

**Files:**
- Modify: `internal/chunk/blockstore_test.go:390-391, 469-470`
- Modify: `internal/chunk/blockstore.go` (if bug fix needed)

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-91-multidevice-tests -b issue-91-multidevice-tests origin/main
cd ../novastor-worktrees/issue-91-multidevice-tests
```

**Step 2: Investigate checksum mismatch**

Read the test implementation at line 390+ and identify:
1. How checksums are calculated
2. How multi-device stripe works
3. What could cause mismatch

**Step 3: Fix root cause**

If the issue is in the test setup, fix the test. If the issue is in blockstore.go, fix the implementation.

Common multi-device issues:
- Incorrect stripe size calculation
- Checksum calculation includes garbage data
- Device boundary handling off-by-one errors

**Step 4: Enable tests**

Remove the TODO skips once fixed:

```go
// TODO: Investigate checksum mismatch with multiple devices
// t.Skip("multi-device test needs investigation")
```

**Step 5: Commit and PR**

```bash
git add internal/chunk/blockstore_test.go internal/chunk/blockstore.go
git commit -m "[Fix #91] Fix multi-device blockstore tests

- Investigate and fix checksum mismatch in multi-device stripe
- Fix recovery test after multi-device write
- Enable previously skipped tests
- Add test coverage for multi-device scenarios

Resolves #91"
```

---

### Issue #90: Implement skipped CLI certificate tests

**Files:**
- Modify: `internal/cli/cert_test.go:24-62`
- Create: `internal/cli/cert_integration_test.go`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-90-cli-cert-tests -b issue-90-cli-cert-tests origin/main
cd ../novastor-worktrees/issue-90-cli-cert-tests
```

**Step 2: Implement TestGenerateCertCLI**

```go
func TestGenerateCertCLI(t *testing.T) {
    // Use cobra testing to execute CLI programmatically
    cmd := NewCertCommand()
    cmd.SetArgs([]string{"generate", "--output", t.TempDir()})

    err := cmd.Execute()
    if err != nil {
        t.Fatalf("cert generate command failed: %v", err)
    }

    // Verify files were created
    certPath := filepath.Join(t.TempDir(), "cert.pem")
    keyPath := filepath.Join(t.TempDir(), "key.pem")

    if _, err := os.Stat(certPath); os.IsNotExist(err) {
        t.Error("certificate file not created")
    }
    if _, err := os.Stat(keyPath); os.IsNotExist(err) {
        t.Error("key file not created")
    }
}
```

**Step 3: Implement TestGenerateCertWithExistingCA**

```go
func TestCertBootstrapExistingCA(t *testing.T) {
    tmpDir := t.TempDir()

    // Create a CA first
    caCert, caKey, err := cert.GenerateCA()
    if err != nil {
        t.Fatal(err)
    }

    // Write CA files
    caCertPath := filepath.Join(tmpDir, "ca.pem")
    caKeyPath := filepath.Join(tmpDir, "ca-key.pem")

    if err := os.WriteFile(caCertPath, caCert, 0600); err != nil {
        t.Fatal(err)
    }
    if err := os.WriteFile(caKeyPath, caKey, 0600); err != nil {
        t.Fatal(err)
    }

    // Generate server cert using existing CA
    serverCert, serverKey, err := cert.GenerateServerCert(caCert, caKey, "test-server")
    if err != nil {
        t.Fatal(err)
    }

    // Verify server cert is signed by CA
    certPool := x509.NewCertPool()
    certPool.AppendCertsFromPEM(caCert)

    parsedCert, _ := x509.ParseCertificate(serverCert)
    opts := x509.VerifyOptions{Roots: certPool}
    if _, err := parsedCert.Verify(opts); err != nil {
        t.Errorf("server cert verification failed: %v", err)
    }
}
```

**Step 4: Implement TestVerifyCertificateChain**

```go
func TestCertValidity(t *testing.T) {
    caCert, caKey, err := cert.GenerateCA()
    if err != nil {
        t.Fatal(err)
    }

    serverCert, serverKey, err := cert.GenerateServerCert(caCert, caKey, "test-server")
    if err != nil {
        t.Fatal(err)
    }

    // Parse CA cert
    parsedCA, _ := x509.ParseCertificate(caCert)
    duration := parsedCA.NotAfter.Sub(parsedCA.NotBefore)

    // Verify CA cert has ~10 year validity
    expectedDuration := 10 * 365 * 24 * time.Hour
    if duration < expectedDuration-time.Hour || duration > expectedDuration+time.Hour {
        t.Errorf("CA cert validity is %v, want ~10 years", duration)
    }

    // Parse server cert
    parsedServer, _ := x509.ParseCertificate(serverCert)
    serverDuration := parsedServer.NotAfter.Sub(parsedServer.NotBefore)

    // Verify server cert has ~1 year validity
    expectedServerDuration := 365 * 24 * time.Hour
    if serverDuration < expectedServerDuration-time.Hour || serverDuration > expectedServerDuration+time.Hour {
        t.Errorf("Server cert validity is %v, want ~1 year", serverDuration)
    }
}
```

**Step 5: Implement TestMTLSComponentCommunication**

```go
func TestMTLSComponentCommunication(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping mTLS integration test in short mode")
    }

    // Create test CA
    caCert, caKey, err := cert.GenerateCA()
    if err != nil {
        t.Fatal(err)
    }

    // Create server cert
    serverCert, serverKey, err := cert.GenerateServerCert(caCert, caKey, "localhost")
    if err != nil {
        t.Fatal(err)
    }

    // Create client cert
    clientCert, clientKey, err := cert.GenerateClientCert(caCert, caKey, "test-client")
    if err != nil {
        t.Fatal(err)
    }

    // Start test server with mTLS
    serverTLS := &tls.Config{
        Certificates: []tls.Certificate{certToTLS(serverCert, serverKey)},
        ClientAuth:   tls.RequireAndVerifyClientCert,
        ClientCAs:    certPool(caCert),
    }

    listener, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
    if err != nil {
        t.Fatal(err)
    }
    defer listener.Close()

    serverReady := make(chan bool)
    go func() {
        close(serverReady)
        conn, err := listener.Accept()
        if err != nil {
            return
        }
        defer conn.Close()
        // Verify connection received
    }()

    <-serverReady

    // Client with valid cert should connect
    clientTLS := &tls.Config{
        Certificates: []tls.Certificate{certToTLS(clientCert, clientKey)},
        RootCAs:      certPool(caCert),
    }

    conn, err := tls.Dial("tcp", listener.Addr().String(), clientTLS)
    if err != nil {
        t.Errorf("mTLS connection with valid cert failed: %v", err)
    }
    conn.Close()

    // Client without cert should be rejected
    insecureTLS := &tls.Config{
        InsecureSkipVerify: true,
    }
    _, err = tls.Dial("tcp", listener.Addr().String(), insecureTLS)
    if err == nil {
        t.Error("expected connection without cert to be rejected")
    }
}
```

**Step 6: Commit and PR**

```bash
git add internal/cli/cert_test.go
git commit -m "[Feature #90] Implement skipped CLI certificate tests

- Implement TestGenerateCertCLI for programmatic CLI execution
- Implement TestCertBootstrapExistingCA for CA reuse
- Implement TestCertValidity for certificate period verification
- Implement TestMTLSComponentCommunication for mTLS testing

Resolves #90"
```

---

## Phase 3: Feature Enhancements (Sequential)

### Issue #88: Implement Erasure Coding Shard Replication

**Files:**
- Create: `internal/policy/shard_replicator.go`
- Create: `internal/policy/shard_replicator_test.go`
- Modify: `cmd/controller/main.go:323`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-88-ec-shard-replication -b issue-88-ec-shard-replication origin/main
cd ../novastor-worktrees/issue-88-ec-shard-replication
```

**Step 2: Define ShardReplicator interface**

Create `internal/policy/shard_replicator.go`:

```go
package policy

import (
    "context"
    "fmt"
)

// ShardReplicator handles replication of missing erasure coding shards.
type ShardReplicator interface {
    // ReplicateShard recovers a single missing shard for an erasure-coded chunk.
    ReplicateShard(ctx context.Context, chunkID string, shardIndex int, pool string) error

    // IsShardMissing checks if a specific shard is unavailable.
    IsShardMissing(ctx context.Context, chunkID string, shardIndex int, pool string) (bool, error)
}

type shardReplicator struct {
    metaClient   MetadataAdapter
    dataClient   DataMoverClient
}

// NewShardReplicator creates a new shard replicator.
func NewShardReplicator(meta MetadataAdapter, data DataMoverClient) ShardReplicator {
    return &shardReplicator{
        metaClient: meta,
        dataClient: data,
    }
}

func (r *shardReplicator) ReplicateShard(ctx context.Context, chunkID string, shardIndex int, pool string) error {
    // 1. Get chunk metadata to find available shards
    chunkMeta, err := r.metaClient.GetChunk(ctx, chunkID)
    if err != nil {
        return fmt.Errorf("get chunk metadata: %w", err)
    }

    // 2. Find nodes with available shards for reconstruction
    availableShards := r.findAvailableShards(chunkMeta)
    if len(availableShards) < chunkMeta.DataShards {
        return fmt.Errorf("insufficient shards for reconstruction: have %d, need %d",
            len(availableShards), chunkMeta.DataShards)
    }

    // 3. Use DataMover to reconstruct missing shard
    err = r.dataClient.ReconstructShard(ctx, chunkID, shardIndex, availableShards)
    if err != nil {
        return fmt.Errorf("reconstruct shard: %w", err)
    }

    // 4. Update metadata to reflect new shard location
    return r.metaClient.AddShardLocation(ctx, chunkID, shardIndex, "")
}

func (r *shardReplicator) IsShardMissing(ctx context.Context, chunkID string, shardIndex int, pool string) (bool, error) {
    chunkMeta, err := r.metaClient.GetChunk(ctx, chunkID)
    if err != nil {
        return false, err
    }

    for _, shard := range chunkMeta.Shards {
        if shard.Index == shardIndex && shard.Available {
            return false, nil
        }
    }
    return true, nil
}
```

**Step 3: Wire up in controller**

Modify `cmd/controller/main.go`:

```go
shardReplicator := policy.NewShardReplicator(metaAdapter, dataMoverClient)

policyRunnable := policy.NewPolicyEngineRunnable(
    metaAdapter,
    poolLookup,
    nodeChecker,
    chunkReplicator,
    shardReplicator, // shardReplicator - now implemented
    eventRecorder,
    policyScanInterval,
    policyRepairEnabled,
)
```

**Step 4: Write tests**

Create `internal/policy/shard_replicator_test.go` with table-driven tests.

**Step 5: Commit and PR**

```bash
git add internal/policy/shard_replicator.go internal/policy/shard_replicator_test.go cmd/controller/main.go
git commit -m "[Feature #88] Implement Erasure Coding Shard Replication

- Add ShardReplicator interface for EC shard recovery
- Implement shard reconstruction using available data shards
- Wire up shard replicator in controller startup
- Add unit tests for shard replication logic

Resolves #88"
```

---

### Issue #93: Implement StorageQuota CRD controller

**Files:**
- Create: `internal/controller/storagequota_controller.go`
- Create: `internal/controller/storagequota_controller_test.go`
- Modify: `cmd/controller/main.go`
- Modify: `config/rbac/role.yaml`

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-93-storagequota-controller -b issue-93-storagequota-controller origin/main
cd ../novastor-worktrees/issue-93-storagequota-controller
```

**Step 2: Implement StorageQuotaReconciler**

Create `internal/controller/storagequota_controller.go`:

```go
package controller

import (
    "context"
    "fmt"

    "k8s.io/apimachinery/pkg/runtime"
    ctrl "sigs.k8s.io/controller-runtime"
    "sigs.k8s.io/controller-runtime/pkg/client"
    "sigs.k8s.io/controller-runtime/pkg/log"

    novastorv1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
)

// StorageQuotaReconciler reconciles StorageQuota objects.
type StorageQuotaReconciler struct {
    client.Client
    Log     log.Logger
    Scheme  *runtime.Scheme
    QuotaStore QuotaStore
}

// QuotaStore manages quota operations.
type QuotaStore interface {
    SetQuota(ctx context.Context, namespace, tenant string, quota *novastorv1alpha1.StorageQuotaSpec) error
    GetUsage(ctx context.Context, namespace, tenant string) (*novastorv1alpha1.StorageQuotaStatus, error)
    DeleteQuota(ctx context.Context, namespace, tenant string) error
}

// +kubebuilder:rbac:groups=novastor.io,resources=storagequotas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=storagequotas/status,verbs=get;update;patch

func (r *StorageQuotaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    log := log.FromContext(ctx)

    quota := &novastorv1alpha1.StorageQuota{}
    if err := r.Get(ctx, req.NamespacedName, quota); err != nil {
        return ctrl.Result{}, client.IgnoreNotFound(err)
    }

    // If deletion timestamp is set, clean up quota
    if !quota.DeletionTimestamp.IsZero() {
        if err := r.QuotaStore.DeleteQuota(ctx, quota.Namespace, quota.Name); err != nil {
            return ctrl.Result{}, fmt.Errorf("delete quota: %w", err)
        }
        return ctrl.Result{}, nil
    }

    // Set quota in metadata service
    if err := r.QuotaStore.SetQuota(ctx, quota.Namespace, quota.Name, &quota.Spec); err != nil {
        log.Error(err, "failed to set quota")
        return ctrl.Result{}, err
    }

    // Update status with current usage
    usage, err := r.QuotaStore.GetUsage(ctx, quota.Namespace, quota.Name)
    if err != nil {
        log.Error(err, "failed to get quota usage")
        return ctrl.Result{}, err
    }

    quota.Status = *usage
    if err := r.Status().Update(ctx, quota); err != nil {
        return ctrl.Result{}, fmt.Errorf("update status: %w", err)
    }

    return ctrl.Result{}, nil
}

func (r *StorageQuotaReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&novastorv1alpha1.StorageQuota{}).
        Complete(r)
}
```

**Step 3: Add controller to manager**

Modify `cmd/controller/main.go` to register the controller.

**Step 4: Commit and PR**

```bash
git add internal/controller/storagequota_controller.go cmd/controller/main.go config/rbac/role.yaml
git commit -m "[Feature #93] Implement StorageQuota CRD controller

- Add StorageQuotaReconciler for quota management
- Sync quota specs to metadata service via QuotaStore
- Update status with current usage from metadata
- Add RBAC rules for storagequotas resources

Resolves #93"
```

---

## Phase 4: Documentation Audits (Parallel)

### Issue #95: Audit and implement missing S3 API features

**Files:**
- Create: `docs/s3-api-compliance.md`
- Modify: `internal/s3/handlers.go` (if adding features)

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-95-s3-audit -b issue-95-s3-audit origin/main
cd ../novastor-worktrees/issue-95-s3-audit
```

**Step 2: Create S3 API compliance document**

Create `docs/s3-api-compliance.md` documenting:
- Implemented S3 operations
- Missing operations
- Intentionally unsupported operations

**Step 3: Implement critical missing features**

Based on audit, add lifecycle and tagging support if needed.

**Step 4: Commit and PR**

```bash
git add docs/s3-api-compliance.md internal/s3/
git commit -m "[Feature #95] Audit S3 API compliance

- Document supported and unsupported S3 operations
- Implement critical missing features (lifecycle, tagging)
- Add SSE headers to responses
- Create S3 API compliance documentation

Resolves #95"
```

---

### Issue #96: Audit CSI spec compliance

**Files:**
- Create: `docs/csi-compliance.md`
- Modify: `internal/csi/` (if adding features)

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-96-csi-audit -b issue-96-csi-audit origin/main
cd ../novastor-worktrees/issue-96-csi-audit
```

**Step 2: Create CSI compliance document**

Document CSI spec compliance and missing features.

**Step 3: Implement ControllerGetVolume if critical**

**Step 4: Commit and PR**

```bash
git add docs/csi-compliance.md internal/csi/
git commit -m "[Feature #96] Audit CSI spec compliance

- Document CSI v1.5.0 compliance status
- Implement ControllerGetVolume RPC
- Add volume condition reporting
- Document RWX support via NFS

Resolves #96"
```

---

### Issue #97: Audit NFS v4.1 protocol compliance

**Files:**
- Create: `docs/nfs-compliance.md`
- Modify: `internal/filer/` (if adding features)

**Step 1: Create worktree**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree add ../novastor-worktrees/issue-97-nfs-audit -b issue-97-nfs-audit origin/main
cd ../novastor-worktrees/issue-97-nfs-audit
```

**Step 2: Create NFS compliance document**

**Step 3: Verify v4.1 required features**

**Step 4: Commit and PR**

```bash
git add docs/nfs-compliance.md internal/filer/
git commit -m "[Feature #97] Audit NFS v4.1 protocol compliance

- Document NFS v4.1 compliance status
- Verify required features implemented
- Test with multiple NFS clients
- Document NFS feature support level

Resolves #97"
```

---

## Execution Order Summary

1. **Wave 1 (Quick Wins, Parallel 3):** #92, #89, #98, #94
2. **Wave 2 (Test Infrastructure, Parallel 3):** #100, #99, #101, #91, #90
3. **Wave 3 (Feature Enhancements, Sequential):** #88, #93
4. **Wave 4 (Documentation Audits, Parallel 3):** #95, #96, #97

---

## Cleanup

After all PRs are merged:

**Step 1: Clean up worktrees**

```bash
cd /Users/pascal/Documents/git/novastor
git worktree list | grep issue- | awk '{print $1}' | xargs -I {} git worktree remove {}
```

**Step 2: Delete merged branches**

```bash
git branch --merged | grep issue- | xargs -I {} git branch -d {}
```

**Step 3: Close issue #102**

```bash
gh issue close 102 --comment "All issues from codebase analysis have been addressed. See individual issues for details."
```
