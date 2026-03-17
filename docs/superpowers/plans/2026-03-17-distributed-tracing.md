# Distributed Tracing + Profiling Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add OpenTelemetry distributed tracing and Pyroscope continuous profiling across all NovaStor components to identify I/O performance bottlenecks.

**Architecture:** Tempo collects traces via OTLP gRPC. Pyroscope collects CPU profiles. Grafana visualizes both. Rust dataplane has per-step spans on the hot I/O path. Go components have automatic gRPC interceptors + manual spans on key operations. Trace context propagates across Go→Rust gRPC boundaries via W3C traceparent.

**Tech Stack:** Grafana Tempo, Grafana Pyroscope, Grafana, OpenTelemetry (Rust: tracing + tracing-opentelemetry; Go: go.opentelemetry.io/otel), pprof-rs

**Spec:** `docs/superpowers/specs/2026-03-17-distributed-tracing-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|---------------|--------|
| `deploy/observability/tempo.yaml` | Tempo Deployment + Service + ConfigMap | Create |
| `deploy/observability/pyroscope.yaml` | Pyroscope Deployment + Service | Create |
| `deploy/observability/grafana.yaml` | Grafana Deployment + Service + datasource provisioning | Create |
| `deploy/observability/deploy.sh` | One-command deploy script | Create |
| `dataplane/Cargo.toml` | Add tracing + OTel crates | Modify |
| `dataplane/src/tracing_init.rs` | OTel tracer provider + Pyroscope init | Create |
| `dataplane/src/main.rs` | Call tracing init | Modify |
| `dataplane/src/bdev/novastor_bdev.rs` | Add spans to sub_block_write/read | Modify |
| `dataplane/src/chunk/sync.rs` | Add spans to sync | Modify |
| `dataplane/src/transport/dataplane_service.rs` | OTel layer on tonic server | Modify |
| `internal/observability/tracer.go` | Go OTel tracer provider init | Create |
| `cmd/agent/main.go` | Init tracer, gRPC interceptors | Modify |
| `cmd/csi/main.go` | Init tracer, gRPC interceptors | Modify |
| `cmd/controller/main.go` | Init tracer | Modify |
| `cmd/meta/main.go` | Init tracer, gRPC interceptors | Modify |
| `internal/agent/spdk_target_server.go` | Manual spans | Modify |
| `internal/agent/backend_assignment.go` | Manual spans | Modify |
| `go.mod` | Add OTel dependencies | Modify |

---

## Chunk 1: Observability Infrastructure

### Task 1: Deploy Tempo

**Files:**
- Create: `deploy/observability/tempo.yaml`

- [ ] **Step 1: Create Tempo manifest**

```yaml
# deploy/observability/tempo.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: tempo-config
  namespace: novastor-system
data:
  tempo.yaml: |
    server:
      http_listen_port: 3200
    distributor:
      receivers:
        otlp:
          protocols:
            grpc:
              endpoint: "0.0.0.0:4317"
    storage:
      trace:
        backend: local
        local:
          path: /var/lib/tempo/traces
        wal:
          path: /var/lib/tempo/wal
    compactor:
      compaction:
        block_retention: 24h
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tempo
  namespace: novastor-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tempo
  template:
    metadata:
      labels:
        app: tempo
    spec:
      nodeSelector:
        kubernetes.io/hostname: master-11
      containers:
      - name: tempo
        image: grafana/tempo:2.6.1
        args: ["-config.file=/etc/tempo/tempo.yaml"]
        ports:
        - containerPort: 4317
          name: otlp-grpc
        - containerPort: 3200
          name: http
        volumeMounts:
        - name: config
          mountPath: /etc/tempo
        - name: data
          mountPath: /var/lib/tempo
      volumes:
      - name: config
        configMap:
          name: tempo-config
      - name: data
        hostPath:
          path: /var/lib/tempo
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: tempo
  namespace: novastor-system
spec:
  selector:
    app: tempo
  ports:
  - name: otlp-grpc
    port: 4317
    targetPort: 4317
  - name: http
    port: 3200
    targetPort: 3200
```

- [ ] **Step 2: Deploy and verify**

```bash
kubectl apply -f deploy/observability/tempo.yaml
kubectl -n novastor-system rollout status deployment/tempo --timeout=60s
kubectl -n novastor-system logs deployment/tempo --tail=5
```
Expected: Pod running, log shows "Tempo started".

- [ ] **Step 3: Commit**

```bash
git add deploy/observability/tempo.yaml
git commit -m "feat(tracing): deploy Grafana Tempo for trace collection"
```

---

### Task 2: Deploy Pyroscope

**Files:**
- Create: `deploy/observability/pyroscope.yaml`

- [ ] **Step 1: Create Pyroscope manifest**

```yaml
# deploy/observability/pyroscope.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pyroscope
  namespace: novastor-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: pyroscope
  template:
    metadata:
      labels:
        app: pyroscope
    spec:
      nodeSelector:
        kubernetes.io/hostname: master-11
      containers:
      - name: pyroscope
        image: grafana/pyroscope:1.10.0
        ports:
        - containerPort: 4040
          name: http
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        hostPath:
          path: /var/lib/pyroscope
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: pyroscope
  namespace: novastor-system
spec:
  selector:
    app: pyroscope
  ports:
  - name: http
    port: 4040
    targetPort: 4040
```

- [ ] **Step 2: Deploy and verify**

```bash
kubectl apply -f deploy/observability/pyroscope.yaml
kubectl -n novastor-system rollout status deployment/pyroscope --timeout=60s
```

- [ ] **Step 3: Commit**

```bash
git add deploy/observability/pyroscope.yaml
git commit -m "feat(tracing): deploy Grafana Pyroscope for continuous profiling"
```

---

### Task 3: Deploy Grafana with pre-configured datasources

**Files:**
- Create: `deploy/observability/grafana.yaml`

- [ ] **Step 1: Create Grafana manifest with datasource provisioning**

```yaml
# deploy/observability/grafana.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
  namespace: novastor-system
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
    - name: Tempo
      type: tempo
      access: proxy
      url: http://tempo.novastor-system.svc:3200
      isDefault: true
      jsonData:
        tracesToProfiles:
          datasourceUid: pyroscope
          profileTypeId: "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
    - name: Pyroscope
      type: grafana-pyroscope-datasource
      uid: pyroscope
      access: proxy
      url: http://pyroscope.novastor-system.svc:4040
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  namespace: novastor-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      nodeSelector:
        kubernetes.io/hostname: master-11
      containers:
      - name: grafana
        image: grafana/grafana:11.4.0
        ports:
        - containerPort: 3000
          name: http
        env:
        - name: GF_AUTH_ANONYMOUS_ENABLED
          value: "true"
        - name: GF_AUTH_ANONYMOUS_ORG_ROLE
          value: "Admin"
        - name: GF_INSTALL_PLUGINS
          value: "grafana-pyroscope-datasource"
        volumeMounts:
        - name: datasources
          mountPath: /etc/grafana/provisioning/datasources
        - name: data
          mountPath: /var/lib/grafana
      volumes:
      - name: datasources
        configMap:
          name: grafana-datasources
      - name: data
        hostPath:
          path: /var/lib/grafana
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
  namespace: novastor-system
spec:
  type: NodePort
  selector:
    app: grafana
  ports:
  - name: http
    port: 3000
    targetPort: 3000
    nodePort: 30300
```

- [ ] **Step 2: Deploy and verify**

```bash
kubectl apply -f deploy/observability/grafana.yaml
kubectl -n novastor-system rollout status deployment/grafana --timeout=120s
```
Verify: Open `http://192.168.100.11:30300` — Grafana should load with Tempo and Pyroscope datasources.

- [ ] **Step 3: Create deploy helper script**

```bash
# deploy/observability/deploy.sh
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
kubectl apply -f "$DIR/tempo.yaml"
kubectl apply -f "$DIR/pyroscope.yaml"
kubectl apply -f "$DIR/grafana.yaml"
echo "Waiting for observability stack..."
kubectl -n novastor-system rollout status deployment/tempo --timeout=60s
kubectl -n novastor-system rollout status deployment/pyroscope --timeout=60s
kubectl -n novastor-system rollout status deployment/grafana --timeout=120s
echo "Grafana: http://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):30300"
```

- [ ] **Step 4: Commit**

```bash
chmod +x deploy/observability/deploy.sh
git add deploy/observability/
git commit -m "feat(tracing): deploy Grafana with Tempo + Pyroscope datasources"
```

---

## Chunk 2: Rust Dataplane Instrumentation

### Task 4: Add tracing dependencies to Cargo.toml

**Files:**
- Modify: `dataplane/Cargo.toml`

- [ ] **Step 1: Add crates**

Add to `[dependencies]` in `dataplane/Cargo.toml`:

```toml
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tracing-opentelemetry = "0.27"
opentelemetry = { version = "0.27", features = ["trace"] }
opentelemetry_sdk = { version = "0.27", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.27", features = ["tonic"] }
```

- [ ] **Step 2: Verify it resolves**

```bash
cd dataplane && cargo check 2>&1 | head -5
```
Pre-existing SPDK errors are OK. No new dependency resolution errors.

- [ ] **Step 3: Commit**

```bash
git add dataplane/Cargo.toml
git commit -m "feat(tracing): add OpenTelemetry tracing crates to Rust dataplane"
```

---

### Task 5: Tracer provider initialization

**Files:**
- Create: `dataplane/src/tracing_init.rs`
- Modify: `dataplane/src/main.rs`
- Modify: `dataplane/src/lib.rs`

- [ ] **Step 1: Create tracing_init.rs**

```rust
//! OpenTelemetry tracer provider initialization.
//!
//! Configures the `tracing` crate to export spans via OTLP gRPC to Tempo.
//! The OTEL_EXPORTER_OTLP_ENDPOINT env var controls the endpoint
//! (default: http://tempo.novastor-system.svc:4317).

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initialize OpenTelemetry tracing with OTLP exporter.
/// Falls back gracefully if Tempo is not reachable — tracing is best-effort.
pub fn init_tracing(service_name: &str) {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://tempo.novastor-system.svc:4317".to_string());

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("OpenTelemetry init failed (tracing disabled): {e}");
            // Fall back to env_logger only (no OTel spans).
            return;
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();

    let tracer = provider.tracer(service_name.to_string());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,novastor_dataplane=debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();

    log::info!("OpenTelemetry tracing initialized (endpoint: {})", endpoint);
}

/// Shutdown the tracer provider (flushes pending spans).
pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}
```

- [ ] **Step 2: Add module to lib.rs**

Add `pub mod tracing_init;` to `dataplane/src/lib.rs`.

- [ ] **Step 3: Call init in main.rs**

In `dataplane/src/main.rs`, replace the `env_logger::Builder` init with:

```rust
novastor_dataplane::tracing_init::init_tracing("novastor-dataplane");
```

And at shutdown (before `runtime.shutdown_background()`):

```rust
novastor_dataplane::tracing_init::shutdown_tracing();
```

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/tracing_init.rs dataplane/src/lib.rs dataplane/src/main.rs
git commit -m "feat(tracing): init OpenTelemetry tracer provider in Rust dataplane"
```

---

### Task 6: Instrument Rust hot I/O path

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs`
- Modify: `dataplane/src/chunk/sync.rs`

- [ ] **Step 1: Add tracing spans to sub_block_write**

At the top of `novastor_bdev.rs`, add:
```rust
use tracing::{instrument, info_span, Instrument};
```

Wrap key sections in `sub_block_write` with spans:

```rust
async fn sub_block_write(volume_name: &str, offset: u64, data: &[u8]) -> Result<()> {
    let _span = info_span!("sub_block_write",
        volume = %volume_name, offset, len = data.len()
    ).entered();

    // ... existing code, with child spans:

    // Before lock acquire:
    let _lock_span = info_span!("lock_acquire").entered();
    let _guard = lock.lock().await;
    drop(_lock_span);

    // Before bdev_read:
    let _read_span = info_span!("bdev_read", sb_idx, bytes = SUB_BLOCK_SIZE).entered();
    let buf = reactor_dispatch::bdev_read_async(...).await;
    drop(_read_span);

    // Before bdev_write:
    let _write_span = info_span!("bdev_write", sb_idx, bytes = SUB_BLOCK_SIZE).entered();
    reactor_dispatch::bdev_write_async(...).await?;
    drop(_write_span);

    // Before bitmap update:
    let _bitmap_span = info_span!("bitmap_update").entered();
    // ... bitmap code
    drop(_bitmap_span);

    // Before metadata persist:
    let _persist_span = info_span!("metadata_persist").entered();
    // ... store.put_chunk_map(...)
    drop(_persist_span);
}
```

- [ ] **Step 2: Add tracing spans to sub_block_read**

Same pattern — wrap with `info_span!("sub_block_read", ...)` and child spans for `bitmap_check` and `bdev_read`.

- [ ] **Step 3: Add tracing spans to sync**

In `chunk/sync.rs`, add `use tracing::info_span;` and wrap `sync_one_chunk` with:
```rust
let _span = info_span!("sync_chunk", volume = %volume_name, chunk_idx).entered();
```

- [ ] **Step 4: Commit**

```bash
git add dataplane/src/bdev/novastor_bdev.rs dataplane/src/chunk/sync.rs
git commit -m "feat(tracing): instrument Rust hot I/O path with OTel spans"
```

---

## Chunk 3: Go Management Plane Instrumentation

### Task 7: Go OTel tracer provider + gRPC interceptors

**Files:**
- Create: `internal/observability/tracer.go`
- Modify: `go.mod`
- Modify: `cmd/agent/main.go`

- [ ] **Step 1: Add Go OTel dependencies**

```bash
cd /Users/pascal/Development/Nova/novastor
go get go.opentelemetry.io/otel@latest
go get go.opentelemetry.io/otel/sdk@latest
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc@latest
go get go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc@latest
```

- [ ] **Step 2: Create tracer.go**

```go
// internal/observability/tracer.go
package observability

import (
    "context"
    "os"
    "time"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
    "go.uber.org/zap"
)

// InitTracer sets up the OpenTelemetry tracer provider with OTLP gRPC export.
// The OTEL_EXPORTER_OTLP_ENDPOINT env var controls the endpoint
// (default: tempo.novastor-system.svc:4317).
func InitTracer(serviceName string, logger *zap.Logger) func() {
    endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint == "" {
        endpoint = "tempo.novastor-system.svc:4317"
    }

    ctx := context.Background()
    exporter, err := otlptracegrpc.New(ctx,
        otlptracegrpc.WithEndpoint(endpoint),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        logger.Warn("OpenTelemetry init failed (tracing disabled)", zap.Error(err))
        return func() {}
    }

    res, _ := resource.New(ctx,
        resource.WithAttributes(
            semconv.ServiceNameKey.String(serviceName),
        ),
    )

    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(res),
    )
    otel.SetTracerProvider(tp)

    logger.Info("OpenTelemetry tracing initialized",
        zap.String("endpoint", endpoint),
        zap.String("service", serviceName),
    )

    return func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = tp.Shutdown(ctx)
    }
}
```

- [ ] **Step 3: Add tracer init + gRPC interceptors to agent**

In `cmd/agent/main.go`, after `logging.Init()`:

```go
import "github.com/azrtydxb/novastor/internal/observability"
import "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

// After logging.Init():
shutdownTracer := observability.InitTracer("novastor-agent", logging.L)
defer shutdownTracer()

// When creating gRPC server, add interceptors:
srv := grpc.NewServer(
    append(serverOpts,
        grpc.StatsHandler(otelgrpc.NewServerHandler()),
    )...,
)

// When dialing dataplane, add interceptor:
dpClient, err := dataplane.Dial(*dataplaneAddr, logging.L,
    grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
)
```

- [ ] **Step 4: Verify build**

```bash
go build ./cmd/agent/
```

- [ ] **Step 5: Commit**

```bash
git add internal/observability/tracer.go go.mod go.sum cmd/agent/main.go
git commit -m "feat(tracing): Go OTel tracer provider + gRPC interceptors on agent"
```

---

### Task 8: Instrument remaining Go components

**Files:**
- Modify: `cmd/csi/main.go`
- Modify: `cmd/controller/main.go`
- Modify: `cmd/meta/main.go`
- Modify: `internal/agent/spdk_target_server.go`
- Modify: `internal/agent/backend_assignment.go`

- [ ] **Step 1: Add tracer init to CSI, controller, meta**

Same pattern as agent — add `InitTracer` call and `otelgrpc` interceptors to each `main.go`. Service names: `novastor-csi`, `novastor-controller`, `novastor-meta`.

- [ ] **Step 2: Add manual spans to key agent operations**

In `internal/agent/spdk_target_server.go` `CreateTarget`:
```go
import "go.opentelemetry.io/otel"

func (s *SPDKTargetServer) CreateTarget(ctx context.Context, req *pb.CreateTargetRequest) (*pb.CreateTargetResponse, error) {
    ctx, span := otel.Tracer("novastor-agent").Start(ctx, "CreateTarget")
    defer span.End()
    // ... existing code
}
```

In `internal/agent/backend_assignment.go` `Reconcile`:
```go
func (r *BackendAssignmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    ctx, span := otel.Tracer("novastor-agent").Start(ctx, "BackendAssignment.Reconcile")
    defer span.End()
    // ... existing code
}
```

- [ ] **Step 3: Build all binaries**

```bash
go build ./cmd/agent/ && go build ./cmd/csi/ && go build ./cmd/controller/ && go build ./cmd/meta/
```

- [ ] **Step 4: Commit**

```bash
git add cmd/ internal/agent/
git commit -m "feat(tracing): instrument Go CSI, controller, meta + manual spans on key ops"
```

---

## Chunk 4: Build, Deploy, Verify

### Task 9: Build and deploy all images

- [ ] **Step 1: Build all images**

```bash
docker build --no-cache -f dataplane/Dockerfile.build -t 192.168.100.11:30500/novastor/dataplane:rmw-fix5 dataplane/
docker build --no-cache -f build/Dockerfile.agent -t 192.168.100.11:30500/novastor/novastor-agent:latest .
docker build --no-cache -f build/Dockerfile.csi -t 192.168.100.11:30500/novastor/novastor-csi:latest .
docker build --no-cache -f build/Dockerfile.controller -t 192.168.100.11:30500/novastor/novastor-controller:latest .
```

- [ ] **Step 2: Push all images**

```bash
for img in novastor/dataplane:rmw-fix5 novastor/novastor-agent:latest novastor/novastor-csi:latest novastor/novastor-controller:latest; do
  podman push 192.168.100.11:30500/$img --tls-verify=false
done
```

- [ ] **Step 3: Deploy observability stack**

```bash
./deploy/observability/deploy.sh
```

- [ ] **Step 4: Restart all NovaStor components**

```bash
kubectl -n novastor-system rollout restart daemonset/novastor-dataplane
kubectl -n novastor-system rollout status daemonset/novastor-dataplane --timeout=180s
kubectl -n novastor-system rollout restart daemonset/novastor-agent deployment/novastor-csi-controller deployment/novastor-controller
kubectl -n novastor-system rollout status daemonset/novastor-agent --timeout=180s
```

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(tracing): all images built with OTel instrumentation"
```

---

### Task 10: Verify traces and profiles

- [ ] **Step 1: Generate I/O traffic**

```bash
kubectl apply -f ...  # create a test volume
kubectl exec test-pod -- dd if=/dev/urandom of=/data/test bs=4k count=1000
```

- [ ] **Step 2: Verify traces in Grafana**

Open `http://192.168.100.11:30300`, go to Explore → Tempo datasource. Search for traces with `service.name = novastor-dataplane`. Should see:
- `sub_block_write` spans with child spans (lock_acquire, bdev_read, bdev_write, bitmap_update, metadata_persist)
- `sub_block_read` spans with child spans (bitmap_check, bdev_read)

- [ ] **Step 3: Verify profiles in Grafana**

Go to Explore → Pyroscope datasource. Select `novastor-dataplane` service. Should see CPU flamegraph showing time distribution across functions.

- [ ] **Step 4: Verify cross-service traces**

Search for traces with `service.name = novastor-agent`. CreateTarget spans should show child spans for the Rust dataplane gRPC calls (InitChunkStore, CreateVolume, CreateNvmfTarget).

- [ ] **Step 5: Commit benchmark with tracing enabled**

```bash
./test/benchmark/storage-bench.sh test-pod --size 64 --label "with tracing"
git add test/benchmark/results-*.txt
git commit -m "bench: I/O performance with tracing enabled"
```
