# Distributed Tracing + Continuous Profiling Design

**Date:** 2026-03-17
**Status:** Approved
**Author:** Pascal Watteel

## Problem

NovaStor achieves 1,285 random write IOPS on NVMe drives capable of 80,000 IOPS. There is a 60x performance gap but no visibility into where time is spent per I/O request. Without per-step timing across the Go management plane and Rust dataplane, optimization is guesswork.

## Solution

Add OpenTelemetry distributed tracing and continuous profiling across all NovaStor components. Traces follow requests from CSI/agent through the Rust dataplane with per-step span visibility on the I/O hot path. Flamegraphs via Pyroscope show CPU-level bottlenecks. Grafana provides the visualization UI.

## Stack

| Component | Purpose | Crate/Package |
|-----------|---------|---------------|
| Tempo | Trace collection + storage (24h) | — |
| Pyroscope | Continuous profiling / flamegraphs | — |
| Grafana | Visualization UI | — |
| tracing + tracing-opentelemetry | Rust instrumentation | `tracing`, `tracing-opentelemetry`, `opentelemetry`, `opentelemetry-otlp` |
| pprof-rs | Rust CPU profiling → Pyroscope | `pprof` |
| go.opentelemetry.io/otel | Go instrumentation | `go.opentelemetry.io/otel`, `go.opentelemetry.io/contrib` |

## Infrastructure

Three pods in the `novastor-system` namespace, pinned to one node (master-11) via nodeSelector. All use hostPath storage (no CSI driver available — NovaStor IS the CSI driver).

| Component | K8s Resource | Storage | Ports |
|-----------|-------------|---------|-------|
| Tempo | Deployment (1 replica) | hostPath `/var/lib/tempo` | 4317 (OTLP gRPC), 3200 (API) |
| Pyroscope | Deployment (1 replica) | hostPath `/var/lib/pyroscope` | 4040 |
| Grafana | Deployment (1 replica) | hostPath `/var/lib/grafana` | 3000 (NodePort 30300) |

**Tempo configuration:**
- OTLP gRPC receiver on port 4317
- 24-hour trace retention
- Local disk backend at `/var/lib/tempo/traces`

**Grafana configuration:**
- Pre-provisioned datasources: Tempo (traces), Pyroscope (profiles)
- Accessible via NodePort 30300

## Rust Dataplane Instrumentation

### Hot I/O Path Spans

Per-I/O request in `bdev/novastor_bdev.rs`, with child spans for each step:

```
novastor_bdev::write                    [total write time]
├── sub_block_write::lock_acquire       [time waiting for sub-block lock]
├── sub_block_write::bdev_read          [64KB read from backend]
├── sub_block_write::overlay            [memcpy of app data into sub-block]
├── sub_block_write::bdev_write         [64KB write to backend]
├── sub_block_write::bitmap_update      [dirty bitmap + chunk map update]
└── sub_block_write::metadata_persist   [write-through to redb]

novastor_bdev::read                     [total read time]
├── sub_block_read::bitmap_check        [dirty bitmap lookup]
└── sub_block_read::bdev_read           [64KB read from backend]
```

~6 spans per write, ~2 spans per read. Estimated overhead: ~5-10μs per I/O.

### Non-Hot-Path Spans

- `sync::sync_one_chunk` — background SHA-256 + replication
- `crush::select` — CRUSH placement decisions
- `policy::reconcile` — PolicyEngine reconcile loop

### Profiling

`pprof-rs` for CPU profiling, pushed to Pyroscope every 10 seconds. Shows flamegraphs of where CPU time goes (SHA-256, tokio scheduling, SPDK FFI, memcpy, etc.).

### OTLP Export

Spans sent to `tempo.novastor-system.svc.cluster.local:4317` via gRPC.

## Go Management Plane Instrumentation

### Automatic gRPC Instrumentation

`otelgrpc` interceptors on all gRPC clients and servers:
- Agent gRPC server (NVMeTargetService, ChunkService)
- Agent dataplane gRPC client (all RPCs to Rust dataplane)
- CSI controller agent client (CreateTarget, DeleteTarget, SetANAState)
- Metadata service gRPC server

Zero manual instrumentation — every gRPC call gets a span automatically.

### Manual Spans

| Component | Function | Purpose |
|-----------|----------|---------|
| Agent | `BackendAssignmentReconciler.Reconcile` | Backend init timing |
| Agent | `ensureDataplaneState` | Re-init after restart |
| Agent | `syncTopology` | Topology push cycle |
| Agent | `registerNode` | Heartbeat timing |
| Agent | `SPDKTargetServer.CreateTarget` | End-to-end volume creation |
| Agent | `SPDKTargetServer.ensureChunkStore` | Init + retry loop |
| Agent | `FailoverController.runCycle` | ANA state transitions |

### Profiling

Go's built-in `pprof` endpoint at `:9101/debug/pprof/`. Pyroscope's Go SDK pulls profiles automatically.

### Trace Context Propagation

gRPC metadata carries the W3C `traceparent` header across Go → Rust boundaries. The Rust `tonic` server extracts it via `tracing-opentelemetry`, linking Go and Rust spans into a single trace.

## Scope of Changes

### New Infrastructure (manifests)

- Tempo: Deployment, Service, ConfigMap, hostPath volume
- Pyroscope: Deployment, Service, hostPath volume
- Grafana: Deployment, NodePort Service, ConfigMap (datasource provisioning), hostPath volume

### Rust Dataplane

| File | Change |
|------|--------|
| `Cargo.toml` | Add tracing, tracing-opentelemetry, opentelemetry, opentelemetry-otlp, pprof |
| `src/main.rs` | Init OTel tracer provider + Pyroscope profiler |
| `src/bdev/novastor_bdev.rs` | Add tracing spans on sub_block_write, sub_block_read, submit_request |
| `src/chunk/sync.rs` | Add spans on sync_one_chunk |
| `src/transport/dataplane_service.rs` | Add OTel layer to tonic server for trace context extraction |

### Go Components

| File | Change |
|------|--------|
| `cmd/agent/main.go` | Init OTel tracer provider, add gRPC interceptors |
| `cmd/csi/main.go` | Init OTel tracer provider, add gRPC interceptors |
| `cmd/controller/main.go` | Init OTel tracer provider |
| `cmd/meta/main.go` | Init OTel tracer provider, add gRPC interceptors |
| `internal/agent/spdk_target_server.go` | Manual spans on CreateTarget |
| `internal/agent/backend_assignment.go` | Manual spans on Reconcile |
| `go.mod` | Add OTel dependencies |

### No Changes To

- Chunk engine logic, sub-block I/O, CRUSH, PolicyEngine
- Proto definitions, CRD types
- Storage classes, Helm chart (tracing infra deployed separately)

## Design Decisions Summary

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Trace backend | Grafana Tempo | Lightweight, OTel-native, local disk storage |
| Profiling | Grafana Pyroscope | CPU flamegraphs, integrates with Tempo |
| Visualization | Grafana | Unified UI for traces + profiles |
| Storage | hostPath | No CSI driver available (NovaStor IS the CSI) |
| Retention | 24 hours | Enough for debugging, ~1-5GB disk |
| Hot path granularity | Per-step child spans | Full visibility into where time goes per I/O |
| Sampling | None (100%) | Need to see every I/O during optimization |
| Go instrumentation | gRPC interceptors + manual key spans | Low effort, high coverage |
| Context propagation | W3C traceparent via gRPC metadata | Standard, works Go ↔ Rust |
| Infrastructure node | master-11 (pinned) | Co-locate hostPath data |
