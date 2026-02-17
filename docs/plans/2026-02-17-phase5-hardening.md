# Phase 5: Hardening — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Production-harden NovaStor with failure recovery, data scrubbing, monitoring, benchmarking, encryption at rest, and mTLS between components.

**Architecture:** Add self-healing capabilities (automatic re-replication/rebuild on node failure), background data integrity verification, comprehensive Prometheus metrics with Grafana dashboards, encryption layer in the chunk engine, and mTLS for all gRPC communication.

**Tech Stack:** Prometheus client_golang, Grafana dashboard JSON, AES-256-GCM encryption, x509 certificates, TLS 1.3.

---

### Task 1: Failure Detection and Recovery

**Files:**
- Create: `internal/operator/recovery.go`
- Create: `internal/operator/recovery_test.go`

Node heartbeat monitoring in the controller. When a node is unreachable for N seconds: mark node as down, identify all under-replicated chunks (via placement maps), schedule re-replication/rebuild to healthy nodes. Priority queue: chunks with fewer surviving copies rebuild first. Bandwidth throttling.

### Task 2: Data Scrubber

**Files:**
- Create: `internal/chunk/scrubber.go`
- Create: `internal/chunk/scrubber_test.go`

Background goroutine on each agent that periodically reads every local chunk and verifies its CRC-32C checksum. Reports corrupt chunks to the controller for re-replication. Configurable scrub interval and throughput limit.

### Task 3: Prometheus Metrics

**Files:**
- Create: `internal/metrics/metrics.go`
- Create: `internal/metrics/collector.go`

Comprehensive metrics for all components:
- **Agent:** chunk_count, disk_bytes_total/used/free, chunk_read/write_ops, chunk_read/write_bytes, scrub_errors
- **Meta:** raft_state, raft_commit_index, raft_apply_latency, metadata_ops_total
- **Controller:** pool_node_count, pool_capacity, recovery_chunks_pending, recovery_chunks_completed
- **CSI:** volume_count, volume_provision/delete_duration
- **S3:** request_count_by_operation, request_duration, bytes_in/out
- **Filer:** nfs_ops_total, nfs_op_duration, active_locks

### Task 4: Grafana Dashboards

**Files:**
- Create: `deploy/monitoring/grafana-dashboard-overview.json`
- Create: `deploy/monitoring/grafana-dashboard-agent.json`
- Create: `deploy/monitoring/prometheus-rules.yaml`

Pre-built dashboards: cluster overview (capacity, health, throughput), per-agent details (disk health, chunk operations), alerting rules (node down, under-replicated, disk full, scrub errors).

### Task 5: Encryption at Rest

**Files:**
- Create: `internal/chunk/encrypt.go`
- Create: `internal/chunk/encrypt_test.go`

AES-256-GCM encryption wrapper around the Store interface. Encryption key from Kubernetes Secret (or derived per-chunk from a master key). Transparent encrypt-on-write, decrypt-on-read. Optional per-StoragePool enable/disable.

### Task 6: mTLS Between Components

**Files:**
- Create: `internal/transport/tls.go`
- Create: `internal/transport/tls_test.go`
- Create: `internal/transport/certgen.go`

TLS 1.3 mutual authentication for all gRPC connections. Self-signed CA generated at install time, stored in Kubernetes Secret. Per-component certificates. Certificate rotation support.

### Task 7: Performance Benchmarks

**Files:**
- Create: `test/benchmark/chunk_bench_test.go`
- Create: `test/benchmark/replication_bench_test.go`
- Create: `test/benchmark/erasure_bench_test.go`
- Create: `test/benchmark/metadata_bench_test.go`

Benchmarks for: chunk write/read throughput (sequential and random), replication overhead, erasure coding encode/decode, metadata operations per second, end-to-end latency for block/file/object operations.

### Task 8: Final Verification

Full test suite with race detection, lint, build all binaries, run benchmarks, verify metrics are exposed, test failure recovery scenario, verify encryption round-trip.
