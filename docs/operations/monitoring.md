# Monitoring Guide

NovaStor exposes Prometheus metrics from every component. This guide covers the full metrics reference, Prometheus integration, Grafana dashboard setup, and alerting rules.

## Metrics Endpoints

Each NovaStor component exposes a `/metrics` HTTP endpoint:

| Component | Default Port | Endpoint |
|---|---|---|
| Node Agent | 9101 | `http://<agent>:9101/metrics` |
| Metadata Service | 7002 | `http://<meta>:7002/metrics` |
| Controller | 8080 | `http://<controller>:8080/metrics` |
| File Gateway (NFS) | 8080 | `http://<filer>:8080/metrics` |
| S3 Gateway | 8081 | `http://<s3gw>:8081/metrics` |

## Prometheus Integration

### Automatic Discovery with ServiceMonitor

If you use the [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator), enable ServiceMonitor creation in Helm values:

```yaml
monitoring:
  serviceMonitor:
    enabled: true
    labels:
      release: prometheus  # Match your Prometheus Operator's label selector
    interval: 15s
    scrapeTimeout: 10s
```

This creates five ServiceMonitor resources:

- `novastor-agent` -- scrapes all agent DaemonSet pods
- `novastor-meta` -- scrapes all metadata StatefulSet pods
- `novastor-controller` -- scrapes the controller Deployment
- `novastor-filer` -- scrapes the filer Deployment pods
- `novastor-s3gw` -- scrapes the s3gw Deployment pods

### Manual Prometheus Configuration

If you manage Prometheus configuration directly, add scrape targets:

```yaml
scrape_configs:
  - job_name: novastor-agent
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        regex: agent
        action: keep
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        target_label: __address__
        regex: (.+)
        replacement: ${1}

  - job_name: novastor-meta
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        regex: meta
        action: keep

  - job_name: novastor-controller
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component]
        regex: controller
        action: keep
```

## Metrics Reference

### Agent Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_agent_chunk_count` | Gauge | -- | Total number of chunks stored on this agent |
| `novastor_agent_disk_bytes_total` | Gauge | `device` | Total disk capacity in bytes per device |
| `novastor_agent_disk_bytes_used` | Gauge | `device` | Used disk space in bytes per device |
| `novastor_agent_disk_bytes_free` | Gauge | `device` | Free disk space in bytes per device |
| `novastor_agent_chunk_ops_total` | Counter | `operation` | Total chunk operations (read, write, delete) |
| `novastor_agent_chunk_bytes_total` | Counter | `direction` | Total chunk bytes transferred (read, write) |
| `novastor_agent_scrub_errors_total` | Counter | -- | Total chunks with checksum errors found by scrubber |

### Metadata / Raft Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_meta_raft_state` | Gauge | -- | Raft state: 0=follower, 1=candidate, 2=leader |
| `novastor_meta_raft_commit_index` | Gauge | -- | Current Raft commit index |
| `novastor_meta_raft_apply_duration_seconds` | Histogram | -- | Time to apply a Raft log entry |
| `novastor_meta_ops_total` | Counter | `operation` | Total metadata operations by type |

### Controller Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_controller_pool_node_count` | Gauge | `pool` | Number of nodes in each storage pool |
| `novastor_controller_pool_capacity_bytes` | Gauge | `pool` | Total capacity of each storage pool in bytes |
| `novastor_controller_recovery_chunks_pending` | Gauge | -- | Chunks currently pending recovery |
| `novastor_controller_recovery_chunks_completed_total` | Counter | -- | Total chunks recovered since startup |
| `novastor_controller_recovery_chunks_failed_total` | Counter | -- | Total chunk recovery failures since startup |

### CSI Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_csi_volume_count` | Gauge | -- | Number of currently provisioned volumes |
| `novastor_csi_volume_provision_duration_seconds` | Histogram | -- | Time to provision a volume |
| `novastor_csi_volume_delete_duration_seconds` | Histogram | -- | Time to delete a volume |

### S3 Gateway Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_s3_requests_total` | Counter | `operation` | Total S3 requests by operation |
| `novastor_s3_request_duration_seconds` | Histogram | `operation` | S3 request duration by operation |
| `novastor_s3_bytes_in_total` | Counter | -- | Total bytes received by S3 gateway |
| `novastor_s3_bytes_out_total` | Counter | -- | Total bytes sent by S3 gateway |

### Filer / NFS Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_filer_nfs_ops_total` | Counter | `operation` | Total NFS operations by type |
| `novastor_filer_nfs_op_duration_seconds` | Histogram | `operation` | NFS operation duration by type |
| `novastor_filer_active_locks` | Gauge | -- | Number of active file locks |

### Dedup Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_dedup_hits_total` | Counter | -- | Total dedup cache hits |
| `novastor_dedup_misses_total` | Counter | -- | Total dedup cache misses |
| `novastor_dedup_ratio` | Gauge | -- | Current dedup ratio |
| `novastor_dedup_saved_bytes` | Gauge | -- | Bytes saved by deduplication |
| `novastor_dedup_index_size` | Gauge | -- | Size of the dedup index |
| `novastor_dedup_chunks_deduped_total` | Counter | -- | Total chunks deduplicated |
| `novastor_dedup_lookup_duration_seconds` | Histogram | -- | Dedup index lookup latency |
| `novastor_dedup_index_entries` | Gauge | -- | Number of entries in dedup index |

### GC Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_gc_chunks_collected_total` | Counter | -- | Total chunks garbage collected |
| `novastor_gc_bytes_reclaimed_total` | Counter | -- | Total bytes reclaimed by GC |
| `novastor_gc_run_duration_seconds` | Histogram | -- | GC run duration |
| `novastor_gc_pending_chunks` | Gauge | -- | Chunks pending garbage collection |
| `novastor_gc_errors_total` | Counter | -- | Total GC errors |

### Policy Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_policy_evaluations_total` | Counter | `policy` | Total policy evaluations |
| `novastor_policy_violations_total` | Counter | `policy` | Total policy violations |
| `novastor_policy_enforcement_duration_seconds` | Histogram | -- | Policy enforcement latency |
| `novastor_policy_active_count` | Gauge | -- | Number of active policies |
| `novastor_policy_migrations_total` | Counter | -- | Total policy-triggered migrations |
| `novastor_policy_migrations_pending` | Gauge | -- | Pending policy migrations |
| `novastor_policy_migrations_failed_total` | Counter | -- | Failed policy migrations |

### Webhook Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_webhook_requests_total` | Counter | `action` | Total webhook admission requests |
| `novastor_webhook_injections_total` | Counter | -- | Total scheduler injections performed |
| `novastor_webhook_errors_total` | Counter | -- | Total webhook errors |
| `novastor_webhook_latency_seconds` | Histogram | -- | Webhook request latency |

### Data Mover Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_datamover_bytes_moved_total` | Counter | -- | Total bytes moved |
| `novastor_datamover_chunks_moved_total` | Counter | -- | Total chunks moved |
| `novastor_datamover_active_moves` | Gauge | -- | Currently active data move operations |
| `novastor_datamover_move_duration_seconds` | Histogram | -- | Data move operation duration |
| `novastor_datamover_errors_total` | Counter | -- | Total data mover errors |
| `novastor_datamover_queue_depth` | Gauge | -- | Number of moves queued |
| `novastor_datamover_throughput_bytes` | Gauge | -- | Current data mover throughput in bytes/sec |

### Lock Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_lock_acquisitions_total` | Counter | `type` | Total lock acquisitions |
| `novastor_lock_releases_total` | Counter | `type` | Total lock releases |
| `novastor_lock_contention_total` | Counter | -- | Total lock contention events |
| `novastor_lock_wait_duration_seconds` | Histogram | -- | Lock wait time distribution |

### Quota Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `novastor_quota_usage_bytes` | Gauge | `scope`, `name` | Current quota usage in bytes |
| `novastor_quota_limit_bytes` | Gauge | `scope`, `name` | Quota hard limit in bytes |
| `novastor_quota_utilization_ratio` | Gauge | `scope`, `name` | Quota utilization ratio (0-1) |
| `novastor_quota_exceeded_total` | Counter | `scope`, `name` | Total quota exceeded events |
| `novastor_quota_warnings_total` | Counter | `scope`, `name` | Total soft quota warning events |
| `novastor_quota_enforcements_total` | Counter | -- | Total quota enforcement actions |
| `novastor_quota_active_count` | Gauge | -- | Number of active quotas |

## Grafana Dashboard

### Recommended Panels

Create a Grafana dashboard with the following panels organized by row:

#### Row: Cluster Overview

**Total Chunks** (Stat panel):
```promql
sum(novastor_agent_chunk_count)
```

**Total Capacity** (Stat panel):
```promql
sum(novastor_agent_disk_bytes_total)
```

**Used Capacity %** (Gauge panel):
```promql
sum(novastor_agent_disk_bytes_used) / sum(novastor_agent_disk_bytes_total) * 100
```

**Provisioned Volumes** (Stat panel):
```promql
novastor_csi_volume_count
```

#### Row: Agent Performance

**Chunk Operations Rate** (Time series):
```promql
sum(rate(novastor_agent_chunk_ops_total[5m])) by (operation)
```

**Chunk Throughput** (Time series):
```promql
sum(rate(novastor_agent_chunk_bytes_total[5m])) by (direction)
```

**Disk Usage per Node** (Time series):
```promql
novastor_agent_disk_bytes_used
```

**Scrub Errors** (Stat panel, alert on non-zero):
```promql
sum(novastor_agent_scrub_errors_total)
```

#### Row: Metadata Service

**Raft Leader** (Stat panel):
```promql
count(novastor_meta_raft_state == 2)
```

**Raft Apply Latency p99** (Time series):
```promql
histogram_quantile(0.99, rate(novastor_meta_raft_apply_duration_seconds_bucket[5m]))
```

**Metadata Operations Rate** (Time series):
```promql
sum(rate(novastor_meta_ops_total[5m])) by (operation)
```

**Raft Commit Index** (Time series):
```promql
novastor_meta_raft_commit_index
```

#### Row: Recovery

**Pending Recoveries** (Stat panel):
```promql
novastor_controller_recovery_chunks_pending
```

**Recovery Rate** (Time series):
```promql
rate(novastor_controller_recovery_chunks_completed_total[5m])
```

#### Row: S3 Gateway

**S3 Request Rate** (Time series):
```promql
sum(rate(novastor_s3_requests_total[5m])) by (operation)
```

**S3 Latency p99** (Time series):
```promql
histogram_quantile(0.99, sum(rate(novastor_s3_request_duration_seconds_bucket[5m])) by (le, operation))
```

**S3 Throughput** (Time series):
```promql
rate(novastor_s3_bytes_in_total[5m]) + rate(novastor_s3_bytes_out_total[5m])
```

#### Row: NFS Gateway

**NFS Operations Rate** (Time series):
```promql
sum(rate(novastor_filer_nfs_ops_total[5m])) by (operation)
```

**NFS Latency p99** (Time series):
```promql
histogram_quantile(0.99, sum(rate(novastor_filer_nfs_op_duration_seconds_bucket[5m])) by (le, operation))
```

**Active File Locks** (Stat panel):
```promql
novastor_filer_active_locks
```

## Alerting Rules

Add these rules to your Prometheus alerting configuration:

```yaml
groups:
  - name: novastor.rules
    rules:
      # No Raft leader elected
      - alert: NovaStorNoRaftLeader
        expr: count(novastor_meta_raft_state == 2) == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No Raft leader in NovaStor metadata cluster"
          description: "No metadata node has been elected leader for over 1 minute. Writes are blocked."

      # Scrub errors detected
      - alert: NovaStorScrubErrors
        expr: increase(novastor_agent_scrub_errors_total[1h]) > 0
        labels:
          severity: warning
        annotations:
          summary: "NovaStor scrub detected corrupt chunks"
          description: "{{ $value }} corrupt chunks detected in the last hour on {{ $labels.instance }}."

      # High disk usage
      - alert: NovaStorDiskUsageHigh
        expr: (novastor_agent_disk_bytes_used / novastor_agent_disk_bytes_total) > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "NovaStor disk usage above 85%"
          description: "Disk {{ $labels.device }} on {{ $labels.instance }} is {{ $value | humanizePercentage }} full."

      # Critical disk usage
      - alert: NovaStorDiskUsageCritical
        expr: (novastor_agent_disk_bytes_used / novastor_agent_disk_bytes_total) > 0.95
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "NovaStor disk usage above 95%"
          description: "Disk {{ $labels.device }} on {{ $labels.instance }} is {{ $value | humanizePercentage }} full. Immediate action required."

      # Recovery pending too long
      - alert: NovaStorRecoveryStalled
        expr: novastor_controller_recovery_chunks_pending > 0
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "NovaStor recovery has been pending for over 30 minutes"
          description: "{{ $value }} chunks pending recovery. Check controller logs."

      # Raft apply latency high
      - alert: NovaStorRaftLatencyHigh
        expr: histogram_quantile(0.99, rate(novastor_meta_raft_apply_duration_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "NovaStor Raft apply latency is high"
          description: "p99 Raft apply latency is {{ $value }}s. Check metadata service health."

      # Agent down
      - alert: NovaStorAgentDown
        expr: up{job="novastor-agent"} == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "NovaStor agent is down"
          description: "Agent on {{ $labels.instance }} has been unreachable for over 2 minutes."
```

## Data Plane I/O Statistics

The SPDK data plane exposes per-volume I/O statistics via the `novastor_io_stats` JSON-RPC method. These metrics are useful for monitoring read distribution across replicas and identifying performance bottlenecks.

### Key Metrics

| Metric | Description |
|---|---|
| `reads_completed` (per replica) | Total read I/Os completed by each replica |
| `read_bytes` (per replica) | Total bytes read from each replica |
| `avg_read_latency_us` (per replica) | Exponential moving average of read latency in microseconds |
| `total_read_iops` | Aggregate read I/O count across all replicas |
| `total_write_iops` | Aggregate write I/O count across all replicas |
| `write_quorum_latency_us` | Average write quorum completion latency in microseconds |

### Verifying Read Distribution

Use the `novastor_io_stats` RPC to verify reads are evenly distributed:

```bash
# Via kubectl exec to the dataplane pod
kubectl exec -n novastor-system <dataplane-pod> -- \
  /usr/local/bin/novastor-dataplane --rpc-call novastor_io_stats '{"volume_id":"<vol>"}'
```

For a 3-replica volume with `round_robin` or `latency_aware` policy, `reads_completed` should be roughly balanced across replicas. For `local_first`, the local replica will have significantly higher `reads_completed`.
