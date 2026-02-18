# Helm Chart Reference

Complete reference for all NovaStor Helm chart values. The chart deploys all NovaStor components into a single namespace.

## Global Settings

| Parameter | Default | Description |
|---|---|---|
| `global.image.registry` | `ghcr.io/piwi3910` | Container image registry |
| `global.image.tag` | `0.1.0` | Image tag for all NovaStor components |
| `global.image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `nameOverride` | `""` | Override the chart name |
| `fullnameOverride` | `""` | Override the full release name |

## CRD Management

| Parameter | Default | Description |
|---|---|---|
| `crds.install` | `true` | Install CRDs as part of the Helm release. Set to `false` when managing CRDs separately via `kubectl apply -f deploy/crds/` |

## Namespace

| Parameter | Default | Description |
|---|---|---|
| `namespace.create` | `false` | Create the namespace if it does not exist |
| `namespace.name` | `""` | Override namespace name (defaults to release namespace) |

## Controller

The controller runs reconciliation loops for all NovaStor CRDs and manages automatic recovery.

| Parameter | Default | Description |
|---|---|---|
| `controller.replicas` | `1` | Number of controller replicas (only leader is active) |
| `controller.image.repository` | `novastor-controller` | Controller image name |
| `controller.leaderElect` | `true` | Enable leader election for HA |
| `controller.resources.requests.cpu` | `100m` | CPU request |
| `controller.resources.requests.memory` | `128Mi` | Memory request |
| `controller.resources.limits.cpu` | `500m` | CPU limit |
| `controller.resources.limits.memory` | `256Mi` | Memory limit |
| `controller.nodeSelector` | `{}` | Node selector for controller pods |
| `controller.tolerations` | `[]` | Tolerations for controller pods |
| `controller.affinity` | `{}` | Affinity rules for controller pods |

## Metadata Service

The metadata service provides Raft-based consensus and metadata storage.

| Parameter | Default | Description |
|---|---|---|
| `meta.replicas` | `3` | Number of metadata replicas (must be odd: 3 or 5) |
| `meta.image.repository` | `novastor-meta` | Metadata service image name |
| `meta.storage` | `10Gi` | PVC size for Raft data per replica |
| `meta.storageClassName` | `""` | StorageClass for metadata PVCs (uses default if empty) |
| `meta.resources.requests.cpu` | `200m` | CPU request |
| `meta.resources.requests.memory` | `256Mi` | Memory request |
| `meta.resources.limits.cpu` | `1` | CPU limit |
| `meta.resources.limits.memory` | `1Gi` | Memory limit |
| `meta.raft.raftPort` | `7000` | TCP port for Raft consensus transport |
| `meta.raft.grpcPort` | `7001` | TCP port for the gRPC client API |
| `meta.raft.metricsPort` | `7002` | TCP port for Prometheus metrics |
| `meta.nodeSelector` | `{}` | Node selector for metadata pods |
| `meta.tolerations` | `[]` | Tolerations for metadata pods |
| `meta.affinity` | `{}` | Affinity rules for metadata pods |

!!! warning "Replica Count"
    The metadata service requires an odd number of replicas for Raft quorum. Use 3 for most deployments, 5 for high availability. A single replica works for development but provides no fault tolerance.

## Node Agent

The agent runs on every storage node as a DaemonSet.

| Parameter | Default | Description |
|---|---|---|
| `agent.image.repository` | `novastor-agent` | Agent image name |
| `agent.resources.requests.cpu` | `200m` | CPU request |
| `agent.resources.requests.memory` | `256Mi` | Memory request |
| `agent.resources.limits.cpu` | `2` | CPU limit |
| `agent.resources.limits.memory` | `2Gi` | Memory limit |
| `agent.nodeSelector` | `{}` | Node selector for agent pods |
| `agent.tolerations` | `[]` | Tolerations for agent pods |

## CSI Driver

### CSI Controller

| Parameter | Default | Description |
|---|---|---|
| `csi.controller.replicas` | `1` | Number of CSI controller replicas |
| `csi.controller.image.repository` | `novastor-csi` | CSI driver image name |
| `csi.controller.resources.requests.cpu` | `100m` | CPU request |
| `csi.controller.resources.requests.memory` | `128Mi` | Memory request |
| `csi.controller.resources.limits.cpu` | `500m` | CPU limit |
| `csi.controller.resources.limits.memory` | `256Mi` | Memory limit |

### CSI Node

| Parameter | Default | Description |
|---|---|---|
| `csi.node.image.repository` | `novastor-csi` | CSI node image name |
| `csi.node.resources.requests.cpu` | `100m` | CPU request |
| `csi.node.resources.requests.memory` | `128Mi` | Memory request |
| `csi.node.resources.limits.cpu` | `500m` | CPU limit |
| `csi.node.resources.limits.memory` | `256Mi` | Memory limit |

### CSI Sidecars

| Parameter | Default | Description |
|---|---|---|
| `csi.provisioner.image` | `registry.k8s.io/sig-storage/csi-provisioner:v5.0.1` | CSI provisioner sidecar |
| `csi.snapshotter.image` | `registry.k8s.io/sig-storage/csi-snapshotter:v8.0.1` | CSI snapshotter sidecar |
| `csi.resizer.image` | `registry.k8s.io/sig-storage/csi-resizer:v1.11.1` | CSI resizer sidecar |
| `csi.registrar.image` | `registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.11.1` | CSI node registrar sidecar |

Each sidecar also supports `resources.requests` and `resources.limits` with defaults of `50m/64Mi` and `200m/128Mi` respectively.

## S3 Gateway

| Parameter | Default | Description |
|---|---|---|
| `s3gw.replicas` | `2` | Number of S3 gateway replicas |
| `s3gw.image.repository` | `novastor-s3gw` | S3 gateway image name |
| `s3gw.accessKey` | `""` | S3 access key (required for authentication) |
| `s3gw.secretKey` | `""` | S3 secret key (required for authentication) |
| `s3gw.resources.requests.cpu` | `200m` | CPU request |
| `s3gw.resources.requests.memory` | `256Mi` | Memory request |
| `s3gw.resources.limits.cpu` | `1` | CPU limit |
| `s3gw.resources.limits.memory` | `1Gi` | Memory limit |
| `s3gw.nodeSelector` | `{}` | Node selector |
| `s3gw.tolerations` | `[]` | Tolerations |
| `s3gw.affinity` | `{}` | Affinity rules |

!!! warning "S3 Credentials"
    Always set `s3gw.accessKey` and `s3gw.secretKey` in production. Without them, the S3 gateway rejects all requests. Use `--set-string` or a values file -- never pass secrets on the command line in shared environments.

## File Gateway (NFS)

| Parameter | Default | Description |
|---|---|---|
| `filer.replicas` | `1` | Number of filer replicas |
| `filer.image.repository` | `novastor-filer` | Filer image name |
| `filer.resources.requests.cpu` | `200m` | CPU request |
| `filer.resources.requests.memory` | `256Mi` | Memory request |
| `filer.resources.limits.cpu` | `1` | CPU limit |
| `filer.resources.limits.memory` | `1Gi` | Memory limit |
| `filer.nodeSelector` | `{}` | Node selector |
| `filer.tolerations` | `[]` | Tolerations |
| `filer.affinity` | `{}` | Affinity rules |

## StorageClass

| Parameter | Default | Description |
|---|---|---|
| `storageClasses.enabled` | `true` | Create default StorageClass resources |
| `storageClass.name` | `novastor-block` | Default StorageClass name |
| `storageClass.isDefault` | `false` | Set as default cluster StorageClass |
| `storageClass.pool` | `nvme-replicated` | Default StoragePool reference |
| `storageClass.dataProtection` | `replication` | Default data protection mode |
| `storageClass.reclaimPolicy` | `Delete` | PV reclaim policy |
| `storageClass.volumeBindingMode` | `WaitForFirstConsumer` | Volume binding mode |

When `storageClasses.enabled` is `true`, the chart creates three StorageClasses:

| Name | Protection | Reclaim Policy |
|---|---|---|
| `novastor-block-replicated` | Replication (factor=3) | Delete |
| `novastor-block-erasure` | Erasure coding (4+2) | Delete |
| `novastor-block-replicated-retain` | Replication (factor=3) | Retain |

## ServiceAccount and RBAC

| Parameter | Default | Description |
|---|---|---|
| `serviceAccount.create` | `true` | Create a ServiceAccount |
| `serviceAccount.name` | `""` | ServiceAccount name (auto-generated if empty) |
| `serviceAccount.annotations` | `{}` | Annotations for the ServiceAccount |
| `rbac.create` | `true` | Create ClusterRole and ClusterRoleBinding |

## Monitoring

| Parameter | Default | Description |
|---|---|---|
| `monitoring.serviceMonitor.enabled` | `false` | Create ServiceMonitor resources for Prometheus Operator |
| `monitoring.serviceMonitor.labels` | `{}` | Additional labels for ServiceMonitor (for Prometheus selector matching) |
| `monitoring.serviceMonitor.interval` | `30s` | Prometheus scrape interval |
| `monitoring.serviceMonitor.scrapeTimeout` | `10s` | Prometheus scrape timeout |

## Common Configurations

### Development / Single-Node

```yaml
meta:
  replicas: 1
  storage: 1Gi

controller:
  replicas: 1

s3gw:
  replicas: 1
  accessKey: "devAccessKey"
  secretKey: "devSecretKey"
```

### Production (3-Node Minimum)

```yaml
global:
  image:
    tag: "0.1.0"

meta:
  replicas: 3
  storage: 50Gi
  resources:
    requests:
      cpu: "1"
      memory: 2Gi
    limits:
      cpu: "2"
      memory: 4Gi
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app.kubernetes.io/component: meta
          topologyKey: kubernetes.io/hostname

controller:
  leaderElect: true

agent:
  resources:
    requests:
      cpu: "1"
      memory: 1Gi
    limits:
      cpu: "4"
      memory: 4Gi

monitoring:
  serviceMonitor:
    enabled: true
    interval: 15s
```

### Production (5-Node HA)

```yaml
meta:
  replicas: 5
  storage: 100Gi
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              app.kubernetes.io/component: meta
          topologyKey: kubernetes.io/hostname

s3gw:
  replicas: 3
  accessKey: "" # Set via Kubernetes Secret
  secretKey: "" # Set via Kubernetes Secret
```
