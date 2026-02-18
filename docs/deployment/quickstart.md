# Quick Start Guide

This guide walks you through deploying NovaStor on a Kubernetes cluster and provisioning your first block volume.

## Prerequisites

| Requirement | Minimum Version |
|---|---|
| Kubernetes | 1.28+ |
| Helm | 3.x |
| kubectl | 1.28+ |
| Nodes | 3+ (for data protection quorum) |
| Storage | Raw disks or host directories on each node |

!!! note "Container Registry"
    NovaStor images are hosted at `ghcr.io/piwi3910`. Ensure your cluster can pull images from this registry.

## Step 1: Install NovaStor via Helm

Add the Helm repository and install the chart:

```bash
# Add the Helm repository
helm repo add novastor https://piwi3910.github.io/novastor
helm repo update

# Create a namespace
kubectl create namespace novastor-system

# Install NovaStor
helm install novastor novastor/novastor \
  --namespace novastor-system \
  --set meta.replicas=3 \
  --set monitoring.serviceMonitor.enabled=true
```

Alternatively, install from the local chart directory:

```bash
helm install novastor deploy/helm/novastor \
  --namespace novastor-system \
  --create-namespace
```

### Verify the Installation

```bash
# Check that all pods are running
kubectl -n novastor-system get pods

# Expected output:
# NAME                                    READY   STATUS    RESTARTS   AGE
# novastor-agent-xxxxx                    1/1     Running   0          60s
# novastor-agent-yyyyy                    1/1     Running   0          60s
# novastor-agent-zzzzz                    1/1     Running   0          60s
# novastor-controller-xxxxxxxxx-xxxxx     1/1     Running   0          60s
# novastor-csi-controller-xxxxxxx-xxxxx   5/5     Running   0          60s
# novastor-csi-node-xxxxx                 2/2     Running   0          60s
# novastor-csi-node-yyyyy                 2/2     Running   0          60s
# novastor-csi-node-zzzzz                 2/2     Running   0          60s
# novastor-meta-0                         1/1     Running   0          60s
# novastor-meta-1                         1/1     Running   0          45s
# novastor-meta-2                         1/1     Running   0          30s
```

Check that the CRDs are installed:

```bash
kubectl get crds | grep novastor
# storagepools.novastor.io
# blockvolumes.novastor.io
# sharedfilesystems.novastor.io
# objectstores.novastor.io
```

Check that the StorageClasses are available:

```bash
kubectl get storageclass | grep novastor
# novastor-block-replicated          novastor.csi.novastor.io   Delete   WaitForFirstConsumer   true    60s
# novastor-block-erasure             novastor.csi.novastor.io   Delete   WaitForFirstConsumer   true    60s
# novastor-block-replicated-retain   novastor.csi.novastor.io   Retain   WaitForFirstConsumer   true    60s
```

## Step 2: Create a StoragePool

A StoragePool defines which nodes and devices participate in storage and how data is protected.

```yaml
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: nvme-replicated
spec:
  nodeSelector:
    matchLabels:
      novastor.io/storage-node: "true"
  deviceFilter:
    type: nvme
    minSize: "100Gi"
  dataProtection:
    mode: replication
    replication:
      factor: 3
      writeQuorum: 2
```

Apply the StoragePool:

```bash
# Label your storage nodes
kubectl label node worker-1 novastor.io/storage-node=true
kubectl label node worker-2 novastor.io/storage-node=true
kubectl label node worker-3 novastor.io/storage-node=true

# Create the StoragePool
kubectl apply -f storagepool.yaml

# Verify the pool is ready
kubectl get storagepool nvme-replicated
# NAME               MODE          NODES   CAPACITY   AGE
# nvme-replicated    replication   3       1.5Ti      30s
```

## Step 3: Provision a Block Volume

Create a `BlockVolume` resource that references the StoragePool:

```yaml
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
metadata:
  name: my-database-volume
  namespace: default
spec:
  pool: nvme-replicated
  size: "50Gi"
  accessMode: ReadWriteOnce
```

Apply the BlockVolume:

```bash
kubectl apply -f blockvolume.yaml

# Check the volume status
kubectl get blockvolume my-database-volume
# NAME                  POOL              SIZE   ACCESS          PHASE
# my-database-volume    nvme-replicated   50Gi   ReadWriteOnce   Bound
```

The controller automatically creates a matching PersistentVolume.

## Step 4: Mount and Use

Use the volume in a Pod via a PersistentVolumeClaim:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-database-pvc
spec:
  storageClassName: novastor-block-replicated
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 50Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: my-database
spec:
  containers:
    - name: postgres
      image: postgres:16
      volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
      env:
        - name: POSTGRES_PASSWORD
          value: "changeme"
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-database-pvc
```

Apply and verify:

```bash
kubectl apply -f pod.yaml

# Verify the PVC is bound
kubectl get pvc my-database-pvc
# NAME              STATUS   VOLUME                    CAPACITY   ACCESS MODES   STORAGECLASS
# my-database-pvc   Bound    novastor-default-my-...   50Gi       RWO            novastor-block-replicated

# Verify the pod is running
kubectl get pod my-database
# NAME          READY   STATUS    RESTARTS   AGE
# my-database   1/1     Running   0          30s
```

## Next Steps

- **[Helm Chart Reference](helm-values.md)** -- Customize your deployment
- **[Monitoring Guide](../operations/monitoring.md)** -- Set up Prometheus and Grafana
- **[CRD Reference](../api/crds.md)** -- Full specification for all custom resources
- **[S3 API](../api/s3.md)** -- Use the S3-compatible object storage API
- **[Recovery Guide](../operations/recovery.md)** -- Understand automatic failure recovery
