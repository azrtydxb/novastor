# File Gateway (NFS) and S3 Gateway Deployment Guide

This guide covers deploying and validating the NovaStor File Gateway (NFS) and S3 Gateway in a Kubernetes cluster.

## Overview

NovaStor provides three access layers to the same underlying chunk storage:

| Gateway | Protocol | Access Pattern | Use Case |
|---------|----------|----------------|----------|
| **CSI Driver** | Block (NVMe-oF) | ReadWriteOnce | Databases, stateful workloads |
| **File Gateway** | NFS v3 | ReadWriteMany | Shared file systems, CI/CD artifacts |
| **S3 Gateway** | S3-compatible HTTP API | Object storage | ML datasets, backups, archival |

## Prerequisites

- NovaStor deployed via Helm (see [Quick Start](quickstart.md))
- kubectl configured for your cluster
- AWS CLI v2 (for S3 testing)
- A test pod with NFS client tools

## Deploying the Gateways

The filer and s3gw are included in the NovaStor Helm chart. Enable them in your values file:

```yaml
# values.yaml
filer:
  enabled: true
  replicas: 2

s3gw:
  enabled: true
  replicas: 2
  accessKey: "your-access-key"
  secretKey: "your-secret-key"
```

Install or upgrade:

```bash
helm install novastor deploy/helm/novastor \
  --namespace novastor-system \
  --create-namespace \
  --values values.yaml

# Or upgrade existing deployment
helm upgrade novastor deploy/helm/novastor \
  --namespace novastor-system \
  --values values.yaml
```

### Verify Deployment

```bash
# Check pods are running
kubectl -n novastor-system get pods -l app.kubernetes.io/component=filer
kubectl -n novastor-system get pods -l app.kubernetes.io/component=s3gw

# Expected output:
# NAME                            READY   STATUS    RESTARTS   AGE
# novastor-filer-xxxxxxxxx-xxxxx   1/1     Running   0          30s
# novastor-filer-yyyyyyy-yyyyyy   1/1     Running   0          30s
# novastor-s3gw-xxxxxxxxx-xxxxx    1/1     Running   0          30s
# novastor-s3gw-yyyyyyy-yyyyyy    1/1     Running   0          30s

# Check services
kubectl -n novastor-system get svc -l app.kubernetes.io/institute=novastor

# Expected services:
# novastor-filer     ClusterIP   2049/TCP,8080/TCP   2m
# novastor-s3gw      ClusterIP   9000/TCP,8081/TCP   2m
```

## Testing the File Gateway (NFS)

### Step 1: Create a SharedFilesystem Resource

Create a `SharedFilesystem` CRD to provision an NFS-exported filesystem:

```yaml
# api/v1alpha1/sharedfilesystem.yaml
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem
metadata:
  name: my-shared-fs
  namespace: default
spec:
  pool: nvme-replicated
  size: "100Gi"
  accessMode: ReadWriteMany
```

```bash
kubectl apply -f sharedfilesystem.yaml
kubectl get sharedfilesystem my-shared-fs
```

### Step 2: Create a PVC

```yaml
# pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-nfs-pvc
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 100Gi
  storageClassName: novastor-nfs
```

```bash
kubectl apply -f pvc.yaml
kubectl get pvc my-nfs-pvc
```

### Step 3: Mount from Multiple Pods

Create two pods that simultaneously access the same NFS volume:

```yaml
# pod1.yaml
apiVersion: v1
kind: Pod
metadata:
  name: nfs-client-1
spec:
  containers:
    - name: writer
      image: busybox
      command: ["/bin/sh", "-c"]
      args:
        - |
          while true; do
            echo "$(date): Pod 1 writing..." >> /data/shared.txt
            sleep 5
          done
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-nfs-pvc
---
# pod2.yaml
apiVersion: v1
kind: Pod
metadata:
  name: nfs-client-2
spec:
  containers:
    - name: reader
      image: busybox
      command: ["/bin/sh", "-c"]
      args:
        - |
          while true; do
            echo "$(date): Pod 2 reading:"
            cat /data/shared.txt
            sleep 5
          done
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: my-nfs-pvc
```

```bash
kubectl apply -f pod1.yaml
kubectl apply -f pod2.yaml

# Verify both pods are running
kubectl get pods nfs-client-1 nfs-client-2

# Check logs from pod 2 (should see writes from pod 1)
kubectl logs nfs-client-2 --tail=20
```

### Expected Output

```
Wed Feb 18 12:00:00 UTC 2026: Pod 1 writing...
Wed Feb 18 12:00:05 UTC 2026: Pod 1 writing...
Wed Feb 18 12:00:10 UTC 2026: Pod 1 writing...
```

This confirms ReadWriteMany access is working correctly.

## Testing the S3 Gateway

### Step 1: Port-forward to S3 Gateway

```bash
kubectl -n novastor-system port-forward svc/novastor-s3gw 9000:9000 &
```

### Step 2: Configure AWS CLI

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1

# Create endpoint configuration
cat > ~/.aws/config << EOF
[profile novastor]
region = us-east-1
output = json
endpoint_url = http://localhost:9000
EOF
```

### Step 3: Test Basic Operations

```bash
# Create a bucket
aws --profile novastor --endpoint-url http://localhost:9000 s3 mb s3://test-bucket

# Upload a file
echo "Hello, NovaStor S3 Gateway!" > test.txt
aws --profile novastor --endpoint-url http://localhost:9000 s3 cp test.txt s3://test-bucket/test.txt

# Download the file
aws --profile novastor --endpoint-url http://localhost:9000 s3 cp s3://test-bucket/test.txt downloaded.txt

# Verify content
cat downloaded.txt
# Output: Hello, NovaStor S3 Gateway!

# List objects
aws --profile novastor --endpoint-url http://localhost:9000 s3 ls s3://test-bucket

# Delete the file
aws --profile novastor --endpoint-url http://localhost:9000 s3 rm s3://test-bucket/test.txt

# Delete the bucket
aws --profile novastor --endpoint-url http://localhost:9000 s3 rb s3://test-bucket
```

### Step 4: Test Multipart Upload

Multipart upload is required for objects larger than 5MB:

```bash
# Create a large test file (10MB)
dd if=/dev/urandom of=largefile.bin bs=1M count=10

# Create multipart upload
UPLOAD_ID=$(aws --profile novastor --endpoint-url http://localhost:9000 s3api create-multipart-upload \
  --bucket test-bucket \
  --key largefile.bin \
  --query UploadId \
  --output text)

# Upload part 1
ETAG1=$(aws --profile novastor --endpoint-url http://localhost:9000 s3api upload-part \
  --bucket test-bucket \
  --key largefile.bin \
  --part-number 1 \
  --upload-id "$UPLOAD_ID" \
  --body largefile.bin \
  --query ETag \
  --output text)

# Complete the multipart upload
aws --profile novastor --endpoint-url http://localhost:9000 s3api complete-multipart-upload \
  --bucket test-bucket \
  --key largefile.bin \
  --upload-id "$UPLOAD_ID" \
  --multipart-upload 'Parts=[{PartNumber:1,ETag:'"$ETAG1"'}]'

# Verify the object
aws --profile novastor --endpoint-url http://localhost:9000 s3api head-object \
  --bucket test-bucket \
  --key largefile.bin
```

### Step 5: Test SigV4 Authentication

```bash
# Verify that SigV4 signature verification works
aws --profile novastor --endpoint-url http://localhost:9000 s3api get-object \
  --bucket test-bucket \
  --key largefile.bin \
  /dev/null \
  --debug 2>&1 | grep -i "signature"

# Should show signature version 4 being used
```

## Verifying Prometheus Metrics

Both gateways expose Prometheus metrics on separate ports.

### Check Metrics Endpoints

```bash
# Filer metrics (port 8080)
kubectl -n novastor-system exec -t novastor-filer-xxxxxxxxx-xxxxx -- wget -qO- http://localhost:8080/metrics

# S3 Gateway metrics (port 8081)
kubectl -n novastor-system exec -t novastor-s3gw-xxxxxxxxx-xxxxx -- wget -qO- http://localhost:8081/metrics
```

### Key Metrics

#### Filer (NFS) Metrics

```
novastor_filer_nfs_ops_total{operation="FSINFO"} 42
novastor_filer_nfs_ops_total{operation="CREATE"} 128
novastor_filer_nfs_ops_total{operation="READ"} 1024
novastor_filer_nfs_ops_total{operation="WRITE"} 512
novastor_filer_nfs_op_duration_seconds_bucket{operation="READ",le="0.001"} 800
novastor_filer_nfs_op_duration_seconds_bucket{operation="READ",le="0.01"} 1010
novastor_filer_active_locks 3
```

#### S3 Gateway Metrics

```
novastor_s3_requests_total{operation="PutObject"} 256
novastor_s3_requests_total{operation="GetObject"} 512
novastor_s3_requests_total{operation="ListObjectsV2"} 64
novastor_s3_request_duration_seconds_bucket{operation="PutObject",le="0.1"} 200
novastor_s3_request_duration_seconds_bucket{operation="PutObject",le="1.0"} 255
novastor_s3_bytes_in_total 104857600
novastor_s3_bytes_out_total 52428800
```

### Configure Prometheus Scrape

If Prometheus Operator is installed, enable ServiceMonitor:

```yaml
# values.yaml
monitoring:
  serviceMonitor:
    enabled: true
    labels:
      release: prometheus
```

```bash
helm upgrade novastor deploy/helm/novastor \
  --namespace novastor-system \
  --values values.yaml
```

Verify ServiceMonitor resources:

```bash
kubectl get servicemonitor -n novastor-system
# NAME                     AGE
# novastor-agent           5m
# novastor-controller       5m
# novastor-filer           5m
# novastor-meta            5m
# novastor-s3gw            5m
```

## Health Checks

Both gateways expose HTTP health check endpoints:

```bash
# Filer health
kubectl -n novastor-system exec -t novastor-filer-xxxxxxxxx-xxxxx -- wget -qO- http://localhost:8080/healthz

# S3 Gateway health (via S3 API)
kubectl -n novastor-system exec -t novastor-s3gw-xxxxxxxxx-xxxxx -- wget -qO- http://localhost:9000/healthz
```

Both should return `ok`.

## Troubleshooting

### Filer Issues

**Problem: NFS mount hangs**

```bash
# Check filer logs for errors
kubectl -n novastor-system logs novastor-filer-xxxxxxxxx-xxxxx

# Verify filer can connect to metadata service
kubectl -n novastor-system exec -t novastor-filer-xxxxxxxxx-xxxxx -- nc -zv novastor-meta 7000
```

**Problem: File lock conflicts**

```bash
# Check active locks metric
kubectl -n novastor-system exec -t novastor-filer-xxxxxxxxx-xxxxx -- wget -qO- http://localhost:8080/metrics | grep active_locks
```

### S3 Gateway Issues

**Problem: Authentication failures**

```bash
# Verify credentials are set
kubectl -n novastor-system get secret novastor-s3gw -o yaml

# Check access key format
kubectl -n novastor-system get secret novastor-s3gw -o jsonpath='{.data.access-key}' | base64 -d
```

**Problem: Large object upload fails**

```bash
# Check multipart upload state
kubectl -n novastor-system logs novastor-s3gw-xxxxxxxxx-xxxxx | grep -i multipart
```

**Problem: Signature verification fails**

Ensure your AWS CLI is configured correctly:

```bash
aws --profile novastor --endpoint-url http://localhost:9000 s3api list-objects --bucket test-bucket --debug 2>&1 | grep -i signature
```

## Next Steps

- **[S3 API Reference](../api/s3.md)** -- Full S3 API documentation
- **[Monitoring Guide](../operations/monitoring.md)** -- Set up Grafana dashboards
- **[Architecture Overview](../architecture/overview.md)** -- NovaStor architecture details
