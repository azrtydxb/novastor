#!/bin/bash
# Validation script for NovaStor controller deployment
# This script validates that the controller is properly deployed and functional

set -e

TEST_NAMESPACE="${TEST_NAMESPACE:-novastor-test}"
CONTROLLER_NAMESPACE="${CONTROLLER_NAMESPACE:-novastor-system}"

echo "=================================="
echo "NovaStor Controller Validation"
echo "=================================="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Helper functions
pass() {
    echo -e "${GREEN}✓${NC} $1"
}

fail() {
    echo -e "${RED}✗${NC} $1"
    exit 1
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# 1. Check CRDs are installed
echo "1. Verifying CRDs are installed..."
CRDS=("storagepools.novastor.io" "blockvolumes.novastor.io" "sharedfilesystems.novastor.io" "objectstores.novastor.io")
for crd in "${CRDS[@]}"; do
    if kubectl get crd "$crd" >/dev/null 2>&1; then
        pass "CRD $crd is installed"
    else
        fail "CRD $crd is NOT installed"
    fi
done
echo

# 2. Check controller deployment
echo "2. Verifying controller deployment..."
if kubectl get deployment -n "$CONTROLLER_NAMESPACE" novastor-controller >/dev/null 2>&1; then
    pass "Controller deployment exists"
    REPLICAS=$(kubectl get deployment -n "$CONTROLLER_NAMESPACE" novastor-controller -o jsonpath='{.spec.replicas}')
    READY_REPLICAS=$(kubectl get deployment -n "$CONTROLLER_NAMESPACE" novastor-controller -o jsonpath='{.status.readyReplicas}')
    echo "   Replicas: $READY_REPLICAS/$REPLICAS"
else
    fail "Controller deployment does NOT exist"
fi

# Check controller pods are running
CONTROLLER_PODS=$(kubectl get pods -n "$CONTROLLER_NAMESPACE" -l app.kubernetes.io/component=controller -o jsonpath='{.items[*].metadata.name}')
if [ -n "$CONTROLLER_PODS" ]; then
    pass "Controller pods are running"
    for pod in $CONTROLLER_PODS; do
        POD_STATUS=$(kubectl get pod -n "$CONTROLLER_NAMESPACE" "$pod" -o jsonpath='{.status.phase}')
        echo "   - $pod: $POD_STATUS"
    done
else
    fail "No controller pods found"
fi
echo

# 3. Check leader election
echo "3. Verifying leader election..."
LEASE=$(kubectl get lease -n "$CONTROLLER_NAMESPACE" novastor-controller-leader-election -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [ -n "$LEASE" ]; then
    pass "Leader election is active"
    echo "   Leader: $LEASE"
else
    warn "Leader election lease not found (may not be enabled)"
fi
echo

# 4. Create test namespace
echo "4. Creating test namespace..."
if kubectl get namespace "$TEST_NAMESPACE" >/dev/null 2>&1; then
    echo "   Test namespace already exists"
else
    kubectl create namespace "$TEST_NAMESPACE" >/dev/null 2>&1
    pass "Created test namespace: $TEST_NAMESPACE"
fi
echo

# 5. Test StoragePool
echo "5. Testing StoragePool reconciliation..."
cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: novastor.io/v1alpha1
kind: StoragePool
metadata:
  name: validation-pool
spec:
  nodeSelector:
    matchLabels:
      novastor.io/storage-tier: nvme
  dataProtection:
    mode: replication
    replication:
      factor: 3
      writeQuorum: 2
EOF

# Wait for pool to be ready
for i in {1..30}; do
    PHASE=$(kubectl get storagepool validation-pool -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$PHASE" = "Ready" ]; then
        NODE_COUNT=$(kubectl get storagepool validation-pool -o jsonpath='{.status.nodeCount}')
        pass "StoragePool is ready with $NODE_COUNT nodes"
        break
    fi
    if [ $i -eq 30 ]; then
        fail "StoragePool did not become ready (phase: $PHASE)"
    fi
    sleep 2
done
echo

# 6. Test BlockVolume
echo "6. Testing BlockVolume reconciliation..."
cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: novastor.io/v1alpha1
kind: BlockVolume
metadata:
  name: validation-blockvolume
  namespace: $TEST_NAMESPACE
spec:
  pool: validation-pool
  size: "10Gi"
  accessMode: ReadWriteOnce
EOF

for i in {1..30}; do
    PHASE=$(kubectl get blockvolume validation-blockvolume -n "$TEST_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$PHASE" = "Bound" ]; then
        pass "BlockVolume is bound"
        PV_NAME="novastor-$TEST_NAMESPACE-validation-blockvolume"
        if kubectl get pv "$PV_NAME" >/dev/null 2>&1; then
            pass "PersistentVolume $PV_NAME created"
        else
            fail "PersistentVolume $PV_NAME NOT created"
        fi
        break
    fi
    if [ $i -eq 30 ]; then
        fail "BlockVolume did not become bound (phase: $PHASE)"
    fi
    sleep 2
done
echo

# 7. Test SharedFilesystem
echo "7. Testing SharedFilesystem reconciliation..."
cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: novastor.io/v1alpha1
kind: SharedFilesystem
metadata:
  name: validation-filesystem
  namespace: $TEST_NAMESPACE
spec:
  pool: validation-pool
  capacity: "50Gi"
  accessMode: ReadWriteMany
  export:
    protocol: nfs
EOF

for i in {1..30}; do
    PHASE=$(kubectl get sharedfilesystem validation-filesystem -n "$TEST_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$PHASE" = "Ready" ]; then
        ENDPOINT=$(kubectl get sharedfilesystem validation-filesystem -n "$TEST_NAMESPACE" -o jsonpath='{.status.endpoint}')
        pass "SharedFilesystem is ready (endpoint: $ENDPOINT)"

        # Check deployment and service
        DEPLOY_NAME="novastor-nfs-validation-filesystem"
        SVC_NAME="novastor-nfs-validation-filesystem"

        if kubectl get deployment -n "$TEST_NAMESPACE" "$DEPLOY_NAME" >/dev/null 2>&1; then
            pass "NFS Deployment $DEPLOY_NAME created"
        else
            fail "NFS Deployment $DEPLOY_NAME NOT created"
        fi

        if kubectl get svc -n "$TEST_NAMESPACE" "$SVC_NAME" >/dev/null 2>&1; then
            pass "NFS Service $SVC_NAME created"
        else
            fail "NFS Service $SVC_NAME NOT created"
        fi
        break
    fi
    if [ $i -eq 30 ]; then
        fail "SharedFilesystem did not become ready (phase: $PHASE)"
    fi
    sleep 2
done
echo

# 8. Test ObjectStore
echo "8. Testing ObjectStore reconciliation..."
cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: novastor.io/v1alpha1
kind: ObjectStore
metadata:
  name: validation-objectstore
  namespace: $TEST_NAMESPACE
spec:
  pool: validation-pool
  endpoint:
    service:
      port: 9000
  bucketPolicy:
    maxBuckets: 10
    versioning: disabled
EOF

for i in {1..30}; do
    PHASE=$(kubectl get objectstore validation-objectstore -n "$TEST_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$PHASE" = "Ready" ]; then
        ENDPOINT=$(kubectl get objectstore validation-objectstore -n "$TEST_NAMESPACE" -o jsonpath='{.status.endpoint}')
        pass "ObjectStore is ready (endpoint: $ENDPOINT)"

        # Check secret, deployment and service
        SECRET_NAME="novastor-s3-validation-objectstore-keys"
        DEPLOY_NAME="novastor-s3-validation-objectstore"
        SVC_NAME="novastor-s3-validation-objectstore"

        if kubectl get secret -n "$TEST_NAMESPACE" "$SECRET_NAME" >/dev/null 2>&1; then
            pass "S3 Secret $SECRET_NAME created"
        else
            fail "S3 Secret $SECRET_NAME NOT created"
        fi

        if kubectl get deployment -n "$TEST_NAMESPACE" "$DEPLOY_NAME" >/dev/null 2>&1; then
            pass "S3 Deployment $DEPLOY_NAME created"
        else
            fail "S3 Deployment $DEPLOY_NAME NOT created"
        fi

        if kubectl get svc -n "$TEST_NAMESPACE" "$SVC_NAME" >/dev/null 2>&1; then
            pass "S3 Service $SVC_NAME created"
        else
            fail "S3 Service $SVC_NAME NOT created"
        fi
        break
    fi
    if [ $i -eq 30 ]; then
        fail "ObjectStore did not become ready (phase: $PHASE)"
    fi
    sleep 2
done
echo

# 9. Test leader election with multiple replicas
echo "9. Testing leader election with HA..."
ORIGINAL_REPLICAS=$(kubectl get deployment -n "$CONTROLLER_NAMESPACE" novastor-controller -o jsonpath='{.spec.replicas}')
kubectl scale deployment -n "$CONTROLLER_NAMESPACE" novastor-controller --replicas=2 >/dev/null 2>&1
sleep 10

POD_COUNT=$(kubectl get pods -n "$CONTROLLER_NAMESPACE" -l app.kubernetes.io/component=controller --no-headers | wc -l | tr -d ' ')
if [ "$POD_COUNT" -eq 2 ]; then
    pass "Scaled to 2 controller pods"
else
    warn "Expected 2 pods, got $POD_COUNT"
fi

# Check only one leader
LEASE_HOLDER=$(kubectl get lease -n "$CONTROLLER_NAMESPACE" novastor-controller-leader-election -o jsonpath='{.spec.holderIdentity}' 2>/dev/null || echo "")
if [ -n "$LEASE_HOLDER" ]; then
    pass "Leader election working: single leader ($LEASE_HOLDER)"
else
    warn "Could not verify leader election"
fi

# Scale back
kubectl scale deployment -n "$CONTROLLER_NAMESPACE" novastor-controller --replicas="$ORIGINAL_REPLICAS" >/dev/null 2>&1
echo

# 10. Cleanup
echo "10. Cleaning up test resources..."
kubectl delete blockvolume validation-blockvolume -n "$TEST_NAMESPACE" >/dev/null 2>&1
kubectl delete sharedfilesystem validation-filesystem -n "$TEST_NAMESPACE" >/dev/null 2>&1
kubectl delete objectstore validation-objectstore -n "$TEST_NAMESPACE" >/dev/null 2>&1
kubectl delete storagepool validation-pool >/dev/null 2>&1
pass "Test resources cleaned up"
echo

echo "=================================="
echo -e "${GREEN}All validations passed!${NC}"
echo "=================================="
