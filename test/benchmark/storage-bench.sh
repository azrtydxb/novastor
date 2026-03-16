#!/usr/bin/env bash
#
# NovaStor Storage Benchmark
# ==========================
# Reproducible I/O and speed test across all protection modes.
# Creates volumes, runs sequential/random read/write benchmarks,
# and produces a summary table.
#
# Usage:
#   ./test/benchmark/storage-bench.sh [--size 64] [--skip-create] [--cleanup]
#
# Options:
#   --size N       Write size in MB (default: 64)
#   --skip-create  Reuse existing volumes (skip PVC/pod creation)
#   --cleanup      Delete all benchmark volumes and pods after run
#
# Prerequisites:
#   - KUBECONFIG set or kubectl configured
#   - StorageClasses: novastor-rep1, novastor-rep3, novastor-rep5,
#     novastor-ec3, novastor-ec5, novastor-ec7
#   - Cluster healthy with all dataplanes and agents running
#
# Output:
#   Results saved to test/benchmark/results-YYYY-MM-DD-HHMMSS.txt

set -euo pipefail

# --- Configuration ---
WRITE_SIZE_MB="${BENCH_SIZE_MB:-64}"
TIMEOUT_SEC=600
NAMESPACE="default"
RESULTS_DIR="$(dirname "$0")"
TIMESTAMP="$(date +%Y-%m-%d-%H%M%S)"
RESULTS_FILE="${RESULTS_DIR}/results-${TIMESTAMP}.txt"
SKIP_CREATE=false
DO_CLEANUP=false

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --size) WRITE_SIZE_MB="$2"; shift 2 ;;
    --skip-create) SKIP_CREATE=true; shift ;;
    --cleanup) DO_CLEANUP=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# --- Storage classes to test ---
declare -a CLASSES=(
  "novastor-rep1:Replication 1x"
  "novastor-rep3:Replication 3x"
  "novastor-rep5:Replication 5x"
  "novastor-ec3:EC 2+1 (3 nodes)"
  "novastor-ec5:EC 3+2 (5 nodes)"
  "novastor-ec7:EC 4+3 (7 nodes)"
)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULTS_FILE"; }

# --- Header ---
{
  echo "============================================================"
  echo "NovaStor Storage Benchmark"
  echo "============================================================"
  echo "Date:       $(date)"
  echo "Write size: ${WRITE_SIZE_MB} MB"
  echo "Cluster:    $(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ') nodes"
  echo "------------------------------------------------------------"
} | tee "$RESULTS_FILE"

# --- Create volumes if needed ---
if [ "$SKIP_CREATE" = false ]; then
  log "Creating benchmark volumes..."
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    NAME="bench-${SC#novastor-}"
    POD="bench-pod-${SC#novastor-}"

    # Delete if exists
    kubectl delete pod "$POD" --force --grace-period=0 2>/dev/null || true
    kubectl delete pvc "$NAME" 2>/dev/null || true
    sleep 2

    kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $NAME
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: $SC
  resources:
    requests:
      storage: 2Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app: novastor-bench
spec:
  containers:
  - name: bench
    image: alpine:3.20
    command: ["sh", "-c", "apk add --no-cache fio coreutils > /dev/null 2>&1 && sleep 86400"]
    volumeMounts:
    - name: vol
      mountPath: /data
  volumes:
  - name: vol
    persistentVolumeClaim:
      claimName: $NAME
YAML
    log "  Created $NAME + $POD"
    sleep 5
  done

  # Wait for all pods
  log "Waiting for benchmark pods to be ready..."
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    POD="bench-pod-${SC#novastor-}"
    for i in $(seq 1 "$((TIMEOUT_SEC / 10))"); do
      STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
      if [ "$STATUS" = "Running" ]; then
        # Wait for fio to install
        kubectl exec "$POD" -- which fio >/dev/null 2>&1 && break
        sleep 5
        continue
      fi
      sleep 10
    done
    FINAL=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
    PATHS=$(kubectl get pv "$(kubectl get pvc "bench-${SC#novastor-}" -o jsonpath='{.spec.volumeName}' 2>/dev/null)" -o jsonpath='{.spec.csi.volumeAttributes.targetAddresses}' 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?")
    log "  $POD: $FINAL (${PATHS} paths)"
  done
fi

# --- Run benchmarks ---
run_fio() {
  local POD="$1" NAME="$2" RW="$3" BS="$4" SIZE="$5" EXTRA="${6:-}"
  kubectl exec "$POD" -- fio \
    --name="$NAME" \
    --filename=/data/fio-test \
    --rw="$RW" \
    --bs="$BS" \
    --size="$SIZE" \
    --ioengine=libaio \
    --iodepth=4 \
    --direct=1 \
    --numjobs=1 \
    --runtime=30 \
    --time_based \
    --group_reporting \
    --output-format=json \
    $EXTRA 2>/dev/null
}

extract_bw() {
  # Extract bandwidth in MB/s from fio JSON
  python3 -c "
import json, sys
d = json.load(sys.stdin)
j = d['jobs'][0]
read_bw = j['read']['bw'] / 1024  # KB/s -> MB/s
write_bw = j['write']['bw'] / 1024
iops_r = j['read']['iops']
iops_w = j['write']['iops']
lat_r = j['read']['lat_ns']['mean'] / 1e6 if j['read']['lat_ns']['mean'] > 0 else 0
lat_w = j['write']['lat_ns']['mean'] / 1e6 if j['write']['lat_ns']['mean'] > 0 else 0
if read_bw > 0:
    print(f'{read_bw:.1f} MB/s | {iops_r:.0f} IOPS | {lat_r:.2f} ms')
else:
    print(f'{write_bw:.1f} MB/s | {iops_w:.0f} IOPS | {lat_w:.2f} ms')
" 2>/dev/null || echo "parse error"
}

{
  echo ""
  echo "============================================================"
  echo "BENCHMARK RESULTS"
  echo "============================================================"
  printf "%-20s | %-8s | %-30s | %-30s | %-30s | %-30s\n" \
    "Volume" "Paths" "Seq Write (${WRITE_SIZE_MB}M)" "Seq Read (${WRITE_SIZE_MB}M)" "Rand Write 4K" "Rand Read 4K"
  printf "%-20s-+-%-8s-+-%-30s-+-%-30s-+-%-30s-+-%-30s\n" \
    "--------------------" "--------" "------------------------------" "------------------------------" "------------------------------" "------------------------------"
} | tee -a "$RESULTS_FILE"

for entry in "${CLASSES[@]}"; do
  SC="${entry%%:*}"
  DESC="${entry#*:}"
  POD="bench-pod-${SC#novastor-}"
  PVC_NAME="bench-${SC#novastor-}"

  # Check pod is running
  STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  if [ "$STATUS" != "Running" ]; then
    printf "%-20s | %-8s | %-30s | %-30s | %-30s | %-30s\n" \
      "$DESC" "?" "SKIPPED (pod not running)" "" "" "" | tee -a "$RESULTS_FILE"
    continue
  fi

  PATHS=$(kubectl get pv "$(kubectl get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}' 2>/dev/null)" -o jsonpath='{.spec.csi.volumeAttributes.targetAddresses}' 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?")

  # Clean test file
  kubectl exec "$POD" -- rm -f /data/fio-test 2>/dev/null || true

  # Sequential write
  SEQ_W=$(run_fio "$POD" seq-write write 1M "${WRITE_SIZE_MB}M" | extract_bw)

  # Sequential read
  SEQ_R=$(run_fio "$POD" seq-read read 1M "${WRITE_SIZE_MB}M" | extract_bw)

  # Random write 4K
  RND_W=$(run_fio "$POD" rand-write randwrite 4k "${WRITE_SIZE_MB}M" | extract_bw)

  # Random read 4K
  RND_R=$(run_fio "$POD" rand-read randread 4k "${WRITE_SIZE_MB}M" | extract_bw)

  printf "%-20s | %-8s | %-30s | %-30s | %-30s | %-30s\n" \
    "$DESC" "$PATHS" "$SEQ_W" "$SEQ_R" "$RND_W" "$RND_R" | tee -a "$RESULTS_FILE"
done

# --- Data integrity test ---
{
  echo ""
  echo "============================================================"
  echo "DATA INTEGRITY TEST"
  echo "============================================================"
} | tee -a "$RESULTS_FILE"

INTEGRITY_PASS=0
INTEGRITY_FAIL=0
for entry in "${CLASSES[@]}"; do
  SC="${entry%%:*}"
  DESC="${entry#*:}"
  POD="bench-pod-${SC#novastor-}"

  STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Unknown")
  if [ "$STATUS" != "Running" ]; then
    continue
  fi

  # Write known pattern, read back, compare
  RESULT=$(kubectl exec "$POD" -- sh -c '
    dd if=/dev/urandom of=/data/integrity-test bs=1M count=8 2>/dev/null
    WRITE_MD5=$(md5sum /data/integrity-test | awk "{print \$1}")
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    READ_MD5=$(md5sum /data/integrity-test | awk "{print \$1}")
    if [ "$WRITE_MD5" = "$READ_MD5" ]; then
      echo "PASS $WRITE_MD5"
    else
      echo "FAIL write=$WRITE_MD5 read=$READ_MD5"
    fi
    rm -f /data/integrity-test
  ' 2>/dev/null || echo "ERROR")

  if echo "$RESULT" | grep -q "^PASS"; then
    MD5=$(echo "$RESULT" | awk '{print $2}')
    log "  $DESC: PASS (md5=$MD5)"
    INTEGRITY_PASS=$((INTEGRITY_PASS + 1))
  else
    log "  $DESC: FAIL ($RESULT)"
    INTEGRITY_FAIL=$((INTEGRITY_FAIL + 1))
  fi
done

{
  echo ""
  echo "Integrity: ${INTEGRITY_PASS} passed, ${INTEGRITY_FAIL} failed"
  echo ""
  echo "============================================================"
  echo "Results saved to: $RESULTS_FILE"
  echo "============================================================"
} | tee -a "$RESULTS_FILE"

# --- Cleanup ---
if [ "$DO_CLEANUP" = true ]; then
  log "Cleaning up benchmark volumes..."
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    POD="bench-pod-${SC#novastor-}"
    PVC="bench-${SC#novastor-}"
    kubectl delete pod "$POD" --force --grace-period=0 2>/dev/null || true
    kubectl delete pvc "$PVC" 2>/dev/null || true
  done
  log "Cleanup complete."
fi
