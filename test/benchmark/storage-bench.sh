#!/usr/bin/env bash
#
# NovaStor Storage Benchmark
# ==========================
# Runs fio benchmarks against an existing PVC/pod.
# Writes first, then reads. Strictly serial — no concurrent I/O.
#
# Usage:
#   ./test/benchmark/storage-bench.sh <pod-name> [--size 64] [--label "my test"]
#
# Examples:
#   ./test/benchmark/storage-bench.sh pod-rep3
#   ./test/benchmark/storage-bench.sh pod-rep3 --size 128
#   ./test/benchmark/storage-bench.sh pod-rep3 --size 64 --label "baseline rep3"
#   ./test/benchmark/storage-bench.sh pod-ec5 --label "after reactor fix"
#
# Prerequisites:
#   - Pod must be Running with fio installed (alpine + apk add fio)
#   - Volume mounted at /data
#
# Output: test/benchmark/results-YYYY-MM-DD-HHMMSS.txt

set -euo pipefail

if [ $# -lt 1 ] || [[ "$1" == --* ]]; then
  echo "Usage: $0 <pod-name> [--size MB] [--label TEXT]"
  echo ""
  echo "Examples:"
  echo "  $0 pod-rep3"
  echo "  $0 pod-rep3 --size 128 --label 'baseline rep3'"
  exit 1
fi

POD="$1"; shift
WRITE_SIZE_MB=64
LABEL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size) WRITE_SIZE_MB="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

RESULTS_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMESTAMP="$(date +%Y-%m-%d-%H%M%S)"
RESULTS_FILE="${RESULTS_DIR}/results-${POD}-${TIMESTAMP}.txt"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULTS_FILE"; }

# --- Verify pod ---
STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "NotFound")
if [ "$STATUS" != "Running" ]; then
  echo "ERROR: Pod '$POD' is not Running (status: $STATUS)"
  exit 1
fi

# Check fio
if ! kubectl exec "$POD" -- which fio >/dev/null 2>&1; then
  echo "ERROR: fio not found in pod '$POD'. Install with: kubectl exec $POD -- apk add --no-cache fio"
  exit 1
fi

# Get PVC/PV info
PVC=$(kubectl get pod "$POD" -o jsonpath='{.spec.volumes[0].persistentVolumeClaim.claimName}' 2>/dev/null || echo "?")
NODE=$(kubectl get pod "$POD" -o jsonpath='{.spec.nodeName}' 2>/dev/null || echo "?")
SC="?"
PATHS="?"
if [ "$PVC" != "?" ]; then
  SC=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.storageClassName}' 2>/dev/null || echo "?")
  PV=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}' 2>/dev/null || echo "")
  if [ -n "$PV" ]; then
    PATHS=$(kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeAttributes.targetAddresses}' 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?")
  fi
fi

# --- Header ---
{
  echo "============================================================"
  echo "NovaStor Storage Benchmark"
  echo "============================================================"
  echo "Date:           $(date)"
  echo "Pod:            $POD"
  echo "PVC:            $PVC"
  echo "StorageClass:   $SC"
  echo "Node:           $NODE"
  echo "NVMe-oF Paths:  $PATHS"
  echo "Write size:     ${WRITE_SIZE_MB} MB"
  if [ -n "$LABEL" ]; then
    echo "Label:          $LABEL"
  fi
  echo "------------------------------------------------------------"
} | tee "$RESULTS_FILE"

run_fio() {
  local NAME="$1" RW="$2" BS="$3" SIZE="$4"
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
    --output-format=json 2>/dev/null
}

parse_fio() {
  python3 -c "
import json, sys
d = json.load(sys.stdin)
j = d['jobs'][0]
r_bw = j['read']['bw'] / 1024
w_bw = j['write']['bw'] / 1024
r_iops = j['read']['iops']
w_iops = j['write']['iops']
r_lat = j['read']['lat_ns']['mean'] / 1e6 if j['read']['lat_ns']['mean'] > 0 else 0
w_lat = j['write']['lat_ns']['mean'] / 1e6 if j['write']['lat_ns']['mean'] > 0 else 0
bw = w_bw if w_bw > 0 else r_bw
iops = w_iops if w_iops > 0 else r_iops
lat = w_lat if w_lat > 0 else r_lat
print(f'{bw:.1f} MB/s | {iops:.0f} IOPS | {lat:.2f} ms lat')
" 2>/dev/null || echo "parse error"
}

# --- Clean test file ---
kubectl exec "$POD" -- rm -f /data/fio-test 2>/dev/null || true

# --- Phase 1: Writes ---
log ""
log "Phase 1: WRITE tests"
log ""

log "  Sequential write (bs=1M, iodepth=4, 30s)..."
SEQ_W=$(run_fio seq-write write 1M "${WRITE_SIZE_MB}M" | parse_fio)
log "    -> $SEQ_W"

log "  Random write 4K (iodepth=4, 30s)..."
RND_W=$(run_fio rand-write randwrite 4k "${WRITE_SIZE_MB}M" | parse_fio)
log "    -> $RND_W"

# --- Phase 2: Reads ---
log ""
log "Phase 2: READ tests"
log ""

log "  Sequential read (bs=1M, iodepth=4, 30s)..."
SEQ_R=$(run_fio seq-read read 1M "${WRITE_SIZE_MB}M" | parse_fio)
log "    -> $SEQ_R"

log "  Random read 4K (iodepth=4, 30s)..."
RND_R=$(run_fio rand-read randread 4k "${WRITE_SIZE_MB}M" | parse_fio)
log "    -> $RND_R"

# --- Phase 3: Integrity ---
log ""
log "Phase 3: Data integrity"
RESULT=$(kubectl exec "$POD" -- sh -c '
  dd if=/dev/urandom of=/data/integrity bs=1M count=8 2>/dev/null && sync
  W=$(md5sum /data/integrity | awk "{print \$1}")
  echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
  R=$(md5sum /data/integrity | awk "{print \$1}")
  [ "$W" = "$R" ] && echo "PASS $W" || echo "FAIL w=$W r=$R"
  rm -f /data/integrity
' 2>/dev/null || echo "ERROR")

if echo "$RESULT" | grep -q "^PASS"; then
  MD5=$(echo "$RESULT" | awk '{print $2}')
  log "  Integrity: PASS (md5=$MD5)"
  INTEGRITY="PASS"
else
  log "  Integrity: FAIL ($RESULT)"
  INTEGRITY="FAIL"
fi

# --- Clean up ---
kubectl exec "$POD" -- rm -f /data/fio-test 2>/dev/null || true

# --- Summary ---
{
  echo ""
  echo "============================================================"
  echo "SUMMARY: $POD ($SC, $PATHS paths)"
  if [ -n "$LABEL" ]; then
    echo "Label: $LABEL"
  fi
  echo "============================================================"
  printf "%-20s | %-35s\n" "Test" "Result"
  printf "%-20s-+-%-35s\n" "--------------------" "-----------------------------------"
  printf "%-20s | %-35s\n" "Seq Write (1M)" "$SEQ_W"
  printf "%-20s | %-35s\n" "Seq Read (1M)" "$SEQ_R"
  printf "%-20s | %-35s\n" "Rand Write (4K)" "$RND_W"
  printf "%-20s | %-35s\n" "Rand Read (4K)" "$RND_R"
  printf "%-20s | %-35s\n" "Integrity" "$INTEGRITY"
  echo "============================================================"
  echo "Results: $RESULTS_FILE"
} | tee -a "$RESULTS_FILE"
