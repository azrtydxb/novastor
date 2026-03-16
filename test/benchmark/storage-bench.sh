#!/usr/bin/env bash
#
# NovaStor Storage Benchmark (Serial)
# ====================================
# Strictly serial: one volume at a time, writes first, then reads.
# No concurrent I/O between volumes to avoid cross-contamination.
#
# Usage:
#   ./test/benchmark/storage-bench.sh [--size 64] [--skip-create] [--cleanup]
#
# Output: test/benchmark/results-YYYY-MM-DD-HHMMSS.txt

set -euo pipefail

WRITE_SIZE_MB="${BENCH_SIZE_MB:-64}"
TIMEOUT_SEC=600
RESULTS_DIR="$(cd "$(dirname "$0")" && pwd)"
TIMESTAMP="$(date +%Y-%m-%d-%H%M%S)"
RESULTS_FILE="${RESULTS_DIR}/results-${TIMESTAMP}.txt"
SKIP_CREATE=false
DO_CLEANUP=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size) WRITE_SIZE_MB="$2"; shift 2 ;;
    --skip-create) SKIP_CREATE=true; shift ;;
    --cleanup) DO_CLEANUP=true; shift ;;
    *) echo "Unknown: $1"; exit 1 ;;
  esac
done

declare -a CLASSES=(
  "novastor-rep1:Replication 1x"
  "novastor-rep3:Replication 3x"
  "novastor-rep5:Replication 5x"
  "novastor-ec3:EC 2+1"
  "novastor-ec5:EC 3+2"
  "novastor-ec7:EC 4+3"
)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULTS_FILE"; }

header() {
  cat <<EOF | tee "$RESULTS_FILE"
============================================================
NovaStor Storage Benchmark (Serial)
============================================================
Date:       $(date)
Write size: ${WRITE_SIZE_MB} MB
Nodes:      $(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')
------------------------------------------------------------
EOF
}

wait_pod() {
  local POD="$1"
  for i in $(seq 1 "$((TIMEOUT_SEC / 5))"); do
    STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    if [ "$STATUS" = "Running" ]; then
      # Wait for fio
      if kubectl exec "$POD" -- which fio >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 5
  done
  return 1
}

create_volume() {
  local SC="$1" PVC="$2" POD="$3"
  kubectl delete pod "$POD" --force --grace-period=0 2>/dev/null || true
  kubectl delete pvc "$PVC" 2>/dev/null || true
  sleep 3
  kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $PVC
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
      claimName: $PVC
YAML
}

get_paths() {
  local PVC="$1"
  local PV
  PV=$(kubectl get pvc "$PVC" -o jsonpath='{.spec.volumeName}' 2>/dev/null)
  kubectl get pv "$PV" -o jsonpath='{.spec.csi.volumeAttributes.targetAddresses}' 2>/dev/null \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?"
}

run_fio() {
  local POD="$1" NAME="$2" RW="$3" BS="$4" SIZE="$5"
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
if w_bw > 0:
    print(f'{w_bw:.1f} MB/s | {w_iops:.0f} IOPS | {w_lat:.2f} ms lat')
elif r_bw > 0:
    print(f'{r_bw:.1f} MB/s | {r_iops:.0f} IOPS | {r_lat:.2f} ms lat')
else:
    print('0 MB/s | 0 IOPS')
" 2>/dev/null || echo "parse error"
}

# ===================== MAIN =====================

header

# Phase 1: Create all volumes sequentially (if needed)
if [ "$SKIP_CREATE" = false ]; then
  log "Phase 1: Creating benchmark volumes (one at a time)..."
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    PVC="bench-${SC#novastor-}"
    POD="bench-pod-${SC#novastor-}"
    log "  Creating $PVC ($SC)..."
    create_volume "$SC" "$PVC" "$POD"
    if wait_pod "$POD"; then
      PATHS=$(get_paths "$PVC")
      log "  $POD: Running ($PATHS paths)"
    else
      log "  $POD: FAILED to start within ${TIMEOUT_SEC}s"
    fi
  done
  log ""
fi

# Phase 2: Sequential write benchmarks (one volume at a time)
log "Phase 2: Sequential WRITE benchmarks..."
log "  (Each volume tested alone — no concurrent I/O)"
log ""

declare -A SEQ_W RND_W SEQ_R RND_R VPATHS

for entry in "${CLASSES[@]}"; do
  SC="${entry%%:*}"
  DESC="${entry#*:}"
  PVC="bench-${SC#novastor-}"
  POD="bench-pod-${SC#novastor-}"

  STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$STATUS" != "Running" ]; then
    log "  $DESC: SKIPPED (pod not running)"
    SEQ_W[$SC]="SKIP"; RND_W[$SC]="SKIP"; SEQ_R[$SC]="SKIP"; RND_R[$SC]="SKIP"; VPATHS[$SC]="?"
    continue
  fi

  VPATHS[$SC]=$(get_paths "$PVC")

  # Clean
  kubectl exec "$POD" -- rm -f /data/fio-test 2>/dev/null || true

  # Sequential write 1M blocks
  log "  $DESC: sequential write ${WRITE_SIZE_MB}M (bs=1M, depth=4, 30s)..."
  SEQ_W[$SC]=$(run_fio "$POD" seq-write write 1M "${WRITE_SIZE_MB}M" | parse_fio)
  log "    -> ${SEQ_W[$SC]}"

  # Random write 4K
  log "  $DESC: random write 4K (depth=4, 30s)..."
  RND_W[$SC]=$(run_fio "$POD" rand-write randwrite 4k "${WRITE_SIZE_MB}M" | parse_fio)
  log "    -> ${RND_W[$SC]}"

  log ""
done

# Phase 3: Sequential read benchmarks (one volume at a time)
log "Phase 3: Sequential READ benchmarks..."
log "  (Reading data written in Phase 2)"
log ""

for entry in "${CLASSES[@]}"; do
  SC="${entry%%:*}"
  DESC="${entry#*:}"
  PVC="bench-${SC#novastor-}"
  POD="bench-pod-${SC#novastor-}"

  STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "$STATUS" != "Running" ]; then
    SEQ_R[$SC]="SKIP"; RND_R[$SC]="SKIP"
    continue
  fi

  # Sequential read 1M blocks
  log "  $DESC: sequential read ${WRITE_SIZE_MB}M (bs=1M, depth=4, 30s)..."
  SEQ_R[$SC]=$(run_fio "$POD" seq-read read 1M "${WRITE_SIZE_MB}M" | parse_fio)
  log "    -> ${SEQ_R[$SC]}"

  # Random read 4K
  log "  $DESC: random read 4K (depth=4, 30s)..."
  RND_R[$SC]=$(run_fio "$POD" rand-read randread 4k "${WRITE_SIZE_MB}M" | parse_fio)
  log "    -> ${RND_R[$SC]}"

  log ""
done

# Phase 4: Data integrity
log "Phase 4: Data integrity verification..."
PASS=0; FAIL=0
for entry in "${CLASSES[@]}"; do
  SC="${entry%%:*}"
  DESC="${entry#*:}"
  POD="bench-pod-${SC#novastor-}"

  STATUS=$(kubectl get pod "$POD" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  [ "$STATUS" != "Running" ] && continue

  RESULT=$(kubectl exec "$POD" -- sh -c '
    dd if=/dev/urandom of=/data/integrity bs=1M count=8 2>/dev/null && sync
    W=$(md5sum /data/integrity | awk "{print \$1}")
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    R=$(md5sum /data/integrity | awk "{print \$1}")
    [ "$W" = "$R" ] && echo "PASS $W" || echo "FAIL w=$W r=$R"
    rm -f /data/integrity
  ' 2>/dev/null || echo "ERROR")

  if echo "$RESULT" | grep -q "^PASS"; then
    log "  $DESC: PASS"; PASS=$((PASS+1))
  else
    log "  $DESC: FAIL ($RESULT)"; FAIL=$((FAIL+1))
  fi
done

# Summary table
{
  echo ""
  echo "============================================================"
  echo "SUMMARY"
  echo "============================================================"
  printf "%-16s | %5s | %-28s | %-28s | %-28s | %-28s\n" \
    "Protection" "Paths" "Seq Write" "Seq Read" "Rand Write 4K" "Rand Read 4K"
  printf "%-16s-+-%5s-+-%-28s-+-%-28s-+-%-28s-+-%-28s\n" \
    "----------------" "-----" "----------------------------" "----------------------------" "----------------------------" "----------------------------"
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    DESC="${entry#*:}"
    printf "%-16s | %5s | %-28s | %-28s | %-28s | %-28s\n" \
      "$DESC" "${VPATHS[$SC]:-?}" "${SEQ_W[$SC]:-?}" "${SEQ_R[$SC]:-?}" "${RND_W[$SC]:-?}" "${RND_R[$SC]:-?}"
  done
  echo ""
  echo "Integrity: ${PASS} passed, ${FAIL} failed"
  echo "Results:   ${RESULTS_FILE}"
  echo "============================================================"
} | tee -a "$RESULTS_FILE"

# Cleanup
if [ "$DO_CLEANUP" = true ]; then
  log "Cleaning up..."
  for entry in "${CLASSES[@]}"; do
    SC="${entry%%:*}"
    kubectl delete pod "bench-pod-${SC#novastor-}" --force --grace-period=0 2>/dev/null || true
    kubectl delete pvc "bench-${SC#novastor-}" 2>/dev/null || true
  done
fi
