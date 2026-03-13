#!/bin/bash
# Integration test for the three unified storage backends (raw_disk, lvm, chunk).
# Runs inside the container with SPDK. Exercises every StorageBackend operation.
set -euo pipefail

SOCKET="/var/tmp/novastor-spdk.sock"
PASS=0
FAIL=0
ID=0

# --- helpers ---------------------------------------------------------------

rpc() {
    local method="$1"
    local params="$2"
    ID=$((ID + 1))
    local req="{\"jsonrpc\":\"2.0\",\"method\":\"${method}\",\"params\":${params},\"id\":${ID}}"
    local resp
    resp=$(echo "$req" | socat - UNIX-CONNECT:"$SOCKET" 2>/dev/null)
    echo "$resp"
}

assert_ok() {
    local label="$1"
    local resp="$2"
    if echo "$resp" | python3 -c "import sys,json; r=json.load(sys.stdin); sys.exit(0 if 'result' in r else 1)" 2>/dev/null; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label"
        echo "        $resp"
        FAIL=$((FAIL + 1))
    fi
}

assert_field() {
    local label="$1"
    local resp="$2"
    local field="$3"
    local expected="$4"
    local actual
    actual=$(echo "$resp" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r['result']${field})" 2>/dev/null || echo "ERROR")
    if [ "$actual" = "$expected" ]; then
        echo "  PASS: $label ($field=$actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (expected $field=$expected, got $actual)"
        echo "        $resp"
        FAIL=$((FAIL + 1))
    fi
}

b64encode() {
    echo -n "$1" | base64 -w0 2>/dev/null || echo -n "$1" | base64
}

b64decode() {
    echo "$1" | base64 -d 2>/dev/null || echo "$1" | base64 -D
}

# --- wait for socket -------------------------------------------------------

echo "=== Waiting for SPDK dataplane socket ==="
for i in $(seq 1 30); do
    if [ -S "$SOCKET" ]; then
        echo "Socket ready after ${i}s"
        break
    fi
    sleep 1
done
if [ ! -S "$SOCKET" ]; then
    echo "FATAL: socket $SOCKET not available after 30s"
    exit 1
fi

# Quick connectivity check
resp=$(rpc "get_version" "{}")
assert_ok "get_version" "$resp"
echo ""

# === TEST RAW DISK BACKEND ==================================================
echo "=== Raw Disk Backend ==="

# Init
resp=$(rpc "backend.init_raw_disk" "{}")
assert_ok "init_raw_disk" "$resp"

# Create volume (64MB, uses malloc bdev under the hood)
resp=$(rpc "backend.create_volume" '{"backend":"raw_disk","name":"rawvol1","size_bytes":67108864,"thin":false}')
assert_ok "create_volume(rawvol1)" "$resp"

# Stat volume
resp=$(rpc "backend.stat_volume" '{"backend":"raw_disk","name":"rawvol1"}')
assert_ok "stat_volume(rawvol1)" "$resp"
assert_field "stat size" "$resp" "['size_bytes']" "67108864"

# Write data
DATA="Hello from raw disk backend test!"
DATA_B64=$(b64encode "$DATA")
resp=$(rpc "backend.write" "{\"backend\":\"raw_disk\",\"name\":\"rawvol1\",\"offset\":0,\"data_base64\":\"${DATA_B64}\"}")
assert_ok "write(rawvol1)" "$resp"

# Read data back
resp=$(rpc "backend.read" '{"backend":"raw_disk","name":"rawvol1","offset":0,"length":33}')
assert_ok "read(rawvol1)" "$resp"
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$DATA" ]; then
    echo "  PASS: read data matches written data"
    PASS=$((PASS + 1))
else
    echo "  FAIL: read data mismatch (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

# List volumes
resp=$(rpc "backend.list_volumes" '{"backend":"raw_disk"}')
assert_ok "list_volumes(raw_disk)" "$resp"

# Snapshot
resp=$(rpc "backend.create_snapshot" '{"backend":"raw_disk","volume_name":"rawvol1","snapshot_name":"rawsnap1"}')
assert_ok "create_snapshot(rawsnap1)" "$resp"

# List snapshots
resp=$(rpc "backend.list_snapshots" '{"backend":"raw_disk","name":"rawvol1"}')
assert_ok "list_snapshots(rawvol1)" "$resp"

# Clone from snapshot
resp=$(rpc "backend.clone" '{"backend":"raw_disk","snapshot_name":"rawsnap1","clone_name":"rawclone1"}')
assert_ok "clone(rawclone1)" "$resp"

# Read from clone (should have same data)
resp=$(rpc "backend.read" '{"backend":"raw_disk","name":"rawclone1","offset":0,"length":33}')
assert_ok "read(rawclone1)" "$resp"
CLONE_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
CLONE_DATA=$(b64decode "$CLONE_B64" 2>/dev/null || echo "")
if [ "$CLONE_DATA" = "$DATA" ]; then
    echo "  PASS: clone data matches original"
    PASS=$((PASS + 1))
else
    echo "  FAIL: clone data mismatch (got: '$CLONE_DATA')"
    FAIL=$((FAIL + 1))
fi

# Resize volume (grow to 128MB)
resp=$(rpc "backend.resize_volume" '{"backend":"raw_disk","name":"rawvol1","new_size_bytes":134217728}')
assert_ok "resize_volume(rawvol1 -> 128MB)" "$resp"

# Stat after resize
resp=$(rpc "backend.stat_volume" '{"backend":"raw_disk","name":"rawvol1"}')
assert_field "stat after resize" "$resp" "['size_bytes']" "134217728"

# Delete clone, snapshot, volume
resp=$(rpc "backend.delete_volume" '{"backend":"raw_disk","name":"rawclone1"}')
assert_ok "delete_volume(rawclone1)" "$resp"

resp=$(rpc "backend.delete_snapshot" '{"backend":"raw_disk","snapshot_name":"rawsnap1"}')
assert_ok "delete_snapshot(rawsnap1)" "$resp"

resp=$(rpc "backend.delete_volume" '{"backend":"raw_disk","name":"rawvol1"}')
assert_ok "delete_volume(rawvol1)" "$resp"

echo ""

# === TEST LVM BACKEND ========================================================
echo "=== LVM Backend ==="

# First create a malloc bdev for the lvol store
resp=$(rpc "bdev_malloc_create" '{"name":"lvmbase","size_mb":256,"block_size":512}')
assert_ok "create malloc bdev for lvol store" "$resp"

# Create lvol store on the malloc bdev
resp=$(rpc "bdev_lvol_create_lvstore" '{"base_bdev":"lvmbase"}')
assert_ok "create lvol store" "$resp"
LVS_UUID=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['uuid'])" 2>/dev/null || echo "")
echo "  lvol store UUID: $LVS_UUID"

# Init lvm backend
resp=$(rpc "backend.init_lvm" "{\"lvol_store\":\"${LVS_UUID}\"}")
assert_ok "init_lvm" "$resp"

# Create volume (64MB, thin provisioned)
resp=$(rpc "backend.create_volume" '{"backend":"lvm","name":"lvmvol1","size_bytes":67108864,"thin":true}')
assert_ok "create_volume(lvmvol1)" "$resp"

# Stat volume
resp=$(rpc "backend.stat_volume" '{"backend":"lvm","name":"lvmvol1"}')
assert_ok "stat_volume(lvmvol1)" "$resp"

# Write data
DATA2="Hello from LVM backend test data!"
DATA2_B64=$(b64encode "$DATA2")
resp=$(rpc "backend.write" "{\"backend\":\"lvm\",\"name\":\"lvmvol1\",\"offset\":0,\"data_base64\":\"${DATA2_B64}\"}")
assert_ok "write(lvmvol1)" "$resp"

# Read data back
resp=$(rpc "backend.read" '{"backend":"lvm","name":"lvmvol1","offset":0,"length":33}')
assert_ok "read(lvmvol1)" "$resp"
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$DATA2" ]; then
    echo "  PASS: read data matches written data"
    PASS=$((PASS + 1))
else
    echo "  FAIL: read data mismatch (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

# List volumes
resp=$(rpc "backend.list_volumes" '{"backend":"lvm"}')
assert_ok "list_volumes(lvm)" "$resp"

# Snapshot (native SPDK lvol CoW snapshot)
resp=$(rpc "backend.create_snapshot" '{"backend":"lvm","volume_name":"lvmvol1","snapshot_name":"lvmsnap1"}')
assert_ok "create_snapshot(lvmsnap1)" "$resp"

# List snapshots
resp=$(rpc "backend.list_snapshots" '{"backend":"lvm","name":"lvmvol1"}')
assert_ok "list_snapshots(lvmvol1)" "$resp"

# Clone from snapshot
resp=$(rpc "backend.clone" '{"backend":"lvm","snapshot_name":"lvmsnap1","clone_name":"lvmclone1"}')
assert_ok "clone(lvmclone1)" "$resp"

# Read from clone
resp=$(rpc "backend.read" '{"backend":"lvm","name":"lvmclone1","offset":0,"length":33}')
assert_ok "read(lvmclone1)" "$resp"
CLONE_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
CLONE_DATA=$(b64decode "$CLONE_B64" 2>/dev/null || echo "")
if [ "$CLONE_DATA" = "$DATA2" ]; then
    echo "  PASS: clone data matches original"
    PASS=$((PASS + 1))
else
    echo "  FAIL: clone data mismatch (got: '$CLONE_DATA')"
    FAIL=$((FAIL + 1))
fi

# Delete clone, snapshot, volume
resp=$(rpc "backend.delete_volume" '{"backend":"lvm","name":"lvmclone1"}')
assert_ok "delete_volume(lvmclone1)" "$resp"

resp=$(rpc "backend.delete_snapshot" '{"backend":"lvm","snapshot_name":"lvmsnap1"}')
assert_ok "delete_snapshot(lvmsnap1)" "$resp"

resp=$(rpc "backend.delete_volume" '{"backend":"lvm","name":"lvmvol1"}')
assert_ok "delete_volume(lvmvol1)" "$resp"

echo ""

# === TEST CHUNK BACKEND ======================================================
echo "=== Chunk Backend ==="

# Create a malloc bdev for the chunk store (256MB)
resp=$(rpc "bdev_malloc_create" '{"name":"chunkbase","size_mb":256,"block_size":4096}')
assert_ok "create malloc bdev for chunk store" "$resp"

# Init chunk backend
resp=$(rpc "backend.init_chunk" '{"bdev_name":"chunkbase","capacity_bytes":268435456}')
assert_ok "init_chunk" "$resp"

# Create volume (64MB virtual)
resp=$(rpc "backend.create_volume" '{"backend":"chunk","name":"chkvol1","size_bytes":67108864,"thin":false}')
assert_ok "create_volume(chkvol1)" "$resp"

# Stat volume
resp=$(rpc "backend.stat_volume" '{"backend":"chunk","name":"chkvol1"}')
assert_ok "stat_volume(chkvol1)" "$resp"

# Write data (will be stored as a 4MB chunk)
DATA3="Hello from Chunk backend integration test!"
DATA3_B64=$(b64encode "$DATA3")
resp=$(rpc "backend.write" "{\"backend\":\"chunk\",\"name\":\"chkvol1\",\"offset\":0,\"data_base64\":\"${DATA3_B64}\"}")
assert_ok "write(chkvol1)" "$resp"

# Read data back
resp=$(rpc "backend.read" '{"backend":"chunk","name":"chkvol1","offset":0,"length":43}')
assert_ok "read(chkvol1)" "$resp"
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$DATA3" ]; then
    echo "  PASS: read data matches written data"
    PASS=$((PASS + 1))
else
    echo "  FAIL: read data mismatch (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

# List volumes
resp=$(rpc "backend.list_volumes" '{"backend":"chunk"}')
assert_ok "list_volumes(chunk)" "$resp"

# Snapshot (instant — copies chunk index)
resp=$(rpc "backend.create_snapshot" '{"backend":"chunk","volume_name":"chkvol1","snapshot_name":"chksnap1"}')
assert_ok "create_snapshot(chksnap1)" "$resp"

# List snapshots
resp=$(rpc "backend.list_snapshots" '{"backend":"chunk","name":"chkvol1"}')
assert_ok "list_snapshots(chkvol1)" "$resp"

# Clone from snapshot (instant — copies chunk index)
resp=$(rpc "backend.clone" '{"backend":"chunk","snapshot_name":"chksnap1","clone_name":"chkclone1"}')
assert_ok "clone(chkclone1)" "$resp"

# Read from clone
resp=$(rpc "backend.read" '{"backend":"chunk","name":"chkclone1","offset":0,"length":43}')
assert_ok "read(chkclone1)" "$resp"
CLONE_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
CLONE_DATA=$(b64decode "$CLONE_B64" 2>/dev/null || echo "")
if [ "$CLONE_DATA" = "$DATA3" ]; then
    echo "  PASS: clone data matches original"
    PASS=$((PASS + 1))
else
    echo "  FAIL: clone data mismatch (got: '$CLONE_DATA')"
    FAIL=$((FAIL + 1))
fi

# Write to clone (should not affect original — CoW)
DATA4="Modified clone data, original unchanged!"
DATA4_B64=$(b64encode "$DATA4")
resp=$(rpc "backend.write" "{\"backend\":\"chunk\",\"name\":\"chkclone1\",\"offset\":0,\"data_base64\":\"${DATA4_B64}\"}")
assert_ok "write(chkclone1) - CoW" "$resp"

# Verify original still has original data
resp=$(rpc "backend.read" '{"backend":"chunk","name":"chkvol1","offset":0,"length":43}')
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$DATA3" ]; then
    echo "  PASS: original unchanged after clone write (CoW works)"
    PASS=$((PASS + 1))
else
    echo "  FAIL: original data changed after clone write! (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

# Resize volume (grow to 128MB)
resp=$(rpc "backend.resize_volume" '{"backend":"chunk","name":"chkvol1","new_size_bytes":134217728}')
assert_ok "resize_volume(chkvol1 -> 128MB)" "$resp"

# Delete clone, snapshot, volume
resp=$(rpc "backend.delete_volume" '{"backend":"chunk","name":"chkclone1"}')
assert_ok "delete_volume(chkclone1)" "$resp"

resp=$(rpc "backend.delete_snapshot" '{"backend":"chunk","snapshot_name":"chksnap1"}')
assert_ok "delete_snapshot(chksnap1)" "$resp"

resp=$(rpc "backend.delete_volume" '{"backend":"chunk","name":"chkvol1"}')
assert_ok "delete_volume(chkvol1)" "$resp"

echo ""

# === TEST REPLICATION =========================================================
echo "=== Replication Backend ==="

# Create 3 malloc bdevs as replica targets (32MB each)
resp=$(rpc "bdev_malloc_create" '{"name":"rep0","size_mb":32,"block_size":512}')
assert_ok "create malloc bdev rep0" "$resp"
resp=$(rpc "bdev_malloc_create" '{"name":"rep1","size_mb":32,"block_size":512}')
assert_ok "create malloc bdev rep1" "$resp"
resp=$(rpc "bdev_malloc_create" '{"name":"rep2","size_mb":32,"block_size":512}')
assert_ok "create malloc bdev rep2" "$resp"

# Create replica bdev with 3 replicas (write quorum = 2)
resp=$(rpc "replica_bdev_create" '{"name":"repvol1","targets":[{"addr":"local0","port":0,"nqn":"nqn-0","bdev_name":"rep0"},{"addr":"local1","port":0,"nqn":"nqn-1","bdev_name":"rep1"},{"addr":"local2","port":0,"nqn":"nqn-2","bdev_name":"rep2"}],"read_policy":"round_robin"}')
assert_ok "replica_bdev_create(repvol1)" "$resp"
assert_field "write_quorum" "$resp" "['write_quorum']" "2"

# Check status
resp=$(rpc "replica_bdev_status" '{"volume_id":"repvol1"}')
assert_ok "replica_bdev_status" "$resp"
assert_field "healthy_count" "$resp" "['healthy_count']" "3"

# Write data via replication
REPDATA="Replicated data test string for NovaStor!"
REPDATA_B64=$(b64encode "$REPDATA")
resp=$(rpc "replica_bdev_write" "{\"volume_id\":\"repvol1\",\"offset\":0,\"data_base64\":\"${REPDATA_B64}\"}")
assert_ok "replica_bdev_write" "$resp"

# Read data back via replication
resp=$(rpc "replica_bdev_read" "{\"volume_id\":\"repvol1\",\"offset\":0,\"length\":${#REPDATA}}")
assert_ok "replica_bdev_read" "$resp"
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$REPDATA" ]; then
    echo "  PASS: replicated read data matches written data"
    PASS=$((PASS + 1))
else
    echo "  FAIL: replicated read data mismatch (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

# Check I/O stats updated
resp=$(rpc "replica_bdev_status" '{"volume_id":"repvol1"}')
assert_ok "replica_bdev_status after I/O" "$resp"

echo ""

# === TEST ERASURE CODING =====================================================
echo "=== Erasure Coding Backend ==="

# Create 6 malloc bdevs as shard targets (32MB each, 4 data + 2 parity)
for i in 0 1 2 3 4 5; do
    resp=$(rpc "bdev_malloc_create" "{\"name\":\"ecshard${i}\",\"size_mb\":32,\"block_size\":512}")
    assert_ok "create malloc bdev ecshard${i}" "$resp"
done

# Create erasure bdev (4+2 RS)
resp=$(rpc "erasure_bdev_create" '{"volume_id":"ecvol1","data_shards":4,"parity_shards":2,"shards":[{"address":"local0","port":0,"nqn":"nqn-s0","bdev_name":"ecshard0"},{"address":"local1","port":0,"nqn":"nqn-s1","bdev_name":"ecshard1"},{"address":"local2","port":0,"nqn":"nqn-s2","bdev_name":"ecshard2"},{"address":"local3","port":0,"nqn":"nqn-s3","bdev_name":"ecshard3"},{"address":"local4","port":0,"nqn":"nqn-s4","bdev_name":"ecshard4"},{"address":"local5","port":0,"nqn":"nqn-s5","bdev_name":"ecshard5"}]}')
assert_ok "erasure_bdev_create(ecvol1)" "$resp"
assert_field "data_shards" "$resp" "['data_shards']" "4"
assert_field "parity_shards" "$resp" "['parity_shards']" "2"

# Check status
resp=$(rpc "erasure_bdev_status" '{"volume_id":"ecvol1"}')
assert_ok "erasure_bdev_status" "$resp"
assert_field "healthy_shards" "$resp" "['healthy_shards']" "6"

# Write data via erasure coding (data will be RS-encoded across 6 shards)
ECDATA="Erasure coded data test for NovaStor Reed-Solomon 4+2!"
ECDATA_B64=$(b64encode "$ECDATA")
resp=$(rpc "erasure_bdev_write" "{\"volume_id\":\"ecvol1\",\"offset\":0,\"data_base64\":\"${ECDATA_B64}\"}")
assert_ok "erasure_bdev_write" "$resp"

# Read data back via erasure coding
resp=$(rpc "erasure_bdev_read" "{\"volume_id\":\"ecvol1\",\"offset\":0,\"original_size\":${#ECDATA}}")
assert_ok "erasure_bdev_read" "$resp"
READ_B64=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
READ_DATA=$(b64decode "$READ_B64" 2>/dev/null || echo "")
if [ "$READ_DATA" = "$ECDATA" ]; then
    echo "  PASS: erasure coded read data matches written data"
    PASS=$((PASS + 1))
else
    echo "  FAIL: erasure coded read data mismatch (got: '$READ_DATA')"
    FAIL=$((FAIL + 1))
fi

echo ""

# === SUMMARY =================================================================
echo "========================================="
echo "  PASSED: $PASS"
echo "  FAILED: $FAIL"
echo "  TOTAL:  $((PASS + FAIL))"
echo "========================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
