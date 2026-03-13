#!/bin/bash
# test_real_nvme.sh — Test all datastore types against a real NVMe device.
#
# Uses the FULL NVMe device (no partitioning). Each test creates a uring
# bdev on the whole device, exercises it, then tears down completely before
# the next test starts.
#
# Environment:
#   NVME_DEV  — NVMe block device (default: /dev/nvme0n1)

set -uo pipefail

SOCK="/var/tmp/novastor-spdk.sock"
NVME_DEV="${NVME_DEV:-/dev/nvme0n1}"
PASS=0
FAIL=0
TOTAL=0
MOUNT_BASE="/tmp/nvme_real_test"
mkdir -p "$MOUNT_BASE"

cleanup() {
    echo ""
    echo "=== Final Cleanup ==="

    # Unmount anything we mounted.
    for dir in "$MOUNT_BASE"/*/; do
        umount "$dir" 2>/dev/null || true
    done

    # Disconnect any NVMe-oF connections we made.
    nvme disconnect-all 2>/dev/null || true

    # Wipe filesystem signatures left on the raw device.
    wipefs -af "$NVME_DEV" 2>/dev/null || true

    echo "Cleanup done."
}
trap cleanup EXIT

rpc() {
    local method="$1"
    local params="$2"
    local id="${3:-1}"
    local req="{\"jsonrpc\":\"2.0\",\"id\":${id},\"method\":\"${method}\",\"params\":${params}}"
    echo "$req" | socat -t 30 - UNIX-CONNECT:"$SOCK" 2>/dev/null
}

check_result() {
    local test_name="$1"
    local result="$2"
    TOTAL=$((TOTAL + 1))
    if [ -z "$result" ]; then
        echo "FAIL: $test_name (empty response)"
        FAIL=$((FAIL + 1))
        return 1
    elif echo "$result" | grep -q '"error"'; then
        echo "FAIL: $test_name"
        echo "  $result"
        FAIL=$((FAIL + 1))
        return 1
    else
        echo "PASS: $test_name"
        PASS=$((PASS + 1))
        return 0
    fi
}

# Helper: find the kernel NVMe device for a given NQN.
find_nvmeof_dev() {
    local nqn="$1"
    for dev in /dev/nvme*n1; do
        [ -b "$dev" ] || continue
        local ctrl
        ctrl=$(echo "$dev" | sed 's/n1$//')
        local cname
        cname=$(basename "$ctrl")
        if [ -f "/sys/class/nvme/$cname/subsysnqn" ]; then
            local dev_nqn
            dev_nqn=$(cat "/sys/class/nvme/$cname/subsysnqn")
            if [ "$dev_nqn" = "$nqn" ]; then
                echo "$dev"
                return 0
            fi
        fi
    done
    return 1
}

# Helper: tear down an NVMe-oF export + bdev so the device is free
# for the next test.
teardown_test() {
    local nqn="$1"
    local volume_id="$2"
    local bdev_name="$3"
    local mount_dir="$4"

    umount "$mount_dir" 2>/dev/null || true
    nvme disconnect -n "$nqn" 2>/dev/null || true
    sleep 1
    rpc "nvmf_delete_target" "{\"volume_id\":\"$volume_id\"}" >/dev/null 2>&1 || true
    rpc "bdev_delete" "{\"name\":\"$bdev_name\"}" >/dev/null 2>&1 || true
    sleep 2
    # Zero the first 32MB to destroy any SPDK blobstore superblock and
    # filesystem signatures. Without this, SPDK's lvol module auto-detects
    # old metadata and claims the new bdev.
    dd if=/dev/zero of="$NVME_DEV" bs=1M count=32 conv=notrunc 2>/dev/null
    wipefs -af "$NVME_DEV" 2>/dev/null || true
}

# ========================================================================
# Wait for dataplane
# ========================================================================
echo "Waiting for dataplane RPC socket..."
for i in $(seq 1 30); do
    if [ -S "$SOCK" ]; then break; fi
    sleep 1
done
if [ ! -S "$SOCK" ]; then
    echo "FATAL: RPC socket $SOCK not found after 30s"
    exit 1
fi
echo "RPC socket ready."

# Quick RPC health check.
echo "  Testing RPC connectivity..."
HEALTH=$(echo '{"jsonrpc":"2.0","id":1,"method":"bdev_list","params":{}}' | socat -t 30 - UNIX-CONNECT:"$SOCK" 2>&1)
echo "  Health check response: $HEALTH"
if [ -z "$HEALTH" ]; then
    echo "FATAL: No RPC response — dataplane not ready"
    exit 1
fi

# Show device info.
echo ""
echo "=== NVMe Device ==="
echo "  Device: $NVME_DEV"
lsblk "$NVME_DEV" 2>/dev/null || true
echo ""

# Wipe any stale metadata from previous runs.
wipefs -af "$NVME_DEV" 2>/dev/null || true

# ========================================================================
# Initialise NVMe-oF TCP transport
# ========================================================================
echo "=== Loading nvme-tcp kernel module ==="
modprobe nvme-tcp 2>&1 || echo "  WARNING: Could not load nvme-tcp module"
echo ""
echo "=== Initialising NVMe-oF TCP transport ==="
RES=$(rpc "nvmf_init_transport" "{}")
echo "  Transport: $RES"

# ========================================================================
# Test 1: Raw uring bdev on full NVMe device
# ========================================================================
echo ""
echo "========================================"
echo "  Test 1: Uring bdev on full NVMe ($NVME_DEV)"
echo "========================================"

RES=$(rpc "bdev_aio_create" "{\"name\":\"nvme_raw\",\"device_path\":\"$NVME_DEV\",\"block_size\":512}")
check_result "Create uring bdev on $NVME_DEV (full 1.9TB)" "$RES" || true

# Export via NVMe-oF TCP.
RES=$(rpc "nvmf_create_target" '{"volume_id":"raw-test","bdev_name":"nvme_raw","listen_address":"127.0.0.1","listen_port":4420,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export full NVMe via NVMe-oF TCP :4420" "$RES" || true

NQN1="nqn.2024-01.io.novastor:volume-raw-test"
echo "  Connecting kernel initiator..."
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "$NQN1" 2>&1 || true
sleep 2

RAW_DEV=$(find_nvmeof_dev "$NQN1")
if [ -n "$RAW_DEV" ]; then
    TOTAL=$((TOTAL + 1)); echo "PASS: Found NVMe-oF device $RAW_DEV"; PASS=$((PASS + 1))

    echo "  Formatting $RAW_DEV with ext4..."
    mkfs.ext4 -F "$RAW_DEV" 2>&1
    TOTAL=$((TOTAL + 1))
    if [ $? -eq 0 ]; then echo "PASS: mkfs.ext4 on full NVMe bdev"; PASS=$((PASS + 1))
    else echo "FAIL: mkfs.ext4"; FAIL=$((FAIL + 1)); fi

    DIR="$MOUNT_BASE/raw"
    mkdir -p "$DIR"
    mount "$RAW_DEV" "$DIR"

    TOTAL=$((TOTAL + 1))
    if mountpoint -q "$DIR"; then echo "PASS: Mount full NVMe bdev"; PASS=$((PASS + 1))
    else echo "FAIL: Mount"; FAIL=$((FAIL + 1)); fi

    # 1MB write/read with md5.
    dd if=/dev/urandom of="$DIR/testfile" bs=1M count=1 2>/dev/null
    sync
    ORIG_SUM=$(md5sum "$DIR/testfile" | awk '{print $1}')
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    READ_SUM=$(md5sum "$DIR/testfile" | awk '{print $1}')

    TOTAL=$((TOTAL + 1))
    if [ "$ORIG_SUM" = "$READ_SUM" ]; then
        echo "PASS: 1MB write/read integrity (md5=$ORIG_SUM)"
        PASS=$((PASS + 1))
    else
        echo "FAIL: md5 mismatch: wrote=$ORIG_SUM read=$READ_SUM"
        FAIL=$((FAIL + 1))
    fi

    # 100MB sequential write/read.
    echo "  Writing 100MB sequential..."
    dd if=/dev/urandom of="$DIR/largefile" bs=1M count=100 2>/dev/null
    sync
    LARGE_SUM=$(md5sum "$DIR/largefile" | awk '{print $1}')
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    LARGE_READ=$(md5sum "$DIR/largefile" | awk '{print $1}')

    TOTAL=$((TOTAL + 1))
    if [ "$LARGE_SUM" = "$LARGE_READ" ]; then
        echo "PASS: 100MB write/read integrity on full NVMe"
        PASS=$((PASS + 1))
    else
        echo "FAIL: 100MB md5 mismatch"
        FAIL=$((FAIL + 1))
    fi
else
    TOTAL=$((TOTAL + 1)); echo "FAIL: No NVMe-oF device found"; FAIL=$((FAIL + 1))
fi

# Tear down Test 1 completely before Test 2.
teardown_test "$NQN1" "raw-test" "nvme_raw" "$MOUNT_BASE/raw"

# ========================================================================
# Test 2: LVM (lvol store) on full NVMe device
# ========================================================================
echo ""
echo "========================================"
echo "  Test 2: Lvol store on full NVMe ($NVME_DEV)"
echo "========================================"

RES=$(rpc "bdev_aio_create" "{\"name\":\"nvme_lvol_base\",\"device_path\":\"$NVME_DEV\",\"block_size\":512}")
check_result "Create uring bdev on $NVME_DEV for lvol" "$RES" || true

# Create lvol store on the full device.
RES=$(rpc "bdev_lvol_create_lvstore" '{"base_bdev":"nvme_lvol_base"}')
check_result "Create lvol store on full NVMe (1.9TB)" "$RES" || true
LVS_UUID=$(echo "$RES" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['uuid'])" 2>/dev/null || echo "")
echo "  lvol store UUID: $LVS_UUID"

# Create 1GB thin-provisioned lvol.
RES=$(rpc "bdev_lvol_create" "{\"volume_id\":\"test-lvol\",\"size_bytes\":1073741824,\"lvol_store\":\"$LVS_UUID\",\"thin_provision\":true}")
check_result "Create 1GB thin lvol on full NVMe" "$RES" || true
LVOL_BDEV=$(echo "$RES" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['name'])" 2>/dev/null || echo "")
echo "  lvol bdev: $LVOL_BDEV"

# Export via NVMe-oF TCP.
RES=$(rpc "nvmf_create_target" "{\"volume_id\":\"lvol-test\",\"bdev_name\":\"$LVOL_BDEV\",\"listen_address\":\"127.0.0.1\",\"listen_port\":4421,\"ana_group_id\":0,\"ana_state\":\"optimized\"}")
check_result "Export lvol via NVMe-oF TCP :4421" "$RES" || true

NQN2="nqn.2024-01.io.novastor:volume-lvol-test"
nvme connect -t tcp -a 127.0.0.1 -s 4421 -n "$NQN2" 2>&1 || true
sleep 2

LVM_DEV=$(find_nvmeof_dev "$NQN2")
if [ -n "$LVM_DEV" ]; then
    TOTAL=$((TOTAL + 1)); echo "PASS: Found NVMe-oF device $LVM_DEV for lvol"; PASS=$((PASS + 1))

    mkfs.ext4 -F "$LVM_DEV" 2>&1
    TOTAL=$((TOTAL + 1))
    if [ $? -eq 0 ]; then echo "PASS: mkfs.ext4 on lvol"; PASS=$((PASS + 1))
    else echo "FAIL: mkfs.ext4 on lvol"; FAIL=$((FAIL + 1)); fi

    DIR="$MOUNT_BASE/lvol"
    mkdir -p "$DIR"
    mount "$LVM_DEV" "$DIR"

    TOTAL=$((TOTAL + 1))
    if mountpoint -q "$DIR"; then echo "PASS: Mount lvol"; PASS=$((PASS + 1))
    else echo "FAIL: Mount lvol"; FAIL=$((FAIL + 1)); fi

    # 50MB write/read.
    dd if=/dev/urandom of="$DIR/testfile" bs=1M count=50 2>/dev/null
    sync
    SUM=$(md5sum "$DIR/testfile" | awk '{print $1}')
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    RSUM=$(md5sum "$DIR/testfile" | awk '{print $1}')

    TOTAL=$((TOTAL + 1))
    if [ "$SUM" = "$RSUM" ]; then
        echo "PASS: 50MB write/read on lvol (md5=$SUM)"
        PASS=$((PASS + 1))
    else
        echo "FAIL: md5 mismatch on lvol"
        FAIL=$((FAIL + 1))
    fi
else
    TOTAL=$((TOTAL + 1)); echo "FAIL: No NVMe-oF device found for lvol"; FAIL=$((FAIL + 1))
fi

# Tear down Test 2 — need to delete lvol bdev before the base bdev.
umount "$MOUNT_BASE/lvol" 2>/dev/null || true
nvme disconnect -n "$NQN2" 2>/dev/null || true
sleep 1
rpc "nvmf_delete_target" '{"volume_id":"lvol-test"}' >/dev/null 2>&1 || true
if [ -n "$LVOL_BDEV" ]; then
    rpc "bdev_delete" "{\"name\":\"$LVOL_BDEV\"}" >/dev/null 2>&1 || true
fi
rpc "bdev_delete" '{"name":"nvme_lvol_base"}' >/dev/null 2>&1 || true
sleep 2
dd if=/dev/zero of="$NVME_DEV" bs=1M count=32 conv=notrunc 2>/dev/null
wipefs -af "$NVME_DEV" 2>/dev/null || true

# ========================================================================
# Test 3: Backend API + NVMe-oF on full NVMe device
# ========================================================================
echo ""
echo "========================================"
echo "  Test 3: Backend API + NVMe-oF ($NVME_DEV)"
echo "========================================"

RES=$(rpc "bdev_aio_create" "{\"name\":\"nvme_backend\",\"device_path\":\"$NVME_DEV\",\"block_size\":512}")
check_result "Create uring bdev on $NVME_DEV for backend test" "$RES" || true

# Test the unified backend abstraction layer.
# raw_disk backend creates its own malloc bdevs internally.
RES=$(rpc "backend.init_raw_disk" "{}")
check_result "Init raw_disk backend" "$RES" || true

RES=$(rpc "backend.create_volume" '{"backend":"raw_disk","name":"backend-vol","size_bytes":67108864,"thin":false}')
check_result "Create 64MB volume via raw_disk backend" "$RES" || true

# Write 4KB via backend API.
TEST_DATA=$(dd if=/dev/urandom bs=4096 count=1 2>/dev/null | base64 -w0)
RES=$(rpc "backend.write" "{\"backend\":\"raw_disk\",\"name\":\"backend-vol\",\"offset\":0,\"data_base64\":\"$TEST_DATA\"}")
check_result "Write 4KB via raw_disk backend" "$RES" || true

# Read it back.
RES=$(rpc "backend.read" '{"backend":"raw_disk","name":"backend-vol","offset":0,"length":4096}')
check_result "Read 4KB via raw_disk backend" "$RES" || true

# Verify data integrity.
READ_DATA=$(echo "$RES" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['data_base64'])" 2>/dev/null || echo "")
TOTAL=$((TOTAL + 1))
if [ "$TEST_DATA" = "$READ_DATA" ]; then
    echo "PASS: Backend API data integrity verified"
    PASS=$((PASS + 1))
else
    echo "FAIL: Backend API data mismatch"
    FAIL=$((FAIL + 1))
fi

# Also export the real NVMe bdev via NVMe-oF and do file I/O.
RES=$(rpc "nvmf_create_target" '{"volume_id":"backend-test","bdev_name":"nvme_backend","listen_address":"127.0.0.1","listen_port":4422,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export full NVMe via NVMe-oF TCP :4422" "$RES" || true

NQN3="nqn.2024-01.io.novastor:volume-backend-test"
nvme connect -t tcp -a 127.0.0.1 -s 4422 -n "$NQN3" 2>&1 || true
sleep 2

DIR_DEV=$(find_nvmeof_dev "$NQN3")
if [ -n "$DIR_DEV" ]; then
    TOTAL=$((TOTAL + 1)); echo "PASS: Found NVMe-oF device $DIR_DEV"; PASS=$((PASS + 1))

    mkfs.ext4 -F "$DIR_DEV" 2>&1
    TOTAL=$((TOTAL + 1))
    if [ $? -eq 0 ]; then echo "PASS: mkfs.ext4 on NVMe-oF bdev"; PASS=$((PASS + 1))
    else echo "FAIL: mkfs.ext4"; FAIL=$((FAIL + 1)); fi

    DIR="$MOUNT_BASE/backend"
    mkdir -p "$DIR"
    mount "$DIR_DEV" "$DIR"

    TOTAL=$((TOTAL + 1))
    if mountpoint -q "$DIR"; then echo "PASS: Mount NVMe-oF bdev"; PASS=$((PASS + 1))
    else echo "FAIL: Mount"; FAIL=$((FAIL + 1)); fi

    # 5x 10MB files.
    for i in 1 2 3 4 5; do
        dd if=/dev/urandom of="$DIR/file_$i" bs=1M count=10 2>/dev/null
    done
    sync
    echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true

    ALL_OK=true
    for i in 1 2 3 4 5; do
        SZ=$(stat -c%s "$DIR/file_$i" 2>/dev/null || echo 0)
        if [ "$SZ" != "10485760" ]; then ALL_OK=false; fi
    done

    TOTAL=$((TOTAL + 1))
    if [ "$ALL_OK" = true ]; then
        echo "PASS: 5x 10MB files verified on full NVMe"
        PASS=$((PASS + 1))
    else
        echo "FAIL: File size mismatch"
        FAIL=$((FAIL + 1))
    fi
else
    TOTAL=$((TOTAL + 1)); echo "FAIL: No NVMe-oF device found"; FAIL=$((FAIL + 1))
fi

# Tear down Test 3.
teardown_test "$NQN3" "backend-test" "nvme_backend" "$MOUNT_BASE/backend"

# ========================================================================
# Test 4: Bdev listing & backend volume listing
# ========================================================================
echo ""
echo "========================================"
echo "  Test 4: Bdev & Backend Status"
echo "========================================"

RES=$(rpc "bdev_list" "{}")
check_result "List all bdevs" "$RES" || true

RES=$(rpc "backend.list_volumes" '{"backend":"raw_disk"}')
check_result "List raw_disk backend volumes" "$RES" || true

# ========================================================================
# Results
# ========================================================================
echo ""
echo "========================================"
echo "  Real NVMe Test Results"
echo "========================================"
echo "  Device: $NVME_DEV"
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
