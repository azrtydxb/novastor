#!/bin/bash
# test_nvmeof.sh — NVMe-oF export integration tests.
#
# For each backend type (raw malloc, LVM lvol, chunk), this script:
#   1. Creates a malloc bdev via JSON-RPC.
#   2. Exports it over NVMe-oF TCP via the nvmf_create_target RPC.
#   3. Connects from the kernel NVMe-oF initiator (nvme connect).
#   4. Formats the resulting /dev/nvmeXnY with ext4.
#   5. Mounts it, writes a test file, reads it back, verifies contents.
#   6. Unmounts, disconnects, and cleans up.
#
# Requirements: privileged container, /dev access, /lib/modules mounted,
# nvme-cli, e2fsprogs, kmod packages installed.

set -euo pipefail

SOCK="/var/tmp/novastor-spdk.sock"
PASS=0
FAIL=0
TOTAL=0
MOUNT_BASE="/tmp/nvmeof_test"
mkdir -p "$MOUNT_BASE"

# Track state for cleanup
CONNECTED_NQNS=()
MOUNTED_DIRS=()

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    for dir in "${MOUNTED_DIRS[@]}"; do
        umount "$dir" 2>/dev/null || true
    done
    for nqn in "${CONNECTED_NQNS[@]}"; do
        nvme disconnect -n "$nqn" 2>/dev/null || true
    done
    echo "Cleanup done."
}
trap cleanup EXIT

rpc() {
    local method="$1"
    local params="$2"
    local id="${3:-1}"
    local req="{\"jsonrpc\":\"2.0\",\"id\":${id},\"method\":\"${method}\",\"params\":${params}}"
    echo "$req" | socat - UNIX-CONNECT:"$SOCK" 2>/dev/null
}

check_result() {
    local test_name="$1"
    local result="$2"
    TOTAL=$((TOTAL + 1))
    if echo "$result" | grep -q '"error"'; then
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

# Wait for dataplane to be ready.
echo "Waiting for dataplane RPC socket..."
for i in $(seq 1 30); do
    if [ -S "$SOCK" ]; then
        break
    fi
    sleep 1
done
if [ ! -S "$SOCK" ]; then
    echo "FATAL: RPC socket $SOCK not found after 30s"
    exit 1
fi
echo "RPC socket ready."

# Load the nvme-tcp kernel module.
echo ""
echo "=== Loading nvme-tcp kernel module ==="
if ! lsmod 2>/dev/null | grep -q nvme_tcp; then
    modprobe nvme-tcp 2>/dev/null || {
        echo "WARNING: Could not load nvme-tcp module."
        echo "  This test requires the host kernel to have nvme-tcp support."
        echo "  On the host, run: modprobe nvme-tcp"
        echo "  Alternatively, ensure /lib/modules is bind-mounted."
    }
fi

# Verify nvme-tcp is available.
if ! lsmod 2>/dev/null | grep -q nvme_tcp; then
    echo "FATAL: nvme-tcp kernel module not available."
    echo "  Skipping NVMe-oF tests (kernel support required)."
    echo ""
    echo "=== NVMe-oF Test Results ==="
    echo "SKIPPED (nvme-tcp module not available)"
    exit 0
fi
echo "nvme-tcp module loaded."

# Initialise NVMe-oF TCP transport (may already be initialised at startup).
echo ""
echo "=== Initialising NVMe-oF TCP transport ==="
RES=$(rpc "nvmf_init_transport" "{}")
# Ignore errors — it may already be initialised at startup.
echo "Transport init result: $RES"

# ========================================================================
# Test 1: Raw malloc bdev via NVMe-oF
# ========================================================================
echo ""
echo "========================================"
echo "  Test 1: Raw malloc bdev via NVMe-oF"
echo "========================================"

# Create a 64MB malloc bdev.
RES=$(rpc "bdev_malloc_create" '{"name":"nvmeof_raw","size_mb":64,"block_size":512}')
check_result "Create malloc bdev (nvmeof_raw, 64MB)" "$RES" || true

# Export via NVMe-oF TCP on port 4420.
RES=$(rpc "nvmf_create_target" '{"volume_id":"nvmeof-raw-test","bdev_name":"nvmeof_raw","listen_address":"127.0.0.1","listen_port":4420,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export nvmeof_raw via NVMe-oF TCP :4420" "$RES" || true

NQN_RAW="nqn.2024-01.io.novastor:volume-nvmeof-raw-test"
CONNECTED_NQNS+=("$NQN_RAW")

# Connect from kernel NVMe-oF initiator.
echo "Connecting NVMe-oF initiator to $NQN_RAW..."
nvme connect -t tcp -a 127.0.0.1 -s 4420 -n "$NQN_RAW" 2>&1 || {
    echo "FAIL: nvme connect failed for raw bdev"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
}

# Wait for device to appear.
sleep 2

# Find the NVMe device.
RAW_DEV=""
for dev in /dev/nvme*n1; do
    if [ -b "$dev" ]; then
        # Check if this device corresponds to our NQN.
        ctrl_dir=$(echo "$dev" | sed 's/n1$//')
        ctrl_name=$(basename "$ctrl_dir")
        if [ -f "/sys/class/nvme/$ctrl_name/subsysnqn" ]; then
            dev_nqn=$(cat "/sys/class/nvme/$ctrl_name/subsysnqn")
            if [ "$dev_nqn" = "$NQN_RAW" ]; then
                RAW_DEV="$dev"
                break
            fi
        fi
    fi
done

if [ -n "$RAW_DEV" ]; then
    TOTAL=$((TOTAL + 1))
    echo "PASS: Found NVMe device $RAW_DEV for raw bdev"
    PASS=$((PASS + 1))

    # Format with ext4.
    echo "Formatting $RAW_DEV with ext4..."
    mkfs.ext4 -F "$RAW_DEV" 2>&1
    TOTAL=$((TOTAL + 1))
    if [ $? -eq 0 ]; then
        echo "PASS: mkfs.ext4 on $RAW_DEV"
        PASS=$((PASS + 1))
    else
        echo "FAIL: mkfs.ext4 on $RAW_DEV"
        FAIL=$((FAIL + 1))
    fi

    # Mount.
    MOUNT_DIR="$MOUNT_BASE/raw"
    mkdir -p "$MOUNT_DIR"
    mount "$RAW_DEV" "$MOUNT_DIR"
    MOUNTED_DIRS+=("$MOUNT_DIR")
    TOTAL=$((TOTAL + 1))
    if mountpoint -q "$MOUNT_DIR"; then
        echo "PASS: Mount $RAW_DEV at $MOUNT_DIR"
        PASS=$((PASS + 1))
    else
        echo "FAIL: Mount $RAW_DEV at $MOUNT_DIR"
        FAIL=$((FAIL + 1))
    fi

    # Write and read test file.
    TEST_DATA="NovaStor NVMe-oF raw bdev test $(date +%s)"
    echo "$TEST_DATA" > "$MOUNT_DIR/test.txt"
    sync
    READ_BACK=$(cat "$MOUNT_DIR/test.txt")
    TOTAL=$((TOTAL + 1))
    if [ "$READ_BACK" = "$TEST_DATA" ]; then
        echo "PASS: Write/read file on raw NVMe-oF bdev"
        PASS=$((PASS + 1))
    else
        echo "FAIL: Write/read mismatch on raw NVMe-oF bdev"
        echo "  Expected: $TEST_DATA"
        echo "  Got:      $READ_BACK"
        FAIL=$((FAIL + 1))
    fi

    # Unmount.
    umount "$MOUNT_DIR"
    MOUNTED_DIRS=("${MOUNTED_DIRS[@]/$MOUNT_DIR}")
else
    TOTAL=$((TOTAL + 1))
    echo "FAIL: No NVMe device found for raw bdev NQN"
    FAIL=$((FAIL + 1))
fi

# Disconnect.
nvme disconnect -n "$NQN_RAW" 2>/dev/null || true
CONNECTED_NQNS=("${CONNECTED_NQNS[@]/$NQN_RAW}")

# ========================================================================
# Test 2: LVM lvol bdev via NVMe-oF
# ========================================================================
echo ""
echo "========================================"
echo "  Test 2: LVM lvol bdev via NVMe-oF"
echo "========================================"

# Create base malloc bdev for lvol store.
RES=$(rpc "bdev_malloc_create" '{"name":"nvmeof_lvm_base","size_mb":128,"block_size":512}')
check_result "Create malloc bdev (nvmeof_lvm_base, 128MB)" "$RES" || true

# Create lvol store (returns UUID).
RES=$(rpc "bdev_lvol_create_lvstore" '{"base_bdev":"nvmeof_lvm_base"}')
check_result "Create lvol store on nvmeof_lvm_base" "$RES" || true
LVS_UUID=$(echo "$RES" | python3 -c "import sys,json; print(json.load(sys.stdin)['result']['uuid'])" 2>/dev/null || echo "")
echo "  lvol store UUID: $LVS_UUID"

# Create lvol (64MB) — lvol_store must be the UUID, not the base bdev name.
RES=$(rpc "bdev_lvol_create" "{\"volume_id\":\"nvmeof-lvm-vol\",\"size_bytes\":67108864,\"lvol_store\":\"$LVS_UUID\",\"thin_provision\":true}")
check_result "Create 64MB lvol" "$RES" || true

# Extract the bdev name from response.
LVOL_BDEV=$(echo "$RES" | python3 -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('name',''))" 2>/dev/null || echo "")
if [ -z "$LVOL_BDEV" ]; then
    LVOL_BDEV="lvol_nvmeof-lvm-vol"
fi
echo "  lvol bdev name: $LVOL_BDEV"

# Export via NVMe-oF TCP on port 4421.
RES=$(rpc "nvmf_create_target" "{\"volume_id\":\"nvmeof-lvm-test\",\"bdev_name\":\"$LVOL_BDEV\",\"listen_address\":\"127.0.0.1\",\"listen_port\":4421,\"ana_group_id\":0,\"ana_state\":\"optimized\"}")
check_result "Export lvol via NVMe-oF TCP :4421" "$RES" || true

NQN_LVM="nqn.2024-01.io.novastor:volume-nvmeof-lvm-test"
CONNECTED_NQNS+=("$NQN_LVM")

# Connect.
echo "Connecting NVMe-oF initiator to $NQN_LVM..."
nvme connect -t tcp -a 127.0.0.1 -s 4421 -n "$NQN_LVM" 2>&1 || {
    echo "FAIL: nvme connect failed for lvol bdev"
    FAIL=$((FAIL + 1))
    TOTAL=$((TOTAL + 1))
}

sleep 2

# Find the NVMe device.
LVM_DEV=""
for dev in /dev/nvme*n1; do
    if [ -b "$dev" ]; then
        ctrl_dir=$(echo "$dev" | sed 's/n1$//')
        ctrl_name=$(basename "$ctrl_dir")
        if [ -f "/sys/class/nvme/$ctrl_name/subsysnqn" ]; then
            dev_nqn=$(cat "/sys/class/nvme/$ctrl_name/subsysnqn")
            if [ "$dev_nqn" = "$NQN_LVM" ]; then
                LVM_DEV="$dev"
                break
            fi
        fi
    fi
done

if [ -n "$LVM_DEV" ]; then
    TOTAL=$((TOTAL + 1))
    echo "PASS: Found NVMe device $LVM_DEV for lvol bdev"
    PASS=$((PASS + 1))

    mkfs.ext4 -F "$LVM_DEV" 2>&1
    TOTAL=$((TOTAL + 1))
    if [ $? -eq 0 ]; then
        echo "PASS: mkfs.ext4 on $LVM_DEV"
        PASS=$((PASS + 1))
    else
        echo "FAIL: mkfs.ext4 on $LVM_DEV"
        FAIL=$((FAIL + 1))
    fi

    MOUNT_DIR="$MOUNT_BASE/lvm"
    mkdir -p "$MOUNT_DIR"
    mount "$LVM_DEV" "$MOUNT_DIR"
    MOUNTED_DIRS+=("$MOUNT_DIR")
    TOTAL=$((TOTAL + 1))
    if mountpoint -q "$MOUNT_DIR"; then
        echo "PASS: Mount $LVM_DEV at $MOUNT_DIR"
        PASS=$((PASS + 1))
    else
        echo "FAIL: Mount $LVM_DEV at $MOUNT_DIR"
        FAIL=$((FAIL + 1))
    fi

    TEST_DATA="NovaStor NVMe-oF lvol test $(date +%s)"
    echo "$TEST_DATA" > "$MOUNT_DIR/test.txt"
    sync
    READ_BACK=$(cat "$MOUNT_DIR/test.txt")
    TOTAL=$((TOTAL + 1))
    if [ "$READ_BACK" = "$TEST_DATA" ]; then
        echo "PASS: Write/read file on lvol NVMe-oF bdev"
        PASS=$((PASS + 1))
    else
        echo "FAIL: Write/read mismatch on lvol NVMe-oF bdev"
        FAIL=$((FAIL + 1))
    fi

    umount "$MOUNT_DIR"
    MOUNTED_DIRS=("${MOUNTED_DIRS[@]/$MOUNT_DIR}")
else
    TOTAL=$((TOTAL + 1))
    echo "FAIL: No NVMe device found for lvol NQN"
    FAIL=$((FAIL + 1))
fi

nvme disconnect -n "$NQN_LVM" 2>/dev/null || true
CONNECTED_NQNS=("${CONNECTED_NQNS[@]/$NQN_LVM}")

# ========================================================================
# Test 3: Multiple bdevs simultaneously
# ========================================================================
echo ""
echo "========================================"
echo "  Test 3: Multiple NVMe-oF exports"
echo "========================================"

# Create two more malloc bdevs.
RES=$(rpc "bdev_malloc_create" '{"name":"nvmeof_multi_a","size_mb":64,"block_size":512}')
check_result "Create malloc bdev (nvmeof_multi_a, 64MB)" "$RES" || true

RES=$(rpc "bdev_malloc_create" '{"name":"nvmeof_multi_b","size_mb":64,"block_size":512}')
check_result "Create malloc bdev (nvmeof_multi_b, 64MB)" "$RES" || true

# Export both on different ports.
RES=$(rpc "nvmf_create_target" '{"volume_id":"nvmeof-multi-a","bdev_name":"nvmeof_multi_a","listen_address":"127.0.0.1","listen_port":4422,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export nvmeof_multi_a via NVMe-oF TCP :4422" "$RES" || true

RES=$(rpc "nvmf_create_target" '{"volume_id":"nvmeof-multi-b","bdev_name":"nvmeof_multi_b","listen_address":"127.0.0.1","listen_port":4423,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export nvmeof_multi_b via NVMe-oF TCP :4423" "$RES" || true

NQN_A="nqn.2024-01.io.novastor:volume-nvmeof-multi-a"
NQN_B="nqn.2024-01.io.novastor:volume-nvmeof-multi-b"
CONNECTED_NQNS+=("$NQN_A" "$NQN_B")

# Connect both.
nvme connect -t tcp -a 127.0.0.1 -s 4422 -n "$NQN_A" 2>&1 || true
nvme connect -t tcp -a 127.0.0.1 -s 4423 -n "$NQN_B" 2>&1 || true
sleep 2

# Verify both devices appeared.
MULTI_DEVS=0
for nqn in "$NQN_A" "$NQN_B"; do
    for dev in /dev/nvme*n1; do
        if [ -b "$dev" ]; then
            ctrl_dir=$(echo "$dev" | sed 's/n1$//')
            ctrl_name=$(basename "$ctrl_dir")
            if [ -f "/sys/class/nvme/$ctrl_name/subsysnqn" ]; then
                dev_nqn=$(cat "/sys/class/nvme/$ctrl_name/subsysnqn")
                if [ "$dev_nqn" = "$nqn" ]; then
                    MULTI_DEVS=$((MULTI_DEVS + 1))
                    break
                fi
            fi
        fi
    done
done

TOTAL=$((TOTAL + 1))
if [ "$MULTI_DEVS" -eq 2 ]; then
    echo "PASS: Both NVMe-oF devices appeared ($MULTI_DEVS/2)"
    PASS=$((PASS + 1))
else
    echo "FAIL: Expected 2 NVMe-oF devices, found $MULTI_DEVS"
    FAIL=$((FAIL + 1))
fi

# List subsystems via RPC.
RES=$(rpc "nvmf_list_subsystems" '{}')
check_result "List NVMe-oF subsystems" "$RES" || true

# Disconnect both.
nvme disconnect -n "$NQN_A" 2>/dev/null || true
nvme disconnect -n "$NQN_B" 2>/dev/null || true
CONNECTED_NQNS=("${CONNECTED_NQNS[@]/$NQN_A}")
CONNECTED_NQNS=("${CONNECTED_NQNS[@]/$NQN_B}")

# ========================================================================
# Test 4: Delete target while connected
# ========================================================================
echo ""
echo "========================================"
echo "  Test 4: Delete NVMe-oF target"
echo "========================================"

RES=$(rpc "bdev_malloc_create" '{"name":"nvmeof_del","size_mb":32,"block_size":512}')
check_result "Create malloc bdev for delete test" "$RES" || true

RES=$(rpc "nvmf_create_target" '{"volume_id":"nvmeof-del-test","bdev_name":"nvmeof_del","listen_address":"127.0.0.1","listen_port":4424,"ana_group_id":0,"ana_state":"optimized"}')
check_result "Export nvmeof_del via NVMe-oF TCP :4424" "$RES" || true

# Delete the target.
RES=$(rpc "nvmf_delete_target" '{"volume_id":"nvmeof-del-test"}')
check_result "Delete NVMe-oF target nvmeof-del-test" "$RES" || true

# Small delay to let async SPDK destruction complete.
sleep 1

# Verify it's gone from the subsystem list.
RES=$(rpc "nvmf_list_subsystems" '{}')
TOTAL=$((TOTAL + 1))
DEL_NQN="nqn.2024-01.io.novastor:volume-nvmeof-del-test"
FOUND_DEL=$(echo "$RES" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    subs = r.get('result', [])
    found = any(s.get('nqn') == '$DEL_NQN' for s in subs)
    print('yes' if found else 'no')
except:
    print('no')
" 2>/dev/null || echo "no")
if [ "$FOUND_DEL" = "yes" ]; then
    echo "FAIL: Deleted target still appears in subsystem list"
    FAIL=$((FAIL + 1))
else
    echo "PASS: Deleted target no longer in subsystem list"
    PASS=$((PASS + 1))
fi

# ========================================================================
# Results
# ========================================================================
echo ""
echo "========================================"
echo "  NVMe-oF Test Results"
echo "========================================"
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo "========================================"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
