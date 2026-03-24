# ChunkEngine Data Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route ALL I/O through the ChunkEngine so any frontend can serve any volume via CRUSH + NDP to backend nodes, with ANA-based path preference.

**Architecture:** The SPDK bdev calls ChunkEngine for every read/write. ChunkEngine uses CRUSH to determine which backend node(s) hold the chunk, then routes via direct bdev I/O (local) or NDP (remote). Frontend and backend are independent components that scale separately.

**Tech Stack:** Rust (tokio, SPDK FFI), NDP binary protocol, CRUSH placement, NVMe ANA, Go (CSI controller, agent)

**Spec:** `docs/superpowers/specs/2026-03-24-chunkengine-datapath-design.md`

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `dataplane/src/chunk/ndp_pool.rs` | **New**: NDP connection pool to backend nodes | 1 |
| `dataplane/src/chunk/engine.rs` | Add `sub_block_read`, `sub_block_write` with CRUSH routing | 2 |
| `dataplane/src/bdev/novastor_bdev.rs` | Wire bdev I/O through ChunkEngine | 3 |
| `dataplane/src/transport/dataplane_service.rs` | CreateNvmfTarget without requiring local chunk store | 4 |
| `internal/agent/spdk_target_server.go` | CreateTarget creates ChunkEngine-backed bdev | 4 |
| `internal/csi/controller.go` | Select frontend nodes for targets, set ANA from CRUSH | 5 |
| `dataplane/src/chunk/mod.rs` | Add `ndp_pool` module | 1 |

---

## Task 1: NDP Connection Pool

**Files:**
- Create: `dataplane/src/chunk/ndp_pool.rs`
- Modify: `dataplane/src/chunk/mod.rs`

- [ ] **Step 1: Create ndp_pool module**

Create `dataplane/src/chunk/ndp_pool.rs`:

```rust
//! NDP connection pool for sub-block I/O to backend nodes.
//!
//! Maintains persistent NDP connections to backend dataplanes,
//! keyed by node address. Connections are created lazily on first
//! use and reused for subsequent I/O. Thread-safe via tokio Mutex.

use std::collections::HashMap;
use tokio::sync::Mutex;

use ndp::connection::NdpConnection;
use ndp::codec::{NdpHeader, NdpMessage, NdpOp};

/// Pool of NDP connections to backend nodes.
pub struct NdpPool {
    connections: Mutex<HashMap<String, NdpConnection>>,
}

impl NdpPool {
    pub fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create an NDP connection to the given backend address.
    /// Address format: "host:port" or "/path/to/unix.sock".
    async fn get_connection(&self, addr: &str) -> crate::error::Result<()> {
        let mut conns = self.connections.lock().await;
        if !conns.contains_key(addr) {
            let conn = NdpConnection::connect(addr)
                .await
                .map_err(|e| crate::error::DataPlaneError::NdpError(
                    format!("connect to backend {}: {}", addr, e)
                ))?;
            conns.insert(addr.to_string(), conn);
        }
        Ok(())
    }

    /// Read a sub-block range from a backend node via NDP.
    pub async fn sub_block_read(
        &self,
        addr: &str,
        volume_hash: u64,
        offset: u64,
        length: u32,
    ) -> crate::error::Result<Vec<u8>> {
        self.get_connection(addr).await?;
        let conns = self.connections.lock().await;
        let conn = conns.get(addr).unwrap();

        let header = NdpHeader::new(NdpOp::Read, volume_hash, offset, length);
        let resp = conn.request(header, None).await
            .map_err(|e| crate::error::DataPlaneError::NdpError(
                format!("sub_block_read from {}: {}", addr, e)
            ))?;

        if resp.header.status != 0 {
            return Err(crate::error::DataPlaneError::NdpError(
                format!("sub_block_read error: status={}", resp.header.status)
            ));
        }

        Ok(resp.data.unwrap_or_default())
    }

    /// Write a sub-block range to a backend node via NDP.
    pub async fn sub_block_write(
        &self,
        addr: &str,
        volume_hash: u64,
        offset: u64,
        data: &[u8],
    ) -> crate::error::Result<()> {
        self.get_connection(addr).await?;
        let conns = self.connections.lock().await;
        let conn = conns.get(addr).unwrap();

        let header = NdpHeader::new(NdpOp::Write, volume_hash, offset, data.len() as u32);
        let resp = conn.request(header, Some(data.to_vec())).await
            .map_err(|e| crate::error::DataPlaneError::NdpError(
                format!("sub_block_write to {}: {}", addr, e)
            ))?;

        if resp.header.status != 0 {
            return Err(crate::error::DataPlaneError::NdpError(
                format!("sub_block_write error: status={}", resp.header.status)
            ));
        }

        Ok(())
    }

    /// Remove a backend connection (e.g., on topology change).
    pub async fn remove(&self, addr: &str) {
        self.connections.lock().await.remove(addr);
    }
}
```

Note: The exact `NdpHeader`, `NdpOp`, and `NdpConnection::request` signatures may differ from the above. Read the actual NDP crate code (`ndp/src/codec.rs` and `ndp/src/connection.rs`) and adapt the API calls to match. The key interface is: send a read/write request with volume_hash + offset + length, receive response with data.

- [ ] **Step 2: Register module**

In `dataplane/src/chunk/mod.rs`, add:
```rust
pub mod ndp_pool;
```

- [ ] **Step 3: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 4: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add dataplane/src/chunk/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] NDP connection pool for backend sub-block I/O

Maintains persistent NDP connections to backend nodes, keyed by
address. Lazy connection creation, thread-safe via tokio Mutex.
Provides sub_block_read and sub_block_write over NDP."
```

---

## Task 2: ChunkEngine Sub-Block Methods

**Files:**
- Modify: `dataplane/src/chunk/engine.rs`

- [ ] **Step 1: Add NDP pool to ChunkEngine struct**

Add field to `ChunkEngine`:
```rust
ndp_pool: NdpPool,
```

Initialize in constructor:
```rust
ndp_pool: NdpPool::new(),
```

Add import: `use crate::chunk::ndp_pool::NdpPool;`

- [ ] **Step 2: Add volume hash helper**

The NDP protocol identifies volumes by a u64 hash. Add a helper:
```rust
fn volume_hash(volume_id: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    volume_id.hash(&mut hasher);
    hasher.finish()
}
```

- [ ] **Step 3: Implement sub_block_read**

```rust
/// Read a sub-block range via CRUSH routing.
/// Tries replicas in CRUSH order until one succeeds.
pub async fn sub_block_read(
    &self,
    volume_id: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>> {
    let chunk_index = offset / CHUNK_SIZE as u64;
    let chunk_key = format!("{}:{}", volume_id, chunk_index);

    // CRUSH: get replica list for this chunk.
    let placements = {
        let topo = self.topology.read().unwrap();
        crush::select(&chunk_key, 3, &topo) // Try up to 3 replicas
    };

    if placements.is_empty() {
        return Err(DataPlaneError::ChunkError(
            "CRUSH returned no placements".into()
        ));
    }

    let vol_hash = Self::volume_hash(volume_id);
    let mut last_err = None;

    for (node_id, _backend_id) in &placements {
        if *node_id == self.node_id {
            // Local: read from local chunk store's backend bdev.
            match self.local_sub_block_read(volume_id, offset, length).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    log::warn!("local sub_block_read failed, trying next replica: {}", e);
                    last_err = Some(e);
                    continue;
                }
            }
        }

        // Remote: read via NDP to that backend node.
        let addr = {
            let topo = self.topology.read().unwrap();
            match topo.get_node(node_id) {
                Some(node) => format!("{}:{}", node.address, node.ndp_port()),
                None => continue,
            }
        };

        match self.ndp_pool.sub_block_read(&addr, vol_hash, offset, length as u32).await {
            Ok(data) => return Ok(data),
            Err(e) => {
                log::warn!("remote sub_block_read from {} failed: {}", node_id, e);
                last_err = Some(e);
                continue;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| DataPlaneError::ChunkError(
        "all replicas failed for sub_block_read".into()
    )))
}
```

Note: `local_sub_block_read` calls the existing `sub_block_read_pub` function from `novastor_bdev.rs` which does the reactor-native bdev I/O. Read the actual function signatures and adapt.

- [ ] **Step 4: Implement sub_block_write**

```rust
/// Write a sub-block range to all replicas via CRUSH routing.
/// Returns success when write quorum (majority) is met.
pub async fn sub_block_write(
    &self,
    volume_id: &str,
    offset: u64,
    data: &[u8],
) -> Result<()> {
    let chunk_index = offset / CHUNK_SIZE as u64;
    let chunk_key = format!("{}:{}", volume_id, chunk_index);

    let factor = {
        let prot = self.protection.read().unwrap();
        match *prot {
            Protection::Replication { factor } => factor as usize,
            Protection::ErasureCoding { data_shards, parity_shards } => {
                (data_shards + parity_shards) as usize
            }
        }
    };

    let placements = {
        let topo = self.topology.read().unwrap();
        crush::select(&chunk_key, factor, &topo)
    };

    if placements.is_empty() {
        return Err(DataPlaneError::ChunkError(
            "CRUSH returned no placements for write".into()
        ));
    }

    let vol_hash = Self::volume_hash(volume_id);
    let quorum = (placements.len() / 2) + 1;

    // Fan-out writes to all replicas concurrently.
    let mut tasks = Vec::new();
    for (node_id, _backend_id) in &placements {
        let nid = node_id.clone();
        let vid = volume_id.to_string();
        let write_data = data.to_vec();
        let off = offset;

        if nid == self.node_id {
            tasks.push(tokio::spawn(async move {
                // Placeholder: call local_sub_block_write
                Ok::<(), DataPlaneError>(())
            }));
        } else {
            let addr = {
                let topo = self.topology.read().unwrap();
                match topo.get_node(&nid) {
                    Some(node) => format!("{}:{}", node.address, node.ndp_port()),
                    None => continue,
                }
            };
            // Clone pool reference for the spawned task
            // Note: NdpPool needs to be Arc-wrapped for this
            tasks.push(tokio::spawn(async move {
                // Placeholder: ndp_pool.sub_block_write(...)
                Ok::<(), DataPlaneError>(())
            }));
        }
    }

    // Wait for quorum.
    let mut successes = 0;
    let mut last_err = None;
    for task in tasks {
        match task.await {
            Ok(Ok(())) => successes += 1,
            Ok(Err(e)) => last_err = Some(e),
            Err(e) => last_err = Some(DataPlaneError::ChunkError(
                format!("write task panicked: {}", e)
            )),
        }
        if successes >= quorum {
            return Ok(());
        }
    }

    Err(last_err.unwrap_or_else(|| DataPlaneError::ChunkError(
        "write quorum not met".into()
    )))
}
```

Note: The write fan-out needs `Arc<NdpPool>` to share across spawned tasks. Adjust the ChunkEngine struct accordingly. The local write path should call the existing `sub_block_write_pub`. Read the actual code and adapt — the above is a structural guide, not copy-paste code.

- [ ] **Step 5: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 6: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add dataplane/src/chunk/engine.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] ChunkEngine sub-block read/write with CRUSH routing

sub_block_read: CRUSH lookup → local bdev or remote NDP, failover
across replicas. sub_block_write: CRUSH fan-out to all replicas
concurrently, wait for write quorum (majority)."
```

---

## Task 3: Wire SPDK Bdev Through ChunkEngine

**Files:**
- Modify: `dataplane/src/bdev/novastor_bdev.rs`

- [ ] **Step 1: Replace direct sub_block_read with ChunkEngine call**

In `bdev_submit_request_cb` READ handler, replace the tokio fallback path (currently calls `sub_block_read(&volume_name, offset, length)`) with:

```rust
let engine = get_chunk_engine()
    .expect("chunk engine not initialized");
let result = engine.sub_block_read(&volume_name, offset, length).await;
```

The reactor fast path for local reads can remain as an optimization — but it should be gated on "ChunkEngine says this chunk is local" rather than always assuming local. For the initial implementation, route ALL reads through ChunkEngine and optimize later.

- [ ] **Step 2: Replace direct sub_block_write with ChunkEngine call**

Same approach for the WRITE handler:

```rust
let engine = get_chunk_engine()
    .expect("chunk engine not initialized");
let result = engine.sub_block_write(&volume_name, offset, &data).await;
```

- [ ] **Step 3: Update WRITE_ZEROES and UNMAP handlers**

Route through ChunkEngine as well — the ChunkEngine should forward these to all replica backends.

- [ ] **Step 4: Remove direct backend dependencies from bdev**

The bdev should no longer call:
- `get_backend_bdev_name()` for I/O (keep for init)
- `get_volume_base_offset()` for I/O (keep for init)
- `reactor_cache_open()` for I/O (keep for readahead if local)

These move into the ChunkEngine's local I/O path.

- [ ] **Step 5: Verify compilation**

```bash
cd dataplane/nvmeof-tcp && cargo check
```

Note: This will require careful work — `novastor_bdev.rs` is 2500+ lines of unsafe FFI code. Read the existing code thoroughly before making changes. The reactor fast path, readahead, write cache, and dirty bitmap tracking all need to be preserved or adapted.

- [ ] **Step 6: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add dataplane/src/bdev/novastor_bdev.rs
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Wire SPDK bdev through ChunkEngine for all I/O

All reads and writes go through ChunkEngine which routes via
CRUSH to the correct backend (local bdev or remote NDP).
No more direct backend bdev access from the presentation layer."
```

---

## Task 4: Frontend-Only CreateTarget

**Files:**
- Modify: `dataplane/src/transport/dataplane_service.rs`
- Modify: `internal/agent/spdk_target_server.go`

- [ ] **Step 1: Allow CreateVolume/CreateNvmfTarget without local chunk store**

In the Rust dataplane's `create_volume` gRPC handler, currently it requires a local backend bdev and allocates a volume offset. Change it to:
- If this node has a local chunk store → allocate volume offset (existing behavior)
- If this node is frontend-only (no chunk store) → create a ChunkEngine-backed bdev with no local allocation. The ChunkEngine routes all I/O to remote backends via NDP.

The novastor bdev creation in `novastor_bdev.rs` needs a mode where it doesn't require a local backend bdev — it only needs the ChunkEngine.

- [ ] **Step 2: Update Go agent CreateTarget**

In `internal/agent/spdk_target_server.go`, `CreateTarget` currently calls `ensureChunkStore()` which requires a local backend. Add a path where:
- If `skipLocalChunkStore` flag is set (new field) → skip `ensureChunkStore`
- Call `CreateVolume` on the dataplane with a flag indicating "frontend-only, no local allocation"
- The dataplane creates a novastor bdev backed by ChunkEngine with remote routing

- [ ] **Step 3: Verify compilation**

```bash
go build ./...
cd dataplane/nvmeof-tcp && cargo check
```

- [ ] **Step 4: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add dataplane/ internal/agent/
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] Frontend-only CreateTarget without local chunk store

NVMe-oF targets can be created on nodes without local NVMe drives.
The bdev routes all I/O through ChunkEngine → CRUSH → NDP to
backend nodes that have the actual storage."
```

---

## Task 5: ANA State From CRUSH Affinity

**Files:**
- Modify: `internal/csi/controller.go`

- [ ] **Step 1: Select frontend nodes for target placement**

In `CreateVolume`, change target node selection:
- Current: select nodes that will hold chunks (backends)
- New: select frontend nodes for NVMe-oF targets:
  1. Compute CRUSH placement for the volume → get backend node list
  2. Nodes that are both frontend AND in CRUSH placement → create target with `ana_state = "optimized"`
  3. Additional frontend-only nodes → create target with `ana_state = "non_optimized"`
  4. Total targets = replication_factor (optimized) + optional extra (non_optimized)

- [ ] **Step 2: Verify compilation**

```bash
go build ./...
```

- [ ] **Step 3: Commit**

```bash
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  add internal/csi/controller.go
git -c user.name="Pascal Watteel" -c user.email="pascal@watteel.com" \
  commit -m "[Feature] ANA state from CRUSH affinity

Frontend nodes with local CRUSH affinity get optimized ANA state.
Frontend-only nodes get non_optimized (failover). Kernel multipath
prefers paths with local chunks automatically."
```

---

## Task 6: Build, Deploy, Integration Test

**Files:**
- No source changes — build, deploy, and verify

- [ ] **Step 1: Build dataplane, agent, and CSI images**

```bash
podman build --no-cache -t novastor-dataplane:latest -f deploy/docker/Dockerfile.dataplane .
podman build -t novastor-agent:latest -f build/Dockerfile.agent .
podman build -t novastor-csi:latest -f build/Dockerfile.csi .
```

- [ ] **Step 2: Deploy to cluster**

Push all three images and update DaemonSets/Deployments.

- [ ] **Step 3: Test — frontend-only node serving a volume**

1. Identify a node that has NO backend (no NVMe drives, no chunk store)
2. Create a rep3 volume — CSI should create targets including one on the frontend-only node
3. Schedule a pod on the frontend-only node
4. Verify I/O works — the frontend routes via NDP to backend nodes
5. Kill a backend node → verify I/O continues on surviving replicas

- [ ] **Step 4: Test — ANA path preference**

1. Create rep3 volume with targets on 2 combo nodes + 1 frontend-only node
2. Pod connects to all 3 paths
3. Verify kernel prefers combo nodes (optimized) over frontend-only (non-optimized)
4. Kill one combo node → verify failover to remaining combo + frontend-only

- [ ] **Step 5: Benchmark — local vs remote I/O**

```bash
# On a combo node (local chunks): should match previous benchmarks
fio --name=local --rw=randread --bs=4k --direct=1 --ioengine=libaio --iodepth=32 --runtime=30

# On a frontend-only node (remote chunks via NDP): measure overhead
fio --name=remote --rw=randread --bs=4k --direct=1 --ioengine=libaio --iodepth=32 --runtime=30
```
