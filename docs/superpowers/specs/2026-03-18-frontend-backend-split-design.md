# Frontend/Backend Architecture Split

## Goal

Split the NovaStor dataplane into two separate components: a lightweight **Frontend Controller** that handles client-facing protocols (NVMe-oF, NFS, S3) and a **Chunk Engine** that owns the NVMe drives and serves data. Communication between all internal components uses a custom binary protocol (NDP) optimized for low-latency I/O.

## Architecture

```
Pod (app)
  └── kernel NVMe-oF initiator (nvme connect)
        └── ANA optimized → LOCAL kernel nvmet target
        └── ANA non-optimized → Remote kernel nvmet targets (failover)

kernel nvmet TCP target (port 4430)
  └── namespace backed by /dev/nbdX

/dev/nbdX ← NBD protocol (Unix domain socket) → Frontend Controller

Frontend Controller (per node, Rust + tokio, NO SPDK)
  ├── NBD server (serves /dev/nbdX, tokio async)
  ├── CRUSH map (read-only, pushed by Go agent)
  ├── NDP client connections to ALL chunk engines (persistent pool)
  └── Routes I/O to chunk engines based on CRUSH placement

Chunk Engine (per node, SPDK, owns NVMe)
  ├── SPDK reactor (pinned to A76 core)
  ├── Sub-block I/O, atomic dirty bitmaps, DMA buffer pool
  ├── NDP server (serves data to frontends and other chunk engines)
  ├── NDP client (replication fan-out to other chunk engines)
  └── gRPC control plane (volume lifecycle, topology — unchanged)
```

### Key Design Decisions

**No SPDK in the frontend.** The frontend is pure Rust + tokio. No reactor core burned. CPU usage is zero when idle, proportional to I/O when active.

**Kernel nvmet for NVMe-oF.** The kernel's built-in NVMe-oF TCP target (`CONFIG_NVME_TARGET_TCP=m`) handles the NVMe-oF protocol. Configured via configfs. No userspace NVMe-oF implementation needed.

**NBD as the bridge.** The kernel NBD client (`CONFIG_BLK_DEV_NBD=m`) creates `/dev/nbdX` backed by our frontend's NBD server over a Unix domain socket. The frontend translates NBD requests to NDP requests to chunk engines.

**NVMe multipath for failover.** Multiple frontends on different nodes create nvmet subsystems with the same NQN. Kernel NVMe multipath handles failover. The frontend is stateless — no stale subsystem issues.

**NDP for all data-path communication.** Frontend→chunk engine and chunk engine→chunk engine use NDP (custom 64-byte header TCP protocol). gRPC retained for control plane only.

### I/O Path

```
Write (4K):
  Pod write → kernel NVMe-oF initiator
    → kernel nvmet TCP target (same node)
    → /dev/nbdX (kernel NBD client)
    → Unix socket → Frontend NBD server (tokio)
    → CRUSH lookup → owner chunk engine
    → NDP WRITE → chunk engine
    → chunk engine: sub-block write to NVMe + REPLICATE to replicas
    → NDP WRITE_RESP → frontend
    → NBD response → kernel → nvmet response → Pod

Read (4K):
  Pod read → kernel NVMe-oF initiator
    → kernel nvmet TCP target
    → /dev/nbdX → Unix socket → Frontend
    → CRUSH lookup → pick closest chunk engine with data
    → NDP READ → chunk engine
    → chunk engine: sub-block read from NVMe
    → NDP READ_RESP (with data) → frontend
    → NBD response → kernel → nvmet response → Pod
```

### Latency Budget (4K random, local path)

| Hop | Estimated Latency |
|---|---|
| NVMe-oF initiator → nvmet (same node, TCP loopback) | ~50-80μs |
| nvmet → NBD → Unix socket → Frontend | ~5-10μs |
| Frontend CRUSH lookup + NDP send | ~2-5μs |
| NDP TCP to local chunk engine | ~10-20μs |
| Chunk engine sub-block I/O to NVMe | ~60-90μs |
| Total | ~130-200μs |

Current single-process path: ~91μs. The split adds ~40-110μs overhead from the extra hops. At qd=32 this will be partially hidden by pipelining.

### What We Gain

- **Read fanout**: Frontend reads from ANY chunk engine, not just local. With rep=3, reads from 3 NVMe drives.
- **Frontend doesn't burn a core**: Pure tokio, zero CPU when idle.
- **Clean NVMe-oF**: No SPDK-managed NVMe-oF subsystems. Kernel nvmet configured via configfs is simple and reliable. No stale subsystem issues.
- **Failover**: Kernel NVMe multipath across frontends on different nodes. Frontends are stateless — restart is instant.
- **Multi-protocol**: NFS and S3 can be added to the frontend without touching the chunk engine.

## Separation of Concerns

| Responsibility | Frontend Controller | Chunk Engine |
|---|---|---|
| NBD server (block device to kernel) | Yes | No |
| nvmet configuration (configfs) | Yes | No |
| CRUSH placement lookup | Yes | No |
| I/O routing to correct engine | Yes | No |
| Failover on engine failure | Yes | No |
| Sub-block I/O to NVMe | No | Yes |
| Dirty bitmap tracking | No | Yes |
| Replication fan-out | No | Yes |
| EC encoding/decoding | No | Yes |
| SPDK reactor | No | Yes |
| Hugepages / privileged | No | Yes (NVMe access) |

## NovaStor Data Protocol (NDP)

Custom binary TCP protocol for all internal data-path communication. Replaces gRPC for data I/O (gRPC retained for control plane only).

### Wire Format

Fixed 64-byte header followed by optional data payload.

```
Bytes 0-3:   magic (0x4E565354 = "NVST")
Byte 4:      op (operation code)
Byte 5:      flags
Bytes 6-7:   status (response only)
Bytes 8-15:  request_id (u64, monotonic per connection)
Bytes 16-23: volume_hash (u64, precomputed hash of volume UUID)
Bytes 24-31: offset (u64, byte offset in volume)
Bytes 32-35: data_length (u32, bytes of payload after header)
Bytes 36-37: chunk_idx (u16)
Byte 38:     sb_idx (u8)
Byte 39:     pad
Bytes 40-47: reserved
Bytes 48-51: header_crc32
Bytes 52-55: data_crc32
Bytes 56-63: reserved
```

### Operations

| Op | Name | Direction | Payload | Description |
|---|---|---|---|---|
| 0x01 | WRITE | request | yes | Write data at volume+offset |
| 0x02 | READ | request | no | Read data at volume+offset |
| 0x03 | WRITE_RESP | response | no | Write completed |
| 0x04 | READ_RESP | response | yes | Read data response |
| 0x05 | WRITE_ZEROES | request | no | Zero a range (thin provisioning no-op) |
| 0x06 | UNMAP | request | no | Discard a range (thin provisioning no-op) |
| 0x07 | REPLICATE | request | yes | Replication write (CE→CE) |
| 0x08 | REPLICATE_RESP | response | no | Replication ack |
| 0x09 | EC_SHARD | request | yes | EC shard write (CE→CE) |
| 0x0A | PING | request | no | Health check |
| 0x0B | PONG | response | no | Health response |

### Used By

- Frontend → Chunk Engine: WRITE, READ, WRITE_ZEROES, UNMAP
- Chunk Engine → Chunk Engine: REPLICATE, EC_SHARD (replaces gRPC PutChunk/GetChunk)
- All components: PING/PONG for health

## Frontend Controller

### Binary

`novastor-frontend` — separate Rust binary, no SPDK dependency.

### Runtime

Tokio async runtime. No SPDK reactor, no hugepages, no privileged mode (except access to configfs and /dev/nbdX). Shares CPU with other workloads when idle.

### NBD Server

Implements the NBD protocol (Linux kernel NBD spec) over a Unix domain socket. Each volume gets one NBD device (`/dev/nbdX`). The frontend:

1. Creates a Unix domain socket at `/var/run/novastor/nbd-<volume_id>.sock`
2. Starts the NBD server on that socket
3. Configures the kernel NBD client to connect: `nbd-client -u /var/run/novastor/nbd-<volume_id>.sock /dev/nbdX`
4. Configures kernel nvmet via configfs to use `/dev/nbdX` as the namespace backend
5. Adds nvmet TCP listener on port 4430

The NBD protocol is simple:
- **Request** (28 bytes): `[magic:4][flags:2][type:2][handle:8][offset:8][length:4]`
- **Response** (16 bytes + data): `[magic:4][error:4][handle:8][data...]`

The frontend translates each NBD request to an NDP request to the appropriate chunk engine.

### Kernel nvmet Configuration

The frontend configures kernel nvmet via configfs:

```
/sys/kernel/config/nvmet/
  subsystems/<nqn>/
    namespaces/1/
      device_path = /dev/nbdX
      enable = 1
    allowed_hosts/
    attr_allow_any_host = 1
  ports/1/
    addr_trtype = tcp
    addr_trsvcid = 4430
    addr_traddr = 0.0.0.0
    addr_adrfam = ipv4
    subsystems/<nqn>  (symlink)
```

This is done programmatically from the frontend binary using standard file writes to configfs.

### Connection Pool

Persistent TCP connections to ALL chunk engines at startup (one per engine). Connections are long-lived and reconnect automatically on failure.

### Write Path

```
1. NBD server receives write request (on tokio async task)
2. Compute chunk_idx, sb_idx, offset_in_sb from volume byte offset
3. CRUSH lookup → owner chunk engine for this chunk
4. Send NDP WRITE to owner (persistent connection)
5. Owner CE writes locally + REPLICATE to replicas (parallel)
6. Owner CE sends WRITE_RESP after write quorum achieved
7. Frontend sends NBD write response
```

If owner is unreachable: send write to any other chunk engine that has the data.

### Read Path

```
1. NBD server receives read request
2. Compute chunk_idx, sb_idx, offset_in_sb
3. CRUSH lookup → all chunk engines with this chunk
4. Pick closest engine (prefer local, then same rack, then any)
5. Send NDP READ
6. CE reads from local NVMe, sends READ_RESP
7. Frontend sends NBD read response with data
```

If preferred engine is unreachable: try next in CRUSH order.

### Failure Detection

- PING/PONG health checks on each chunk engine connection (every 1-2s)
- If chunk engine unreachable: mark down, route to alternates, reconnect in background

## Chunk Engine Changes

### Removed

- NVMe-oF target (moves to frontend via kernel nvmet)
- CSI-related bdev presentation code

### Added

- NDP server on port 4500 (hostNetwork)
- NDP client for REPLICATE/EC_SHARD to other chunk engines

### Unchanged

- SPDK reactor pinned to A76 core
- Sub-block I/O, atomic dirty bitmaps, DMA buffer pool
- Background sync, CRUSH, protection policy
- gRPC control plane (volume lifecycle, topology)

## Deployment

### Kubernetes Resources

| Component | K8s Resource | Image | Ports | Privileged |
|---|---|---|---|---|
| Frontend Controller | DaemonSet | `novastor-frontend` | 4430 (nvmet TCP) | Yes (configfs, NBD) |
| Chunk Engine | DaemonSet | `novastor-dataplane` | 4500 (NDP) | Yes (hugepages, NVMe) |
| Go Agent | DaemonSet | `novastor-agent` | 9100 (gRPC) | No |

### Prerequisites per Node

Kernel modules must be loaded:
```bash
modprobe nbd max_part=0 nbds_max=64
modprobe nvmet
modprobe nvmet-tcp
```

These can be loaded via a node init DaemonSet or systemd unit.

### Frontend Controller Pod

- Needs access to `/sys/kernel/config/nvmet/` (configfs) for nvmet setup
- Needs access to `/dev/nbd*` for NBD devices
- Needs access to `/var/run/novastor/` for Unix domain sockets
- TLS certs mounted for mTLS to chunk engines (future)
- hostNetwork for nvmet port 4430
- Resource requests: 100m CPU, 128Mi memory

### Chunk Engine Pod

- Same as current dataplane DaemonSet minus NVMe-oF target
- SPDK reactor pinned to A76 core (reactor_mask=0x10)
- NDP server on port 4500 (hostNetwork)
- Hugepages, privileged, NVMe access

## Implementation Order

1. **NDP protocol library** ✅ Done (ndp crate)
2. **NDP server in chunk engine** ✅ Done (ndp_server.rs)
3. **NBD server library** — Rust NBD protocol implementation on tokio
4. **Frontend controller binary** — NBD server + NDP client + CRUSH routing
5. **Kernel nvmet configuration** — configfs setup from frontend
6. **CSI changes** — CreateTarget creates nvmet subsystems on frontends
7. **Helm charts** — New DaemonSet, kernel module loading
8. **Replace gRPC replication** — Chunk engine uses NDP for REPLICATE/EC_SHARD
9. **Testing** — Benchmarks, failover
