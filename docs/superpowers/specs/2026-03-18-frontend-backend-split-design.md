# Frontend/Backend Architecture Split

## Goal

Split the NovaStor dataplane into two separate components: a lightweight **Frontend Controller** that handles client-facing protocols (NVMe-oF, NFS, S3) and a **Chunk Engine** that owns the NVMe drives and serves data. Communication between all internal components uses a custom binary protocol (NDP) optimized for low-latency I/O.

## Architecture

```
Pod (app)
  └── kernel NVMe-oF initiator
        └── ANA optimized → LOCAL Frontend Controller
        └── ANA non-optimized → Remote Frontend Controllers (failover)

Frontend Controller (per node, Rust + tokio, no SPDK)
  ├── NVMe-oF TCP target (userspace, on tokio)
  ├── CRUSH map (read-only, pushed by Go agent)
  ├── NDP client connections to ALL chunk engines (persistent pool)
  └── Routes I/O to chunk engines based on CRUSH placement

Chunk Engine (per node, Rust + SPDK, owns NVMe)
  ├── SPDK reactor (pinned to A76 core)
  ├── Sub-block I/O, atomic dirty bitmaps, DMA buffer pool
  ├── NDP server (serves data to frontends and other chunk engines)
  ├── NDP client (replication fan-out to other chunk engines)
  └── gRPC control plane (volume lifecycle, topology — unchanged)
```

### Separation of Concerns

| Responsibility | Frontend Controller | Chunk Engine |
|---|---|---|
| Client protocols (NVMe-oF, NFS, S3) | Yes | No |
| CRUSH placement lookup | Yes | No |
| I/O routing to correct engine | Yes | No |
| Failover on engine failure | Yes | No |
| Sub-block I/O to NVMe | No | Yes |
| Dirty bitmap tracking | No | Yes |
| Replication fan-out | No | Yes |
| EC encoding/decoding | No | Yes |
| SPDK reactor | No | Yes |
| Hugepages / privileged | No | Yes |

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

### Design Properties

- **Multiplexed**: multiple in-flight requests per connection via `request_id`
- **Zero-copy friendly**: fixed header parsed by struct cast, no serialization
- **Checksummed**: CRC32 on header and data for integrity
- **Volume identity**: `volume_hash` is precomputed u64 hash, avoids 32-byte UUID per I/O

### Used By

- Frontend → Chunk Engine: WRITE, READ, WRITE_ZEROES, UNMAP
- Chunk Engine → Chunk Engine: REPLICATE, EC_SHARD (replaces gRPC PutChunk/GetChunk)
- All components: PING/PONG for health

## Frontend Controller

### Binary

`novastor-frontend` — separate Rust binary, no SPDK dependency.

### Runtime

Tokio async runtime. No SPDK reactor, no hugepages, no privileged mode. Shares CPU with other workloads when idle (no 100% poll loop).

### NVMe-oF Target

Userspace NVMe-oF TCP implementation on tokio. The frontend implements the NVMe-oF TCP protocol directly — no kernel nvmet, no SPDK. Handles NVMe commands: read, write, write_zeroes, unmap, flush, reset.

### Connection Pool

Persistent TCP connections to ALL chunk engines at startup (one per engine). With 8 nodes, this is 8 connections. Connections are long-lived and reconnect automatically on failure.

### Write Path

```
1. NVMe-oF target receives write command
2. Compute chunk_idx, sb_idx, offset_in_sb from volume byte offset
3. CRUSH lookup → owner chunk engine for this chunk
4. Send NDP WRITE to owner (persistent connection)
5. Owner CE writes locally + REPLICATE to replicas (parallel)
6. Owner CE sends WRITE_RESP after write quorum achieved
7. Frontend completes NVMe-oF write command
```

If owner is unreachable: send write to any other chunk engine that has the data (CRUSH knows all replicas). That engine handles replication.

### Read Path

```
1. NVMe-oF target receives read command
2. Compute chunk_idx, sb_idx, offset_in_sb
3. CRUSH lookup → all chunk engines with this chunk
4. Pick closest engine (prefer local, then same rack, then any)
5. Send NDP READ
6. CE reads from local NVMe, sends READ_RESP
7. Frontend completes NVMe-oF read command
```

If preferred engine is unreachable: try next in CRUSH order.

### Read Preference Order

1. Local chunk engine (same node — zero network hop)
2. Same rack/zone
3. Any engine with the data

### ANA Configuration

- Every frontend runs an NVMe-oF subsystem for every volume it can serve
- Local frontend: ANA optimized (kernel routes I/O here)
- Remote frontends: ANA non-optimized (failover only)
- CSI ConnectMultipath connects to all frontends
- Kernel handles failover via ANA state change

### Failure Detection

- PING/PONG health checks on each chunk engine connection (every 1-2s)
- If a chunk engine becomes unreachable:
  - In-flight writes: retry on another engine that has the data
  - In-flight reads: retry on next engine in CRUSH order
  - Mark engine as down, stop routing new I/O to it
  - Reconnect in background, resume routing when healthy

## Chunk Engine Changes

### Removed

- NVMe-oF target (moves to frontend)
- CSI-related bdev presentation code

### Added

- NDP server on port 4500 (hostNetwork)
- NDP client for REPLICATE/EC_SHARD to other chunk engines

### Unchanged

- SPDK reactor pinned to A76 core
- Sub-block I/O (direct write, exact-size read)
- Atomic dirty bitmaps in BdevCtx
- DMA buffer pool, write context pool
- Background sync task (SHA-256, dirty bitmap destage)
- CRUSH map, protection policy
- gRPC control plane (volume lifecycle, topology)
- mTLS for gRPC control plane

### NDP Server

Runs on a tokio task (not the SPDK reactor). Receives NDP messages and dispatches I/O to the SPDK reactor via `spdk_thread_send_msg`. Completion is signaled back to the tokio task which sends the NDP response.

For the reactor-native fast path: the NDP server can submit I/O directly to the reactor and handle completion in the reactor callback — same pattern as the current `bdev_submit_request_cb` but triggered by NDP instead of NVMe-oF.

### Replication via NDP

Current gRPC `PutChunk`/`GetChunk` RPCs for inter-chunk-engine replication are replaced by NDP REPLICATE/EC_SHARD messages. Same semantics, ~10× lower overhead (no protobuf, no HTTP/2, no TLS framing on data path).

## Deployment

### Kubernetes Resources

| Component | K8s Resource | Image | Ports | Privileged |
|---|---|---|---|---|
| Frontend Controller | DaemonSet | `novastor-frontend` | 4430 (NVMe-oF) | No |
| Chunk Engine | DaemonSet | `novastor-dataplane` | 4500 (NDP) | Yes |
| Go Agent | DaemonSet | `novastor-agent` | 9100 (gRPC) | No |

### Frontend Controller Pod

- No hugepages
- No `/dev` access
- TLS certs mounted for mTLS to chunk engines (future)
- hostNetwork for NVMe-oF port
- Resource requests: 100m CPU, 128Mi memory

### Chunk Engine Pod

- Same as current dataplane DaemonSet
- Hugepages, privileged, NVMe access
- SPDK reactor pinned to A76 core (reactor_mask=0x10)
- NDP server on port 4500 (hostNetwork)

## Performance Expectations

### Latency (4K random write, qd=1)

| Path | Latency | Notes |
|---|---|---|
| Current (single process) | ~91μs | SPDK reactor handles everything |
| New, local CE | ~110μs | +10-20μs NDP hop on same node |
| New, remote CE | ~200-300μs | +100-200μs network hop |

### IOPS (qd=32)

| Metric | Current | Expected (local CE) | Expected (read fanout, rep=3) |
|---|---|---|---|
| 4K Random Write | 60K | ~50-55K | Same (writes go to owner) |
| 4K Random Read | 66K | ~60K | Up to ~180K aggregate |
| Seq Write | 740 MB/s | ~600 MB/s | Same |
| Seq Read | 846 MB/s | ~700 MB/s | Up to ~2.5 GB/s aggregate |

### Trade-off

10-15% reduction in single-node latency/IOPS in exchange for:
- Horizontal read scaling across all replica nodes
- Proper failover without kernel ANA complexity
- Multi-protocol support (NFS, S3) without touching chunk engine
- Frontend doesn't burn a reactor core

## Control Plane (unchanged)

gRPC remains for all control plane operations:
- Volume create/delete (Go agent → chunk engine)
- Topology/CRUSH map updates (Go agent → frontend + chunk engine)
- Chunk store initialization
- Health monitoring, metrics

Only data-path I/O moves to NDP.

## Implementation Order

1. **NDP protocol library** — Rust crate with header struct, encode/decode, CRC, connection handling
2. **NDP server in chunk engine** — Accept connections, dispatch I/O to SPDK reactor
3. **NDP client library** — Async connection pool, request/response matching, reconnect
4. **Frontend Controller binary** — Skeleton with NDP client, CRUSH map, volume registry
5. **Userspace NVMe-oF target** — NVMe-oF TCP protocol on tokio in the frontend
6. **Wire it up** — Frontend receives NVMe-oF, routes via NDP to chunk engines
7. **Replace gRPC replication** — Chunk engine uses NDP for REPLICATE/EC_SHARD
8. **CSI changes** — CreateTarget creates targets on frontends, ANA configuration
9. **Helm charts** — New DaemonSet for frontend controller
10. **Testing** — Benchmarks, failover, multi-protocol
