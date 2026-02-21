# SPDK Data Plane

NovaStor's high-performance data plane uses [SPDK (Storage Performance Development Kit)](https://spdk.io/) to provide kernel-bypass NVMe-oF target and initiator functionality. The data plane is implemented in Rust with SPDK FFI bindings and controlled by the Go agent via JSON-RPC.

## Architecture

```mermaid
graph TB
    subgraph Node
        subgraph Go Control Plane
            AGENT["Node Agent<br/>(Go gRPC server)"]
            CSI_NODE["CSI Node Plugin"]
        end

        subgraph Rust Data Plane
            DP["novastor-dataplane<br/>(Rust + SPDK)"]
            REACTOR["SPDK Reactor<br/>(Polling Thread)"]
            BDEV["Bdev Manager"]
            NVMF["NVMe-oF Manager"]
            REPLICA["Replica Bdev"]
        end

        JSONRPC["JSON-RPC 2.0<br/>Unix Socket"]
    end

    AGENT -->|"control"| JSONRPC
    CSI_NODE -->|"connect"| JSONRPC
    JSONRPC --> DP
    DP --> REACTOR
    REACTOR --> BDEV
    REACTOR --> NVMF
    REACTOR --> REPLICA

    style AGENT fill:#f3e5f5
    style CSI_NODE fill:#f3e5f5
    style DP fill:#e8f5e9
    style REACTOR fill:#e8f5e9
    style BDEV fill:#e8f5e9
    style NVMF fill:#e8f5e9
    style REPLICA fill:#e8f5e9
    style JSONRPC fill:#fff4e6
```

## Data Flow: Replicated Volume

```mermaid
sequenceDiagram
    participant CSI as CSI Controller
    participant Agent as Node Agent (Go)
    participant DP as Data Plane (Rust)
    participant R1 as Replica 1 (Local)
    participant R2 as Replica 2 (Remote)
    participant R3 as Replica 3 (Remote)

    CSI->>Agent: CreateTarget(volumeID, size)
    Agent->>DP: bdev_lvol_create(vol, size)
    DP-->>Agent: lvol_name
    Agent->>DP: nvmf_create_target(nqn, addr, port, lvol)
    DP-->>Agent: OK
    Agent-->>CSI: nqn, addr, port

    Note over CSI: Volume published to pod

    CSI->>Agent: Setup replica bdev
    Agent->>DP: replica_bdev_create(targets=[R1,R2,R3])
    DP->>R1: NVMe-oF connect
    DP->>R2: NVMe-oF connect
    DP->>R3: NVMe-oF connect
    DP-->>Agent: replica bdev ready

    Note over DP: Write path: fan-out to all replicas, ACK on majority
    Note over DP: Read path: round-robin across healthy replicas
```

## Components

### novastor-dataplane (Rust binary)

The data-plane binary runs as a DaemonSet sidecar alongside the Go agent on each storage node. It provides:

| Component | Description |
|---|---|
| **SPDK Reactor** | Polling-mode event loop, avoids kernel context switches |
| **Bdev Manager** | Creates/deletes AIO, malloc, and logical volume bdevs |
| **NVMe-oF Manager** | Exposes bdevs as NVMe-oF/TCP targets, connects to remote targets |
| **Replica Bdev** | Custom bdev that replicates writes with quorum ACK and distributes reads |
| **JSON-RPC Server** | Unix domain socket server for Go control plane communication |

### JSON-RPC Interface

The Go agent communicates with the Rust data-plane over a Unix domain socket at `/var/tmp/novastor-spdk.sock` using JSON-RPC 2.0 (newline-delimited).

**Available methods:**

| Method | Description |
|---|---|
| `bdev_aio_create` | Create an AIO bdev backed by a file |
| `bdev_malloc_create` | Create an in-memory bdev (testing) |
| `bdev_lvol_create_lvstore` | Create a logical volume store |
| `bdev_lvol_create` | Create a logical volume |
| `bdev_delete` | Delete a bdev |
| `nvmf_create_target` | Create NVMe-oF target subsystem |
| `nvmf_delete_target` | Delete NVMe-oF target subsystem |
| `nvmf_connect_initiator` | Connect to a remote NVMe-oF target |
| `nvmf_disconnect_initiator` | Disconnect from a remote target |
| `nvmf_export_local` | Create loopback NVMe-oF for local consumption |
| `replica_bdev_create` | Create a replica bdev across targets |
| `replica_bdev_status` | Query replica health |
| `get_version` | Get data-plane version |

### Go Integration

| Package | File | Description |
|---|---|---|
| `internal/spdk` | `client.go` | JSON-RPC client with typed methods |
| `internal/spdk` | `process.go` | Start/stop/monitor the data-plane binary |
| `internal/agent` | `spdk_target_server.go` | SPDK-based NVMe-oF target gRPC service |
| `internal/agent` | `spdk_replica.go` | Replica bdev setup helper |
| `internal/csi` | `spdk_initiator.go` | SPDK-based NVMe-oF initiator |

## Configuration

### Feature Flag

The data plane mode is selected via the `--data-plane` flag:

```bash
# Agent
novastor-agent --data-plane=spdk --spdk-socket=/var/tmp/novastor-spdk.sock

# CSI Driver
novastor-csi --data-plane=spdk --spdk-socket=/var/tmp/novastor-spdk.sock
```

### Helm Values

```yaml
dataplane:
  enabled: true
  reactorMask: "0x1"     # CPU cores for SPDK reactor (hex mask)
  memSize: 2048           # HugePages memory in MB
  resources:
    limits:
      hugepages-2Mi: 2Gi  # Must match memSize
```

### HugePages

SPDK requires HugePages for DMA-safe memory. Each node must have HugePages configured:

```bash
# Configure 2GB of 2MB HugePages
echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

## Build

### Stub Mode (no SPDK libraries needed)

```bash
make build-dataplane
```

### Full SPDK Mode

```bash
make build-dataplane-spdk
```

### Docker Image

```bash
make docker-build-dataplane
```

## Deployment Topology

```mermaid
graph LR
    subgraph Node 1
        A1["Agent Pod"]
        D1["Dataplane Pod"]
        A1 ---|socket| D1
    end

    subgraph Node 2
        A2["Agent Pod"]
        D2["Dataplane Pod"]
        A2 ---|socket| D2
    end

    subgraph Node 3
        A3["Agent Pod"]
        D3["Dataplane Pod"]
        A3 ---|socket| D3
    end

    D1 ---|"NVMe-oF/TCP"| D2
    D1 ---|"NVMe-oF/TCP"| D3
    D2 ---|"NVMe-oF/TCP"| D3

    style A1 fill:#f3e5f5
    style A2 fill:#f3e5f5
    style A3 fill:#f3e5f5
    style D1 fill:#e8f5e9
    style D2 fill:#e8f5e9
    style D3 fill:#e8f5e9
```

Each node runs the Go agent (control plane) and the Rust data-plane (SPDK) as separate DaemonSet pods sharing a Unix socket via hostPath volume. Data replication between nodes uses NVMe-oF/TCP directly between data-plane instances.
