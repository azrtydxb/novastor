// Package spdk provides a Go client for the NovaStor SPDK data-plane process.
// Communication uses JSON-RPC 2.0 over a Unix domain socket.
package spdk

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultSocketPath is the Unix domain socket used by the data-plane.
	DefaultSocketPath = "/var/tmp/novastor-spdk.sock"
)

const (
	// NativeSocketPath is the SPDK built-in JSON-RPC socket. We use this
	// for NVMe-oF target operations because SPDK's own RPC handlers run
	// directly on the reactor thread, which is required for the TCP
	// transport's accept poller to function correctly.
	NativeSocketPath = "/var/tmp/spdk.sock"
)

// Client talks to the novastor-dataplane Rust process via JSON-RPC 2.0.
type Client struct {
	socketPath string
	mu         sync.Mutex
	conn       net.Conn
	reader     *bufio.Reader
	nextID     atomic.Int64

	// nativeMu serialises calls to SPDK's built-in RPC socket for
	// NVMe-oF target operations. Each call uses a fresh connection.
	nativeMu      sync.Mutex
	nativeID      atomic.Int64
	transportInit sync.Once
}

// NewClient creates a new SPDK JSON-RPC client.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// Connect establishes a connection to the data-plane socket.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("connecting to SPDK data-plane at %s: %w", c.socketPath, err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return nil
}

// Close tears down the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// rpcRequest is the JSON-RPC 2.0 request envelope.
type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int64       `json:"id"`
}

// rpcResponse is the JSON-RPC 2.0 response envelope.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      int64           `json:"id"`
}

type rpcError struct {
	Code    int64  `json:"code"`
	Message string `json:"message"`
}

// callTimeout is the maximum time to wait for an SPDK data-plane response.
// If the data-plane hangs (e.g. due to a bug in async callback handling),
// this prevents the Go client from blocking forever and holding the mutex.
const callTimeout = 30 * time.Second

// reconnectLocked closes the current connection and establishes a new one.
// Must be called with c.mu held.
func (c *Client) reconnectLocked() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("reconnecting to SPDK data-plane at %s: %w", c.socketPath, err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	return nil
}

// call sends a JSON-RPC request and waits for the response. On connection
// errors (broken pipe, EOF, timeout), it reconnects and retries once.
func (c *Client) call(method string, params interface{}, result interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.callLocked(method, params, result)
	if err == nil {
		return nil
	}

	// Retry on connection-level errors (broken pipe, EOF, connection reset).
	if isConnectionError(err) {
		if reconnErr := c.reconnectLocked(); reconnErr != nil {
			return fmt.Errorf("%s failed: %w (reconnect also failed: %v)", method, err, reconnErr)
		}
		return c.callLocked(method, params, result)
	}

	return err
}

// isConnectionError returns true for errors that indicate the socket
// connection is dead and a reconnect may help.
func isConnectionError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "EOF") ||
		strings.Contains(s, "not connected")
}

// callLocked performs the actual JSON-RPC call. Must be called with c.mu held.
func (c *Client) callLocked(method string, params interface{}, result interface{}) error {
	if c.conn == nil {
		return fmt.Errorf("spdk client: not connected")
	}

	id := c.nextID.Add(1)
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}
	data = append(data, '\n')

	// Set a write deadline to avoid blocking if the socket buffer is full.
	if err := c.conn.SetWriteDeadline(time.Now().Add(callTimeout)); err != nil {
		return fmt.Errorf("setting write deadline: %w", err)
	}

	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("writing request: %w", err)
	}

	// Set a read deadline so we don't block forever if the data-plane
	// fails to send a response (e.g. SPDK async callback never fires).
	if err := c.conn.SetReadDeadline(time.Now().Add(callTimeout)); err != nil {
		return fmt.Errorf("setting read deadline: %w", err)
	}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("reading response for %s: %w", method, err)
	}

	// Clear deadlines for the next call.
	_ = c.conn.SetWriteDeadline(time.Time{})
	_ = c.conn.SetReadDeadline(time.Time{})

	var resp rpcResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Errorf("unmarshalling response: %w", err)
	}

	if resp.Error != nil {
		return fmt.Errorf("spdk rpc error %d: %s", resp.Error.Code, resp.Error.Message)
	}

	if result != nil && resp.Result != nil {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("unmarshalling result: %w", err)
		}
	}

	return nil
}

// --------------------------------------------------------------------------
// Typed RPC methods
// --------------------------------------------------------------------------

// CreateAioBdev creates an AIO bdev backed by a file.
func (c *Client) CreateAioBdev(name, filename string, blockSize uint32) error {
	params := map[string]interface{}{
		"name":       name,
		"filename":   filename,
		"block_size": blockSize,
	}
	return c.call("bdev_aio_create", params, nil)
}

// CreateMallocBdev creates an in-memory bdev for testing.
func (c *Client) CreateMallocBdev(name string, sizeMB uint64, blockSize uint32) error {
	params := map[string]interface{}{
		"name":       name,
		"size_mb":    sizeMB,
		"block_size": blockSize,
	}
	return c.call("bdev_malloc_create", params, nil)
}

// CreateLvolStore creates a logical volume store on a bdev.
// The store name is auto-generated by the data-plane as "lvs_{bdevName}".
func (c *Client) CreateLvolStore(bdevName string) (string, error) {
	params := map[string]interface{}{
		"base_bdev": bdevName,
	}
	var result struct {
		UUID string `json:"uuid"`
	}
	if err := c.call("bdev_lvol_create_lvstore", params, &result); err != nil {
		return "", err
	}
	return result.UUID, nil
}

// CreateLvol creates a logical volume inside a store.
func (c *Client) CreateLvol(lvsName, volumeID string, sizeBytes uint64) (string, error) {
	params := map[string]interface{}{
		"lvol_store": lvsName,
		"volume_id":  volumeID,
		"size_bytes": sizeBytes,
	}
	var result struct {
		Name string `json:"name"`
	}
	if err := c.call("bdev_lvol_create", params, &result); err != nil {
		return "", err
	}
	return result.Name, nil
}

// DeleteBdev removes a bdev by name.
func (c *Client) DeleteBdev(name string) error {
	return c.call("bdev_delete", map[string]interface{}{"name": name}, nil)
}

// NativeCreateMallocBdev creates an in-memory bdev via SPDK's built-in RPC.
func (c *Client) NativeCreateMallocBdev(name string, sizeMB uint64, blockSize uint32) error {
	numBlocks := (sizeMB * 1024 * 1024) / uint64(blockSize)
	return c.nativeCall("bdev_malloc_create", map[string]interface{}{
		"name":       name,
		"num_blocks": numBlocks,
		"block_size": blockSize,
	}, nil)
}

// NativeCreateLvolStore creates a logical volume store via SPDK's built-in RPC.
func (c *Client) NativeCreateLvolStore(bdevName, lvsName string) (string, error) {
	var result string
	if err := c.nativeCall("bdev_lvol_create_lvstore", map[string]interface{}{
		"bdev_name":  bdevName,
		"lvs_name":   lvsName,
		"cluster_sz": 1048576, // 1MB clusters for small test bdevs
	}, &result); err != nil {
		return "", err
	}
	return result, nil
}

// NativeGetLvolStores lists lvol stores via SPDK's built-in RPC.
// If lvsName is non-empty, only that store is returned.
func (c *Client) NativeGetLvolStores(lvsName string) ([]LvolStoreInfo, error) {
	params := map[string]interface{}{}
	if lvsName != "" {
		params["lvs_name"] = lvsName
	}
	var result []LvolStoreInfo
	if err := c.nativeCall("bdev_lvol_get_lvstores", params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// LvolStoreInfo describes an lvol store returned by bdev_lvol_get_lvstores.
type LvolStoreInfo struct {
	UUID          string `json:"uuid"`
	Name          string `json:"name"`
	BaseBdev      string `json:"base_bdev"`
	FreeClusters  uint64 `json:"free_clusters"`
	TotalClusters uint64 `json:"total_data_clusters"`
}

// NativeCreateLvol creates an lvol via SPDK's built-in RPC.
// sizeBytes is converted to MiB (rounded up) since SPDK expects size_in_mib.
func (c *Client) NativeCreateLvol(lvsName, lvolName string, sizeBytes uint64) (string, error) {
	sizeMiB := (sizeBytes + 1048575) / 1048576 // round up
	var result string
	if err := c.nativeCall("bdev_lvol_create", map[string]interface{}{
		"lvs_name":    lvsName,
		"lvol_name":   lvolName,
		"size_in_mib": sizeMiB,
	}, &result); err != nil {
		return "", err
	}
	return result, nil
}

// NativeDeleteBdev removes a bdev via SPDK's built-in RPC.
func (c *Client) NativeDeleteBdev(name string) error {
	return c.nativeCall("bdev_lvol_delete", map[string]interface{}{
		"name": name,
	}, nil)
}

// nativeCall sends a JSON-RPC request to SPDK's built-in RPC socket.
// This is used for NVMe-oF target operations where SPDK's own handlers
// must run directly on the reactor thread for correct accept poller behavior.
//
// Each call uses a fresh connection. SPDK's nvmf_subsystem_add_listener
// handler is async (pause → add listener → resume) and the TCP accept
// poller only picks up the new listen socket after the connection handler
// fully completes. Reusing a persistent connection can cause the response
// to arrive before the async work is finalized on the reactor, leaving
// the listen socket unregistered in the accept poller.
func (c *Client) nativeCall(method string, params interface{}, result interface{}) error {
	c.nativeMu.Lock()
	defer c.nativeMu.Unlock()

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "unix", NativeSocketPath)
	if err != nil {
		return fmt.Errorf("connecting to SPDK native RPC at %s: %w", NativeSocketPath, err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	id := c.nativeID.Add(1)
	req := rpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}
	data = append(data, '\n')

	if err := conn.SetWriteDeadline(time.Now().Add(callTimeout)); err != nil {
		return fmt.Errorf("setting write deadline: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("writing request: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(callTimeout)); err != nil {
		return fmt.Errorf("setting read deadline: %w", err)
	}

	line, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("reading response for %s: %w", method, err)
	}

	var resp rpcResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Errorf("unmarshalling response: %w", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("spdk native rpc error %d: %s", resp.Error.Code, resp.Error.Message)
	}
	if result != nil && resp.Result != nil {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			return fmt.Errorf("unmarshalling result: %w", err)
		}
	}
	return nil
}

// ensureNativeTransport creates the TCP transport via SPDK's native RPC once.
func (c *Client) ensureNativeTransport() error {
	var initErr error
	c.transportInit.Do(func() {
		err := c.nativeCall("nvmf_create_transport", map[string]interface{}{
			"trtype": "TCP",
		}, nil)
		if err != nil {
			// Transport may already exist if the dataplane wasn't restarted.
			if !strings.Contains(err.Error(), "already exists") {
				initErr = err
			}
		}
	})
	return initErr
}

// CreateNvmfTarget creates an NVMe-oF target subsystem exposing a bdev.
// Uses SPDK's built-in RPC methods which run directly on the reactor thread,
// ensuring the TCP transport's accept poller functions correctly.
func (c *Client) CreateNvmfTarget(volumeID, listenAddr string, port uint16, bdevName string) (string, error) {
	if err := c.ensureNativeTransport(); err != nil {
		return "", fmt.Errorf("ensuring NVMe-oF transport: %w", err)
	}

	nqn := fmt.Sprintf("nqn.2024-01.io.novastor:volume-%s", volumeID)
	portStr := fmt.Sprintf("%d", port)

	// Step 1: Create subsystem.
	if err := c.nativeCall("nvmf_create_subsystem", map[string]interface{}{
		"nqn":            nqn,
		"allow_any_host": true,
	}, nil); err != nil {
		return "", fmt.Errorf("creating NVMe-oF subsystem: %w", err)
	}

	// Step 2: Add namespace (bdev).
	if err := c.nativeCall("nvmf_subsystem_add_ns", map[string]interface{}{
		"nqn":       nqn,
		"namespace": map[string]interface{}{"bdev_name": bdevName},
	}, nil); err != nil {
		// Best-effort cleanup.
		_ = c.nativeCall("nvmf_delete_subsystem", map[string]interface{}{"nqn": nqn}, nil)
		return "", fmt.Errorf("adding namespace to subsystem: %w", err)
	}

	// Step 3: Add listener with the specific host IP so the SPDK listener
	// ACL matches the address clients connect to.
	if err := c.nativeCall("nvmf_subsystem_add_listener", map[string]interface{}{
		"nqn": nqn,
		"listen_address": map[string]interface{}{
			"trtype":  "TCP",
			"adrfam":  "IPv4",
			"traddr":  listenAddr,
			"trsvcid": portStr,
		},
	}, nil); err != nil {
		_ = c.nativeCall("nvmf_delete_subsystem", map[string]interface{}{"nqn": nqn}, nil)
		return "", fmt.Errorf("adding listener to subsystem: %w", err)
	}

	return nqn, nil
}

// DeleteNvmfTarget removes an NVMe-oF target subsystem by volume ID.
func (c *Client) DeleteNvmfTarget(volumeID string) error {
	nqn := fmt.Sprintf("nqn.2024-01.io.novastor:volume-%s", volumeID)
	return c.nativeCall("nvmf_delete_subsystem", map[string]interface{}{"nqn": nqn}, nil)
}

// ConnectInitiator connects to a remote NVMe-oF target and returns the local bdev name.
func (c *Client) ConnectInitiator(remoteAddr string, remotePort uint16, nqn, bdevName string) (string, error) {
	params := map[string]interface{}{
		"remote_address": remoteAddr,
		"remote_port":    remotePort,
		"nqn":            nqn,
		"bdev_name":      bdevName,
	}
	var result struct {
		BdevName string `json:"bdev_name"`
	}
	if err := c.call("nvmf_connect_initiator", params, &result); err != nil {
		return "", err
	}
	return result.BdevName, nil
}

// DisconnectInitiator disconnects from a remote NVMe-oF target by bdev name.
func (c *Client) DisconnectInitiator(bdevName string) error {
	return c.call("nvmf_disconnect_initiator", map[string]interface{}{"bdev_name": bdevName}, nil)
}

// ExportLocal creates an NVMe-oF target for local consumption. Returns the NQN.
func (c *Client) ExportLocal(volumeID, bdevName string) (string, error) {
	params := map[string]interface{}{
		"volume_id": volumeID,
		"bdev_name": bdevName,
	}
	var result struct {
		NQN string `json:"nqn"`
	}
	if err := c.call("nvmf_export_local", params, &result); err != nil {
		return "", err
	}
	return result.NQN, nil
}

// ReplicaTarget describes a single replica endpoint.
type ReplicaTarget struct {
	Addr    string `json:"addr"`
	Port    uint16 `json:"port"`
	NQN     string `json:"nqn"`
	IsLocal bool   `json:"is_local"`
}

// ReplicaBdevStatus represents the status of a replica bdev.
type ReplicaBdevStatus struct {
	VolumeID             string              `json:"volume_id"`
	Replicas             []ReplicaStatusInfo `json:"replicas"`
	WriteQuorum          uint32              `json:"write_quorum"`
	HealthyCount         uint32              `json:"healthy_count"`
	TotalReadIOPS        uint64              `json:"total_read_iops"`
	TotalWriteIOPS       uint64              `json:"total_write_iops"`
	WriteQuorumLatencyUs uint64              `json:"write_quorum_latency_us"`
}

// ReplicaStatusInfo describes the state and I/O statistics of a single replica.
type ReplicaStatusInfo struct {
	Address          string `json:"address"`
	Port             uint16 `json:"port"`
	State            string `json:"state"`
	ReadsCompleted   uint64 `json:"reads_completed"`
	WritesCompleted  uint64 `json:"writes_completed"`
	ReadErrors       uint64 `json:"read_errors"`
	WriteErrors      uint64 `json:"write_errors"`
	ReadBytes        uint64 `json:"read_bytes"`
	AvgReadLatencyUs uint64 `json:"avg_read_latency_us"`
}

// IOStats represents per-volume I/O statistics from the data-plane.
type IOStats struct {
	VolumeID             string           `json:"volume_id"`
	Replicas             []ReplicaIOStats `json:"replicas"`
	TotalReadIOPS        uint64           `json:"total_read_iops"`
	TotalWriteIOPS       uint64           `json:"total_write_iops"`
	WriteQuorumLatencyUs uint64           `json:"write_quorum_latency_us"`
}

// ReplicaIOStats describes per-replica I/O distribution metrics.
type ReplicaIOStats struct {
	Addr             string `json:"addr"`
	Port             uint16 `json:"port"`
	ReadsCompleted   uint64 `json:"reads_completed"`
	ReadBytes        uint64 `json:"read_bytes"`
	AvgReadLatencyUs uint64 `json:"avg_read_latency_us"`
}

// CreateReplicaBdev creates a composite bdev that replicates writes across targets.
func (c *Client) CreateReplicaBdev(name string, targets []ReplicaTarget, readPolicy string) error {
	params := map[string]interface{}{
		"name":        name,
		"targets":     targets,
		"read_policy": readPolicy,
	}
	return c.call("replica_bdev_create", params, nil)
}

// InitChunkBackend initialises the chunk storage backend on the given bdev.
func (c *Client) InitChunkBackend(bdevName string, capacityBytes uint64) error {
	params := map[string]interface{}{
		"bdev_name":      bdevName,
		"capacity_bytes": capacityBytes,
	}
	return c.call("backend.init_chunk", params, nil)
}

// CreateChunkVolume creates a volume in the chunk backend.
func (c *Client) CreateChunkVolume(name string, sizeBytes uint64, thin bool) error {
	params := map[string]interface{}{
		"backend":    "chunk",
		"name":       name,
		"size_bytes": sizeBytes,
		"thin":       thin,
	}
	return c.call("backend.create_volume", params, nil)
}

// DeleteChunkVolume deletes a volume from the chunk backend.
func (c *Client) DeleteChunkVolume(name string) error {
	params := map[string]interface{}{
		"backend": "chunk",
		"name":    name,
	}
	return c.call("backend.delete_volume", params, nil)
}

// GetVersion returns the data-plane version info.
func (c *Client) GetVersion() (string, error) {
	var result struct {
		Version string `json:"version"`
		Name    string `json:"name"`
	}
	if err := c.call("get_version", struct{}{}, &result); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s %s", result.Name, result.Version), nil
}

// SetANAState sets the ANA state for an NVMe-oF target subsystem.
func (c *Client) SetANAState(nqn string, anaGroupID uint32, state string) error {
	return c.call("nvmf_set_ana_state", map[string]interface{}{
		"nqn":          nqn,
		"ana_group_id": anaGroupID,
		"ana_state":    state,
	}, nil)
}

// GetANAState retrieves the ANA state for an NVMe-oF target subsystem.
func (c *Client) GetANAState(nqn string) (uint32, string, error) {
	var result struct {
		GroupID uint32 `json:"ana_group_id"`
		State   string `json:"ana_state"`
	}
	if err := c.call("nvmf_get_ana_state", map[string]interface{}{"nqn": nqn}, &result); err != nil {
		return 0, "", err
	}
	return result.GroupID, result.State, nil
}

// AddReplica dynamically adds a replica target to a running replica bdev.
func (c *Client) AddReplica(bdevName string, target ReplicaTarget) error {
	return c.call("replica_bdev_add_replica", map[string]interface{}{
		"volume_id": bdevName,
		"target":    target,
	}, nil)
}

// RemoveReplica dynamically removes a replica target from a running replica bdev.
func (c *Client) RemoveReplica(bdevName, targetAddr string) error {
	return c.call("replica_bdev_remove_replica", map[string]interface{}{
		"volume_id": bdevName,
		"address":   targetAddr,
	}, nil)
}

// GetReplicaBdevStatus returns the detailed status of a replica bdev.
func (c *Client) GetReplicaBdevStatus(bdevName string) (*ReplicaBdevStatus, error) {
	var result ReplicaBdevStatus
	if err := c.call("replica_bdev_status", map[string]interface{}{"volume_id": bdevName}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetIOStats returns per-volume I/O statistics including per-replica read distribution.
func (c *Client) GetIOStats(volumeID string) (*IOStats, error) {
	var result IOStats
	if err := c.call("novastor_io_stats", map[string]interface{}{"volume_id": volumeID}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
