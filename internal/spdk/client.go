// Package spdk provides a Go client for the NovaStor SPDK data-plane process.
// Communication uses JSON-RPC 2.0 over a Unix domain socket.
package spdk

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

const (
	// DefaultSocketPath is the Unix domain socket used by the data-plane.
	DefaultSocketPath = "/var/tmp/novastor-spdk.sock"
)

// Client talks to the novastor-dataplane Rust process via JSON-RPC 2.0.
type Client struct {
	socketPath string
	mu         sync.Mutex
	conn       net.Conn
	reader     *bufio.Reader
	nextID     atomic.Int64
}

// NewClient creates a new SPDK JSON-RPC client.
func NewClient(socketPath string) *Client {
	return &Client{socketPath: socketPath}
}

// Connect establishes a connection to the data-plane socket.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := net.Dial("unix", c.socketPath)
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

// call sends a JSON-RPC request and waits for the response.
func (c *Client) call(method string, params interface{}, result interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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

	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("writing request: %w", err)
	}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("reading response: %w", err)
	}

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
func (c *Client) CreateLvolStore(bdevName, lvsName string) (string, error) {
	params := map[string]interface{}{
		"bdev_name": bdevName,
		"lvs_name":  lvsName,
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
func (c *Client) CreateLvol(lvsName, lvolName string, sizeMB uint64) (string, error) {
	params := map[string]interface{}{
		"lvs_name":  lvsName,
		"lvol_name": lvolName,
		"size_mb":   sizeMB,
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

// CreateNvmfTarget creates an NVMe-oF target subsystem exposing a bdev.
func (c *Client) CreateNvmfTarget(nqn, listenAddr string, port uint16, bdevName string) error {
	params := map[string]interface{}{
		"nqn":         nqn,
		"listen_addr": listenAddr,
		"port":        port,
		"bdev_name":   bdevName,
	}
	return c.call("nvmf_create_target", params, nil)
}

// DeleteNvmfTarget removes an NVMe-oF target subsystem.
func (c *Client) DeleteNvmfTarget(nqn string) error {
	return c.call("nvmf_delete_target", map[string]interface{}{"nqn": nqn}, nil)
}

// ConnectInitiator connects to a remote NVMe-oF target and returns the local bdev name.
func (c *Client) ConnectInitiator(addr string, port uint16, nqn string) (string, error) {
	params := map[string]interface{}{
		"addr": addr,
		"port": port,
		"nqn":  nqn,
	}
	var result struct {
		BdevName string `json:"bdev_name"`
	}
	if err := c.call("nvmf_connect_initiator", params, &result); err != nil {
		return "", err
	}
	return result.BdevName, nil
}

// DisconnectInitiator disconnects from a remote NVMe-oF target.
func (c *Client) DisconnectInitiator(nqn string) error {
	return c.call("nvmf_disconnect_initiator", map[string]interface{}{"nqn": nqn}, nil)
}

// ExportLocal creates an NVMe-oF target for local consumption.
func (c *Client) ExportLocal(nqn, listenAddr string, port uint16, bdevName string) error {
	params := map[string]interface{}{
		"nqn":         nqn,
		"listen_addr": listenAddr,
		"port":        port,
		"bdev_name":   bdevName,
	}
	return c.call("nvmf_export_local", params, nil)
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
	VolumeID     string              `json:"volume_id"`
	Replicas     []ReplicaStatusInfo `json:"replicas"`
	WriteQuorum  uint32              `json:"write_quorum"`
	HealthyCount uint32              `json:"healthy_count"`
}

// ReplicaStatusInfo describes the state and I/O statistics of a single replica.
type ReplicaStatusInfo struct {
	Address         string `json:"address"`
	Port            uint16 `json:"port"`
	State           string `json:"state"`
	ReadsCompleted  uint64 `json:"reads_completed"`
	WritesCompleted uint64 `json:"writes_completed"`
	ReadErrors      uint64 `json:"read_errors"`
	WriteErrors     uint64 `json:"write_errors"`
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
