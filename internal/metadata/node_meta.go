package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// NodeMeta holds registration and capacity information for a storage agent node.
type NodeMeta struct {
	// NodeID is the unique identifier for the storage node.
	NodeID string `json:"nodeID"`

	// Address is the gRPC listen address of the agent (e.g. "host:9100").
	Address string `json:"address"`

	// DiskCount is the number of physical disks attached to the node.
	DiskCount int `json:"diskCount"`

	// TotalCapacity is the total raw storage capacity of the node in bytes.
	TotalCapacity int64 `json:"totalCapacity"`

	// AvailableCapacity is the currently available storage in bytes.
	AvailableCapacity int64 `json:"availableCapacity"`

	// Zone is the failure domain zone for CRUSH placement (e.g. "us-east-1a").
	Zone string `json:"zone"`

	// Rack is the failure domain rack for CRUSH placement (e.g. "rack-A").
	Rack string `json:"rack"`

	// LastHeartbeat is the Unix timestamp (seconds) of the last successful
	// heartbeat received from this node.
	LastHeartbeat int64 `json:"lastHeartbeat"`

	// Status is the human-readable node state: "ready" or "offline".
	Status string `json:"status"`
}

const bucketNodes = "nodes"

// PutNodeMeta stores or updates node metadata in the Raft store.
func (s *RaftStore) PutNodeMeta(_ context.Context, meta *NodeMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling NodeMeta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketNodes, Key: meta.NodeID, Value: data})
}

// GetNodeMeta retrieves node metadata by node ID.
func (s *RaftStore) GetNodeMeta(_ context.Context, nodeID string) (*NodeMeta, error) {
	data, err := s.fsm.Get(bucketNodes, nodeID)
	if err != nil {
		return nil, fmt.Errorf("node %s not found: %w", nodeID, err)
	}
	var meta NodeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling NodeMeta: %w", err)
	}
	return &meta, nil
}

// DeleteNodeMeta removes node metadata by node ID.
func (s *RaftStore) DeleteNodeMeta(_ context.Context, nodeID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketNodes, Key: nodeID})
}

// ListNodeMetas returns all registered storage nodes.
func (s *RaftStore) ListNodeMetas(_ context.Context) ([]*NodeMeta, error) {
	all, err := s.fsm.GetAll(bucketNodes)
	if err != nil {
		return nil, fmt.Errorf("listing node metas: %w", err)
	}
	metas := make([]*NodeMeta, 0, len(all))
	for _, raw := range all {
		var meta NodeMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling NodeMeta: %w", err)
		}
		metas = append(metas, &meta)
	}
	return metas, nil
}

// ListLiveNodeMetas returns nodes whose LastHeartbeat is within the given TTL.
// Nodes that have not sent a heartbeat recently are considered stale and excluded.
// This avoids accumulating dead entries from pods that have restarted with new IPs.
func (s *RaftStore) ListLiveNodeMetas(ctx context.Context, ttl time.Duration) ([]*NodeMeta, error) {
	all, err := s.ListNodeMetas(ctx)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-ttl).Unix()
	live := make([]*NodeMeta, 0, len(all))
	for _, m := range all {
		if m.LastHeartbeat >= cutoff {
			live = append(live, m)
		}
	}
	return live, nil
}
