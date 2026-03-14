package csi

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/azrtydxb/novastor/internal/agent"
)

// NodeTargetClient implements AgentTargetClient by managing gRPC connections
// to agent nodes keyed by agent address. It is safe for concurrent use.
type NodeTargetClient struct {
	mu       sync.Mutex
	clients  map[string]*agent.NVMeTargetClient
	dialOpts []grpc.DialOption
}

// NewNodeTargetClient creates a NodeTargetClient that will dial agents using
// the provided dial options (e.g. TLS credentials).
func NewNodeTargetClient(opts ...grpc.DialOption) *NodeTargetClient {
	return &NodeTargetClient{
		clients:  make(map[string]*agent.NVMeTargetClient),
		dialOpts: opts,
	}
}

// clientFor returns (and caches) an NVMeTargetClient for the given agent address.
func (n *NodeTargetClient) clientFor(agentAddr string) (*agent.NVMeTargetClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.clients[agentAddr]; ok {
		return c, nil
	}
	c, err := agent.NewNVMeTargetClient(agentAddr, n.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dialing NVMe target service at %s: %w", agentAddr, err)
	}
	n.clients[agentAddr] = c
	return c, nil
}

// CreateTarget implements AgentTargetClient.
func (n *NodeTargetClient) CreateTarget(ctx context.Context, agentAddr string, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32) (string, string, string, error) {
	c, err := n.clientFor(agentAddr)
	if err != nil {
		return "", "", "", err
	}
	result, err := c.CreateTarget(ctx, volumeID, sizeBytes, anaState, anaGroupID)
	if err != nil {
		return "", "", "", err
	}
	return result.SubsystemNQN, result.TargetAddress, result.TargetPort, nil
}

// DeleteTarget implements AgentTargetClient.
func (n *NodeTargetClient) DeleteTarget(ctx context.Context, agentAddr string, volumeID string) error {
	c, err := n.clientFor(agentAddr)
	if err != nil {
		return err
	}
	return c.DeleteTarget(ctx, volumeID)
}

// SetANAState implements AgentTargetClient.
func (n *NodeTargetClient) SetANAState(ctx context.Context, agentAddr string, volumeID string, anaState string, anaGroupID uint32) error {
	c, err := n.clientFor(agentAddr)
	if err != nil {
		return err
	}
	return c.SetANAState(ctx, volumeID, anaState, anaGroupID)
}

// SetupReplication implements AgentTargetClient.
func (n *NodeTargetClient) SetupReplication(ctx context.Context, agentAddr, volumeID, localBdevName string, remoteTargets []ReplicaTarget) (string, string, error) {
	c, err := n.clientFor(agentAddr)
	if err != nil {
		return "", "", err
	}
	// Convert CSI ReplicaTarget to agent ReplicaTargetInfo.
	agentTargets := make([]agent.ReplicaTargetInfo, len(remoteTargets))
	for i, rt := range remoteTargets {
		agentTargets[i] = agent.ReplicaTargetInfo{
			Address: rt.Address,
			Port:    rt.Port,
			NQN:     rt.NQN,
		}
	}
	result, err := c.SetupReplication(ctx, volumeID, localBdevName, agentTargets)
	if err != nil {
		return "", "", err
	}
	return result.ReplicaBdevName, result.SubsystemNQN, nil
}

// Close closes all cached gRPC connections.
func (n *NodeTargetClient) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var firstErr error
	for addr, client := range n.clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("closing connection to %s: %w", addr, err)
		}
	}
	n.clients = make(map[string]*agent.NVMeTargetClient)
	return firstErr
}
