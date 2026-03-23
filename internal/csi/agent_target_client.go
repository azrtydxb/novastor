package csi

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/azrtydxb/novastor/internal/agent"
)

// NodeTargetClient implements AgentTargetClient by managing gRPC connections
// to agent nodes keyed by agent address. It is safe for concurrent use.
//
// When frontendPort is set, target creation is routed to the frontend controller
// on the same node (replacing the agent's port with the frontend's port).
type NodeTargetClient struct {
	mu           sync.Mutex
	clients      map[string]*agent.NVMeTargetClient
	dialOpts     []grpc.DialOption
	frontendPort string // If set, replace agent port with this for target operations.
}

// NewNodeTargetClient creates a NodeTargetClient that will dial agents using
// the provided dial options (e.g. TLS credentials).
func NewNodeTargetClient(opts ...grpc.DialOption) *NodeTargetClient {
	return &NodeTargetClient{
		clients:  make(map[string]*agent.NVMeTargetClient),
		dialOpts: opts,
	}
}

// NewFrontendTargetClient creates a NodeTargetClient that routes target
// creation to the frontend controller (port 9600) instead of the agent.
func NewFrontendTargetClient(frontendPort string, opts ...grpc.DialOption) *NodeTargetClient {
	return &NodeTargetClient{
		clients:      make(map[string]*agent.NVMeTargetClient),
		dialOpts:     opts,
		frontendPort: frontendPort,
	}
}

// resolveAddr returns the address to dial. If frontendPort is set, it replaces
// the port in the given address with the frontend port.
func (n *NodeTargetClient) resolveAddr(agentAddr string) string {
	if n.frontendPort == "" {
		return agentAddr
	}
	host, _, err := net.SplitHostPort(agentAddr)
	if err != nil {
		// Not a host:port — just use the address with the frontend port.
		return net.JoinHostPort(agentAddr, n.frontendPort)
	}
	return net.JoinHostPort(host, n.frontendPort)
}

// clientFor returns (and caches) an NVMeTargetClient for the given agent address.
func (n *NodeTargetClient) clientFor(agentAddr string) (*agent.NVMeTargetClient, error) {
	dialAddr := n.resolveAddr(agentAddr)

	// Frontend connections are not cached — the frontend may restart and
	// cached connections become stale. Agent connections are long-lived.
	if n.frontendPort != "" {
		c, err := agent.NewNVMeTargetClient(dialAddr, n.dialOpts...)
		if err != nil {
			return nil, fmt.Errorf("dialing NVMe target service at %s: %w", dialAddr, err)
		}
		return c, nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.clients[dialAddr]; ok {
		return c, nil
	}
	c, err := agent.NewNVMeTargetClient(dialAddr, n.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dialing NVMe target service at %s: %w", dialAddr, err)
	}
	n.clients[dialAddr] = c
	return c, nil
}

// CreateTarget implements AgentTargetClient.
func (n *NodeTargetClient) CreateTarget(ctx context.Context, agentAddr string, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32, prot protectionConfig) (string, string, string, error) {
	c, err := n.clientFor(agentAddr)
	if err != nil {
		return "", "", "", err
	}
	result, err := c.CreateTarget(ctx, volumeID, sizeBytes, anaState, anaGroupID, prot.toVolumeProtection())
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
