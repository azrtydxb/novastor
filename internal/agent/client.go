package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	pb "github.com/azrtydxb/novastor/api/proto/chunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps a gRPC connection to a chunk server (agent).
type Client struct {
	conn   *grpc.ClientConn
	client pb.ChunkServiceClient
}

// Dial connects to the chunk server at the given address. If no dial options
// are provided, insecure credentials are used by default.
func Dial(addr string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing chunk server %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		client: pb.NewChunkServiceClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// PutChunk stores a chunk with the given ID, data, and CRC-32C checksum.
func (c *Client) PutChunk(ctx context.Context, chunkID string, data []byte, checksum uint32) error {
	stream, err := c.client.PutChunk(ctx)
	if err != nil {
		return fmt.Errorf("opening PutChunk stream: %w", err)
	}

	if err := stream.Send(&pb.PutChunkRequest{
		ChunkId:  chunkID,
		Data:     data,
		Checksum: checksum,
	}); err != nil {
		return fmt.Errorf("sending PutChunkRequest: %w", err)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("closing PutChunk stream: %w", err)
	}

	return nil
}

// GetChunk retrieves a chunk by ID. It returns the concatenated data and the
// checksum from the first streaming response message.
func (c *Client) GetChunk(ctx context.Context, chunkID string) (data []byte, checksum uint32, err error) {
	stream, err := c.client.GetChunk(ctx, &pb.GetChunkRequest{ChunkId: chunkID})
	if err != nil {
		return nil, 0, fmt.Errorf("calling GetChunk: %w", err)
	}

	first := true
	for {
		resp, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return nil, 0, fmt.Errorf("receiving GetChunk stream: %w", recvErr)
		}
		if first {
			checksum = resp.GetChecksum()
			first = false
		}
		data = append(data, resp.GetData()...)
	}

	return data, checksum, nil
}

// DeleteChunk removes a chunk by ID.
func (c *Client) DeleteChunk(ctx context.Context, chunkID string) error {
	_, err := c.client.DeleteChunk(ctx, &pb.DeleteChunkRequest{ChunkId: chunkID})
	if err != nil {
		return fmt.Errorf("deleting chunk %s: %w", chunkID, err)
	}
	return nil
}

// HasChunk checks whether a chunk exists on the server.
func (c *Client) HasChunk(ctx context.Context, chunkID string) (bool, error) {
	resp, err := c.client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: chunkID})
	if err != nil {
		return false, fmt.Errorf("checking chunk %s: %w", chunkID, err)
	}
	return resp.GetExists(), nil
}

// ---------------------------------------------------------------------------
// NodeChunkClient — adapts Client to satisfy the CSI ChunkClient interface.
// It maintains a map of nodeID -> *Client connections.
// ---------------------------------------------------------------------------

// NodeChunkClient manages per-node gRPC connections to chunk servers.
type NodeChunkClient struct {
	mu      sync.RWMutex
	clients map[string]*Client // nodeID -> client
	opts    []grpc.DialOption
}

// NewNodeChunkClient creates a NodeChunkClient with the given dial options.
func NewNodeChunkClient(opts ...grpc.DialOption) *NodeChunkClient {
	return &NodeChunkClient{
		clients: make(map[string]*Client),
		opts:    opts,
	}
}

// AddNode dials the chunk server at addr and registers it under nodeID.
func (n *NodeChunkClient) AddNode(nodeID, addr string) error {
	c, err := Dial(addr, n.opts...)
	if err != nil {
		return fmt.Errorf("adding node %s at %s: %w", nodeID, addr, err)
	}
	n.mu.Lock()
	old, exists := n.clients[nodeID]
	n.clients[nodeID] = c
	n.mu.Unlock()
	if exists {
		_ = old.Close()
	}
	return nil
}

// RemoveNode closes and removes the connection for the given nodeID.
func (n *NodeChunkClient) RemoveNode(nodeID string) {
	n.mu.Lock()
	c, exists := n.clients[nodeID]
	delete(n.clients, nodeID)
	n.mu.Unlock()
	if exists {
		_ = c.Close()
	}
}

func (n *NodeChunkClient) getClient(nodeID string) (*Client, error) {
	n.mu.RLock()
	c, ok := n.clients[nodeID]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no connection for node %q", nodeID)
	}
	return c, nil
}

// GetChunk satisfies the CSI ChunkClient interface.
func (n *NodeChunkClient) GetChunk(ctx context.Context, nodeID string, chunkID string) ([]byte, error) {
	c, err := n.getClient(nodeID)
	if err != nil {
		return nil, err
	}
	data, _, err := c.GetChunk(ctx, chunkID)
	return data, err
}

// PutChunk satisfies the CSI ChunkClient interface. It computes the CRC-32C
// checksum automatically.
func (n *NodeChunkClient) PutChunk(ctx context.Context, nodeID string, chunkID string, data []byte) error {
	c, err := n.getClient(nodeID)
	if err != nil {
		return err
	}
	checksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	return c.PutChunk(ctx, chunkID, data, checksum)
}

// ---------------------------------------------------------------------------
// LocalChunkStore — adapts a single Client to satisfy the S3 ChunkStore
// interface.
// ---------------------------------------------------------------------------

// LocalChunkStore wraps a Client and satisfies the S3 ChunkStore interface.
type LocalChunkStore struct {
	client *Client
}

// NewLocalChunkStore creates a LocalChunkStore backed by the given Client.
func NewLocalChunkStore(client *Client) *LocalChunkStore {
	return &LocalChunkStore{client: client}
}

// PutChunkData computes the chunk ID (SHA-256 hex) and CRC-32C checksum from
// data, stores the chunk, and returns the generated chunk ID.
func (s *LocalChunkStore) PutChunkData(ctx context.Context, data []byte) (string, error) {
	h := sha256.Sum256(data)
	chunkID := hex.EncodeToString(h[:])
	checksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if err := s.client.PutChunk(ctx, chunkID, data, checksum); err != nil {
		return "", err
	}
	return chunkID, nil
}

// GetChunkData retrieves chunk data by ID.
func (s *LocalChunkStore) GetChunkData(ctx context.Context, chunkID string) ([]byte, error) {
	data, _, err := s.client.GetChunk(ctx, chunkID)
	return data, err
}

// DeleteChunkData removes a chunk by ID.
func (s *LocalChunkStore) DeleteChunkData(ctx context.Context, chunkID string) error {
	return s.client.DeleteChunk(ctx, chunkID)
}
