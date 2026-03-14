// Package dataplane provides a Go gRPC client for the Rust SPDK dataplane.
// This replaces the JSON-RPC client in internal/spdk/client.go per the
// architecture spec (invariant #9: gRPC is the only communication protocol).
package dataplane

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/azrtydxb/novastor/api/proto/dataplane"
)

const (
	// DefaultGRPCPort is the default gRPC port for the Rust dataplane.
	DefaultGRPCPort = 9500

	// callTimeout is the maximum time for a unary RPC.
	callTimeout = 30 * time.Second

	// streamChunkSize is the fragment size for streaming chunk data (1MB).
	streamChunkSize = 1 * 1024 * 1024
)

// Client communicates with the local Rust SPDK dataplane via gRPC.
// It replaces the JSON-RPC spdk.Client.
type Client struct {
	conn   *grpc.ClientConn
	svc    pb.DataplaneServiceClient
	logger *zap.Logger
}

// Dial connects to the Rust dataplane gRPC server.
func Dial(addr string, logger *zap.Logger, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialling dataplane at %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		svc:    pb.NewDataplaneServiceClient(conn),
		logger: logger,
	}, nil
}

// Close tears down the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ctx returns a context with the default call timeout.
func (c *Client) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), callTimeout)
}

// --------------------------------------------------------------------------
// Bdev Management
// --------------------------------------------------------------------------

// CreateAioBdev creates an SPDK AIO bdev backed by a file.
func (c *Client) CreateAioBdev(name, devicePath string, blockSize uint32) (*pb.BdevInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.CreateAioBdev(ctx, &pb.CreateAioBdevRequest{
		Name:       name,
		DevicePath: devicePath,
		BlockSize:  blockSize,
	})
}

// CreateMallocBdev creates an in-memory SPDK Malloc bdev (test mode only).
func (c *Client) CreateMallocBdev(name string, sizeMB uint64, blockSize uint32) (*pb.BdevInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.CreateMallocBdev(ctx, &pb.CreateMallocBdevRequest{
		Name:      name,
		SizeMb:    sizeMB,
		BlockSize: blockSize,
	})
}

// CreateLvolStore creates an SPDK lvol store on a bdev.
func (c *Client) CreateLvolStore(bdevName, lvsName string, clusterSize uint32) (*pb.LvolStoreInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.CreateLvolStore(ctx, &pb.CreateLvolStoreRequest{
		BdevName:    bdevName,
		LvsName:     lvsName,
		ClusterSize: clusterSize,
	})
}

// CreateLvol creates a logical volume in an lvol store.
func (c *Client) CreateLvol(lvsName, lvolName string, sizeBytes uint64, thin bool) (*pb.BdevInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.CreateLvol(ctx, &pb.CreateLvolRequest{
		LvsName:       lvsName,
		LvolName:      lvolName,
		SizeBytes:     sizeBytes,
		ThinProvision: thin,
	})
}

// DeleteBdev removes an SPDK bdev.
func (c *Client) DeleteBdev(name string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.DeleteBdev(ctx, &pb.DeleteBdevRequest{Name: name})
	return err
}

// ListBdevs returns all registered bdevs.
func (c *Client) ListBdevs() ([]*pb.BdevInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ListBdevs(ctx, &pb.ListBdevsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetBdevs(), nil
}

// GetBdevInfo returns info for a single bdev.
func (c *Client) GetBdevInfo(name string) (*pb.BdevInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.GetBdevInfo(ctx, &pb.GetBdevInfoRequest{Name: name})
}

// --------------------------------------------------------------------------
// NVMe-oF Target Management
// --------------------------------------------------------------------------

// InitTransport initialises the NVMe-oF TCP transport.
func (c *Client) InitTransport() error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.InitTransport(ctx, &pb.InitTransportRequest{})
	return err
}

// CreateNvmfTarget creates an NVMe-oF subsystem exposing a bdev. Returns the NQN.
func (c *Client) CreateNvmfTarget(volumeID, bdevName, listenAddr string, port uint32, anaState string, anaGroupID uint32) (string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.CreateNvmfTarget(ctx, &pb.CreateNvmfTargetRequest{
		VolumeId:      volumeID,
		BdevName:      bdevName,
		ListenAddress: listenAddr,
		ListenPort:    port,
		AnaState:      anaState,
		AnaGroupId:    anaGroupID,
	})
	if err != nil {
		return "", err
	}
	return resp.GetNqn(), nil
}

// DeleteNvmfTarget tears down an NVMe-oF subsystem.
func (c *Client) DeleteNvmfTarget(volumeID string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.DeleteNvmfTarget(ctx, &pb.DeleteNvmfTargetRequest{VolumeId: volumeID})
	return err
}

// SetANAState changes the ANA state for a volume's NVMe-oF target.
func (c *Client) SetANAState(volumeID string, anaGroupID uint32, state string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.SetAnaState(ctx, &pb.SetAnaStateRequest{
		VolumeId:   volumeID,
		AnaState:   state,
		AnaGroupId: anaGroupID,
	})
	return err
}

// GetANAState returns the current ANA state for a volume's target.
func (c *Client) GetANAState(volumeID string) (uint32, string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.GetAnaState(ctx, &pb.GetAnaStateRequest{VolumeId: volumeID})
	if err != nil {
		return 0, "", err
	}
	return resp.GetAnaGroupId(), resp.GetAnaState(), nil
}

// ListSubsystems lists all NVMe-oF subsystems.
func (c *Client) ListSubsystems() ([]*pb.SubsystemInfo, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ListSubsystems(ctx, &pb.ListSubsystemsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetSubsystems(), nil
}

// ExportBdev exports a bdev as an NVMe-oF namespace.
func (c *Client) ExportBdev(nqn, bdevName string, nsid uint32) (uint32, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ExportBdev(ctx, &pb.ExportBdevRequest{
		Nqn:      nqn,
		BdevName: bdevName,
		Nsid:     nsid,
	})
	if err != nil {
		return 0, err
	}
	return resp.GetNsid(), nil
}

// --------------------------------------------------------------------------
// NVMe-oF Initiator
// --------------------------------------------------------------------------

// ConnectInitiator connects to a remote NVMe-oF target. Returns local bdev name.
func (c *Client) ConnectInitiator(addr, port, nqn string) (string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ConnectInitiator(ctx, &pb.ConnectInitiatorRequest{
		Address: addr,
		Port:    port,
		Nqn:     nqn,
	})
	if err != nil {
		return "", err
	}
	return resp.GetBdevName(), nil
}

// DisconnectInitiator disconnects from a remote NVMe-oF target.
func (c *Client) DisconnectInitiator(nqn string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.DisconnectInitiator(ctx, &pb.DisconnectInitiatorRequest{Nqn: nqn})
	return err
}

// --------------------------------------------------------------------------
// Replica Bdev
// --------------------------------------------------------------------------

// CreateReplicaBdev creates a composite bdev that fans out writes.
func (c *Client) CreateReplicaBdev(volumeID string, targets []*pb.ReplicaTarget, sizeBytes uint64, readPolicy string) (string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.CreateReplicaBdev(ctx, &pb.CreateReplicaBdevRequest{
		VolumeId:   volumeID,
		Targets:    targets,
		SizeBytes:  sizeBytes,
		ReadPolicy: readPolicy,
	})
	if err != nil {
		return "", err
	}
	return resp.GetBdevName(), nil
}

// ReplicaStatus returns health/status of a replica bdev.
func (c *Client) ReplicaStatus(volumeID string) (*pb.ReplicaStatusResponse, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.ReplicaStatus(ctx, &pb.ReplicaStatusRequest{VolumeId: volumeID})
}

// --------------------------------------------------------------------------
// Chunk Store
// --------------------------------------------------------------------------

// InitChunkStore initialises a chunk store on a bdev.
func (c *Client) InitChunkStore(bdevName string) (*pb.InitChunkStoreResponse, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.InitChunkStore(ctx, &pb.InitChunkStoreRequest{BdevName: bdevName})
}

// ChunkStoreStats returns capacity/usage statistics.
func (c *Client) ChunkStoreStats(bdevName string) (*pb.ChunkStoreStatsResponse, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	return c.svc.ChunkStoreStats(ctx, &pb.ChunkStoreStatsRequest{BdevName: bdevName})
}

// WriteChunk writes a content-addressed chunk using streaming.
func (c *Client) WriteChunk(bdevName string, data []byte) (string, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*callTimeout)
	defer cancel()

	stream, err := c.svc.WriteChunk(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("opening write stream: %w", err)
	}

	// Send data in fragments.
	for offset := 0; offset < len(data); offset += streamChunkSize {
		end := min(offset+streamChunkSize, len(data))
		req := &pb.WriteChunkRequest{
			BdevName: bdevName,
			Data:     data[offset:end],
		}
		if err := stream.Send(req); err != nil {
			return "", 0, fmt.Errorf("sending chunk fragment: %w", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", 0, fmt.Errorf("closing write stream: %w", err)
	}
	return resp.GetChunkId(), resp.GetBytesWritten(), nil
}

// ReadChunk reads a chunk by ID using streaming. Returns raw data.
func (c *Client) ReadChunk(bdevName, chunkID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*callTimeout)
	defer cancel()

	stream, err := c.svc.ReadChunk(ctx, &pb.ReadChunkRequest{
		ChunkId:  chunkID,
		BdevName: bdevName,
	})
	if err != nil {
		return nil, fmt.Errorf("opening read stream: %w", err)
	}

	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading chunk fragment: %w", err)
		}
		data = append(data, resp.GetData()...)
	}
	return data, nil
}

// DeleteChunk removes a chunk by ID.
func (c *Client) DeleteChunk(bdevName, chunkID string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.DeleteChunk(ctx, &pb.DeleteChunkRequest{
		ChunkId:  chunkID,
		BdevName: bdevName,
	})
	return err
}

// ChunkExists checks if a chunk ID exists.
func (c *Client) ChunkExists(bdevName, chunkID string) (bool, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ChunkExists(ctx, &pb.ChunkExistsRequest{
		ChunkId:  chunkID,
		BdevName: bdevName,
	})
	if err != nil {
		return false, err
	}
	return resp.GetExists(), nil
}

// ListChunks returns all chunk IDs in a store.
func (c *Client) ListChunks(bdevName string) ([]string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.ListChunks(ctx, &pb.ListChunksRequest{BdevName: bdevName})
	if err != nil {
		return nil, err
	}
	return resp.GetChunkIds(), nil
}

// GarbageCollect removes orphan chunks not in the liveChunkIDs set.
func (c *Client) GarbageCollect(bdevName string, liveChunkIDs []string) (uint32, uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	resp, err := c.svc.GarbageCollect(ctx, &pb.GarbageCollectRequest{
		BdevName:     bdevName,
		LiveChunkIds: liveChunkIDs,
	})
	if err != nil {
		return 0, 0, err
	}
	return resp.GetDeleted(), resp.GetErrors(), nil
}

// --------------------------------------------------------------------------
// Backend Management
// --------------------------------------------------------------------------

// InitBackend initialises a storage backend.
func (c *Client) InitBackend(backendType, configJSON string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.InitBackend(ctx, &pb.InitBackendRequest{
		BackendType: backendType,
		ConfigJson:  configJSON,
	})
	return err
}

// CreateVolume creates a volume on a backend.
func (c *Client) CreateVolume(backendType, name string, sizeBytes uint64) (string, uint64, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.CreateVolume(ctx, &pb.CreateVolumeRequest{
		BackendType: backendType,
		Name:        name,
		SizeBytes:   sizeBytes,
	})
	if err != nil {
		return "", 0, err
	}
	return resp.GetName(), resp.GetSizeBytes(), nil
}

// DeleteVolume deletes a volume from a backend.
func (c *Client) DeleteVolume(backendType, name string) error {
	ctx, cancel := c.ctx()
	defer cancel()
	_, err := c.svc.DeleteVolume(ctx, &pb.DeleteVolumeRequest{
		BackendType: backendType,
		Name:        name,
	})
	return err
}

// --------------------------------------------------------------------------
// Health & Fencing
// --------------------------------------------------------------------------

// GetVersion returns the dataplane version.
func (c *Client) GetVersion() (string, string, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.GetVersion(ctx, &pb.GetVersionRequest{})
	if err != nil {
		return "", "", err
	}
	return resp.GetVersion(), resp.GetCommit(), nil
}

// Heartbeat sends a heartbeat to the dataplane.
// Returns true if the dataplane is fenced.
func (c *Client) Heartbeat(nodeID string) (bool, error) {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.svc.Heartbeat(ctx, &pb.HeartbeatRequest{
		NodeId:    nodeID,
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		return false, err
	}
	return resp.GetFenced(), nil
}
