package agent

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/azrtydxb/novastor/api/proto/nvme"
)

// NVMeTargetResult holds the connection parameters returned by CreateTarget.
type NVMeTargetResult struct {
	SubsystemNQN  string
	TargetAddress string
	TargetPort    string
}

// NVMeTargetClient dials an agent's NVMeTargetService and wraps
// CreateTarget / DeleteTarget calls.
type NVMeTargetClient struct {
	conn   *grpc.ClientConn
	client pb.NVMeTargetServiceClient
}

// NewNVMeTargetClient dials the NVMeTargetService at addr and returns a client.
// The caller is responsible for calling Close when done.
func NewNVMeTargetClient(addr string, opts ...grpc.DialOption) (*NVMeTargetClient, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing nvme target service at %s: %w", addr, err)
	}
	return &NVMeTargetClient{
		conn:   conn,
		client: pb.NewNVMeTargetServiceClient(conn),
	}, nil
}

// Close releases the underlying gRPC connection.
func (c *NVMeTargetClient) Close() error {
	return c.conn.Close()
}

// CreateTarget calls the agent's CreateTarget RPC.
func (c *NVMeTargetClient) CreateTarget(ctx context.Context, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32) (*NVMeTargetResult, error) {
	resp, err := c.client.CreateTarget(ctx, &pb.CreateTargetRequest{
		VolumeId:   volumeID,
		SizeBytes:  sizeBytes,
		AnaState:   anaState,
		AnaGroupId: anaGroupID,
	})
	if err != nil {
		return nil, fmt.Errorf("CreateTarget RPC for volume %s: %w", volumeID, err)
	}
	return &NVMeTargetResult{
		SubsystemNQN:  resp.GetSubsystemNqn(),
		TargetAddress: resp.GetTargetAddress(),
		TargetPort:    resp.GetTargetPort(),
	}, nil
}

// DeleteTarget calls the agent's DeleteTarget RPC.
func (c *NVMeTargetClient) DeleteTarget(ctx context.Context, volumeID string) error {
	_, err := c.client.DeleteTarget(ctx, &pb.DeleteTargetRequest{VolumeId: volumeID})
	if err != nil {
		return fmt.Errorf("DeleteTarget RPC for volume %s: %w", volumeID, err)
	}
	return nil
}

// SetANAState calls the agent's SetANAState RPC.
func (c *NVMeTargetClient) SetANAState(ctx context.Context, volumeID, anaState string, anaGroupID uint32) error {
	_, err := c.client.SetANAState(ctx, &pb.SetANAStateRequest{
		VolumeId:   volumeID,
		AnaState:   anaState,
		AnaGroupId: anaGroupID,
	})
	if err != nil {
		return fmt.Errorf("SetANAState RPC for volume %s: %w", volumeID, err)
	}
	return nil
}

// SetupReplicationResult holds the result of a SetupReplication call.
type SetupReplicationResult struct {
	ReplicaBdevName string
	SubsystemNQN    string
}

// SetupReplication calls the agent's SetupReplication RPC.
func (c *NVMeTargetClient) SetupReplication(ctx context.Context, volumeID, localBdevName string, remoteTargets []ReplicaTargetInfo, sizeBytes int64) (*SetupReplicationResult, error) {
	pbTargets := make([]*pb.ReplicaTargetInfo, len(remoteTargets))
	for i, rt := range remoteTargets {
		pbTargets[i] = &pb.ReplicaTargetInfo{
			Address: rt.Address,
			Port:    rt.Port,
			Nqn:     rt.NQN,
		}
	}
	resp, err := c.client.SetupReplication(ctx, &pb.SetupReplicationRequest{
		VolumeId:      volumeID,
		LocalBdevName: localBdevName,
		RemoteTargets: pbTargets,
		SizeBytes:     sizeBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("SetupReplication RPC for volume %s: %w", volumeID, err)
	}
	return &SetupReplicationResult{
		ReplicaBdevName: resp.GetReplicaBdevName(),
		SubsystemNQN:    resp.GetSubsystemNqn(),
	}, nil
}
