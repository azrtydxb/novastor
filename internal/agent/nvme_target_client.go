package agent

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/piwi3910/novastor/api/proto/nvme"
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
func (c *NVMeTargetClient) CreateTarget(ctx context.Context, volumeID string, sizeBytes int64) (*NVMeTargetResult, error) {
	resp, err := c.client.CreateTarget(ctx, &pb.CreateTargetRequest{
		VolumeId:  volumeID,
		SizeBytes: sizeBytes,
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
