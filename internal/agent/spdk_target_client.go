package agent

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/azrtydxb/novastor/api/proto/nvme"
)

// SPDKTargetClient dials an agent's NVMeTargetService and wraps
// CreateTarget / DeleteTarget calls. This client is used by the CSI
// controller regardless of whether the agent is running in SPDK or
// legacy mode — the server side decides the data path.
//
// This type also implements the csi.AgentTargetClient interface so
// the CSI controller can use it directly.
type SPDKTargetClient struct {
	dialOpts []grpc.DialOption
}

// NewSPDKTargetClient creates a client that can dial any agent address.
// Unlike NVMeTargetClient which holds a single connection, this client
// dials per-call so the CSI controller can reach any agent in the cluster.
func NewSPDKTargetClient(opts ...grpc.DialOption) *SPDKTargetClient {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &SPDKTargetClient{dialOpts: opts}
}

// CreateTarget dials the agent at agentAddr and calls CreateTarget.
func (c *SPDKTargetClient) CreateTarget(ctx context.Context, agentAddr, volumeID string, sizeBytes int64, anaState string, anaGroupID uint32) (subsystemNQN, targetAddress, targetPort string, err error) {
	conn, err := grpc.NewClient(agentAddr, c.dialOpts...)
	if err != nil {
		return "", "", "", fmt.Errorf("dialing agent at %s: %w", agentAddr, err)
	}
	defer conn.Close()

	client := pb.NewNVMeTargetServiceClient(conn)
	resp, err := client.CreateTarget(ctx, &pb.CreateTargetRequest{
		VolumeId:   volumeID,
		SizeBytes:  sizeBytes,
		AnaState:   anaState,
		AnaGroupId: anaGroupID,
	})
	if err != nil {
		return "", "", "", fmt.Errorf("CreateTarget RPC for volume %s on %s: %w", volumeID, agentAddr, err)
	}
	return resp.GetSubsystemNqn(), resp.GetTargetAddress(), resp.GetTargetPort(), nil
}

// DeleteTarget dials the agent at agentAddr and calls DeleteTarget.
func (c *SPDKTargetClient) DeleteTarget(ctx context.Context, agentAddr, volumeID string) error {
	conn, err := grpc.NewClient(agentAddr, c.dialOpts...)
	if err != nil {
		return fmt.Errorf("dialing agent at %s: %w", agentAddr, err)
	}
	defer conn.Close()

	client := pb.NewNVMeTargetServiceClient(conn)
	_, err = client.DeleteTarget(ctx, &pb.DeleteTargetRequest{VolumeId: volumeID})
	if err != nil {
		return fmt.Errorf("DeleteTarget RPC for volume %s on %s: %w", volumeID, agentAddr, err)
	}
	return nil
}

// SetANAState dials the agent at agentAddr and calls SetANAState.
func (c *SPDKTargetClient) SetANAState(ctx context.Context, agentAddr, volumeID, anaState string, anaGroupID uint32) error {
	conn, err := grpc.NewClient(agentAddr, c.dialOpts...)
	if err != nil {
		return fmt.Errorf("dialing agent at %s: %w", agentAddr, err)
	}
	defer conn.Close()

	client := pb.NewNVMeTargetServiceClient(conn)
	_, err = client.SetANAState(ctx, &pb.SetANAStateRequest{
		VolumeId:   volumeID,
		AnaState:   anaState,
		AnaGroupId: anaGroupID,
	})
	if err != nil {
		return fmt.Errorf("SetANAState RPC for volume %s on %s: %w", volumeID, agentAddr, err)
	}
	return nil
}

// ReplicaTargetInfo describes a remote NVMe-oF target for replication setup.
type ReplicaTargetInfo struct {
	Address string
	Port    string
	NQN     string
}

// SetupReplication dials the owner agent and calls SetupReplication to
// configure cross-node write replication via a composite replica bdev.
func (c *SPDKTargetClient) SetupReplication(ctx context.Context, agentAddr, volumeID, localBdevName string, remoteTargets []ReplicaTargetInfo, sizeBytes int64) (replicaBdevName, subsystemNQN string, err error) {
	conn, err := grpc.NewClient(agentAddr, c.dialOpts...)
	if err != nil {
		return "", "", fmt.Errorf("dialing agent at %s: %w", agentAddr, err)
	}
	defer conn.Close()

	pbTargets := make([]*pb.ReplicaTargetInfo, len(remoteTargets))
	for i, rt := range remoteTargets {
		pbTargets[i] = &pb.ReplicaTargetInfo{
			Address: rt.Address,
			Port:    rt.Port,
			Nqn:     rt.NQN,
		}
	}

	client := pb.NewNVMeTargetServiceClient(conn)
	resp, err := client.SetupReplication(ctx, &pb.SetupReplicationRequest{
		VolumeId:      volumeID,
		LocalBdevName: localBdevName,
		RemoteTargets: pbTargets,
		SizeBytes:     sizeBytes,
	})
	if err != nil {
		return "", "", fmt.Errorf("SetupReplication RPC for volume %s on %s: %w", volumeID, agentAddr, err)
	}
	return resp.GetReplicaBdevName(), resp.GetSubsystemNqn(), nil
}
