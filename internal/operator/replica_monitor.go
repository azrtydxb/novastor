package operator

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
)

// ReplicaStatusMonitor periodically checks the replica status of volumes
// by querying each volume's target node's Rust dataplane via gRPC.
// This calls dataplane.Client.ReplicaStatus() which would otherwise go unused.
type ReplicaStatusMonitor struct {
	metaClient *metadata.GRPCClient
	logger     *zap.Logger
}

// NewReplicaStatusMonitor creates a monitor that uses the metadata client
// to discover volumes and their target nodes, then queries each node's
// Rust dataplane for replica health.
func NewReplicaStatusMonitor(metaClient *metadata.GRPCClient) *ReplicaStatusMonitor {
	return &ReplicaStatusMonitor{
		metaClient: metaClient,
		logger:     logging.L.Named("replica-monitor"),
	}
}

// CheckAll queries all volumes from metadata and checks replica status
// for each volume that has a target node. Errors are logged but do not
// stop the scan — this is a best-effort monitoring operation.
func (m *ReplicaStatusMonitor) CheckAll(ctx context.Context) {
	volumes, err := m.metaClient.ListVolumesMeta(ctx)
	if err != nil {
		m.logger.Warn("failed to list volumes for replica status check", zap.Error(err))
		return
	}

	if len(volumes) == 0 {
		return
	}

	for _, vol := range volumes {
		if vol.TargetNodeID == "" || vol.TargetAddress == "" {
			continue
		}
		m.checkVolume(ctx, vol)
	}
}

// checkVolume queries the dataplane on the volume's target node for replica status.
func (m *ReplicaStatusMonitor) checkVolume(ctx context.Context, vol *metadata.VolumeMeta) {
	addr := fmt.Sprintf("%s:%d", vol.TargetAddress, dataplane.DefaultGRPCPort)
	dpClient, err := dataplane.Dial(addr, m.logger, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		m.logger.Debug("failed to dial dataplane for replica status",
			zap.String("volumeID", vol.VolumeID),
			zap.String("targetNode", vol.TargetNodeID),
			zap.String("addr", addr),
			zap.Error(err),
		)
		return
	}
	defer dpClient.Close()

	resp, err := dpClient.ReplicaStatus(vol.VolumeID)
	if err != nil {
		m.logger.Debug("replica status check failed",
			zap.String("volumeID", vol.VolumeID),
			zap.String("targetNode", vol.TargetNodeID),
			zap.Error(err),
		)
		return
	}

	m.logger.Info("replica status",
		zap.String("volumeID", resp.GetVolumeId()),
		zap.String("bdevName", resp.GetBdevName()),
		zap.Uint32("numTargets", resp.GetNumTargets()),
		zap.String("status", resp.GetStatus()),
	)
}
