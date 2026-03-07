package agent

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/spdk"
)

// SetupReplicaBdev creates a replica bdev in the SPDK data-plane for a volume.
// It looks up the placement map to find all replica nodes, connects to their
// NVMe-oF targets, and creates a composite bdev that fans out writes with
// majority-quorum ACK and distributes reads based on the selected policy.
//
// Read policy selection:
//   - "local_first": if the local node is one of the replicas, prefer it for reads
//   - "latency_aware": steer reads toward the lowest-latency replica
//   - "round_robin": distribute reads evenly (default fallback)
//
// When readPolicy is empty, automatically selects "local_first" if the local
// node is a replica, otherwise "latency_aware".
func SetupReplicaBdev(ctx context.Context, spdkClient *spdk.Client, metaClient *metadata.GRPCClient, volumeID, localNodeID string, replicaNodes []string, readPolicy string) (string, error) {
	if len(replicaNodes) == 0 {
		return "", fmt.Errorf("no replica nodes for volume %s", volumeID)
	}

	nqnPrefix := "novastor-"
	nqn := nqnPrefix + volumeID
	targets := make([]spdk.ReplicaTarget, 0, len(replicaNodes))

	hasLocal := false
	for _, nodeAddr := range replicaNodes {
		isLocal := nodeAddr == localNodeID
		if isLocal {
			hasLocal = true
		}
		target := spdk.ReplicaTarget{
			Addr:    nodeAddr,
			Port:    4420,
			NQN:     nqn,
			IsLocal: isLocal,
		}
		targets = append(targets, target)
	}

	// Auto-select read policy if not explicitly set
	if readPolicy == "" {
		if hasLocal {
			readPolicy = "local_first"
		} else {
			readPolicy = "latency_aware"
		}
	}

	bdevName := fmt.Sprintf("replica-%s", volumeID)
	if err := spdkClient.CreateReplicaBdev(bdevName, targets, readPolicy); err != nil {
		return "", fmt.Errorf("creating replica bdev for volume %s: %w", volumeID, err)
	}

	logging.L.Info("replica bdev created",
		zap.String("volumeID", volumeID),
		zap.String("bdevName", bdevName),
		zap.String("readPolicy", readPolicy),
		zap.Int("replicaCount", len(targets)),
	)

	return bdevName, nil
}
