package agent

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/spdk"
)

// SetupReplicaBdev creates a replica bdev in the SPDK data-plane for a volume.
// It looks up the placement map to find all replica nodes, connects to their
// NVMe-oF targets, and creates a composite bdev that fans out writes with
// majority-quorum ACK and distributes reads via round-robin.
func SetupReplicaBdev(ctx context.Context, spdkClient *spdk.Client, metaClient *metadata.GRPCClient, volumeID, localNodeID string, replicaNodes []string) (string, error) {
	if len(replicaNodes) == 0 {
		return "", fmt.Errorf("no replica nodes for volume %s", volumeID)
	}

	nqnPrefix := "novastor-"
	nqn := nqnPrefix + volumeID
	targets := make([]spdk.ReplicaTarget, 0, len(replicaNodes))

	for _, nodeAddr := range replicaNodes {
		isLocal := nodeAddr == localNodeID
		target := spdk.ReplicaTarget{
			Addr:    nodeAddr,
			Port:    4420,
			NQN:     nqn,
			IsLocal: isLocal,
		}
		targets = append(targets, target)
	}

	bdevName := fmt.Sprintf("replica-%s", volumeID)
	if err := spdkClient.CreateReplicaBdev(bdevName, targets, "round-robin"); err != nil {
		return "", fmt.Errorf("creating replica bdev for volume %s: %w", volumeID, err)
	}

	logging.L.Info("replica bdev created",
		zap.String("volumeID", volumeID),
		zap.String("bdevName", bdevName),
		zap.Int("replicaCount", len(targets)),
	)

	return bdevName, nil
}
