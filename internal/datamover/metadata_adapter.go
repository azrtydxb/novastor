package datamover

import (
	"context"
	"fmt"

	"github.com/piwi3910/novastor/internal/metadata"
)

// GRPCMetadataStore adapts a metadata.GRPCClient to implement MetadataStore.
// It bridges the generic gRPC Execute interface to the specific MetadataStore methods.
type GRPCMetadataStore struct {
	client *metadata.GRPCClient
}

// NewGRPCMetadataStore creates a new MetadataStore backed by a gRPC metadata client.
func NewGRPCMetadataStore(client *metadata.GRPCClient) *GRPCMetadataStore {
	return &GRPCMetadataStore{client: client}
}

// ---- Heal task operations ----

func (s *GRPCMetadataStore) PutHealTask(ctx context.Context, task *metadata.HealTask) error {
	// TODO: implement via generic Execute
	return fmt.Errorf("PutHealTask: not implemented via gRPC")
}

func (s *GRPCMetadataStore) GetHealTask(ctx context.Context, taskID string) (*metadata.HealTask, error) {
	// TODO: implement via generic Execute
	return nil, fmt.Errorf("GetHealTask: not implemented via gRPC")
}

func (s *GRPCMetadataStore) ListPendingHealTasks(ctx context.Context) ([]*metadata.HealTask, error) {
	// TODO: implement via generic Execute
	return nil, fmt.Errorf("ListPendingHealTasks: not implemented via gRPC")
}

func (s *GRPCMetadataStore) ListHealTasksByVolume(ctx context.Context, volumeID string) ([]*metadata.HealTask, error) {
	// TODO: implement via generic Execute
	return nil, fmt.Errorf("ListHealTasksByVolume: not implemented via gRPC")
}

func (s *GRPCMetadataStore) DeleteHealTask(ctx context.Context, taskID string) error {
	// TODO: implement via generic Execute
	return fmt.Errorf("DeleteHealTask: not implemented via gRPC")
}

// ---- Shard placement operations ----

func (s *GRPCMetadataStore) PutShardPlacement(ctx context.Context, sp *metadata.ShardPlacement) error {
	// TODO: implement via generic Execute
	return fmt.Errorf("PutShardPlacement: not implemented via gRPC")
}

func (s *GRPCMetadataStore) GetShardPlacements(ctx context.Context, chunkID string) ([]*metadata.ShardPlacement, error) {
	// TODO: implement via generic Execute
	return nil, fmt.Errorf("GetShardPlacements: not implemented via gRPC")
}

func (s *GRPCMetadataStore) DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error {
	// TODO: implement via generic Execute
	return fmt.Errorf("DeleteShardPlacement: not implemented via gRPC")
}

// ---- Lock operations ----

func (s *GRPCMetadataStore) AcquireChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	// Use the existing lock lease functionality
	result, err := s.client.AcquireLock(ctx, &metadata.AcquireLockArgs{
		LockID:     lockID,
		OwnerTaskID: ownerTaskID,
		TTL:        5 * 60, // 5 minutes
	})
	if err != nil {
		return err
	}
	if !result.Acquired {
		return fmt.Errorf("lock %s already held", lockID)
	}
	_ = result // Store lease for renewal if needed
	return nil
}

func (s *GRPCMetadataStore) HeartbeatChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	lease, err := s.client.RenewLock(ctx, &metadata.RenewLockArgs{
		LockID:     lockID,
		OwnerTaskID: ownerTaskID,
		AddTTL:     60, // Add 60 seconds
	})
	if err != nil {
		return err
	}
	_ = lease // Store lease if needed
	return nil
}

func (s *GRPCMetadataStore) ReleaseChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	return s.client.ReleaseLock(ctx, &metadata.ReleaseLockArgs{
		LockID:     lockID,
		OwnerTaskID: ownerTaskID,
	})
}
