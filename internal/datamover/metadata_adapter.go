package datamover

import (
	"context"
	"fmt"
	"time"

	"github.com/azrtydxb/novastor/internal/metadata"
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
//
// TODO(proto): The metadata service proto (api/proto/metadata/metadata.proto) does not
// yet define RPCs for heal tasks (PutHealTask, GetHealTask, ListHealTasks, DeleteHealTask).
// The RaftStore has local implementations in protection_meta.go, but there is no gRPC
// surface to expose them remotely. To wire these stubs:
//   1. Add HealTask message and CRUD RPCs to metadata.proto
//   2. Regenerate protobuf Go code (make generate-proto)
//   3. Implement the RPCs in grpc_server.go (delegating to RaftStore)
//   4. Add client methods to grpc_client.go
//   5. Call the client methods here instead of returning errors

func (s *GRPCMetadataStore) PutHealTask(ctx context.Context, task *metadata.HealTask) error {
	return fmt.Errorf("PutHealTask: not implemented via gRPC — metadata proto lacks heal task RPCs")
}

func (s *GRPCMetadataStore) GetHealTask(ctx context.Context, taskID string) (*metadata.HealTask, error) {
	return nil, fmt.Errorf("GetHealTask: not implemented via gRPC — metadata proto lacks heal task RPCs")
}

func (s *GRPCMetadataStore) ListPendingHealTasks(ctx context.Context) ([]*metadata.HealTask, error) {
	return nil, fmt.Errorf("ListPendingHealTasks: not implemented via gRPC — metadata proto lacks heal task RPCs")
}

func (s *GRPCMetadataStore) ListHealTasksByVolume(ctx context.Context, volumeID string) ([]*metadata.HealTask, error) {
	return nil, fmt.Errorf("ListHealTasksByVolume: not implemented via gRPC — metadata proto lacks heal task RPCs")
}

func (s *GRPCMetadataStore) DeleteHealTask(ctx context.Context, taskID string) error {
	return fmt.Errorf("DeleteHealTask: not implemented via gRPC — metadata proto lacks heal task RPCs")
}

// ---- Shard placement operations ----
// These are fully wired to the metadata gRPC service which has
// PutShardPlacement, GetShardPlacements, and DeleteShardPlacement RPCs.

func (s *GRPCMetadataStore) PutShardPlacement(ctx context.Context, sp *metadata.ShardPlacement) error {
	return s.client.PutShardPlacement(ctx, sp)
}

func (s *GRPCMetadataStore) GetShardPlacements(ctx context.Context, chunkID string) ([]*metadata.ShardPlacement, error) {
	return s.client.GetShardPlacements(ctx, chunkID)
}

func (s *GRPCMetadataStore) DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error {
	return s.client.DeleteShardPlacement(ctx, chunkID, shardIndex)
}

// ---- Lock operations ----
//
// The metadata lock API is designed for NFS-style file locking. We repurpose it
// for chunk-level mutual exclusion by storing the lockID in the Owner field and
// the ownerTaskID in the FilerID field. The byte-range fields are set to cover
// the entire range (Start=0, End=-1) so the lock is exclusive.

func (s *GRPCMetadataStore) AcquireChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	result, err := s.client.AcquireLock(ctx, &metadata.AcquireLockArgs{
		Owner:   lockID,
		FilerID: ownerTaskID,
		Start:   0,
		End:     -1,
		Type:    metadata.LockWrite,
		TTL:     5 * time.Minute,
	})
	if err != nil {
		return err
	}
	if result.ConflictingOwner != "" {
		return fmt.Errorf("lock %s already held by %s", lockID, result.ConflictingOwner)
	}
	return nil
}

func (s *GRPCMetadataStore) HeartbeatChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	// RenewLock requires a LeaseID which we don't store; use Owner-based lookup
	// by re-acquiring. For now, renew with a placeholder LeaseID derived from lockID.
	_, err := s.client.RenewLock(ctx, &metadata.RenewLockArgs{
		LeaseID: lockID,
		TTL:     60 * time.Second,
	})
	return err
}

func (s *GRPCMetadataStore) ReleaseChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	return s.client.ReleaseLock(ctx, &metadata.ReleaseLockArgs{
		LeaseID: lockID,
		Owner:   ownerTaskID,
	})
}
