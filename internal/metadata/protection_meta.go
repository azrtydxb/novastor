package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ShardPlacement represents the location of a single shard (replica or EC shard)
// on a storage node. This replaces the flat PlacementMap for protection-aware
// storage tracking.
type ShardPlacement struct {
	ChunkID      string `json:"chunkID"`
	VolumeID     string `json:"volumeID"`
	ShardIndex   int    `json:"shardIndex"` // -1 for replicated, 0..N for EC
	NodeID       string `json:"nodeID"`
	LastVerified int64  `json:"lastVerified"` // Unix timestamp of last CRC-32C verification
}

// VolumeComplianceStatus tracks the compliance state of a volume's protection profile.
type VolumeComplianceStatus struct {
	VolumeID         string   `json:"volumeID"`
	Status           string   `json:"status"`           // "compliant", "degraded", "healing", "unknown"
	LiveReplicaCount int      `json:"liveReplicaCount"` // For replication mode
	LiveShardCount   int      `json:"liveShardCount"`   // For EC mode
	Issues           []string `json:"issues"`           // Specific issues detected
	LastCheckedAt    int64    `json:"lastCheckedAt"`
	UpdatedAt        int64    `json:"updatedAt"`
}

// HealTask represents a single chunk healing operation to be executed by the
// DataMover. Tasks are created by the PolicyEngine and consumed by workers.
type HealTask struct {
	ID          string   `json:"id"` // UUID, used as idempotency key
	VolumeID    string   `json:"volumeID"`
	ChunkID     string   `json:"chunkID"`
	ShardIndex  int      `json:"shardIndex"`  // -1 for replicated, 0..N for EC
	Type        string   `json:"type"`        // "replicate", "reconstruct", "delete-excess"
	Priority    int      `json:"priority"`    // Lower = higher priority
	SourceNodes []string `json:"sourceNodes"` // Nodes to read from
	DestNode    string   `json:"destNode"`    // Target node for write/delete
	SizeBytes   int64    `json:"sizeBytes"`
	Status      string   `json:"status"`    // "pending", "in-progress", "completed", "failed"
	BytesDone   int64    `json:"bytesDone"` // Progress tracking
	RetryCount  int      `json:"retryCount"`
	LastError   string   `json:"lastError"`
	CreatedAt   int64    `json:"createdAt"`
	UpdatedAt   int64    `json:"updatedAt"`
}

// IsExpired returns true if a lock's TTL has expired (now > ExpiresAt).
func (t *HealTask) IsExpired(now time.Time) bool {
	return now.Unix() > t.UpdatedAt+300 // 5 minute timeout for in-progress tasks
}

// ChunkHealLock provides distributed locking to prevent multiple workers
// from healing the same (chunkID, destNode) pair simultaneously.
type ChunkHealLock struct {
	LockID      string `json:"lockID"`      // chunkID + ":" + destNode
	OwnerTaskID string `json:"ownerTaskID"` // Which HealTask holds this lock
	ExpiresAt   int64  `json:"expiresAt"`   // TTL: 5 minutes from last heartbeat
}

// IsExpired returns true if the lock's TTL has expired.
func (l *ChunkHealLock) IsExpired(now time.Time) bool {
	return now.Unix() > l.ExpiresAt
}

// ExtendTTL updates the expiration time to 5 minutes from now.
func (l *ChunkHealLock) ExtendTTL(now time.Time) {
	l.ExpiresAt = now.Add(5 * time.Minute).Unix()
}

const (
	lockDefaultTTLSeconds = 300
)

// PutShardPlacement stores a shard placement entry.
func (s *RaftStore) PutShardPlacement(_ context.Context, sp *ShardPlacement) error {
	// Composite key: chunkID:shardIndex
	key := fmt.Sprintf("%s:%d", sp.ChunkID, sp.ShardIndex)
	data, err := json.Marshal(sp)
	if err != nil {
		return fmt.Errorf("marshaling shard placement: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketShardPlacements, Key: key, Value: data})
}

// GetShardPlacements retrieves all shard placements for a chunk.
func (s *RaftStore) GetShardPlacements(_ context.Context, chunkID string) ([]*ShardPlacement, error) {
	all, err := s.fsm.GetAll(bucketShardPlacements)
	if err != nil {
		return nil, fmt.Errorf("listing shard placements: %w", err)
	}
	var result []*ShardPlacement
	prefix := chunkID + ":"
	for key, data := range all {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			var sp ShardPlacement
			if err := json.Unmarshal(data, &sp); err != nil {
				return nil, fmt.Errorf("unmarshaling shard placement: %w", err)
			}
			result = append(result, &sp)
		}
	}
	return result, nil
}

// DeleteShardPlacement removes a shard placement entry.
func (s *RaftStore) DeleteShardPlacement(_ context.Context, chunkID string, shardIndex int) error {
	key := fmt.Sprintf("%s:%d", chunkID, shardIndex)
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketShardPlacements, Key: key})
}

// PutVolumeCompliance stores a volume's compliance status.
func (s *RaftStore) PutVolumeCompliance(_ context.Context, vc *VolumeComplianceStatus) error {
	data, err := json.Marshal(vc)
	if err != nil {
		return fmt.Errorf("marshaling volume compliance: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketVolumeCompliance, Key: vc.VolumeID, Value: data})
}

// GetVolumeCompliance retrieves a volume's compliance status.
func (s *RaftStore) GetVolumeCompliance(_ context.Context, volumeID string) (*VolumeComplianceStatus, error) {
	data, err := s.fsm.Get(bucketVolumeCompliance, volumeID)
	if err != nil {
		return nil, err
	}
	var vc VolumeComplianceStatus
	if err := json.Unmarshal(data, &vc); err != nil {
		return nil, fmt.Errorf("unmarshaling volume compliance: %w", err)
	}
	return &vc, nil
}

// PutHealTask stores or updates a heal task.
func (s *RaftStore) PutHealTask(_ context.Context, task *HealTask) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshaling heal task: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketHealTasks, Key: task.ID, Value: data})
}

// GetHealTask retrieves a heal task by ID.
func (s *RaftStore) GetHealTask(_ context.Context, taskID string) (*HealTask, error) {
	data, err := s.fsm.Get(bucketHealTasks, taskID)
	if err != nil {
		return nil, err
	}
	var task HealTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshaling heal task: %w", err)
	}
	return &task, nil
}

// ListHealTasksByVolume returns all heal tasks for a specific volume.
func (s *RaftStore) ListHealTasksByVolume(_ context.Context, volumeID string) ([]*HealTask, error) {
	all, err := s.fsm.GetAll(bucketHealTasks)
	if err != nil {
		return nil, fmt.Errorf("listing heal tasks: %w", err)
	}
	var result []*HealTask
	for _, data := range all {
		var task HealTask
		if err := json.Unmarshal(data, &task); err != nil {
			return nil, fmt.Errorf("unmarshaling heal task: %w", err)
		}
		if task.VolumeID == volumeID {
			result = append(result, &task)
		}
	}
	return result, nil
}

// ListPendingHealTasks returns all heal tasks with status "pending" or "in-progress".
func (s *RaftStore) ListPendingHealTasks(_ context.Context) ([]*HealTask, error) {
	all, err := s.fsm.GetAll(bucketHealTasks)
	if err != nil {
		return nil, fmt.Errorf("listing heal tasks: %w", err)
	}
	var result []*HealTask
	now := time.Now()
	for _, data := range all {
		var task HealTask
		if err := json.Unmarshal(data, &task); err != nil {
			continue
		}
		// Reset expired in-progress tasks to pending
		if task.Status == "in-progress" && task.IsExpired(now) {
			task.Status = "pending"
			updatedData, err := json.Marshal(task)
			if err == nil {
				_ = s.apply(&fsmOp{Op: opPut, Bucket: bucketHealTasks, Key: task.ID, Value: updatedData})
			}
		}
		if task.Status == "pending" || task.Status == "in-progress" {
			result = append(result, &task)
		}
	}
	return result, nil
}

// DeleteHealTask removes a heal task.
func (s *RaftStore) DeleteHealTask(_ context.Context, taskID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketHealTasks, Key: taskID})
}

// AcquireChunkLock atomically attempts to acquire a distributed lock for healing.
// Returns nil on success, error if lock is held by another live task.
func (s *RaftStore) AcquireChunkLock(_ context.Context, lockID, ownerTaskID string) error {
	now := time.Now()
	// Check if lock exists
	data, err := s.fsm.Get(bucketChunkHealLocks, lockID)
	if err == nil {
		// Lock exists - check if expired
		var existingLock ChunkHealLock
		if err := json.Unmarshal(data, &existingLock); err == nil {
			if !existingLock.IsExpired(now) && existingLock.OwnerTaskID != ownerTaskID {
				return fmt.Errorf("lock held by %s until %d", existingLock.OwnerTaskID, existingLock.ExpiresAt)
			}
		}
	}

	// Acquire or steal the lock
	newLock := &ChunkHealLock{
		LockID:      lockID,
		OwnerTaskID: ownerTaskID,
		ExpiresAt:   now.Add(lockDefaultTTLSeconds * time.Second).Unix(),
	}
	lockData, err := json.Marshal(newLock)
	if err != nil {
		return fmt.Errorf("marshaling chunk lock: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketChunkHealLocks, Key: lockID, Value: lockData})
}

// HeartbeatChunkLock extends the TTL of a lock held by the given task.
func (s *RaftStore) HeartbeatChunkLock(_ context.Context, lockID, ownerTaskID string) error {
	data, err := s.fsm.Get(bucketChunkHealLocks, lockID)
	if err != nil {
		return fmt.Errorf("lock not found: %w", err)
	}
	var lock ChunkHealLock
	if err := json.Unmarshal(data, &lock); err != nil {
		return fmt.Errorf("unmarshaling chunk lock: %w", err)
	}
	if lock.OwnerTaskID != ownerTaskID {
		return fmt.Errorf("lock owned by %s, not %s", lock.OwnerTaskID, ownerTaskID)
	}
	lock.ExtendTTL(time.Now())
	lockData, err := json.Marshal(lock)
	if err != nil {
		return fmt.Errorf("marshaling chunk lock: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketChunkHealLocks, Key: lockID, Value: lockData})
}

// ReleaseChunkLock removes a lock held by the given task.
// Returns nil if the lock doesn't exist (already released is OK).
func (s *RaftStore) ReleaseChunkLock(_ context.Context, lockID, ownerTaskID string) error {
	data, err := s.fsm.Get(bucketChunkHealLocks, lockID)
	if err != nil {
		// Already released or never existed - not found is acceptable.
		if errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("getting chunk lock: %w", err)
	}
	var lock ChunkHealLock
	if err := json.Unmarshal(data, &lock); err != nil {
		return s.apply(&fsmOp{Op: opDelete, Bucket: bucketChunkHealLocks, Key: lockID})
	}
	if lock.OwnerTaskID == ownerTaskID {
		return s.apply(&fsmOp{Op: opDelete, Bucket: bucketChunkHealLocks, Key: lockID})
	}
	return fmt.Errorf("lock owned by %s, not %s", lock.OwnerTaskID, ownerTaskID)
}
