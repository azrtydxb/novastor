package datamover

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/metadata"
)

// mockMetadataStore is a test double for MetadataStore.
type mockMetadataStore struct {
	mu               sync.Mutex
	healTasks        map[string]*metadata.HealTask
	shardPlacements  map[string][]*metadata.ShardPlacement
	locks            map[string]*metadata.ChunkHealLock
	acquireLockDelay time.Duration
}

func newMockMetadataStore() *mockMetadataStore {
	return &mockMetadataStore{
		healTasks:       make(map[string]*metadata.HealTask),
		shardPlacements: make(map[string][]*metadata.ShardPlacement),
		locks:           make(map[string]*metadata.ChunkHealLock),
	}
}

func (m *mockMetadataStore) PutHealTask(ctx context.Context, task *metadata.HealTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healTasks[task.ID] = task
	return nil
}

func (m *mockMetadataStore) GetHealTask(ctx context.Context, taskID string) (*metadata.HealTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.healTasks[taskID]
	if !ok {
		return nil, fmt.Errorf("task not found")
	}
	return task, nil
}

func (m *mockMetadataStore) ListPendingHealTasks(ctx context.Context) ([]*metadata.HealTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*metadata.HealTask
	for _, task := range m.healTasks {
		if task.Status == "pending" || task.Status == "in-progress" {
			// Reset expired in-progress tasks
			if task.Status == "in-progress" && task.IsExpired(time.Now()) {
				task.Status = "pending"
			}
			result = append(result, task)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) ListHealTasksByVolume(ctx context.Context, volumeID string) ([]*metadata.HealTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*metadata.HealTask
	for _, task := range m.healTasks {
		if task.VolumeID == volumeID {
			result = append(result, task)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) DeleteHealTask(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.healTasks, taskID)
	return nil
}

func (m *mockMetadataStore) PutShardPlacement(ctx context.Context, sp *metadata.ShardPlacement) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d", sp.ChunkID, sp.ShardIndex)
	m.shardPlacements[key] = append(m.shardPlacements[key], sp)
	return nil
}

func (m *mockMetadataStore) GetShardPlacements(ctx context.Context, chunkID string) ([]*metadata.ShardPlacement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*metadata.ShardPlacement
	for key, placements := range m.shardPlacements {
		if len(key) >= len(chunkID) && key[:len(chunkID)] == chunkID {
			result = append(result, placements...)
		}
	}
	return result, nil
}

func (m *mockMetadataStore) DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d", chunkID, shardIndex)
	delete(m.shardPlacements, key)
	return nil
}

func (m *mockMetadataStore) AcquireChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	if m.acquireLockDelay > 0 {
		time.Sleep(m.acquireLockDelay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.locks[lockID]; ok {
		if !existing.IsExpired(time.Now()) && existing.OwnerTaskID != ownerTaskID {
			return fmt.Errorf("lock held by %s", existing.OwnerTaskID)
		}
	}
	m.locks[lockID] = &metadata.ChunkHealLock{
		LockID:      lockID,
		OwnerTaskID: ownerTaskID,
		ExpiresAt:   time.Now().Add(5 * time.Minute).Unix(),
	}
	return nil
}

func (m *mockMetadataStore) HeartbeatChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[lockID]
	if !ok {
		return fmt.Errorf("lock not found")
	}
	if lock.OwnerTaskID != ownerTaskID {
		return fmt.Errorf("lock owned by %s", lock.OwnerTaskID)
	}
	lock.ExtendTTL(time.Now())
	return nil
}

func (m *mockMetadataStore) ReleaseChunkLock(ctx context.Context, lockID, ownerTaskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[lockID]
	if !ok {
		return nil
	}
	if lock.OwnerTaskID == ownerTaskID {
		delete(m.locks, lockID)
	}
	return nil
}

func TestDataMover_Submit(t *testing.T) {
	tests := []struct {
		name      string
		existing  *metadata.HealTask
		submit    *metadata.HealTask
		wantErr   bool
		wantTasks int
	}{
		{
			name: "new pending task is submitted",
			submit: &metadata.HealTask{
				ID:        "task-1",
				VolumeID:  "vol-1",
				ChunkID:   "chunk-1",
				Type:      "replicate",
				Status:    "pending",
				Priority:  1,
				SizeBytes: 4 * 1024 * 1024,
			},
			wantErr:   false,
			wantTasks: 1,
		},
		{
			name: "completed task is no-op",
			existing: &metadata.HealTask{
				ID:        "task-1",
				VolumeID:  "vol-1",
				ChunkID:   "chunk-1",
				Type:      "replicate",
				Status:    "completed",
				Priority:  1,
				SizeBytes: 4 * 1024 * 1024,
			},
			submit: &metadata.HealTask{
				ID:        "task-1",
				VolumeID:  "vol-1",
				ChunkID:   "chunk-1",
				Type:      "replicate",
				Status:    "pending",
				Priority:  1,
				SizeBytes: 4 * 1024 * 1024,
			},
			wantErr:   false,
			wantTasks: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := newMockMetadataStore()
			if tt.existing != nil {
				meta.healTasks[tt.existing.ID] = tt.existing
			}

			mockRep := &mockReplicator{}
			ecFactory := NewDefaultErasureCoderFactory()

			dm := NewDataMover(DefaultConfig(), mockRep, meta, ecFactory)

			ctx := context.Background()
			err := dm.Submit(ctx, tt.submit)

			if (err != nil) != tt.wantErr {
				t.Errorf("Submit() error = %v, wantErr %v", err, tt.wantErr)
			}

			if len(meta.healTasks) != tt.wantTasks {
				t.Errorf("expected %d tasks, got %d", tt.wantTasks, len(meta.healTasks))
			}
		})
	}
}

func TestDataMover_Status(t *testing.T) {
	meta := newMockMetadataStore()
	task := &metadata.HealTask{
		ID:        "task-1",
		VolumeID:  "vol-1",
		ChunkID:   "chunk-1",
		Type:      "replicate",
		Status:    "pending",
		Priority:  1,
		SizeBytes: 4 * 1024 * 1024,
	}
	meta.healTasks[task.ID] = task

	mockRep := &mockReplicator{}
	ecFactory := NewDefaultErasureCoderFactory()
	dm := NewDataMover(DefaultConfig(), mockRep, meta, ecFactory)

	ctx := context.Background()
	got, err := dm.Status(ctx, "task-1")
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}

	if got.ID != "task-1" {
		t.Errorf("expected task ID task-1, got %s", got.ID)
	}
}

func TestDataMover_Cancel(t *testing.T) {
	meta := newMockMetadataStore()
	task := &metadata.HealTask{
		ID:        "task-1",
		VolumeID:  "vol-1",
		ChunkID:   "chunk-1",
		Type:      "replicate",
		Status:    "pending",
		Priority:  1,
		SizeBytes: 4 * 1024 * 1024,
	}
	meta.healTasks[task.ID] = task

	mockRep := &mockReplicator{}
	ecFactory := NewDefaultErasureCoderFactory()
	dm := NewDataMover(DefaultConfig(), mockRep, meta, ecFactory)

	ctx := context.Background()
	err := dm.Cancel(ctx, "task-1")
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	got, _ := dm.Status(ctx, "task-1")
	if got.Status != "cancelled" {
		t.Errorf("expected status cancelled, got %s", got.Status)
	}
}

func TestMetadataChunkLocker(t *testing.T) {
	meta := newMockMetadataStore()
	locker := NewMetadataChunkLocker(meta)
	ctx := context.Background()

	lockID := "chunk-1:node-1"
	ownerTask := "task-1"

	// First acquire should succeed
	err := locker.Acquire(ctx, lockID, ownerTask)
	if err != nil {
		t.Fatalf("first Acquire() failed: %v", err)
	}

	// Second acquire from different owner should fail
	err = locker.Acquire(ctx, lockID, "task-2")
	if err == nil {
		t.Error("second Acquire() should have failed")
	}

	// Heartbeat from owner should succeed
	err = locker.Heartbeat(ctx, lockID, ownerTask)
	if err != nil {
		t.Fatalf("Heartbeat() failed: %v", err)
	}

	// Release from owner should succeed
	err = locker.Release(ctx, lockID, ownerTask)
	if err != nil {
		t.Fatalf("Release() failed: %v", err)
	}

	// Now acquire from different owner should succeed
	err = locker.Acquire(ctx, lockID, "task-2")
	if err != nil {
		t.Fatalf("third Acquire() failed: %v", err)
	}
}

func TestWorker_ProcessTask(t *testing.T) {
	tests := []struct {
		name        string
		task        *metadata.HealTask
		replicateFn func(ctx context.Context, chunkID string, sourceNode, destNode string) error
		wantStatus  string
		wantErr     bool
	}{
		{
			name: "successful replicate task",
			task: &metadata.HealTask{
				ID:          "task-1",
				VolumeID:    "vol-1",
				ChunkID:     "chunk-1",
				ShardIndex:  -1,
				Type:        "replicate",
				Priority:    1,
				SourceNodes: []string{"node-1"},
				DestNode:    "node-2",
				SizeBytes:   4 * 1024 * 1024,
				Status:      "pending",
			},
			replicateFn: func(ctx context.Context, chunkID string, sourceNode, destNode string) error {
				return nil
			},
			wantStatus: "completed",
			wantErr:    false,
		},
		{
			name: "failed replicate task retries",
			task: &metadata.HealTask{
				ID:          "task-1",
				VolumeID:    "vol-1",
				ChunkID:     "chunk-1",
				ShardIndex:  -1,
				Type:        "replicate",
				Priority:    1,
				SourceNodes: []string{"node-1"},
				DestNode:    "node-2",
				SizeBytes:   4 * 1024 * 1024,
				Status:      "pending",
				RetryCount:  0,
			},
			replicateFn: func(ctx context.Context, chunkID string, sourceNode, destNode string) error {
				return fmt.Errorf("connection refused")
			},
			wantStatus: "pending", // Should be pending for retry
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := newMockMetadataStore()
			mockRep := &mockReplicator{
				replicateChunkFunc: tt.replicateFn,
			}
			ecFactory := NewDefaultErasureCoderFactory()

			w := newWorker(0, mockRep, NewMetadataChunkLocker(meta), meta, ecFactory)
			cfg := DefaultConfig()
			cfg.RetryMaxAttempts = 3

			ctx := context.Background()
			err := w.processTask(ctx, tt.task, cfg)

			if (err != nil) != tt.wantErr {
				t.Errorf("processTask() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tt.task.Status != tt.wantStatus {
				t.Errorf("expected status %s, got %s", tt.wantStatus, tt.task.Status)
			}
		})
	}
}

func TestWithLock(t *testing.T) {
	meta := newMockMetadataStore()
	locker := NewMetadataChunkLocker(meta)
	ctx := context.Background()

	lockID := "chunk-1:node-1"
	ownerTask := "task-1"

	var fnCalled bool
	// Use a shorter context for the test - WithLock returns ctx.Err() when context is done
	testCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	err := WithLock(testCtx, locker, lockID, ownerTask, 1*time.Millisecond, func() error {
		fnCalled = true
		return nil
	})

	// Context will be canceled, so we expect that error
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		t.Fatalf("WithLock() error = %v", err)
	}

	if !fnCalled {
		t.Error("function was not called")
	}
}
