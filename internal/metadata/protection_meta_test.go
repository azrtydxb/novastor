package metadata

import (
	"testing"
	"time"
)

func TestChunkHealLock_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt int64
		now       time.Time
		want      bool
	}{
		{
			name:      "not expired",
			expiresAt: time.Now().Add(5 * time.Minute).Unix(),
			now:       time.Now(),
			want:      false,
		},
		{
			name:      "expired",
			expiresAt: time.Now().Add(-1 * time.Minute).Unix(),
			now:       time.Now(),
			want:      true,
		},
		{
			name:      "just expired",
			expiresAt: time.Now().Add(-1 * time.Second).Unix(),
			now:       time.Now(),
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &ChunkHealLock{ExpiresAt: tt.expiresAt}
			if got := l.IsExpired(tt.now); got != tt.want {
				t.Errorf("ChunkHealLock.IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestChunkHealLock_ExtendTTL(t *testing.T) {
	now := time.Now()
	l := &ChunkHealLock{
		ExpiresAt: now.Unix(),
	}

	l.ExtendTTL(now)

	expectedExpires := now.Add(5 * time.Minute).Unix()
	if l.ExpiresAt != expectedExpires {
		t.Errorf("ExpiresAt = %d, want %d", l.ExpiresAt, expectedExpires)
	}
}

func TestHealTask_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		updatedAt int64
		now       time.Time
		want      bool
	}{
		{
			name:      "not expired",
			updatedAt: time.Now().Unix(),
			now:       time.Now(),
			want:      false,
		},
		{
			name:      "expired",
			updatedAt: time.Now().Add(-10 * time.Minute).Unix(),
			now:       time.Now(),
			want:      true,
		},
		{
			name:      "just at threshold",
			updatedAt: time.Now().Add(-6 * time.Minute).Unix(),
			now:       time.Now(),
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &HealTask{UpdatedAt: tt.updatedAt}
			if got := task.IsExpired(tt.now); got != tt.want {
				t.Errorf("HealTask.IsExpired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShardPlacement(t *testing.T) {
	sp := &ShardPlacement{
		ChunkID:      "chunk-1",
		VolumeID:     "vol-1",
		ShardIndex:   0,
		NodeID:       "node-1",
		LastVerified: time.Now().Unix(),
	}

	if sp.ChunkID != "chunk-1" {
		t.Errorf("ChunkID = %s, want chunk-1", sp.ChunkID)
	}
	if sp.ShardIndex != 0 {
		t.Errorf("ShardIndex = %d, want 0", sp.ShardIndex)
	}
}

func TestVolumeComplianceStatus(t *testing.T) {
	vc := &VolumeComplianceStatus{
		VolumeID:         "vol-1",
		Status:           "compliant",
		LiveReplicaCount: 3,
		LiveShardCount:   6,
		Issues:           []string{},
		LastCheckedAt:    time.Now().Unix(),
		UpdatedAt:        time.Now().Unix(),
	}

	if vc.VolumeID != "vol-1" {
		t.Errorf("VolumeID = %s, want vol-1", vc.VolumeID)
	}
	if vc.Status != "compliant" {
		t.Errorf("Status = %s, want compliant", vc.Status)
	}
}

func TestHealTask(t *testing.T) {
	task := &HealTask{
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
		BytesDone:   0,
		RetryCount:  0,
		LastError:   "",
		CreatedAt:   time.Now().Unix(),
		UpdatedAt:   time.Now().Unix(),
	}

	if task.ID != "task-1" {
		t.Errorf("ID = %s, want task-1", task.ID)
	}
	if task.Type != "replicate" {
		t.Errorf("Type = %s, want replicate", task.Type)
	}
	if task.ShardIndex != -1 {
		t.Errorf("ShardIndex = %d, want -1", task.ShardIndex)
	}
}
