package chunk

import (
	"testing"

	"github.com/piwi3910/novastor/internal/metadata"
)

func TestChunk_WithProtectionProfile(t *testing.T) {
	data := []byte("test data for protection")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Set a replication protection profile
	c.ProtectionProfile = &metadata.ProtectionProfile{
		Mode: metadata.ProtectionModeReplication,
		Replication: &metadata.ReplicationProfile{
			Factor:      3,
			WriteQuorum: 2,
		},
	}

	if c.ProtectionProfile == nil {
		t.Error("expected protection profile to be set")
	}
	if c.ProtectionProfile.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected replication mode, got %v", c.ProtectionProfile.Mode)
	}
	if c.ProtectionProfile.Replication.Factor != 3 {
		t.Errorf("expected factor 3, got %d", c.ProtectionProfile.Replication.Factor)
	}
}

func TestChunk_WithComplianceInfo(t *testing.T) {
	data := []byte("test data for compliance")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Set compliance info
	c.ComplianceInfo = &metadata.ComplianceInfo{
		State:             metadata.ComplianceStateCompliant,
		AvailableReplicas: 3,
		RequiredReplicas:  2,
		Reason:            "All replicas available",
	}

	if c.ComplianceInfo == nil {
		t.Error("expected compliance info to be set")
	}
	if c.ComplianceInfo.State != metadata.ComplianceStateCompliant {
		t.Errorf("expected compliant state, got %v", c.ComplianceInfo.State)
	}
	if c.ComplianceInfo.AvailableReplicas != 3 {
		t.Errorf("expected 3 available replicas, got %d", c.ComplianceInfo.AvailableReplicas)
	}
}

func TestChunk_WithErasureCoding(t *testing.T) {
	data := make([]byte, ChunkSize)
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]

	// Set erasure coding protection profile
	c.ProtectionProfile = &metadata.ProtectionProfile{
		Mode: metadata.ProtectionModeErasureCoding,
		ErasureCoding: &metadata.ErasureCodingProfile{
			DataShards:   4,
			ParityShards: 2,
		},
	}

	if c.ProtectionProfile.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected erasure coding mode, got %v", c.ProtectionProfile.Mode)
	}
	if c.ProtectionProfile.ErasureCoding.DataShards != 4 {
		t.Errorf("expected 4 data shards, got %d", c.ProtectionProfile.ErasureCoding.DataShards)
	}
	if c.ProtectionProfile.ErasureCoding.ParityShards != 2 {
		t.Errorf("expected 2 parity shards, got %d", c.ProtectionProfile.ErasureCoding.ParityShards)
	}
}

func TestChunk_RequiredReplicas(t *testing.T) {
	data := []byte("test data")
	c := &Chunk{
		ID:       NewChunkID(data),
		Data:     data,
		Checksum: 0,
	}

	tests := []struct {
		name     string
		profile  *metadata.ProtectionProfile
		expected int
	}{
		{
			name:     "nil profile defaults to 1",
			profile:  nil,
			expected: 1,
		},
		{
			name: "replication with explicit quorum",
			profile: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeReplication,
				Replication: &metadata.ReplicationProfile{
					Factor:      5,
					WriteQuorum: 3,
				},
			},
			expected: 3,
		},
		{
			name: "replication with default quorum",
			profile: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeReplication,
				Replication: &metadata.ReplicationProfile{
					Factor: 3,
				},
			},
			expected: 2, // (3/2)+1
		},
		{
			name: "erasure coding needs data shards",
			profile: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeErasureCoding,
				ErasureCoding: &metadata.ErasureCodingProfile{
					DataShards:   6,
					ParityShards: 3,
				},
			},
			expected: 6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.ProtectionProfile = tt.profile
			got := c.ProtectionProfile.RequiredReplicas()
			if got != tt.expected {
				t.Errorf("RequiredReplicas() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestChunk_TargetReplicas(t *testing.T) {
	data := []byte("test data")
	c := &Chunk{
		ID:       NewChunkID(data),
		Data:     data,
		Checksum: 0,
	}

	tests := []struct {
		name     string
		profile  *metadata.ProtectionProfile
		expected int
	}{
		{
			name:     "nil profile defaults to 1",
			profile:  nil,
			expected: 1,
		},
		{
			name: "replication factor",
			profile: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeReplication,
				Replication: &metadata.ReplicationProfile{
					Factor: 5,
				},
			},
			expected: 5,
		},
		{
			name: "erasure coding total shards",
			profile: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeErasureCoding,
				ErasureCoding: &metadata.ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
			expected: 6, // 4+2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.ProtectionProfile = tt.profile
			got := c.ProtectionProfile.TargetReplicas()
			if got != tt.expected {
				t.Errorf("TargetReplicas() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestChunk_UpdateCompliance(t *testing.T) {
	data := []byte("test data")
	c := &Chunk{
		ID:       NewChunkID(data),
		Data:     data,
		Checksum: 0,
		ProtectionProfile: &metadata.ProtectionProfile{
			Mode: metadata.ProtectionModeReplication,
			Replication: &metadata.ReplicationProfile{
				Factor:      3,
				WriteQuorum: 2,
			},
		},
		ComplianceInfo: &metadata.ComplianceInfo{},
	}

	metadata.UpdateComplianceState(c.ComplianceInfo, 3, c.ProtectionProfile)

	if c.ComplianceInfo.State != metadata.ComplianceStateCompliant {
		t.Errorf("expected compliant state, got %v", c.ComplianceInfo.State)
	}
	if c.ComplianceInfo.AvailableReplicas != 3 {
		t.Errorf("expected 3 available replicas, got %d", c.ComplianceInfo.AvailableReplicas)
	}
	if c.ComplianceInfo.RequiredReplicas != 2 {
		t.Errorf("expected 2 required replicas, got %d", c.ComplianceInfo.RequiredReplicas)
	}
}
