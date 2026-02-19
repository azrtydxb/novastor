package csi

import (
	"strconv"
	"testing"

	"github.com/piwi3910/novastor/internal/metadata"
)

func TestParseProtectionParams_Default(t *testing.T) {
	params, err := ParseProtectionParams(nil)
	if err != nil {
		t.Fatalf("ParseProtectionParams with nil params failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected default mode replication, got %s", params.Config.Mode)
	}
	if params.Config.Replication.Factor != defaultReplicationFactor {
		t.Errorf("expected default replicas %d, got %d", defaultReplicationFactor, params.Config.Replication.Factor)
	}
	if params.Config.Replication.WriteQuorum != defaultWriteQuorum {
		t.Errorf("expected default write quorum %d, got %d", defaultWriteQuorum, params.Config.Replication.WriteQuorum)
	}
}

func TestParseProtectionParams_EmptyMap(t *testing.T) {
	params, err := ParseProtectionParams(map[string]string{})
	if err != nil {
		t.Fatalf("ParseProtectionParams with empty map failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected default mode replication, got %s", params.Config.Mode)
	}
}

func TestParseProtectionParams_ReplicationDefaults(t *testing.T) {
	params, err := ParseProtectionParams(map[string]string{
		"protection": "replication",
	})
	if err != nil {
		t.Fatalf("ParseProtectionParams failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected mode replication, got %s", params.Config.Mode)
	}
	if params.Config.Replication.Factor != defaultReplicationFactor {
		t.Errorf("expected default replicas %d, got %d", defaultReplicationFactor, params.Config.Replication.Factor)
	}
}

func TestParseProtectionParams_ReplicationCustom(t *testing.T) {
	params, err := ParseProtectionParams(map[string]string{
		"protection":  "replication",
		"replicas":    "5",
		"writeQuorum": "3",
	})
	if err != nil {
		t.Fatalf("ParseProtectionParams failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeReplication {
		t.Errorf("expected mode replication, got %s", params.Config.Mode)
	}
	if params.Config.Replication.Factor != 5 {
		t.Errorf("expected replicas 5, got %d", params.Config.Replication.Factor)
	}
	if params.Config.Replication.WriteQuorum != 3 {
		t.Errorf("expected write quorum 3, got %d", params.Config.Replication.WriteQuorum)
	}
}

func TestParseProtectionParams_ReplicationWriteQuorumDefault(t *testing.T) {
	tests := []struct {
		name           string
		replicas       int
		expectedQuorum int
	}{
		{"factor 2 -> quorum 2", 2, 2},
		{"factor 3 -> quorum 2", 3, 2},
		{"factor 4 -> quorum 3", 4, 3},
		{"factor 5 -> quorum 3", 5, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := ParseProtectionParams(map[string]string{
				"protection": "replication",
				"replicas":   strconv.Itoa(tt.replicas),
			})
			if err != nil {
				t.Fatalf("ParseProtectionParams failed: %v", err)
			}
			if params.Config.Replication.WriteQuorum != tt.expectedQuorum {
				t.Errorf("expected write quorum %d, got %d", tt.expectedQuorum, params.Config.Replication.WriteQuorum)
			}
		})
	}
}

func TestParseProtectionParams_ErasureCodingDefaults(t *testing.T) {
	params, err := ParseProtectionParams(map[string]string{
		"protection": "erasure-coding",
	})
	if err != nil {
		t.Fatalf("ParseProtectionParams failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", params.Config.Mode)
	}
	if params.Config.ErasureCoding.DataShards != defaultDataShards {
		t.Errorf("expected default dataShards %d, got %d", defaultDataShards, params.Config.ErasureCoding.DataShards)
	}
	if params.Config.ErasureCoding.ParityShards != defaultParityShards {
		t.Errorf("expected default parityShards %d, got %d", defaultParityShards, params.Config.ErasureCoding.ParityShards)
	}
}

func TestParseProtectionParams_ErasureCodingCustom(t *testing.T) {
	params, err := ParseProtectionParams(map[string]string{
		"protection":   "erasure-coding",
		"dataShards":   "8",
		"parityShards": "3",
	})
	if err != nil {
		t.Fatalf("ParseProtectionParams failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", params.Config.Mode)
	}
	if params.Config.ErasureCoding.DataShards != 8 {
		t.Errorf("expected dataShards 8, got %d", params.Config.ErasureCoding.DataShards)
	}
	if params.Config.ErasureCoding.ParityShards != 3 {
		t.Errorf("expected parityShards 3, got %d", params.Config.ErasureCoding.ParityShards)
	}
}

func TestParseProtectionParams_ErasureCodingCamelCase(t *testing.T) {
	// Test that "erasureCoding" (camelCase) is also accepted.
	params, err := ParseProtectionParams(map[string]string{
		"protection": "erasureCoding",
		"dataShards": "6",
	})
	if err != nil {
		t.Fatalf("ParseProtectionParams failed: %v", err)
	}
	if params.Config.Mode != metadata.ProtectionModeErasureCoding {
		t.Errorf("expected mode erasure-coding, got %s", params.Config.Mode)
	}
}

func TestParseProtectionParams_InvalidMode(t *testing.T) {
	_, err := ParseProtectionParams(map[string]string{
		"protection": "invalid-mode",
	})
	if err == nil {
		t.Fatal("expected error for invalid protection mode")
	}
}

func TestParseProtectionParams_InvalidReplicas(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected string
	}{
		{
			name: "non-integer replicas",
			params: map[string]string{
				"protection": "replication",
				"replicas":   "not-a-number",
			},
			expected: "invalid replicas value",
		},
		{
			name: "replicas too low",
			params: map[string]string{
				"protection": "replication",
				"replicas":   "0",
			},
			expected: "must be between 1 and 5",
		},
		{
			name: "replicas too high",
			params: map[string]string{
				"protection": "replication",
				"replicas":   "10",
			},
			expected: "must be between 1 and 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProtectionParams(tt.params)
			if err == nil {
				t.Fatal("expected error for invalid replicas")
			}
		})
	}
}

func TestParseProtectionParams_InvalidWriteQuorum(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected string
	}{
		{
			name: "non-integer write quorum",
			params: map[string]string{
				"protection":  "replication",
				"replicas":    "3",
				"writeQuorum": "not-a-number",
			},
			expected: "invalid writeQuorum value",
		},
		{
			name: "write quorum too low",
			params: map[string]string{
				"protection":  "replication",
				"replicas":    "3",
				"writeQuorum": "0",
			},
			expected: "must be between 1 and replicas",
		},
		{
			name: "write quorum exceeds replicas",
			params: map[string]string{
				"protection":  "replication",
				"replicas":    "3",
				"writeQuorum": "5",
			},
			expected: "must be between 1 and replicas",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProtectionParams(tt.params)
			if err == nil {
				t.Fatal("expected error for invalid write quorum")
			}
		})
	}
}

func TestParseProtectionParams_InvalidDataShards(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
	}{
		{
			name: "non-integer data shards",
			params: map[string]string{
				"protection": "erasure-coding",
				"dataShards": "not-a-number",
			},
		},
		{
			name: "data shards too low",
			params: map[string]string{
				"protection": "erasure-coding",
				"dataShards": "1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProtectionParams(tt.params)
			if err == nil {
				t.Fatal("expected error for invalid dataShards")
			}
		})
	}
}

func TestParseProtectionParams_InvalidParityShards(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
	}{
		{
			name: "non-integer parity shards",
			params: map[string]string{
				"protection":   "erasure-coding",
				"parityShards": "not-a-number",
			},
		},
		{
			name: "parity shards too low",
			params: map[string]string{
				"protection":   "erasure-coding",
				"parityShards": "0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProtectionParams(tt.params)
			if err == nil {
				t.Fatal("expected error for invalid parityShards")
			}
		})
	}
}

func TestProtectionParams_ShardCountForChunk(t *testing.T) {
	tests := []struct {
		name           string
		params         map[string]string
		expectedShards int
	}{
		{
			name:           "default replication",
			params:         map[string]string{},
			expectedShards: defaultReplicationFactor,
		},
		{
			name: "replication factor 5",
			params: map[string]string{
				"protection": "replication",
				"replicas":   "5",
			},
			expectedShards: 5,
		},
		{
			name: "default erasure coding",
			params: map[string]string{
				"protection": "erasure-coding",
			},
			expectedShards: defaultDataShards + defaultParityShards,
		},
		{
			name: "custom erasure coding",
			params: map[string]string{
				"protection":   "erasure-coding",
				"dataShards":   "8",
				"parityShards": "3",
			},
			expectedShards: 11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := ParseProtectionParams(tt.params)
			if err != nil {
				t.Fatalf("ParseProtectionParams failed: %v", err)
			}
			if params.ShardCountForChunk() != tt.expectedShards {
				t.Errorf("expected shard count %d, got %d", tt.expectedShards, params.ShardCountForChunk())
			}
		})
	}
}

func TestProtectionParams_IsErasureCoding(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected bool
	}{
		{
			name:     "default is replication",
			params:   map[string]string{},
			expected: false,
		},
		{
			name: "explicit replication",
			params: map[string]string{
				"protection": "replication",
			},
			expected: false,
		},
		{
			name: "erasure coding",
			params: map[string]string{
				"protection": "erasure-coding",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := ParseProtectionParams(tt.params)
			if err != nil {
				t.Fatalf("ParseProtectionParams failed: %v", err)
			}
			if params.IsErasureCoding() != tt.expected {
				t.Errorf("expected IsErasureCoding()=%v, got %v", tt.expected, params.IsErasureCoding())
			}
		})
	}
}

func TestProtectionParams_IsReplication(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string]string
		expected bool
	}{
		{
			name:     "default is replication",
			params:   map[string]string{},
			expected: true,
		},
		{
			name: "explicit replication",
			params: map[string]string{
				"protection": "replication",
			},
			expected: true,
		},
		{
			name: "erasure coding",
			params: map[string]string{
				"protection": "erasure-coding",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := ParseProtectionParams(tt.params)
			if err != nil {
				t.Fatalf("ParseProtectionParams failed: %v", err)
			}
			if params.IsReplication() != tt.expected {
				t.Errorf("expected IsReplication()=%v, got %v", tt.expected, params.IsReplication())
			}
		})
	}
}
