package csi

import (
	"testing"

	"github.com/piwi3910/novastor/internal/metadata"
)

func TestParseProtectionParams(t *testing.T) {
	tests := []struct {
		name       string
		params     map[string]string
		wantRepl   int
		wantData   int
		wantParity int
	}{
		{
			name:       "nil params returns default",
			params:     nil,
			wantRepl:   3, // defaultReplicationFactor
			wantData:   0,
			wantParity: 0,
		},
		{
			name:       "empty params returns default",
			params:     map[string]string{},
			wantRepl:   3, // defaultReplicationFactor
			wantData:   0,
			wantParity: 0,
		},
		{
			name: "replicas parameter",
			params: map[string]string{
				"replicas": "3",
			},
			wantRepl:   3,
			wantData:   0,
			wantParity: 0,
		},
		{
			name: "erasure coding parameters",
			params: map[string]string{
				"dataShards":   "4",
				"parityShards": "2",
			},
			wantRepl:   3, // defaultReplicationFactor is still used
			wantData:   4,
			wantParity: 2,
		},
		{
			name: "invalid replicas ignored",
			params: map[string]string{
				"replicas": "invalid",
			},
			wantRepl:   3, // defaultReplicationFactor
			wantData:   0,
			wantParity: 0,
		},
		{
			name: "negative replicas ignored",
			params: map[string]string{
				"replicas": "-1",
			},
			wantRepl:   3, // defaultReplicationFactor
			wantData:   0,
			wantParity: 0,
		},
		{
			name: "zero replicas ignored",
			params: map[string]string{
				"replicas": "0",
			},
			wantRepl:   3, // defaultReplicationFactor
			wantData:   0,
			wantParity: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := parseProtectionParams(tt.params)
			if cfg.replicas != tt.wantRepl {
				t.Errorf("parseProtectionParams() replicas = %v, want %v", cfg.replicas, tt.wantRepl)
			}
			if cfg.dataShards != tt.wantData {
				t.Errorf("parseProtectionParams() dataShards = %v, want %v", cfg.dataShards, tt.wantData)
			}
			if cfg.parityShards != tt.wantParity {
				t.Errorf("parseProtectionParams() parityShards = %v, want %v", cfg.parityShards, tt.wantParity)
			}
		})
	}
}

func TestProtectionConfig_toProtectionProfile(t *testing.T) {
	tests := []struct {
		name string
		cfg  protectionConfig
		want *metadata.ProtectionProfile
	}{
		{
			name: "single replica config returns nil",
			cfg:  protectionConfig{replicas: 1, dataShards: 0, parityShards: 0},
			want: nil,
		},
		{
			name: "replication config",
			cfg:  protectionConfig{replicas: 3, dataShards: 0, parityShards: 0},
			want: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeReplication,
				Replication: &metadata.ReplicationProfile{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
		},
		{
			name: "erasure coding config",
			cfg:  protectionConfig{replicas: 1, dataShards: 4, parityShards: 2},
			want: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeErasureCoding,
				ErasureCoding: &metadata.ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
		},
		{
			name: "both EC and replication prefers EC",
			cfg:  protectionConfig{replicas: 3, dataShards: 4, parityShards: 2},
			want: &metadata.ProtectionProfile{
				Mode: metadata.ProtectionModeErasureCoding,
				ErasureCoding: &metadata.ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.toProtectionProfile()
			if tt.want == nil {
				if got != nil {
					t.Errorf("toProtectionProfile() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Errorf("toProtectionProfile() = nil, want %v", tt.want)
				return
			}
			if got.Mode != tt.want.Mode {
				t.Errorf("toProtectionProfile() Mode = %v, want %v", got.Mode, tt.want.Mode)
			}
			if got.Replication != nil && tt.want.Replication != nil {
				if got.Replication.Factor != tt.want.Replication.Factor {
					t.Errorf("toProtectionProfile() Replication.Factor = %v, want %v", got.Replication.Factor, tt.want.Replication.Factor)
				}
				if got.Replication.WriteQuorum != tt.want.Replication.WriteQuorum {
					t.Errorf("toProtectionProfile() Replication.WriteQuorum = %v, want %v", got.Replication.WriteQuorum, tt.want.Replication.WriteQuorum)
				}
			}
			if got.ErasureCoding != nil && tt.want.ErasureCoding != nil {
				if got.ErasureCoding.DataShards != tt.want.ErasureCoding.DataShards {
					t.Errorf("toProtectionProfile() ErasureCoding.DataShards = %v, want %v", got.ErasureCoding.DataShards, tt.want.ErasureCoding.DataShards)
				}
				if got.ErasureCoding.ParityShards != tt.want.ErasureCoding.ParityShards {
					t.Errorf("toProtectionProfile() ErasureCoding.ParityShards = %v, want %v", got.ErasureCoding.ParityShards, tt.want.ErasureCoding.ParityShards)
				}
			}
		})
	}
}

func TestProtectionConfig_requiredNodes(t *testing.T) {
	tests := []struct {
		name string
		cfg  protectionConfig
		want int
	}{
		{
			name: "default single node",
			cfg:  protectionConfig{replicas: 1, dataShards: 0, parityShards: 0},
			want: 1,
		},
		{
			name: "3-way replication",
			cfg:  protectionConfig{replicas: 3, dataShards: 0, parityShards: 0},
			want: 3,
		},
		{
			name: "4+2 erasure coding",
			cfg:  protectionConfig{replicas: 1, dataShards: 4, parityShards: 2},
			want: 6,
		},
		{
			name: "8+2 erasure coding",
			cfg:  protectionConfig{replicas: 1, dataShards: 8, parityShards: 2},
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.requiredNodes(); got != tt.want {
				t.Errorf("requiredNodes() = %v, want %v", got, tt.want)
			}
		})
	}
}
