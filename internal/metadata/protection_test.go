package metadata

import (
	"testing"

	"github.com/azrtydxb/novastor/api/v1alpha1"
)

func TestProtectionProfile_Validate(t *testing.T) {
	tests := []struct {
		name    string
		profile *ProtectionProfile
		wantErr bool
	}{
		{
			name:    "nil profile",
			profile: nil,
			wantErr: true,
		},
		{
			name: "valid replication profile",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
			wantErr: false,
		},
		{
			name: "replication mode without profile",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
			},
			wantErr: true,
		},
		{
			name: "replication factor too high",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 6,
				},
			},
			wantErr: true,
		},
		{
			name: "replication factor zero",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 0,
				},
			},
			wantErr: true,
		},
		{
			name: "write quorum exceeds factor",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor:      3,
					WriteQuorum: 4,
				},
			},
			wantErr: true,
		},
		{
			name: "negative write quorum",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor:      3,
					WriteQuorum: -1,
				},
			},
			wantErr: true,
		},
		{
			name: "valid erasure coding profile",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
			wantErr: false,
		},
		{
			name: "erasure coding mode without profile",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
			},
			wantErr: true,
		},
		{
			name: "data shards too few",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   1,
					ParityShards: 2,
				},
			},
			wantErr: true,
		},
		{
			name: "parity shards zero",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 0,
				},
			},
			wantErr: true,
		},
		{
			name: "unknown mode",
			profile: &ProtectionProfile{
				Mode: ProtectionMode("unknown"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.profile.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProtectionProfile_RequiredReplicas(t *testing.T) {
	tests := []struct {
		name    string
		profile *ProtectionProfile
		want    int
	}{
		{
			name:    "nil profile",
			profile: nil,
			want:    1,
		},
		{
			name: "replication with explicit quorum",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
			want: 2,
		},
		{
			name: "replication with default quorum (majority)",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 3,
				},
			},
			want: 2, // (3/2)+1 = 2
		},
		{
			name: "replication factor 5",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 5,
				},
			},
			want: 3, // (5/2)+1 = 3
		},
		{
			name: "erasure coding 4+2",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
			want: 4, // need all data shards
		},
		{
			name: "erasure coding 8+3",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   8,
					ParityShards: 3,
				},
			},
			want: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.profile.RequiredReplicas(); got != tt.want {
				t.Errorf("RequiredReplicas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProtectionProfile_TargetReplicas(t *testing.T) {
	tests := []struct {
		name    string
		profile *ProtectionProfile
		want    int
	}{
		{
			name:    "nil profile",
			profile: nil,
			want:    1,
		},
		{
			name: "replication factor 3",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 3,
				},
			},
			want: 3,
		},
		{
			name: "replication factor 5",
			profile: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor: 5,
				},
			},
			want: 5,
		},
		{
			name: "erasure coding 4+2",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
			want: 6, // 4+2
		},
		{
			name: "erasure coding 8+3",
			profile: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   8,
					ParityShards: 3,
				},
			},
			want: 11, // 8+3
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.profile.TargetReplicas(); got != tt.want {
				t.Errorf("TargetReplicas() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewProtectionProfileFromV1Alpha1(t *testing.T) {
	tests := []struct {
		name string
		spec *v1alpha1.DataProtectionSpec
		want *ProtectionProfile
	}{
		{
			name: "nil spec",
			spec: nil,
			want: nil,
		},
		{
			name: "replication mode",
			spec: &v1alpha1.DataProtectionSpec{
				Mode: "replication",
				Replication: &v1alpha1.ReplicationSpec{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
			want: &ProtectionProfile{
				Mode: ProtectionModeReplication,
				Replication: &ReplicationProfile{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
		},
		{
			name: "erasure coding mode",
			spec: &v1alpha1.DataProtectionSpec{
				Mode: "erasureCoding",
				ErasureCoding: &v1alpha1.ErasureCodingSpec{
					DataShards:   4,
					ParityShards: 2,
				},
			},
			want: &ProtectionProfile{
				Mode: ProtectionModeErasureCoding,
				ErasureCoding: &ErasureCodingProfile{
					DataShards:   4,
					ParityShards: 2,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewProtectionProfileFromV1Alpha1(tt.spec)
			if got == nil && tt.want == nil {
				return
			}
			if got == nil || tt.want == nil {
				t.Fatalf("NewProtectionProfileFromV1Alpha1() = %v, want %v", got, tt.want)
			}
			if got.Mode != tt.want.Mode {
				t.Errorf("Mode = %v, want %v", got.Mode, tt.want.Mode)
			}
			if got.Replication != nil && tt.want.Replication != nil {
				if got.Replication.Factor != tt.want.Replication.Factor {
					t.Errorf("Replication.Factor = %v, want %v", got.Replication.Factor, tt.want.Replication.Factor)
				}
				if got.Replication.WriteQuorum != tt.want.Replication.WriteQuorum {
					t.Errorf("Replication.WriteQuorum = %v, want %v", got.Replication.WriteQuorum, tt.want.Replication.WriteQuorum)
				}
			}
			if got.ErasureCoding != nil && tt.want.ErasureCoding != nil {
				if got.ErasureCoding.DataShards != tt.want.ErasureCoding.DataShards {
					t.Errorf("ErasureCoding.DataShards = %v, want %v", got.ErasureCoding.DataShards, tt.want.ErasureCoding.DataShards)
				}
				if got.ErasureCoding.ParityShards != tt.want.ErasureCoding.ParityShards {
					t.Errorf("ErasureCoding.ParityShards = %v, want %v", got.ErasureCoding.ParityShards, tt.want.ErasureCoding.ParityShards)
				}
			}
		})
	}
}

func TestUpdateComplianceState(t *testing.T) {
	profile := &ProtectionProfile{
		Mode: ProtectionModeReplication,
		Replication: &ReplicationProfile{
			Factor:      3,
			WriteQuorum: 2,
		},
	}

	tests := []struct {
		name              string
		info              ComplianceInfo
		availableReplicas int
		wantState         ComplianceState
		wantReason        string
	}{
		{
			name:              "fully compliant",
			info:              ComplianceInfo{},
			availableReplicas: 3,
			wantState:         ComplianceStateCompliant,
			wantReason:        "All 3 replicas available",
		},
		{
			name:              "degraded",
			info:              ComplianceInfo{},
			availableReplicas: 2,
			wantState:         ComplianceStateDegraded,
			wantReason:        "2 of 3 replicas available (degraded)",
		},
		{
			name:              "non-compliant",
			info:              ComplianceInfo{},
			availableReplicas: 1,
			wantState:         ComplianceStateNonCompliant,
			wantReason:        "Only 1 of 2 required replicas available",
		},
		{
			name:              "zero replicas",
			info:              ComplianceInfo{},
			availableReplicas: 0,
			wantState:         ComplianceStateNonCompliant,
			wantReason:        "Only 0 of 2 required replicas available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := tt.info // Create a copy to modify
			UpdateComplianceState(&info, tt.availableReplicas, profile)
			if info.State != tt.wantState {
				t.Errorf("State = %v, want %v", info.State, tt.wantState)
			}
			if info.Reason != tt.wantReason {
				t.Errorf("Reason = %v, want %v", info.Reason, tt.wantReason)
			}
			if info.AvailableReplicas != tt.availableReplicas {
				t.Errorf("AvailableReplicas = %v, want %v", info.AvailableReplicas, tt.availableReplicas)
			}
			if info.RequiredReplicas != 2 {
				t.Errorf("RequiredReplicas = %v, want 2", info.RequiredReplicas)
			}
		})
	}
}
