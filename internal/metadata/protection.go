package metadata

import (
	"fmt"

	"github.com/azrtydxb/novastor/api/v1alpha1"
)

// ProtectionMode represents the data protection mode.
type ProtectionMode string

const (
	// ProtectionModeReplication uses synchronous replication for data protection.
	ProtectionModeReplication ProtectionMode = "replication"
	// ProtectionModeErasureCoding uses Reed-Solomon erasure coding for data protection.
	ProtectionModeErasureCoding ProtectionMode = "erasureCoding"
)

// ProtectionProfile captures the data protection settings for a volume or chunk.
// It specifies how data is protected across nodes (replication or erasure coding).
type ProtectionProfile struct {
	// Mode is the protection mode: replication or erasureCoding.
	Mode ProtectionMode `json:"mode"`

	// Replication holds replication-specific settings when Mode is replication.
	Replication *ReplicationProfile `json:"replication,omitempty"`

	// ErasureCoding holds erasure coding-specific settings when Mode is erasureCoding.
	ErasureCoding *ErasureCodingProfile `json:"erasureCoding,omitempty"`
}

// ReplicationProfile defines synchronous replication parameters.
type ReplicationProfile struct {
	// Factor is the number of replicas to maintain (e.g., 3 for 3-way replication).
	Factor int `json:"factor"`

	// WriteQuorum is the minimum number of successful writes required for a write to complete.
	// Defaults to majority (factor/2 + 1) if not specified.
	WriteQuorum int `json:"writeQuorum,omitempty"`
}

// ErasureCodingProfile defines Reed-Solomon erasure coding parameters.
type ErasureCodingProfile struct {
	// DataShards is the number of data shards (e.g., 4).
	DataShards int `json:"dataShards"`

	// ParityShards is the number of parity shards (e.g., 2 for 4+2 encoding).
	ParityShards int `json:"parityShards"`
}

// ComplianceState represents the compliance state of a volume or chunk.
type ComplianceState string

const (
	// ComplianceStateUnknown means the compliance state has not been determined.
	ComplianceStateUnknown ComplianceState = "unknown"
	// ComplianceStateCompliant means the volume/chunk meets all protection requirements.
	ComplianceStateCompliant ComplianceState = "compliant"
	// ComplianceStateDegraded means the volume/chunk is operational but below optimal redundancy.
	ComplianceStateDegraded ComplianceState = "degraded"
	// ComplianceStateNonCompliant means the volume/chunk does not meet protection requirements.
	ComplianceStateNonCompliant ComplianceState = "nonCompliant"
)

// ComplianceInfo tracks the compliance state and related metrics.
type ComplianceInfo struct {
	// State is the current compliance state.
	State ComplianceState `json:"state"`

	// LastCheckTime is the Unix timestamp (nanoseconds) of the last compliance check.
	LastCheckTime int64 `json:"lastCheckTime,omitempty"`

	// AvailableReplicas is the current number of available replicas.
	AvailableReplicas int `json:"availableReplicas,omitempty"`

	// RequiredReplicas is the minimum number of replicas required for compliance.
	RequiredReplicas int `json:"requiredReplicas,omitempty"`

	// Reason provides a human-readable explanation for the current state.
	Reason string `json:"reason,omitempty"`
}

// NewProtectionProfileFromV1Alpha1 converts a v1alpha1 DataProtectionSpec to a ProtectionProfile.
func NewProtectionProfileFromV1Alpha1(spec *v1alpha1.DataProtectionSpec) *ProtectionProfile {
	if spec == nil {
		return nil
	}

	profile := &ProtectionProfile{
		Mode: ProtectionMode(spec.Mode),
	}

	switch ProtectionMode(spec.Mode) {
	case ProtectionModeReplication:
		if spec.Replication != nil {
			profile.Replication = &ReplicationProfile{
				Factor:      spec.Replication.Factor,
				WriteQuorum: spec.Replication.WriteQuorum,
			}
		}
	case ProtectionModeErasureCoding:
		if spec.ErasureCoding != nil {
			profile.ErasureCoding = &ErasureCodingProfile{
				DataShards:   spec.ErasureCoding.DataShards,
				ParityShards: spec.ErasureCoding.ParityShards,
			}
		}
	}

	return profile
}

// Validate checks if the ProtectionProfile is valid.
func (p *ProtectionProfile) Validate() error {
	if p == nil {
		return fmt.Errorf("protection profile is nil")
	}

	switch p.Mode {
	case ProtectionModeReplication:
		if p.Replication == nil {
			return fmt.Errorf("replication mode requires replication profile")
		}
		if p.Replication.Factor < 1 || p.Replication.Factor > 5 {
			return fmt.Errorf("replication factor must be between 1 and 5, got %d", p.Replication.Factor)
		}
		if p.Replication.WriteQuorum < 0 {
			return fmt.Errorf("write quorum cannot be negative, got %d", p.Replication.WriteQuorum)
		}
		if p.Replication.WriteQuorum > p.Replication.Factor {
			return fmt.Errorf("write quorum (%d) cannot exceed replication factor (%d)",
				p.Replication.WriteQuorum, p.Replication.Factor)
		}
	case ProtectionModeErasureCoding:
		if p.ErasureCoding == nil {
			return fmt.Errorf("erasure coding mode requires erasure coding profile")
		}
		if p.ErasureCoding.DataShards < 2 {
			return fmt.Errorf("data shards must be at least 2, got %d", p.ErasureCoding.DataShards)
		}
		if p.ErasureCoding.ParityShards < 1 {
			return fmt.Errorf("parity shards must be at least 1, got %d", p.ErasureCoding.ParityShards)
		}
	default:
		return fmt.Errorf("unknown protection mode: %s", p.Mode)
	}

	return nil
}

// RequiredReplicas returns the minimum number of replicas required for compliance.
func (p *ProtectionProfile) RequiredReplicas() int {
	if p == nil {
		return 1
	}

	switch p.Mode {
	case ProtectionModeReplication:
		if p.Replication != nil && p.Replication.WriteQuorum > 0 {
			return p.Replication.WriteQuorum
		}
		if p.Replication != nil {
			// Default to majority
			return (p.Replication.Factor / 2) + 1
		}
		return 1
	case ProtectionModeErasureCoding:
		if p.ErasureCoding != nil {
			// Need at least dataShards to reconstruct
			return p.ErasureCoding.DataShards
		}
		return 1
	default:
		return 1
	}
}

// TargetReplicas returns the target number of replicas for optimal redundancy.
func (p *ProtectionProfile) TargetReplicas() int {
	if p == nil {
		return 1
	}

	switch p.Mode {
	case ProtectionModeReplication:
		if p.Replication != nil {
			return p.Replication.Factor
		}
		return 1
	case ProtectionModeErasureCoding:
		if p.ErasureCoding != nil {
			// Total shards = data + parity
			return p.ErasureCoding.DataShards + p.ErasureCoding.ParityShards
		}
		return 1
	default:
		return 1
	}
}

// UpdateComplianceState updates the ComplianceInfo based on available replicas.
func UpdateComplianceState(info *ComplianceInfo, availableReplicas int, profile *ProtectionProfile) {
	if info == nil {
		return
	}

	required := profile.RequiredReplicas()
	target := profile.TargetReplicas()

	info.AvailableReplicas = availableReplicas
	info.RequiredReplicas = required

	switch {
	case availableReplicas >= target:
		info.State = ComplianceStateCompliant
		info.Reason = fmt.Sprintf("All %d replicas available", target)
	case availableReplicas >= required:
		info.State = ComplianceStateDegraded
		info.Reason = fmt.Sprintf("%d of %d replicas available (degraded)", availableReplicas, target)
	default:
		info.State = ComplianceStateNonCompliant
		info.Reason = fmt.Sprintf("Only %d of %d required replicas available", availableReplicas, required)
	}
}
