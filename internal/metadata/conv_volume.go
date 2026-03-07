package metadata

import (
	pb "github.com/azrtydxb/novastor/api/proto/metadata"
)

// VolumeMetaToProto converts a Go VolumeMeta to its protobuf representation.
func VolumeMetaToProto(v *VolumeMeta) *pb.VolumeMeta {
	if v == nil {
		return nil
	}
	return &pb.VolumeMeta{
		VolumeId:          v.VolumeID,
		Pool:              v.Pool,
		SizeBytes:         v.SizeBytes,
		ChunkIds:          v.ChunkIDs,
		DataProtection:    DataProtectionConfigToProto(v.DataProtection),
		TargetNodeId:      v.TargetNodeID,
		TargetAddress:     v.TargetAddress,
		TargetPort:        v.TargetPort,
		SubsystemNqn:      v.SubsystemNQN,
		ProtectionProfile: ProtectionProfileToProto(v.ProtectionProfile),
		ComplianceInfo:    ComplianceInfoToProto(v.ComplianceInfo),
	}
}

// VolumeMetaFromProto converts a protobuf VolumeMeta to its Go representation.
func VolumeMetaFromProto(v *pb.VolumeMeta) *VolumeMeta {
	if v == nil {
		return nil
	}
	return &VolumeMeta{
		VolumeID:          v.VolumeId,
		Pool:              v.Pool,
		SizeBytes:         v.SizeBytes,
		ChunkIDs:          v.ChunkIds,
		DataProtection:    DataProtectionConfigFromProto(v.DataProtection),
		TargetNodeID:      v.TargetNodeId,
		TargetAddress:     v.TargetAddress,
		TargetPort:        v.TargetPort,
		SubsystemNQN:      v.SubsystemNqn,
		ProtectionProfile: ProtectionProfileFromProto(v.ProtectionProfile),
		ComplianceInfo:    ComplianceInfoFromProto(v.ComplianceInfo),
	}
}

// PlacementMapToProto converts a Go PlacementMap to its protobuf representation.
func PlacementMapToProto(pm *PlacementMap) *pb.PlacementMap {
	if pm == nil {
		return nil
	}
	return &pb.PlacementMap{
		ChunkId: pm.ChunkID,
		Nodes:   pm.Nodes,
	}
}

// PlacementMapFromProto converts a protobuf PlacementMap to its Go representation.
func PlacementMapFromProto(pm *pb.PlacementMap) *PlacementMap {
	if pm == nil {
		return nil
	}
	return &PlacementMap{
		ChunkID: pm.ChunkId,
		Nodes:   pm.Nodes,
	}
}

// ProtectionProfileToProto converts a Go ProtectionProfile to its protobuf representation.
func ProtectionProfileToProto(p *ProtectionProfile) *pb.ProtectionProfile {
	if p == nil {
		return nil
	}
	pp := &pb.ProtectionProfile{
		Mode: protectionModeToProto(p.Mode),
	}
	if p.Replication != nil {
		pp.Replication = &pb.ReplicationProfile{
			Factor:      int32(p.Replication.Factor),
			WriteQuorum: int32(p.Replication.WriteQuorum),
		}
	}
	if p.ErasureCoding != nil {
		pp.ErasureCoding = &pb.ErasureCodingProfile{
			DataShards:   int32(p.ErasureCoding.DataShards),
			ParityShards: int32(p.ErasureCoding.ParityShards),
		}
	}
	return pp
}

// ProtectionProfileFromProto converts a protobuf ProtectionProfile to its Go representation.
func ProtectionProfileFromProto(p *pb.ProtectionProfile) *ProtectionProfile {
	if p == nil {
		return nil
	}
	pp := &ProtectionProfile{
		Mode: protectionModeFromProto(p.Mode),
	}
	if p.Replication != nil {
		pp.Replication = &ReplicationProfile{
			Factor:      int(p.Replication.Factor),
			WriteQuorum: int(p.Replication.WriteQuorum),
		}
	}
	if p.ErasureCoding != nil {
		pp.ErasureCoding = &ErasureCodingProfile{
			DataShards:   int(p.ErasureCoding.DataShards),
			ParityShards: int(p.ErasureCoding.ParityShards),
		}
	}
	return pp
}

// DataProtectionConfigToProto converts using the ProtectionProfile alias.
func DataProtectionConfigToProto(p *DataProtectionConfig) *pb.DataProtectionConfig {
	if p == nil {
		return nil
	}
	dp := &pb.DataProtectionConfig{
		Mode: protectionModeToProto(p.Mode),
	}
	if p.Replication != nil {
		dp.Replication = &pb.ReplicationProfile{
			Factor:      int32(p.Replication.Factor),
			WriteQuorum: int32(p.Replication.WriteQuorum),
		}
	}
	if p.ErasureCoding != nil {
		dp.ErasureCoding = &pb.ErasureCodingProfile{
			DataShards:   int32(p.ErasureCoding.DataShards),
			ParityShards: int32(p.ErasureCoding.ParityShards),
		}
	}
	return dp
}

// DataProtectionConfigFromProto converts a protobuf DataProtectionConfig to Go.
func DataProtectionConfigFromProto(p *pb.DataProtectionConfig) *DataProtectionConfig {
	if p == nil {
		return nil
	}
	dp := &DataProtectionConfig{
		Mode: protectionModeFromProto(p.Mode),
	}
	if p.Replication != nil {
		dp.Replication = &ReplicationProfile{
			Factor:      int(p.Replication.Factor),
			WriteQuorum: int(p.Replication.WriteQuorum),
		}
	}
	if p.ErasureCoding != nil {
		dp.ErasureCoding = &ErasureCodingProfile{
			DataShards:   int(p.ErasureCoding.DataShards),
			ParityShards: int(p.ErasureCoding.ParityShards),
		}
	}
	return dp
}

// ComplianceInfoToProto converts a Go ComplianceInfo to its protobuf representation.
func ComplianceInfoToProto(c *ComplianceInfo) *pb.ComplianceInfo {
	if c == nil {
		return nil
	}
	return &pb.ComplianceInfo{
		State:             complianceStateToProto(c.State),
		LastCheckTime:     c.LastCheckTime,
		AvailableReplicas: int32(c.AvailableReplicas),
		RequiredReplicas:  int32(c.RequiredReplicas),
		Reason:            c.Reason,
	}
}

// ComplianceInfoFromProto converts a protobuf ComplianceInfo to its Go representation.
func ComplianceInfoFromProto(c *pb.ComplianceInfo) *ComplianceInfo {
	if c == nil {
		return nil
	}
	return &ComplianceInfo{
		State:             complianceStateFromProto(c.State),
		LastCheckTime:     c.LastCheckTime,
		AvailableReplicas: int(c.AvailableReplicas),
		RequiredReplicas:  int(c.RequiredReplicas),
		Reason:            c.Reason,
	}
}

func protectionModeToProto(m ProtectionMode) pb.ProtectionMode {
	switch m {
	case ProtectionModeReplication:
		return pb.ProtectionMode_PROTECTION_MODE_REPLICATION
	case ProtectionModeErasureCoding:
		return pb.ProtectionMode_PROTECTION_MODE_ERASURE_CODING
	default:
		return pb.ProtectionMode_PROTECTION_MODE_UNSPECIFIED
	}
}

func protectionModeFromProto(m pb.ProtectionMode) ProtectionMode {
	switch m {
	case pb.ProtectionMode_PROTECTION_MODE_REPLICATION:
		return ProtectionModeReplication
	case pb.ProtectionMode_PROTECTION_MODE_ERASURE_CODING:
		return ProtectionModeErasureCoding
	default:
		return ""
	}
}

func complianceStateToProto(s ComplianceState) pb.ComplianceState {
	switch s {
	case ComplianceStateUnknown:
		return pb.ComplianceState_COMPLIANCE_STATE_UNKNOWN
	case ComplianceStateCompliant:
		return pb.ComplianceState_COMPLIANCE_STATE_COMPLIANT
	case ComplianceStateDegraded:
		return pb.ComplianceState_COMPLIANCE_STATE_DEGRADED
	case ComplianceStateNonCompliant:
		return pb.ComplianceState_COMPLIANCE_STATE_NON_COMPLIANT
	default:
		return pb.ComplianceState_COMPLIANCE_STATE_UNSPECIFIED
	}
}

func complianceStateFromProto(s pb.ComplianceState) ComplianceState {
	switch s {
	case pb.ComplianceState_COMPLIANCE_STATE_UNKNOWN:
		return ComplianceStateUnknown
	case pb.ComplianceState_COMPLIANCE_STATE_COMPLIANT:
		return ComplianceStateCompliant
	case pb.ComplianceState_COMPLIANCE_STATE_DEGRADED:
		return ComplianceStateDegraded
	case pb.ComplianceState_COMPLIANCE_STATE_NON_COMPLIANT:
		return ComplianceStateNonCompliant
	default:
		return ""
	}
}
