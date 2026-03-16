package metadata

import (
	pb "github.com/azrtydxb/novastor/api/proto/metadata"
)

// QuotaSpecToProto converts Go QuotaScope + QuotaSpec to a protobuf QuotaSpecMsg.
func QuotaSpecToProto(scope QuotaScope, spec *QuotaSpec) *pb.QuotaSpecMsg {
	if spec == nil {
		return nil
	}
	return &pb.QuotaSpecMsg{
		Kind:            scope.Kind,
		Name:            scope.Name,
		StorageHard:     spec.StorageHard,
		StorageSoft:     spec.StorageSoft,
		ObjectCountHard: spec.ObjectCountHard,
	}
}

// QuotaSpecFromProto converts a protobuf QuotaSpecMsg to Go QuotaScope and QuotaSpec.
func QuotaSpecFromProto(msg *pb.QuotaSpecMsg) (QuotaScope, *QuotaSpec) {
	if msg == nil {
		return QuotaScope{}, nil
	}
	scope := QuotaScope{
		Kind: msg.Kind,
		Name: msg.Name,
	}
	spec := &QuotaSpec{
		StorageHard:     msg.StorageHard,
		StorageSoft:     msg.StorageSoft,
		ObjectCountHard: msg.ObjectCountHard,
	}
	return scope, spec
}

// QuotaUsageToProto converts Go QuotaScope + QuotaUsage to a protobuf QuotaUsageMsg.
func QuotaUsageToProto(scope QuotaScope, usage *QuotaUsage) *pb.QuotaUsageMsg {
	if usage == nil {
		return nil
	}
	return &pb.QuotaUsageMsg{
		Kind:            scope.Kind,
		Name:            scope.Name,
		StorageUsed:     usage.StorageUsed,
		ObjectCountUsed: usage.ObjectCountUsed,
	}
}

// QuotaUsageFromProto converts a protobuf QuotaUsageMsg to Go QuotaUsage.
func QuotaUsageFromProto(msg *pb.QuotaUsageMsg) *QuotaUsage {
	if msg == nil {
		return nil
	}
	return &QuotaUsage{
		StorageUsed:     msg.StorageUsed,
		ObjectCountUsed: msg.ObjectCountUsed,
	}
}
