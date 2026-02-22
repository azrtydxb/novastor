package metadata

import (
	pb "github.com/piwi3910/novastor/api/proto/metadata"
)

// NodeMetaToProto converts a Go NodeMeta to its protobuf representation.
func NodeMetaToProto(n *NodeMeta) *pb.NodeMeta {
	if n == nil {
		return nil
	}
	return &pb.NodeMeta{
		NodeId:            n.NodeID,
		Address:           n.Address,
		DiskCount:         int32(n.DiskCount),
		TotalCapacity:     n.TotalCapacity,
		AvailableCapacity: n.AvailableCapacity,
		Zone:              n.Zone,
		Rack:              n.Rack,
		LastHeartbeat:     n.LastHeartbeat,
		Status:            n.Status,
	}
}

// NodeMetaFromProto converts a protobuf NodeMeta to its Go representation.
func NodeMetaFromProto(n *pb.NodeMeta) *NodeMeta {
	if n == nil {
		return nil
	}
	return &NodeMeta{
		NodeID:            n.NodeId,
		Address:           n.Address,
		DiskCount:         int(n.DiskCount),
		TotalCapacity:     n.TotalCapacity,
		AvailableCapacity: n.AvailableCapacity,
		Zone:              n.Zone,
		Rack:              n.Rack,
		LastHeartbeat:     n.LastHeartbeat,
		Status:            n.Status,
	}
}

// SnapshotMetaToProto converts a Go SnapshotMeta to its protobuf representation.
func SnapshotMetaToProto(s *SnapshotMeta) *pb.SnapshotMeta {
	if s == nil {
		return nil
	}
	return &pb.SnapshotMeta{
		SnapshotId:     s.SnapshotID,
		SourceVolumeId: s.SourceVolumeID,
		SizeBytes:      s.SizeBytes,
		ChunkIds:       s.ChunkIDs,
		CreationTime:   s.CreationTime,
		ReadyToUse:     s.ReadyToUse,
	}
}

// SnapshotMetaFromProto converts a protobuf SnapshotMeta to its Go representation.
func SnapshotMetaFromProto(s *pb.SnapshotMeta) *SnapshotMeta {
	if s == nil {
		return nil
	}
	return &SnapshotMeta{
		SnapshotID:     s.SnapshotId,
		SourceVolumeID: s.SourceVolumeId,
		SizeBytes:      s.SizeBytes,
		ChunkIDs:       s.ChunkIds,
		CreationTime:   s.CreationTime,
		ReadyToUse:     s.ReadyToUse,
	}
}
