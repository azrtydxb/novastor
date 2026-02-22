package metadata

import (
	pb "github.com/piwi3910/novastor/api/proto/metadata"
)

// ShardPlacementToProto converts a Go ShardPlacement to its protobuf representation.
func ShardPlacementToProto(sp *ShardPlacement) *pb.ShardPlacementMsg {
	if sp == nil {
		return nil
	}
	return &pb.ShardPlacementMsg{
		ChunkId:      sp.ChunkID,
		VolumeId:     sp.VolumeID,
		ShardIndex:   int32(sp.ShardIndex),
		NodeId:       sp.NodeID,
		LastVerified: sp.LastVerified,
	}
}

// ShardPlacementFromProto converts a protobuf ShardPlacementMsg to its Go representation.
func ShardPlacementFromProto(sp *pb.ShardPlacementMsg) *ShardPlacement {
	if sp == nil {
		return nil
	}
	return &ShardPlacement{
		ChunkID:      sp.ChunkId,
		VolumeID:     sp.VolumeId,
		ShardIndex:   int(sp.ShardIndex),
		NodeID:       sp.NodeId,
		LastVerified: sp.LastVerified,
	}
}
