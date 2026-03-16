package metadata

import (
	pb "github.com/azrtydxb/novastor/api/proto/metadata"
)

// HealTaskToProto converts a Go HealTask to its protobuf representation.
func HealTaskToProto(t *HealTask) *pb.HealTaskMsg {
	if t == nil {
		return nil
	}
	return &pb.HealTaskMsg{
		Id:          t.ID,
		VolumeId:    t.VolumeID,
		ChunkId:     t.ChunkID,
		ShardIndex:  int32(t.ShardIndex),
		Type:        t.Type,
		Priority:    int32(t.Priority),
		SourceNodes: t.SourceNodes,
		DestNode:    t.DestNode,
		SizeBytes:   t.SizeBytes,
		Status:      t.Status,
		BytesDone:   t.BytesDone,
		RetryCount:  int32(t.RetryCount),
		LastError:   t.LastError,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
	}
}

// HealTaskFromProto converts a protobuf HealTaskMsg to its Go representation.
func HealTaskFromProto(t *pb.HealTaskMsg) *HealTask {
	if t == nil {
		return nil
	}
	return &HealTask{
		ID:          t.Id,
		VolumeID:    t.VolumeId,
		ChunkID:     t.ChunkId,
		ShardIndex:  int(t.ShardIndex),
		Type:        t.Type,
		Priority:    int(t.Priority),
		SourceNodes: t.SourceNodes,
		DestNode:    t.DestNode,
		SizeBytes:   t.SizeBytes,
		Status:      t.Status,
		BytesDone:   t.BytesDone,
		RetryCount:  int(t.RetryCount),
		LastError:   t.LastError,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
	}
}
