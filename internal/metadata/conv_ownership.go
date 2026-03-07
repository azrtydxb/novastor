package metadata

import (
	pb "github.com/azrtydxb/novastor/api/proto/metadata"
)

// VolumeOwnershipToProto converts a Go VolumeOwnership to its protobuf representation.
func VolumeOwnershipToProto(o *VolumeOwnership) *pb.VolumeOwnership {
	if o == nil {
		return nil
	}
	return &pb.VolumeOwnership{
		VolumeId:   o.VolumeID,
		OwnerAddr:  o.OwnerAddr,
		OwnerSince: o.OwnerSince,
		Generation: o.Generation,
	}
}

// VolumeOwnershipFromProto converts a protobuf VolumeOwnership to its Go representation.
func VolumeOwnershipFromProto(o *pb.VolumeOwnership) *VolumeOwnership {
	if o == nil {
		return nil
	}
	return &VolumeOwnership{
		VolumeID:   o.VolumeId,
		OwnerAddr:  o.OwnerAddr,
		OwnerSince: o.OwnerSince,
		Generation: o.Generation,
	}
}
