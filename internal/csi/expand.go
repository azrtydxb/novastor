package csi

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ExpandController implements the CSI ControllerExpandVolume RPC.
type ExpandController struct {
	csi.UnimplementedControllerServer
	meta   MetadataStore
	placer PlacementEngine
}

// NewExpandController creates an ExpandController backed by the given stores.
func NewExpandController(meta MetadataStore, placer PlacementEngine) *ExpandController {
	return &ExpandController{
		meta:   meta,
		placer: placer,
	}
}

// ControllerExpandVolume expands a volume by adding additional chunks as needed.
func (ec *ExpandController) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Get existing volume metadata.
	vol, err := ec.meta.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found: %v", volumeID, err)
	}

	// Determine the requested new capacity.
	var requestedBytes uint64
	if capRange := req.GetCapacityRange(); capRange != nil {
		if capRange.GetRequiredBytes() > 0 {
			requestedBytes = uint64(capRange.GetRequiredBytes())
		}
	}

	if requestedBytes == 0 {
		requestedBytes = vol.SizeBytes
	}

	// Shrinking is not allowed.
	if requestedBytes < vol.SizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "cannot shrink volume from %d to %d bytes", vol.SizeBytes, requestedBytes)
	}

	// If the size is the same, no-op.
	if requestedBytes == vol.SizeBytes {
		return &csi.ControllerExpandVolumeResponse{
			CapacityBytes:         int64(vol.SizeBytes),
			NodeExpansionRequired: false,
		}, nil
	}

	// Calculate new total chunk count.
	newChunkCount := int((requestedBytes + chunkSize - 1) / chunkSize)
	existingChunkCount := len(vol.ChunkIDs)

	if newChunkCount > existingChunkCount {
		additionalCount := newChunkCount - existingChunkCount

		// Place additional chunks across storage nodes.
		nodeIDs := ec.placer.Place(additionalCount)
		if len(nodeIDs) == 0 {
			return nil, status.Error(codes.ResourceExhausted, "no storage nodes available for placement")
		}

		// Generate new chunk IDs and append to existing list.
		for i := range additionalCount {
			chunkID := fmt.Sprintf("%s-chunk-%04d", volumeID, existingChunkCount+i)
			vol.ChunkIDs = append(vol.ChunkIDs, chunkID)
		}
	}

	// Update volume metadata with new size and chunks.
	vol.SizeBytes = requestedBytes
	if err := ec.meta.PutVolumeMeta(ctx, vol); err != nil {
		return nil, status.Errorf(codes.Internal, "updating volume metadata: %v", err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         int64(requestedBytes),
		NodeExpansionRequired: false,
	}, nil
}
