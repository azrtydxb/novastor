package csi

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/piwi3910/novastor/internal/metadata"
)

const (
	// chunkSize is the fixed chunk size in bytes (4 MiB).
	chunkSize uint64 = 4 * 1024 * 1024

	// defaultVolumeSize is the fallback volume size when none is requested.
	defaultVolumeSize uint64 = 1 * 1024 * 1024 * 1024 // 1 GiB
)

// MetadataStore is the subset of the metadata service used by the controller.
type MetadataStore interface {
	PutVolumeMeta(ctx context.Context, meta *metadata.VolumeMeta) error
	GetVolumeMeta(ctx context.Context, volumeID string) (*metadata.VolumeMeta, error)
	DeleteVolumeMeta(ctx context.Context, volumeID string) error
}

// PlacementEngine selects storage nodes for new chunks.
type PlacementEngine interface {
	Place(count int) []string
}

// ControllerServer implements the CSI Controller service.
type ControllerServer struct {
	csi.UnimplementedControllerServer
	meta   MetadataStore
	placer PlacementEngine
}

// NewControllerServer creates a ControllerServer backed by the given stores.
func NewControllerServer(meta MetadataStore, placer PlacementEngine) *ControllerServer {
	return &ControllerServer{
		meta:   meta,
		placer: placer,
	}
}

// hasRWXCapability checks whether any of the volume capabilities request
// MULTI_NODE_MULTI_WRITER (ReadWriteMany) access.
func hasRWXCapability(caps []*csi.VolumeCapability) bool {
	for _, cap := range caps {
		if am := cap.GetAccessMode(); am != nil {
			if am.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER {
				return true
			}
		}
	}
	return false
}

// CreateVolume provisions a new volume by computing chunks and persisting metadata.
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume name is required")
	}

	// Determine requested capacity.
	requiredBytes := defaultVolumeSize
	if capRange := req.GetCapacityRange(); capRange != nil {
		if capRange.GetRequiredBytes() > 0 {
			requiredBytes = uint64(capRange.GetRequiredBytes())
		}
	}

	// Calculate chunk count (round up).
	chunkCount := int((requiredBytes + chunkSize - 1) / chunkSize)

	// Place chunks across storage nodes.
	nodeIDs := cs.placer.Place(chunkCount)
	if len(nodeIDs) == 0 {
		return nil, status.Error(codes.ResourceExhausted, "no storage nodes available for placement")
	}

	volumeID := uuid.New().String()

	chunkIDs := make([]string, chunkCount)
	for i := range chunkCount {
		chunkIDs[i] = fmt.Sprintf("%s-chunk-%04d", volumeID, i)
	}

	vm := &metadata.VolumeMeta{
		VolumeID:  volumeID,
		SizeBytes: requiredBytes,
		ChunkIDs:  chunkIDs,
	}

	if err := cs.meta.PutVolumeMeta(ctx, vm); err != nil {
		return nil, status.Errorf(codes.Internal, "storing volume metadata: %v", err)
	}

	// Build accessible topology from the first placed node.
	topology := &csi.Topology{
		Segments: map[string]string{
			"novastor.io/node": nodeIDs[0],
		},
	}

	// Set volume context for RWX (NFS-backed) volumes.
	volContext := map[string]string{}
	if hasRWXCapability(req.GetVolumeCapabilities()) {
		volContext["nfsServer"] = nodeIDs[0]
		volContext["nfsShare"] = fmt.Sprintf("/exports/%s", volumeID)
		volContext["accessMode"] = "RWX"
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           volumeID,
			CapacityBytes:      int64(requiredBytes),
			AccessibleTopology: []*csi.Topology{topology},
			VolumeContext:      volContext,
		},
	}, nil
}

// DeleteVolume removes a volume's metadata.
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Verify the volume exists. If not found, succeed idempotently per CSI spec.
	if _, err := cs.meta.GetVolumeMeta(ctx, volumeID); err != nil {
		return &csi.DeleteVolumeResponse{}, nil
	}

	if err := cs.meta.DeleteVolumeMeta(ctx, volumeID); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting volume metadata: %v", err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

// ValidateVolumeCapabilities checks that the requested access modes are supported.
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume ID is required")
	}

	// Verify the volume exists.
	if _, err := cs.meta.GetVolumeMeta(ctx, req.GetVolumeId()); err != nil {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}

	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}

	for _, cap := range req.GetVolumeCapabilities() {
		accessMode := cap.GetAccessMode()
		if accessMode == nil {
			continue
		}
		switch accessMode.GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
			// Supported -- ReadWriteOnce.
		case csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
			// Supported -- ReadWriteMany via NFS.
		default:
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: fmt.Sprintf("unsupported access mode: %v", accessMode.GetMode()),
			}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

// ListVolumes is not supported.
func (cs *ControllerServer) ListVolumes(_ context.Context, _ *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not supported")
}

// ControllerPublishVolume is handled at the node level.
func (cs *ControllerServer) ControllerPublishVolume(_ context.Context, _ *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not supported")
}

// ControllerUnpublishVolume is handled at the node level.
func (cs *ControllerServer) ControllerUnpublishVolume(_ context.Context, _ *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is not supported")
}

// GetCapacity is not supported.
func (cs *ControllerServer) GetCapacity(_ context.Context, _ *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not supported")
}

// ControllerGetCapabilities returns the controller capabilities.
func (cs *ControllerServer) ControllerGetCapabilities(_ context.Context, _ *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}

	var capabilities []*csi.ControllerServiceCapability
	for _, c := range caps {
		capabilities = append(capabilities, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}

	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: capabilities,
	}, nil
}
