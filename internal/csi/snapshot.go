package csi

import (
	"context"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotMeta holds the metadata for a volume snapshot.
// Snapshots are metadata-only since chunks are immutable (content-addressed).
type SnapshotMeta struct {
	SnapshotID     string
	SourceVolumeID string
	SizeBytes      uint64
	ChunkIDs       []string
	CreationTime   int64 // Unix nanoseconds
}

// SnapshotStore extends MetadataStore with snapshot operations.
type SnapshotStore interface {
	MetadataStore
	PutSnapshotMeta(ctx context.Context, meta *SnapshotMeta) error
	GetSnapshotMeta(ctx context.Context, snapshotID string) (*SnapshotMeta, error)
	DeleteSnapshotMeta(ctx context.Context, snapshotID string) error
	ListSnapshotMetas(ctx context.Context) ([]*SnapshotMeta, error)
}

// SnapshotController implements the CSI snapshot RPCs.
type SnapshotController struct {
	csi.UnimplementedControllerServer
	store SnapshotStore
}

// NewSnapshotController creates a SnapshotController backed by the given store.
func NewSnapshotController(store SnapshotStore) *SnapshotController {
	return &SnapshotController{store: store}
}

// CreateSnapshot creates a metadata-only snapshot of an existing volume.
func (sc *SnapshotController) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot name is required")
	}

	sourceVolumeID := req.GetSourceVolumeId()
	if sourceVolumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume ID is required")
	}

	// Look up the source volume to copy its chunk references.
	vol, err := sc.store.GetVolumeMeta(ctx, sourceVolumeID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source volume %s not found: %v", sourceVolumeID, err)
	}

	now := time.Now()
	snapshotID := uuid.New().String()

	// Copy chunk IDs — since chunks are content-addressed and immutable,
	// a snapshot is just a reference to the same set of chunks.
	chunksCopy := make([]string, len(vol.ChunkIDs))
	copy(chunksCopy, vol.ChunkIDs)

	snapMeta := &SnapshotMeta{
		SnapshotID:     snapshotID,
		SourceVolumeID: sourceVolumeID,
		SizeBytes:      vol.SizeBytes,
		ChunkIDs:       chunksCopy,
		CreationTime:   now.UnixNano(),
	}

	if err := sc.store.PutSnapshotMeta(ctx, snapMeta); err != nil {
		return nil, status.Errorf(codes.Internal, "storing snapshot metadata: %v", err)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: sourceVolumeID,
			SizeBytes:      int64(vol.SizeBytes),
			CreationTime:   timestamppb.New(now),
			ReadyToUse:     true,
		},
	}, nil
}

// DeleteSnapshot removes a snapshot's metadata. Idempotent for not-found per CSI spec.
func (sc *SnapshotController) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	snapshotID := req.GetSnapshotId()
	if snapshotID == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot ID is required")
	}

	// If the snapshot does not exist, succeed idempotently per CSI spec.
	_, getErr := sc.store.GetSnapshotMeta(ctx, snapshotID)
	if getErr != nil {
		// Snapshot doesn't exist - succeed idempotently (no error returned)
		// nolint:nilerr // Idempotent delete per CSI spec
		return &csi.DeleteSnapshotResponse{}, nil
	}

	if deleteErr := sc.store.DeleteSnapshotMeta(ctx, snapshotID); deleteErr != nil {
		return nil, status.Errorf(codes.Internal, "deleting snapshot metadata: %v", deleteErr)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

// ListSnapshots returns all snapshots, optionally filtered by source volume ID.
func (sc *SnapshotController) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	allSnaps, err := sc.store.ListSnapshotMetas(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing snapshots: %v", err)
	}

	filterVolumeID := req.GetSourceVolumeId()

	var entries []*csi.ListSnapshotsResponse_Entry
	for _, snap := range allSnaps {
		if filterVolumeID != "" && snap.SourceVolumeID != filterVolumeID {
			continue
		}
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{
			Snapshot: &csi.Snapshot{
				SnapshotId:     snap.SnapshotID,
				SourceVolumeId: snap.SourceVolumeID,
				SizeBytes:      int64(snap.SizeBytes),
				CreationTime:   timestamppb.New(time.Unix(0, snap.CreationTime)),
				ReadyToUse:     true,
			},
		})
	}

	return &csi.ListSnapshotsResponse{
		Entries: entries,
	}, nil
}
