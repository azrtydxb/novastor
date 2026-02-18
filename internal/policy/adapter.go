package policy

import (
	"context"

	"github.com/piwi3910/novastor/internal/metadata"
)

// GRPCMetadataAdapter wraps a metadata.GRPCClient to implement MetadataClient.
// It converts between the policy package types and the metadata package types.
type GRPCMetadataAdapter struct {
	client *metadata.GRPCClient
}

// NewGRPCMetadataAdapter creates a new GRPCMetadataAdapter.
func NewGRPCMetadataAdapter(client *metadata.GRPCClient) *GRPCMetadataAdapter {
	return &GRPCMetadataAdapter{client: client}
}

// GetPlacementMap implements MetadataClient.
func (a *GRPCMetadataAdapter) GetPlacementMap(ctx context.Context, chunkID string) (*PlacementMap, error) {
	pm, err := a.client.GetPlacementMap(ctx, chunkID)
	if err != nil {
		return nil, err
	}
	return &PlacementMap{
		ChunkID: pm.ChunkID,
		Nodes:   pm.Nodes,
	}, nil
}

// PutPlacementMap implements MetadataClient.
func (a *GRPCMetadataAdapter) PutPlacementMap(ctx context.Context, pm *PlacementMap) error {
	return a.client.PutPlacementMap(ctx, &metadata.PlacementMap{
		ChunkID: pm.ChunkID,
		Nodes:   pm.Nodes,
	})
}

// ListPlacementMaps implements MetadataClient.
func (a *GRPCMetadataAdapter) ListPlacementMaps(ctx context.Context) ([]*PlacementMap, error) {
	pms, err := a.client.ListPlacementMaps(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*PlacementMap, len(pms))
	for i, pm := range pms {
		result[i] = &PlacementMap{
			ChunkID: pm.ChunkID,
			Nodes:   pm.Nodes,
		}
	}
	return result, nil
}

// ListNodeMetas implements MetadataClient.
func (a *GRPCMetadataAdapter) ListNodeMetas(ctx context.Context) ([]*NodeMeta, error) {
	nodes, err := a.client.ListNodeMetas(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*NodeMeta, len(nodes))
	for i, n := range nodes {
		result[i] = &NodeMeta{
			NodeID:        n.NodeID,
			Address:       n.Address,
			LastHeartbeat: n.LastHeartbeat,
		}
	}
	return result, nil
}

// GetVolumeMeta implements MetadataClient.
func (a *GRPCMetadataAdapter) GetVolumeMeta(ctx context.Context, volumeID string) (*VolumeMeta, error) {
	vol, err := a.client.GetVolumeMeta(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	return &VolumeMeta{
		VolumeID:  vol.VolumeID,
		Pool:      vol.Pool,
		SizeBytes: vol.SizeBytes,
		ChunkIDs:  vol.ChunkIDs,
	}, nil
}

// ListVolumesMeta implements MetadataClient.
func (a *GRPCMetadataAdapter) ListVolumesMeta(ctx context.Context) ([]*VolumeMeta, error) {
	vols, err := a.client.ListVolumesMeta(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*VolumeMeta, len(vols))
	for i, v := range vols {
		result[i] = &VolumeMeta{
			VolumeID:  v.VolumeID,
			Pool:      v.Pool,
			SizeBytes: v.SizeBytes,
			ChunkIDs:  v.ChunkIDs,
		}
	}
	return result, nil
}
