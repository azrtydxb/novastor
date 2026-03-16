package controller

import (
	"context"

	"github.com/azrtydxb/novastor/internal/metadata"
)

// GRPCQuotaAdapter adapts a metadata.GRPCClient to implement QuotaReconciler's MetadataClient interface.
// It bridges the controller's quota operations to the generic gRPC metadata service.
type GRPCQuotaAdapter struct {
	client *metadata.GRPCClient
}

// NewGRPCQuotaAdapter creates a new MetadataClient backed by a gRPC metadata client.
func NewGRPCQuotaAdapter(client *metadata.GRPCClient) *GRPCQuotaAdapter {
	return &GRPCQuotaAdapter{client: client}
}

// SetQuota sets a quota for a scope via the gRPC metadata service.
func (a *GRPCQuotaAdapter) SetQuota(ctx context.Context, scope QuotaScopeSpec, storageHard, storageSoft int64, objectCountHard int64) error {
	metaScope := metadata.QuotaScope{
		Kind: scope.Kind,
		Name: scope.Name,
	}
	spec := &metadata.QuotaSpec{
		StorageHard:     storageHard,
		StorageSoft:     storageSoft,
		ObjectCountHard: objectCountHard,
	}
	return a.client.SetQuota(ctx, metaScope, spec)
}

// GetUsage retrieves the current usage for a scope via the gRPC metadata service.
func (a *GRPCQuotaAdapter) GetUsage(ctx context.Context, scope QuotaScopeSpec) (storageUsed, objectCountUsed int64, err error) {
	metaScope := metadata.QuotaScope{
		Kind: scope.Kind,
		Name: scope.Name,
	}
	usage, err := a.client.GetUsage(ctx, metaScope)
	if err != nil {
		return 0, 0, err
	}
	return usage.StorageUsed, usage.ObjectCountUsed, nil
}

// LocalMetadataAdapter implements MetadataClient using a local metadata.RaftStore.
// This is used when the controller runs in the same process as the metadata service.
type LocalMetadataAdapter struct {
	quotaStore *metadata.QuotaStore
}

// NewLocalMetadataAdapter creates a new MetadataClient backed by a local RaftStore.
func NewLocalMetadataAdapter(store *metadata.QuotaStore) *LocalMetadataAdapter {
	return &LocalMetadataAdapter{quotaStore: store}
}

// SetQuota sets a quota using the local QuotaStore.
func (a *LocalMetadataAdapter) SetQuota(ctx context.Context, scope QuotaScopeSpec, storageHard, storageSoft int64, objectCountHard int64) error {
	metaScope := metadata.QuotaScope{
		Kind: scope.Kind,
		Name: scope.Name,
	}

	spec := &metadata.QuotaSpec{
		StorageHard:     storageHard,
		StorageSoft:     storageSoft,
		ObjectCountHard: objectCountHard,
	}

	return a.quotaStore.SetQuota(ctx, metaScope, spec)
}

// GetUsage retrieves usage using the local QuotaStore.
func (a *LocalMetadataAdapter) GetUsage(ctx context.Context, scope QuotaScopeSpec) (storageUsed, objectCountUsed int64, err error) {
	metaScope := metadata.QuotaScope{
		Kind: scope.Kind,
		Name: scope.Name,
	}

	usage, err := a.quotaStore.GetUsage(ctx, metaScope)
	if err != nil {
		return 0, 0, err
	}

	return usage.StorageUsed, usage.ObjectCountUsed, nil
}
