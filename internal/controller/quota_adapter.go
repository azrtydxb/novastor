package controller

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/piwi3910/novastor/internal/metadata"
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
	// Use the metadata service's generic Execute operation
	args := map[string]any{
		"scope": map[string]any{
			"kind": scope.Kind,
			"name": scope.Name,
		},
		"storage": map[string]any{
			"hard": storageHard,
			"soft": storageSoft,
		},
		"objectCount": map[string]any{
			"hard": objectCountHard,
		},
	}

	// Call via raw Execute - for now we'll store this in the RaftStore's QuotaStore format
	// The metadata service needs to support "SetQuota" operation
	// For now, this is a stub that documents the needed integration

	// TODO: Implement gRPC call to metadata service
	// The metadata service should expose SetQuota/GetUsage operations
	// similar to how it exposes PutVolumeMeta/GetVolumeMeta

	_ = args
	_ = ctx
	return fmt.Errorf("SetQuota via gRPC: not yet implemented - metadata service needs SetQuota operation")
}

// GetUsage retrieves the current usage for a scope via the gRPC metadata service.
func (a *GRPCQuotaAdapter) GetUsage(ctx context.Context, scope QuotaScopeSpec) (storageUsed, objectCountUsed int64, err error) {
	// TODO: Implement gRPC call to metadata service
	// The metadata service should expose GetUsage operation

	_ = scope
	_ = ctx
	return 0, 0, fmt.Errorf("GetUsage via gRPC: not yet implemented - metadata service needs GetUsage operation")
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

// execArgs is a helper for executing generic metadata operations.
func execArgs[T any](ctx context.Context, client *metadata.GRPCClient, op string, args any) (*T, error) {
	// This helper would use the gRPC client's internal exec method
	// For now, we document what's needed
	_ = ctx
	_ = client
	_ = op
	_ = args
	var result T
	return &result, fmt.Errorf("exec: not implemented")
}

// jsonEncode is a helper for encoding arguments to JSON.
func jsonEncode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// jsonDecode is a helper for decoding results from JSON.
func jsonDecode[T any](data []byte) (*T, error) {
	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
