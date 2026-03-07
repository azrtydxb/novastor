package s3

import (
	"context"
	"fmt"

	"github.com/azrtydxb/novastor/internal/metadata"
)

// quotaChecker implements storage quota checking using the metadata quota service.
// It translates S3 bucket quota requests to the generic quota scope format.
type quotaChecker struct {
	store *metadata.QuotaStore
}

// NewQuotaChecker creates a new QuotaChecker backed by the given QuotaStore.
func NewQuotaChecker(store *metadata.QuotaStore) QuotaChecker {
	return &quotaChecker{store: store}
}

// CheckStorageQuota checks if a storage allocation would exceed the quota.
// The scope is expected to be in the format "bucket:<bucket-name>".
func (q *quotaChecker) CheckStorageQuota(ctx context.Context, scope string, requestedBytes int64) error {
	// S3 bucket scopes are formatted as "bucket:<name>".
	quotaScope := metadata.QuotaScope{
		Kind: "Bucket",
		Name: scope,
	}
	return q.store.CheckStorageQuota(ctx, quotaScope, requestedBytes)
}

// ReserveStorage reserves storage capacity for a scope.
func (q *quotaChecker) ReserveStorage(ctx context.Context, scope string, bytes int64) error {
	quotaScope := metadata.QuotaScope{
		Kind: "Bucket",
		Name: scope,
	}
	return q.store.ReserveStorage(ctx, quotaScope, bytes)
}

// ReleaseStorage releases storage capacity for a scope.
func (q *quotaChecker) ReleaseStorage(ctx context.Context, scope string, bytes int64) error {
	quotaScope := metadata.QuotaScope{
		Kind: "Bucket",
		Name: scope,
	}
	return q.store.ReleaseStorage(ctx, quotaScope, bytes)
}

// GetBucketUsage returns the current usage for a bucket.
// It sums up the sizes of all objects in the bucket.
func (q *quotaChecker) GetBucketUsage(ctx context.Context, bucket string) (int64, error) {
	// For S3 buckets, we use the metadata.QuotaStore to get usage.
	quotaScope := metadata.QuotaScope{
		Kind: "Bucket",
		Name: "bucket:" + bucket,
	}
	usage, err := q.store.GetUsage(ctx, quotaScope)
	if err != nil {
		return 0, fmt.Errorf("getting bucket usage: %w", err)
	}
	return usage.StorageUsed, nil
}
