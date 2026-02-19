package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/piwi3910/novastor/internal/metrics"
)

// QuotaScope defines the scope of a quota.
type QuotaScope struct {
	// Kind is the scope type: Namespace, StoragePool, Bucket, or Volume.
	Kind string `json:"kind"`

	// Name identifies the scope target.
	Name string `json:"name"`
}

// String returns a unique string identifier for the quota scope.
func (q QuotaScope) String() string {
	return fmt.Sprintf("%s:%s", q.Kind, q.Name)
}

// QuotaSpec defines the limits for a quota.
type QuotaSpec struct {
	// StorageHard is the hard limit in bytes.
	StorageHard int64 `json:"storageHard"`

	// StorageSoft is the soft limit in bytes.
	StorageSoft int64 `json:"storageSoft"`

	// ObjectCountHard is the hard limit for object/bucket count.
	ObjectCountHard int64 `json:"objectCountHard"`
}

// QuotaUsage tracks current usage against a quota.
type QuotaUsage struct {
	// StorageUsed is the current storage usage in bytes.
	StorageUsed int64 `json:"storageUsed"`

	// ObjectCountUsed is the current object/bucket count.
	ObjectCountUsed int64 `json:"objectCountUsed"`
}

// QuotaError is returned when a quota check fails.
type QuotaError struct {
	Scope     QuotaScope
	Resource  string
	Requested int64
	Limit     int64
	Used      int64
}

func (e *QuotaError) Error() string {
	return fmt.Sprintf("quota exceeded for %s: requested %d bytes, limit %d bytes, used %d bytes",
		e.Scope.String(), e.Requested, e.Limit, e.Used)
}

// QuotaStore provides quota management operations.
// It wraps a RaftStore to provide quota-specific functionality.
type QuotaStore struct {
	store *RaftStore
}

// NewQuotaStore creates a new QuotaStore backed by the given RaftStore.
func NewQuotaStore(store *RaftStore) *QuotaStore {
	return &QuotaStore{store: store}
}

// SetQuota sets a quota for a scope.
func (q *QuotaStore) SetQuota(_ context.Context, scope QuotaScope, spec *QuotaSpec) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshaling quota spec: %w", err)
	}
	return q.store.apply(&fsmOp{
		Op:     opPut,
		Bucket: bucketQuotas,
		Key:    scope.String(),
		Value:  data,
	})
}

// GetQuota retrieves the quota spec for a scope.
// Returns nil, nil if no quota is set for the scope (not found is OK).
func (q *QuotaStore) GetQuota(_ context.Context, scope QuotaScope) (*QuotaSpec, error) {
	data, err := q.store.fsm.Get(bucketQuotas, scope.String())
	if err != nil {
		// No quota set for this scope - not found is acceptable.
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting quota: %w", err)
	}
	var spec QuotaSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("unmarshaling quota spec: %w", err)
	}
	return &spec, nil
}

// GetUsage retrieves the current usage for a scope.
func (q *QuotaStore) GetUsage(_ context.Context, scope QuotaScope) (*QuotaUsage, error) {
	data, err := q.store.fsm.Get(bucketUsage, scope.String())
	if err != nil {
		// No usage recorded yet - not found is acceptable.
		if errors.Is(err, ErrKeyNotFound) {
			return &QuotaUsage{}, nil
		}
		return nil, fmt.Errorf("getting usage: %w", err)
	}
	var usage QuotaUsage
	if err := json.Unmarshal(data, &usage); err != nil {
		return nil, fmt.Errorf("unmarshaling quota usage: %w", err)
	}
	return &usage, nil
}

// CheckStorageQuota checks if a storage allocation would exceed the quota.
// Returns a QuotaError if the quota would be exceeded, nil otherwise.
func (q *QuotaStore) CheckStorageQuota(ctx context.Context, scope QuotaScope, requestedBytes int64) error {
	spec, err := q.GetQuota(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	if spec == nil || spec.StorageHard == 0 {
		// No quota set.
		return nil
	}

	usage, err := q.GetUsage(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting usage: %w", err)
	}

	if usage.StorageUsed+requestedBytes > spec.StorageHard {
		metrics.QuotaRejections.WithLabelValues(scope.Kind).Inc()
		return &QuotaError{
			Scope:     scope,
			Resource:  "storage",
			Requested: requestedBytes,
			Limit:     spec.StorageHard,
			Used:      usage.StorageUsed,
		}
	}

	if spec.StorageSoft > 0 && usage.StorageUsed+requestedBytes > spec.StorageSoft {
		// Soft limit exceeded - log warning but allow.
		metrics.QuotaSoftWarnings.Inc()
	}

	return nil
}

// ReserveStorage reserves storage capacity for a scope.
// It first checks the quota, then atomically increments the usage.
// If the quota check fails, the usage is not incremented.
func (q *QuotaStore) ReserveStorage(ctx context.Context, scope QuotaScope, bytes int64) error {
	// Check quota first.
	if err := q.CheckStorageQuota(ctx, scope, bytes); err != nil {
		return err
	}

	// Atomically increment usage.
	delta, err := json.Marshal(bytes)
	if err != nil {
		return fmt.Errorf("marshaling quota delta: %w", err)
	}
	if err := q.store.apply(&fsmOp{
		Op:     opAddQuota,
		Bucket: bucketUsage,
		Key:    scope.String(),
		Value:  delta,
	}); err != nil {
		return fmt.Errorf("reserving storage: %w", err)
	}

	metrics.QuotaReservations.Add(float64(bytes))
	return nil
}

// ReleaseStorage releases storage capacity for a scope.
// It atomically decrements the usage counter.
func (q *QuotaStore) ReleaseStorage(_ context.Context, scope QuotaScope, bytes int64) error {
	delta, err := json.Marshal(bytes)
	if err != nil {
		return fmt.Errorf("marshaling quota delta: %w", err)
	}
	if err := q.store.apply(&fsmOp{
		Op:     opSubQuota,
		Bucket: bucketUsage,
		Key:    scope.String(),
		Value:  delta,
	}); err != nil {
		return fmt.Errorf("releasing storage: %w", err)
	}

	metrics.QuotaReleases.Add(float64(bytes))
	return nil
}

// CheckObjectCountQuota checks if creating additional objects would exceed the quota.
func (q *QuotaStore) CheckObjectCountQuota(ctx context.Context, scope QuotaScope, requestedCount int64) error {
	spec, err := q.GetQuota(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting quota: %w", err)
	}
	if spec == nil || spec.ObjectCountHard == 0 {
		// No quota set.
		return nil
	}

	usage, err := q.GetUsage(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting usage: %w", err)
	}

	if usage.ObjectCountUsed+requestedCount > spec.ObjectCountHard {
		metrics.QuotaRejections.WithLabelValues(scope.Kind).Inc()
		return &QuotaError{
			Scope:     scope,
			Resource:  "objectCount",
			Requested: requestedCount,
			Limit:     spec.ObjectCountHard,
			Used:      usage.ObjectCountUsed,
		}
	}

	return nil
}

// ReserveObjects reserves object count capacity for a scope.
func (q *QuotaStore) ReserveObjects(ctx context.Context, scope QuotaScope, count int64) error {
	// Check quota first.
	if err := q.CheckObjectCountQuota(ctx, scope, count); err != nil {
		return err
	}

	// Get current usage, increment object count.
	usage, err := q.GetUsage(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting usage: %w", err)
	}

	usage.ObjectCountUsed += count
	data, _ := json.Marshal(usage)
	if err := q.store.apply(&fsmOp{
		Op:     opPut,
		Bucket: bucketUsage,
		Key:    scope.String(),
		Value:  data,
	}); err != nil {
		return fmt.Errorf("reserving objects: %w", err)
	}

	metrics.QuotaObjectReservations.Add(float64(count))
	return nil
}

// ReleaseObjects releases object count capacity for a scope.
func (q *QuotaStore) ReleaseObjects(ctx context.Context, scope QuotaScope, count int64) error {
	usage, err := q.GetUsage(ctx, scope)
	if err != nil {
		return fmt.Errorf("getting usage: %w", err)
	}

	usage.ObjectCountUsed -= count
	if usage.ObjectCountUsed < 0 {
		usage.ObjectCountUsed = 0
	}

	data, _ := json.Marshal(usage)
	if err := q.store.apply(&fsmOp{
		Op:     opPut,
		Bucket: bucketUsage,
		Key:    scope.String(),
		Value:  data,
	}); err != nil {
		return fmt.Errorf("releasing objects: %w", err)
	}

	metrics.QuotaObjectReleases.Add(float64(count))
	return nil
}

// DeleteQuota removes a quota for a scope.
func (q *QuotaStore) DeleteQuota(_ context.Context, scope QuotaScope) error {
	return q.store.apply(&fsmOp{
		Op:     opDelete,
		Bucket: bucketQuotas,
		Key:    scope.String(),
	})
}

// ListQuotas returns all defined quotas.
func (q *QuotaStore) ListQuotas(_ context.Context) (map[QuotaScope]*QuotaSpec, error) {
	all, err := q.store.fsm.GetAll(bucketQuotas)
	if err != nil {
		return nil, fmt.Errorf("listing quotas: %w", err)
	}
	result := make(map[QuotaScope]*QuotaSpec)
	for key, data := range all {
		var spec QuotaSpec
		if err := json.Unmarshal(data, &spec); err != nil {
			return nil, fmt.Errorf("unmarshaling quota spec: %w", err)
		}
		// Parse the scope from the key.
		var kind, name string
		if n, err := fmt.Sscanf(key, "%s:%s", &kind, &name); n == 2 && err == nil {
			result[QuotaScope{Kind: kind, Name: name}] = &spec
		}
	}
	return result, nil
}
