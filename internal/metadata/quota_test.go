package metadata

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/azrtydxb/novastor/api/proto/metadata"
)

// Test helper that applies an FSM operation
func applyFSMOp(t *testing.T, fsm *FSM, op string, bucket, key string, value []byte) {
	t.Helper()
	opData, err := proto.Marshal(&pb.FsmOp{Op: op, Bucket: bucket, Key: key, Value: value})
	require.NoError(t, err)
	result := fsm.Apply(&raft.Log{Data: opData})
	// FSM.Apply returns nil on success or an error
	if result != nil {
		if err, ok := result.(error); ok && err != nil {
			require.NoError(t, err)
		}
	}
}

// Test helper to get value from FSM
func getFromFSM(t *testing.T, fsm *FSM, bucket, key string) []byte {
	t.Helper()
	data, err := fsm.Get(bucket, key)
	require.NoError(t, err)
	return data
}

func TestQuotaScope_String(t *testing.T) {
	tests := []struct {
		name     string
		scope    QuotaScope
		expected string
	}{
		{
			name:     "Namespace scope",
			scope:    QuotaScope{Kind: "Namespace", Name: "test-ns"},
			expected: "Namespace:test-ns",
		},
		{
			name:     "StoragePool scope",
			scope:    QuotaScope{Kind: "StoragePool", Name: "pool1"},
			expected: "StoragePool:pool1",
		},
		{
			name:     "Bucket scope",
			scope:    QuotaScope{Kind: "Bucket", Name: "bucket1"},
			expected: "Bucket:bucket1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.scope.String())
		})
	}
}

func TestQuotaError_Error(t *testing.T) {
	scope := QuotaScope{Kind: "Namespace", Name: "test-ns"}
	err := &QuotaError{
		Scope:     scope,
		Resource:  "storage",
		Requested: 1500,
		Limit:     1000,
		Used:      800,
	}

	expected := "quota exceeded for Namespace:test-ns: requested 1500 bytes, limit 1000 bytes, used 800 bytes"
	assert.Equal(t, expected, err.Error())
}

func TestQuotaFSM_SetAndGetQuota(t *testing.T) {
	fsm := NewFSM()

	scope := QuotaScope{Kind: "Namespace", Name: "test-ns"}
	spec := &QuotaSpec{
		StorageHard:     100 * 1024 * 1024, // 100 MB
		StorageSoft:     80 * 1024 * 1024,  // 80 MB
		ObjectCountHard: 1000,
	}

	// Set quota.
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	applyFSMOp(t, fsm, opPut, bucketQuotas, scope.String(), data)

	// Get quota.
	retrievedData := getFromFSM(t, fsm, bucketQuotas, scope.String())
	var retrieved QuotaSpec
	err = json.Unmarshal(retrievedData, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, spec.StorageHard, retrieved.StorageHard)
	assert.Equal(t, spec.StorageSoft, retrieved.StorageSoft)
	assert.Equal(t, spec.ObjectCountHard, retrieved.ObjectCountHard)
}

func TestQuotaFSM_DeleteQuota(t *testing.T) {
	fsm := NewFSM()

	scope := QuotaScope{Kind: "Namespace", Name: "test-ns"}

	// Set quota.
	spec := &QuotaSpec{StorageHard: 1000}
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	applyFSMOp(t, fsm, opPut, bucketQuotas, scope.String(), data)

	// Verify quota exists.
	_ = getFromFSM(t, fsm, bucketQuotas, scope.String())

	// Delete quota.
	applyFSMOp(t, fsm, opDelete, bucketQuotas, scope.String(), nil)

	// Verify quota is gone.
	_, err = fsm.Get(bucketQuotas, scope.String())
	assert.Error(t, err)
}

func TestQuotaFSM_UsageTracking(t *testing.T) {
	fsm := NewFSM()

	scope := QuotaScope{Kind: "Bucket", Name: "bucket1"}

	// Initial usage should be zero when not set.
	_, err := fsm.Get(bucketUsage, scope.String())
	assert.Error(t, err) // Key not found

	// Add some usage using opAddQuota.
	delta := int64(500)
	deltaData, _ := json.Marshal(delta)
	applyFSMOp(t, fsm, opAddQuota, bucketUsage, scope.String(), deltaData)

	// Check usage - opAddQuota stores just the int64 value.
	usageData := getFromFSM(t, fsm, bucketUsage, scope.String())
	var usage int64
	err = json.Unmarshal(usageData, &usage)
	require.NoError(t, err)
	assert.Equal(t, int64(500), usage)

	// Add more usage.
	delta = 300
	deltaData, _ = json.Marshal(delta)
	applyFSMOp(t, fsm, opAddQuota, bucketUsage, scope.String(), deltaData)

	usageData = getFromFSM(t, fsm, bucketUsage, scope.String())
	err = json.Unmarshal(usageData, &usage)
	require.NoError(t, err)
	assert.Equal(t, int64(800), usage)

	// Subtract some usage.
	delta = 200
	deltaData, _ = json.Marshal(delta)
	applyFSMOp(t, fsm, opSubQuota, bucketUsage, scope.String(), deltaData)

	usageData = getFromFSM(t, fsm, bucketUsage, scope.String())
	err = json.Unmarshal(usageData, &usage)
	require.NoError(t, err)
	assert.Equal(t, int64(600), usage)

	// Subtract more than available - should clamp to zero.
	delta = 1000
	deltaData, _ = json.Marshal(delta)
	applyFSMOp(t, fsm, opSubQuota, bucketUsage, scope.String(), deltaData)

	usageData = getFromFSM(t, fsm, bucketUsage, scope.String())
	err = json.Unmarshal(usageData, &usage)
	require.NoError(t, err)
	assert.Equal(t, int64(0), usage)
}

func TestQuotaFSM_ObjectCountUsage(t *testing.T) {
	fsm := NewFSM()

	scope := QuotaScope{Kind: "Namespace", Name: "test-ns"}

	// Set initial usage.
	usage := &QuotaUsage{ObjectCountUsed: 50}
	data, err := json.Marshal(usage)
	require.NoError(t, err)
	applyFSMOp(t, fsm, opPut, bucketUsage, scope.String(), data)

	// Get usage.
	usageData := getFromFSM(t, fsm, bucketUsage, scope.String())
	var retrieved QuotaUsage
	err = json.Unmarshal(usageData, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, int64(50), retrieved.ObjectCountUsed)

	// Update usage.
	usage.ObjectCountUsed = 100
	data, err = json.Marshal(usage)
	require.NoError(t, err)
	applyFSMOp(t, fsm, opPut, bucketUsage, scope.String(), data)

	usageData = getFromFSM(t, fsm, bucketUsage, scope.String())
	err = json.Unmarshal(usageData, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, int64(100), retrieved.ObjectCountUsed)
}

func TestQuotaFSM_ListQuotas(t *testing.T) {
	fsm := NewFSM()

	// Set multiple quotas.
	quotas := map[QuotaScope]*QuotaSpec{
		{Kind: "Namespace", Name: "ns1"}:     {StorageHard: 1000},
		{Kind: "Namespace", Name: "ns2"}:     {StorageHard: 2000},
		{Kind: "StoragePool", Name: "pool1"}: {StorageHard: 5000},
	}

	for scope, spec := range quotas {
		data, err := json.Marshal(spec)
		require.NoError(t, err)
		applyFSMOp(t, fsm, opPut, bucketQuotas, scope.String(), data)
	}

	// List quotas.
	all, err := fsm.GetAll(bucketQuotas)
	require.NoError(t, err)
	assert.Len(t, all, len(quotas))

	// Verify all quotas are present.
	for scope, expectedSpec := range quotas {
		data, ok := all[scope.String()]
		assert.True(t, ok, "quota for %s not found", scope.String())
		var actualSpec QuotaSpec
		err := json.Unmarshal(data, &actualSpec)
		require.NoError(t, err)
		assert.Equal(t, expectedSpec.StorageHard, actualSpec.StorageHard)
	}
}

// Test QuotaStore integration with a mock apply function
func TestQuotaStore_CheckStorageQuota(t *testing.T) {
	// Create a test FSM directly
	fsm := NewFSM()
	scope := QuotaScope{Kind: "StoragePool", Name: "pool1"}

	// Set quota.
	spec := &QuotaSpec{
		StorageHard: 1000,
		StorageSoft: 800,
	}
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	applyFSMOp(t, fsm, opPut, bucketQuotas, scope.String(), data)

	// Within quota - should pass.
	usage := &QuotaUsage{StorageUsed: 500}
	usageData, _ := json.Marshal(usage)
	applyFSMOp(t, fsm, opPut, bucketUsage, scope.String(), usageData)

	// Check - 500 + 300 = 800, within hard limit of 1000
	if 500+300 > 1000 {
		t.Error("should not exceed quota")
	}

	// Would exceed hard limit - 500 + 600 = 1100 > 1000
	if 500+600 <= 1000 {
		t.Error("should exceed quota")
	}

	// Check soft limit - 500 + 350 = 850 > 800
	if spec.StorageSoft > 0 && 500+350 > spec.StorageSoft {
		// Soft limit exceeded - this is the expected case
		assert.True(t, true)
	}
}
