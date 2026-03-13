package s3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketInfo(t *testing.T) {
	bucket := &BucketInfo{
		Name:         "test-bucket",
		CreationDate: 1609459200,
		Versioning:   "Enabled",
		Owner:        "test-user",
		MaxSize:      1000000000,
	}

	assert.Equal(t, "test-bucket", bucket.Name)
	assert.Equal(t, int64(1609459200), bucket.CreationDate)
	assert.Equal(t, "Enabled", bucket.Versioning)
	assert.Equal(t, "test-user", bucket.Owner)
	assert.Equal(t, int64(1000000000), bucket.MaxSize)
}

func TestBucketInfo_Empty(t *testing.T) {
	bucket := &BucketInfo{}

	assert.Equal(t, "", bucket.Name)
	assert.Equal(t, int64(0), bucket.CreationDate)
	assert.Equal(t, "", bucket.Versioning)
	assert.Equal(t, "", bucket.Owner)
	assert.Equal(t, int64(0), bucket.MaxSize)
}

func TestBucketInfo_ZeroMaxSize(t *testing.T) {
	bucket := &BucketInfo{
		Name:    "unlimited-bucket",
		MaxSize: 0, // Unlimited
	}

	assert.Equal(t, int64(0), bucket.MaxSize)
}

func TestObjectInfo(t *testing.T) {
	obj := &ObjectInfo{
		Bucket:      "test-bucket",
		Key:         "test-key",
		Size:        1024,
		ETag:        "abc123",
		ContentType: "application/json",
		UserMeta: map[string]string{
			"custom-key": "custom-value",
		},
		ChunkIDs:  []string{"chunk1", "chunk2"},
		VersionID: "v1",
		ModTime:   1609459200,
	}

	assert.Equal(t, "test-bucket", obj.Bucket)
	assert.Equal(t, "test-key", obj.Key)
	assert.Equal(t, int64(1024), obj.Size)
	assert.Equal(t, "abc123", obj.ETag)
	assert.Equal(t, "application/json", obj.ContentType)
	assert.Equal(t, "custom-value", obj.UserMeta["custom-key"])
	assert.Len(t, obj.ChunkIDs, 2)
	assert.Equal(t, "v1", obj.VersionID)
	assert.Equal(t, int64(1609459200), obj.ModTime)
}

func TestObjectInfo_Empty(t *testing.T) {
	obj := &ObjectInfo{}

	assert.Equal(t, "", obj.Bucket)
	assert.Equal(t, "", obj.Key)
	assert.Equal(t, int64(0), obj.Size)
	assert.Equal(t, "", obj.ETag)
	assert.Equal(t, "", obj.ContentType)
	assert.Nil(t, obj.UserMeta)
	assert.Nil(t, obj.ChunkIDs)
	assert.Equal(t, "", obj.VersionID)
	assert.Equal(t, int64(0), obj.ModTime)
}

func TestObjectInfo_NilMaps(t *testing.T) {
	obj := &ObjectInfo{
		UserMeta: nil,
		ChunkIDs: nil,
	}

	assert.Nil(t, obj.UserMeta)
	assert.Nil(t, obj.ChunkIDs)
}

func TestMultipartInfo(t *testing.T) {
	mp := &MultipartInfo{
		UploadID:     "upload-123",
		Bucket:       "test-bucket",
		Key:          "test-key",
		Parts:        []PartInfo{{PartNumber: 1, Size: 100, ETag: "etag1"}},
		CreationDate: 1609459200,
	}

	assert.Equal(t, "upload-123", mp.UploadID)
	assert.Equal(t, "test-bucket", mp.Bucket)
	assert.Equal(t, "test-key", mp.Key)
	assert.Len(t, mp.Parts, 1)
	assert.Equal(t, int64(1609459200), mp.CreationDate)
}

func TestMultipartInfo_Empty(t *testing.T) {
	mp := &MultipartInfo{}

	assert.Equal(t, "", mp.UploadID)
	assert.Equal(t, "", mp.Bucket)
	assert.Equal(t, "", mp.Key)
	assert.Nil(t, mp.Parts)
	assert.Equal(t, int64(0), mp.CreationDate)
}

func TestMultipartInfo_MultipleParts(t *testing.T) {
	mp := &MultipartInfo{
		UploadID: "upload-123",
		Bucket:   "test-bucket",
		Key:      "large-file.dat",
		Parts: []PartInfo{
			{PartNumber: 1, Size: 5242880, ETag: "etag1", ChunkIDs: []string{"c1"}},
			{PartNumber: 2, Size: 5242880, ETag: "etag2", ChunkIDs: []string{"c2"}},
			{PartNumber: 3, Size: 1048576, ETag: "etag3", ChunkIDs: []string{"c3"}},
		},
		CreationDate: 1609459200,
	}

	assert.Len(t, mp.Parts, 3)
	assert.Equal(t, 1, mp.Parts[0].PartNumber)
	assert.Equal(t, 2, mp.Parts[1].PartNumber)
	assert.Equal(t, 3, mp.Parts[2].PartNumber)
}

func TestPartInfo(t *testing.T) {
	part := PartInfo{
		PartNumber: 1,
		Size:       5242880,
		ETag:       "abc123def456",
		ChunkIDs:   []string{"chunk-1", "chunk-2"},
	}

	assert.Equal(t, 1, part.PartNumber)
	assert.Equal(t, int64(5242880), part.Size)
	assert.Equal(t, "abc123def456", part.ETag)
	assert.Len(t, part.ChunkIDs, 2)
}

func TestPartInfo_Empty(t *testing.T) {
	part := PartInfo{}

	assert.Equal(t, 0, part.PartNumber)
	assert.Equal(t, int64(0), part.Size)
	assert.Equal(t, "", part.ETag)
	assert.Nil(t, part.ChunkIDs)
}

func TestPartInfo_MaxPartNumber(t *testing.T) {
	part := PartInfo{
		PartNumber: 10000, // S3 max parts
		Size:       1,
	}

	assert.Equal(t, 10000, part.PartNumber)
}

// Mock implementations for interface testing
type mockBucketStore struct {
	putErr     error
	getResult  *BucketInfo
	getErr     error
	deleteErr  error
	listResult []*BucketInfo
	listErr    error
}

func (m *mockBucketStore) PutBucket(ctx context.Context, info *BucketInfo) error {
	return m.putErr
}

func (m *mockBucketStore) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	return m.getResult, m.getErr
}

func (m *mockBucketStore) DeleteBucket(ctx context.Context, name string) error {
	return m.deleteErr
}

func (m *mockBucketStore) ListBuckets(ctx context.Context) ([]*BucketInfo, error) {
	return m.listResult, m.listErr
}

func TestBucketStore_Interface(t *testing.T) {
	var store BucketStore = &mockBucketStore{}
	assert.NotNil(t, store)
}

type mockObjectStore struct {
	putErr     error
	getResult  *ObjectInfo
	getErr     error
	deleteErr  error
	listResult []*ObjectInfo
	listErr    error
}

func (m *mockObjectStore) PutObject(ctx context.Context, info *ObjectInfo) error {
	return m.putErr
}

func (m *mockObjectStore) GetObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	return m.getResult, m.getErr
}

func (m *mockObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	return m.deleteErr
}

func (m *mockObjectStore) ListObjects(ctx context.Context, bucket, prefix string) ([]*ObjectInfo, error) {
	return m.listResult, m.listErr
}

func TestObjectStore_Interface(t *testing.T) {
	var store ObjectStore = &mockObjectStore{}
	assert.NotNil(t, store)
}

type mockChunkStore struct {
	putResult string
	putErr    error
	getResult []byte
	getErr    error
	deleteErr error
}

func (m *mockChunkStore) PutChunkData(ctx context.Context, data []byte) (string, error) {
	return m.putResult, m.putErr
}

func (m *mockChunkStore) GetChunkData(ctx context.Context, chunkID string) ([]byte, error) {
	return m.getResult, m.getErr
}

func (m *mockChunkStore) DeleteChunkData(ctx context.Context, chunkID string) error {
	return m.deleteErr
}

func TestChunkStore_Interface(t *testing.T) {
	var store ChunkStore = &mockChunkStore{}
	assert.NotNil(t, store)
}

type mockMultipartStore struct {
	putErr    error
	getResult *MultipartInfo
	getErr    error
	deleteErr error
}

func (m *mockMultipartStore) PutMultipart(ctx context.Context, info *MultipartInfo) error {
	return m.putErr
}

func (m *mockMultipartStore) GetMultipart(ctx context.Context, uploadID string) (*MultipartInfo, error) {
	return m.getResult, m.getErr
}

func (m *mockMultipartStore) DeleteMultipart(ctx context.Context, uploadID string) error {
	return m.deleteErr
}

func TestMultipartStore_Interface(t *testing.T) {
	var store MultipartStore = &mockMultipartStore{}
	assert.NotNil(t, store)
}

type mockQuotaChecker struct {
	checkErr    error
	reserveErr  error
	releaseErr  error
	usageResult int64
	usageErr    error
}

func (m *mockQuotaChecker) CheckStorageQuota(ctx context.Context, scope string, requestedBytes int64) error {
	return m.checkErr
}

func (m *mockQuotaChecker) ReserveStorage(ctx context.Context, scope string, bytes int64) error {
	return m.reserveErr
}

func (m *mockQuotaChecker) ReleaseStorage(ctx context.Context, scope string, bytes int64) error {
	return m.releaseErr
}

func (m *mockQuotaChecker) GetBucketUsage(ctx context.Context, bucket string) (int64, error) {
	return m.usageResult, m.usageErr
}

func TestQuotaChecker_Interface(t *testing.T) {
	var checker QuotaChecker = &mockQuotaChecker{}
	assert.NotNil(t, checker)
}
