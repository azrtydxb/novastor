package s3

import "context"

// BucketInfo represents S3 bucket metadata.
type BucketInfo struct {
	Name         string
	CreationDate int64
	Versioning   string
	Owner        string
	MaxSize      int64 // Per-bucket quota limit in bytes (0 = unlimited)
}

// ObjectInfo represents S3 object metadata.
type ObjectInfo struct {
	Bucket      string
	Key         string
	Size        int64
	ETag        string
	ContentType string
	UserMeta    map[string]string
	ChunkIDs    []string
	VersionID   string
	ModTime     int64
}

// MultipartInfo represents a multipart upload.
type MultipartInfo struct {
	UploadID     string
	Bucket       string
	Key          string
	Parts        []PartInfo
	CreationDate int64
}

// PartInfo represents a single part of a multipart upload.
type PartInfo struct {
	PartNumber int
	Size       int64
	ETag       string
	ChunkIDs   []string
}

// BucketStore is the interface for bucket metadata operations.
type BucketStore interface {
	PutBucket(ctx context.Context, info *BucketInfo) error
	GetBucket(ctx context.Context, name string) (*BucketInfo, error)
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]*BucketInfo, error)
}

// ObjectStore is the interface for object metadata operations.
type ObjectStore interface {
	PutObject(ctx context.Context, info *ObjectInfo) error
	GetObject(ctx context.Context, bucket, key string) (*ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string) ([]*ObjectInfo, error)
}

// ChunkStore is the interface for reading/writing chunk data.
type ChunkStore interface {
	PutChunkData(ctx context.Context, data []byte) (chunkID string, err error)
	GetChunkData(ctx context.Context, chunkID string) ([]byte, error)
	DeleteChunkData(ctx context.Context, chunkID string) error
}

// MultipartStore is the interface for multipart upload metadata.
type MultipartStore interface {
	PutMultipart(ctx context.Context, info *MultipartInfo) error
	GetMultipart(ctx context.Context, uploadID string) (*MultipartInfo, error)
	DeleteMultipart(ctx context.Context, uploadID string) error
}

// QuotaChecker defines the interface for checking storage quotas.
type QuotaChecker interface {
	// CheckStorageQuota checks if a storage allocation would exceed the quota.
	CheckStorageQuota(ctx context.Context, scope string, requestedBytes int64) error
	// ReserveStorage reserves storage capacity for a scope.
	ReserveStorage(ctx context.Context, scope string, bytes int64) error
	// ReleaseStorage releases storage capacity for a scope.
	ReleaseStorage(ctx context.Context, scope string, bytes int64) error
	// GetBucketUsage returns the current usage for a bucket.
	GetBucketUsage(ctx context.Context, bucket string) (int64, error)
}
