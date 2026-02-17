package s3

import (
	"context"

	"github.com/piwi3910/novastor/internal/metadata"
)

// MetadataAdapter wraps a metadata.GRPCClient to satisfy the S3 BucketStore,
// ObjectStore, and MultipartStore interfaces by converting between S3 local
// types and metadata service types.
type MetadataAdapter struct {
	client *metadata.GRPCClient
}

// NewMetadataAdapter creates a MetadataAdapter backed by the given GRPCClient.
func NewMetadataAdapter(client *metadata.GRPCClient) *MetadataAdapter {
	return &MetadataAdapter{client: client}
}

// ---------------------------------------------------------------------------
// BucketStore implementation
// ---------------------------------------------------------------------------

// PutBucket stores bucket metadata via the metadata service.
func (a *MetadataAdapter) PutBucket(ctx context.Context, info *BucketInfo) error {
	return a.client.PutBucketMeta(ctx, &metadata.BucketMeta{
		Name:         info.Name,
		CreationDate: info.CreationDate,
		Versioning:   info.Versioning,
		Owner:        info.Owner,
	})
}

// GetBucket retrieves bucket metadata from the metadata service.
func (a *MetadataAdapter) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	bm, err := a.client.GetBucketMeta(ctx, name)
	if err != nil {
		return nil, err
	}
	return &BucketInfo{
		Name:         bm.Name,
		CreationDate: bm.CreationDate,
		Versioning:   bm.Versioning,
		Owner:        bm.Owner,
	}, nil
}

// DeleteBucket removes bucket metadata via the metadata service.
func (a *MetadataAdapter) DeleteBucket(ctx context.Context, name string) error {
	return a.client.DeleteBucketMeta(ctx, name)
}

// ListBuckets returns all bucket metadata from the metadata service.
func (a *MetadataAdapter) ListBuckets(ctx context.Context) ([]*BucketInfo, error) {
	metas, err := a.client.ListBucketMetas(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]*BucketInfo, len(metas))
	for i, bm := range metas {
		result[i] = &BucketInfo{
			Name:         bm.Name,
			CreationDate: bm.CreationDate,
			Versioning:   bm.Versioning,
			Owner:        bm.Owner,
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// ObjectStore implementation
// ---------------------------------------------------------------------------

// PutObject stores object metadata via the metadata service.
func (a *MetadataAdapter) PutObject(ctx context.Context, info *ObjectInfo) error {
	return a.client.PutObjectMeta(ctx, &metadata.ObjectMeta{
		Bucket:      info.Bucket,
		Key:         info.Key,
		Size:        info.Size,
		ETag:        info.ETag,
		ContentType: info.ContentType,
		UserMeta:    info.UserMeta,
		ChunkIDs:    info.ChunkIDs,
		VersionID:   info.VersionID,
		ModTime:     info.ModTime,
	})
}

// GetObject retrieves object metadata from the metadata service.
func (a *MetadataAdapter) GetObject(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	om, err := a.client.GetObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return &ObjectInfo{
		Bucket:      om.Bucket,
		Key:         om.Key,
		Size:        om.Size,
		ETag:        om.ETag,
		ContentType: om.ContentType,
		UserMeta:    om.UserMeta,
		ChunkIDs:    om.ChunkIDs,
		VersionID:   om.VersionID,
		ModTime:     om.ModTime,
	}, nil
}

// DeleteObject removes object metadata via the metadata service.
func (a *MetadataAdapter) DeleteObject(ctx context.Context, bucket, key string) error {
	return a.client.DeleteObjectMeta(ctx, bucket, key)
}

// ListObjects returns objects matching the given bucket and prefix.
func (a *MetadataAdapter) ListObjects(ctx context.Context, bucket, prefix string) ([]*ObjectInfo, error) {
	metas, err := a.client.ListObjectMetas(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	result := make([]*ObjectInfo, len(metas))
	for i, om := range metas {
		result[i] = &ObjectInfo{
			Bucket:      om.Bucket,
			Key:         om.Key,
			Size:        om.Size,
			ETag:        om.ETag,
			ContentType: om.ContentType,
			UserMeta:    om.UserMeta,
			ChunkIDs:    om.ChunkIDs,
			VersionID:   om.VersionID,
			ModTime:     om.ModTime,
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// MultipartStore implementation
// ---------------------------------------------------------------------------

// PutMultipart stores multipart upload metadata via the metadata service.
func (a *MetadataAdapter) PutMultipart(ctx context.Context, info *MultipartInfo) error {
	parts := make([]metadata.MultipartPart, len(info.Parts))
	for i, p := range info.Parts {
		parts[i] = metadata.MultipartPart{
			PartNumber: p.PartNumber,
			Size:       p.Size,
			ETag:       p.ETag,
			ChunkIDs:   p.ChunkIDs,
		}
	}
	return a.client.PutMultipartUpload(ctx, &metadata.MultipartUpload{
		UploadID:     info.UploadID,
		Bucket:       info.Bucket,
		Key:          info.Key,
		Parts:        parts,
		CreationDate: info.CreationDate,
	})
}

// GetMultipart retrieves multipart upload metadata from the metadata service.
func (a *MetadataAdapter) GetMultipart(ctx context.Context, uploadID string) (*MultipartInfo, error) {
	mu, err := a.client.GetMultipartUpload(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	parts := make([]PartInfo, len(mu.Parts))
	for i, p := range mu.Parts {
		parts[i] = PartInfo{
			PartNumber: p.PartNumber,
			Size:       p.Size,
			ETag:       p.ETag,
			ChunkIDs:   p.ChunkIDs,
		}
	}
	return &MultipartInfo{
		UploadID:     mu.UploadID,
		Bucket:       mu.Bucket,
		Key:          mu.Key,
		Parts:        parts,
		CreationDate: mu.CreationDate,
	}, nil
}

// DeleteMultipart removes multipart upload metadata via the metadata service.
func (a *MetadataAdapter) DeleteMultipart(ctx context.Context, uploadID string) error {
	return a.client.DeleteMultipartUpload(ctx, uploadID)
}
