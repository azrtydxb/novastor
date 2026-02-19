package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// ObjectMeta holds metadata for an S3-like object stored in the system.
type ObjectMeta struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	ETag        string            `json:"etag"`
	ContentType string            `json:"contentType"`
	UserMeta    map[string]string `json:"userMeta,omitempty"`
	ChunkIDs    []string          `json:"chunkIDs"`
	VersionID   string            `json:"versionID,omitempty"`
	ModTime     int64             `json:"modTime"` // Unix nanoseconds
}

// BucketMeta holds metadata for an S3-like bucket.
type BucketMeta struct {
	Name         string `json:"name"`
	CreationDate int64  `json:"creationDate"` // Unix nanoseconds
	Versioning   string `json:"versioning"`   // "enabled", "suspended", ""
	Owner        string `json:"owner"`
	MaxSize      int64  `json:"maxSize"` // Per-bucket quota limit in bytes (0 = unlimited)
}

// MultipartUpload tracks an in-progress multipart upload.
type MultipartUpload struct {
	UploadID     string          `json:"uploadID"`
	Bucket       string          `json:"bucket"`
	Key          string          `json:"key"`
	Parts        []MultipartPart `json:"parts"`
	CreationDate int64           `json:"creationDate"`
}

// MultipartPart describes a single part within a multipart upload.
type MultipartPart struct {
	PartNumber int      `json:"partNumber"`
	Size       int64    `json:"size"`
	ETag       string   `json:"etag"`
	ChunkIDs   []string `json:"chunkIDs"`
}

// objectKey returns the composite FSM key for an object: "bucket/key".
func objectKey(bucket, key string) string {
	return bucket + "/" + key
}

// PutObjectMeta stores object metadata via Raft consensus.
func (s *RaftStore) PutObjectMeta(_ context.Context, meta *ObjectMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling object meta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketObjects, Key: objectKey(meta.Bucket, meta.Key), Value: data})
}

// GetObjectMeta retrieves object metadata by bucket and key.
func (s *RaftStore) GetObjectMeta(_ context.Context, bucket, key string) (*ObjectMeta, error) {
	data, err := s.fsm.Get(bucketObjects, objectKey(bucket, key))
	if err != nil {
		return nil, err
	}
	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling object meta: %w", err)
	}
	return &meta, nil
}

// DeleteObjectMeta removes object metadata via Raft consensus.
func (s *RaftStore) DeleteObjectMeta(_ context.Context, bucket, key string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketObjects, Key: objectKey(bucket, key)})
}

// ListObjectMetas returns all objects in the given bucket whose key starts with prefix.
func (s *RaftStore) ListObjectMetas(_ context.Context, bucket, prefix string) ([]*ObjectMeta, error) {
	all, err := s.fsm.GetAll(bucketObjects)
	if err != nil {
		return nil, err
	}
	fullPrefix := bucket + "/" + prefix
	var result []*ObjectMeta
	for k, v := range all {
		if !strings.HasPrefix(k, fullPrefix) {
			continue
		}
		var meta ObjectMeta
		if err := json.Unmarshal(v, &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling object meta for key %s: %w", k, err)
		}
		result = append(result, &meta)
	}
	return result, nil
}

// PutBucketMeta stores S3 bucket metadata via Raft consensus.
func (s *RaftStore) PutBucketMeta(_ context.Context, meta *BucketMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling bucket meta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketBuckets, Key: meta.Name, Value: data})
}

// GetBucketMeta retrieves S3 bucket metadata by name.
func (s *RaftStore) GetBucketMeta(_ context.Context, name string) (*BucketMeta, error) {
	data, err := s.fsm.Get(bucketBuckets, name)
	if err != nil {
		return nil, err
	}
	var meta BucketMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling bucket meta: %w", err)
	}
	return &meta, nil
}

// DeleteBucketMeta removes S3 bucket metadata via Raft consensus.
func (s *RaftStore) DeleteBucketMeta(_ context.Context, name string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketBuckets, Key: name})
}

// ListBucketMetas returns all S3 bucket metadata entries.
func (s *RaftStore) ListBucketMetas(_ context.Context) ([]*BucketMeta, error) {
	all, err := s.fsm.GetAll(bucketBuckets)
	if err != nil {
		return nil, err
	}
	var result []*BucketMeta
	for k, v := range all {
		var meta BucketMeta
		if err := json.Unmarshal(v, &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling bucket meta for key %s: %w", k, err)
		}
		result = append(result, &meta)
	}
	return result, nil
}

// PutMultipartUpload stores multipart upload metadata via Raft consensus.
func (s *RaftStore) PutMultipartUpload(_ context.Context, mu *MultipartUpload) error {
	data, err := json.Marshal(mu)
	if err != nil {
		return fmt.Errorf("marshaling multipart upload: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketMultipart, Key: mu.UploadID, Value: data})
}

// GetMultipartUpload retrieves multipart upload metadata by upload ID.
func (s *RaftStore) GetMultipartUpload(_ context.Context, uploadID string) (*MultipartUpload, error) {
	data, err := s.fsm.Get(bucketMultipart, uploadID)
	if err != nil {
		return nil, err
	}
	var mu MultipartUpload
	if err := json.Unmarshal(data, &mu); err != nil {
		return nil, fmt.Errorf("unmarshaling multipart upload: %w", err)
	}
	return &mu, nil
}

// DeleteMultipartUpload removes multipart upload metadata via Raft consensus.
func (s *RaftStore) DeleteMultipartUpload(_ context.Context, uploadID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketMultipart, Key: uploadID})
}
