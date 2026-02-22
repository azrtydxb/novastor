package metadata

import (
	pb "github.com/piwi3910/novastor/api/proto/metadata"
)

// ObjectMetaToProto converts a Go ObjectMeta to its protobuf representation.
func ObjectMetaToProto(o *ObjectMeta) *pb.ObjectMeta {
	if o == nil {
		return nil
	}
	return &pb.ObjectMeta{
		Bucket:      o.Bucket,
		Key:         o.Key,
		Size:        o.Size,
		Etag:        o.ETag,
		ContentType: o.ContentType,
		UserMeta:    o.UserMeta,
		ChunkIds:    o.ChunkIDs,
		VersionId:   o.VersionID,
		ModTime:     o.ModTime,
	}
}

// ObjectMetaFromProto converts a protobuf ObjectMeta to its Go representation.
func ObjectMetaFromProto(o *pb.ObjectMeta) *ObjectMeta {
	if o == nil {
		return nil
	}
	return &ObjectMeta{
		Bucket:      o.Bucket,
		Key:         o.Key,
		Size:        o.Size,
		ETag:        o.Etag,
		ContentType: o.ContentType,
		UserMeta:    o.UserMeta,
		ChunkIDs:    o.ChunkIds,
		VersionID:   o.VersionId,
		ModTime:     o.ModTime,
	}
}

// BucketMetaToProto converts a Go BucketMeta to its protobuf representation.
func BucketMetaToProto(b *BucketMeta) *pb.BucketMeta {
	if b == nil {
		return nil
	}
	return &pb.BucketMeta{
		Name:         b.Name,
		CreationDate: b.CreationDate,
		Versioning:   b.Versioning,
		Owner:        b.Owner,
		MaxSize:      b.MaxSize,
	}
}

// BucketMetaFromProto converts a protobuf BucketMeta to its Go representation.
func BucketMetaFromProto(b *pb.BucketMeta) *BucketMeta {
	if b == nil {
		return nil
	}
	return &BucketMeta{
		Name:         b.Name,
		CreationDate: b.CreationDate,
		Versioning:   b.Versioning,
		Owner:        b.Owner,
		MaxSize:      b.MaxSize,
	}
}

// MultipartUploadToProto converts a Go MultipartUpload to its protobuf representation.
func MultipartUploadToProto(mu *MultipartUpload) *pb.MultipartUpload {
	if mu == nil {
		return nil
	}
	parts := make([]*pb.MultipartPart, len(mu.Parts))
	for i, p := range mu.Parts {
		parts[i] = &pb.MultipartPart{
			PartNumber: int32(p.PartNumber),
			Size:       p.Size,
			Etag:       p.ETag,
			ChunkIds:   p.ChunkIDs,
		}
	}
	return &pb.MultipartUpload{
		UploadId:     mu.UploadID,
		Bucket:       mu.Bucket,
		Key:          mu.Key,
		Parts:        parts,
		CreationDate: mu.CreationDate,
	}
}

// MultipartUploadFromProto converts a protobuf MultipartUpload to its Go representation.
func MultipartUploadFromProto(mu *pb.MultipartUpload) *MultipartUpload {
	if mu == nil {
		return nil
	}
	parts := make([]MultipartPart, len(mu.Parts))
	for i, p := range mu.Parts {
		parts[i] = MultipartPart{
			PartNumber: int(p.PartNumber),
			Size:       p.Size,
			ETag:       p.Etag,
			ChunkIDs:   p.ChunkIds,
		}
	}
	return &MultipartUpload{
		UploadID:     mu.UploadId,
		Bucket:       mu.Bucket,
		Key:          mu.Key,
		Parts:        parts,
		CreationDate: mu.CreationDate,
	}
}
