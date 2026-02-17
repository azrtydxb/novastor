package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient is a metadata store client that communicates with the
// metadata service over gRPC. It implements the same method set as
// RaftStore so that other components (CSI driver, S3 gateway, filer)
// can use it as a drop-in replacement when they run in a separate
// process.
type GRPCClient struct {
	client pb.MetadataServiceClient
	conn   *grpc.ClientConn
}

// NewGRPCClient wraps an existing gRPC client connection.
func NewGRPCClient(conn *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{
		client: pb.NewMetadataServiceClient(conn),
		conn:   conn,
	}
}

// Dial creates a new GRPCClient connected to the given address.
func Dial(addr string, opts ...grpc.DialOption) (*GRPCClient, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing metadata service at %s: %w", addr, err)
	}
	return NewGRPCClient(conn), nil
}

// Close shuts down the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// exec is the internal helper that marshals the payload, calls Execute,
// and checks for server-side errors.
func (c *GRPCClient) exec(ctx context.Context, op string, args any) ([]byte, error) {
	var payload []byte
	if args != nil {
		var err error
		payload, err = json.Marshal(args)
		if err != nil {
			return nil, fmt.Errorf("marshaling %s args: %w", op, err)
		}
	}
	resp, err := c.client.Execute(ctx, &pb.MetadataRequest{
		Operation: op,
		Payload:   payload,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC %s: %w", op, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.Payload, nil
}

// ---- Volume operations ----

// PutVolumeMeta stores volume metadata via the remote metadata service.
func (c *GRPCClient) PutVolumeMeta(ctx context.Context, meta *VolumeMeta) error {
	_, err := c.exec(ctx, "PutVolumeMeta", meta)
	return err
}

// GetVolumeMeta retrieves volume metadata by volume ID.
func (c *GRPCClient) GetVolumeMeta(ctx context.Context, volumeID string) (*VolumeMeta, error) {
	data, err := c.exec(ctx, "GetVolumeMeta", struct {
		VolumeID string `json:"volumeID"`
	}{VolumeID: volumeID})
	if err != nil {
		return nil, err
	}
	var meta VolumeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling VolumeMeta: %w", err)
	}
	return &meta, nil
}

// DeleteVolumeMeta removes volume metadata by volume ID.
func (c *GRPCClient) DeleteVolumeMeta(ctx context.Context, volumeID string) error {
	_, err := c.exec(ctx, "DeleteVolumeMeta", struct {
		VolumeID string `json:"volumeID"`
	}{VolumeID: volumeID})
	return err
}

// ---- Placement operations ----

// PutPlacementMap stores a placement map via the remote metadata service.
func (c *GRPCClient) PutPlacementMap(ctx context.Context, pm *PlacementMap) error {
	_, err := c.exec(ctx, "PutPlacementMap", pm)
	return err
}

// GetPlacementMap retrieves a placement map by chunk ID.
func (c *GRPCClient) GetPlacementMap(ctx context.Context, chunkID string) (*PlacementMap, error) {
	data, err := c.exec(ctx, "GetPlacementMap", struct {
		ChunkID string `json:"chunkID"`
	}{ChunkID: chunkID})
	if err != nil {
		return nil, err
	}
	var pm PlacementMap
	if err := json.Unmarshal(data, &pm); err != nil {
		return nil, fmt.Errorf("unmarshaling PlacementMap: %w", err)
	}
	return &pm, nil
}

// ---- Object operations ----

// PutObjectMeta stores object metadata via the remote metadata service.
func (c *GRPCClient) PutObjectMeta(ctx context.Context, meta *ObjectMeta) error {
	_, err := c.exec(ctx, "PutObjectMeta", meta)
	return err
}

// GetObjectMeta retrieves object metadata by bucket and key.
func (c *GRPCClient) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	data, err := c.exec(ctx, "GetObjectMeta", struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}{Bucket: bucket, Key: key})
	if err != nil {
		return nil, err
	}
	var meta ObjectMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling ObjectMeta: %w", err)
	}
	return &meta, nil
}

// DeleteObjectMeta removes object metadata by bucket and key.
func (c *GRPCClient) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	_, err := c.exec(ctx, "DeleteObjectMeta", struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}{Bucket: bucket, Key: key})
	return err
}

// ListObjectMetas returns all objects in the given bucket whose key
// starts with prefix.
func (c *GRPCClient) ListObjectMetas(ctx context.Context, bucket, prefix string) ([]*ObjectMeta, error) {
	data, err := c.exec(ctx, "ListObjectMetas", struct {
		Bucket string `json:"bucket"`
		Prefix string `json:"prefix"`
	}{Bucket: bucket, Prefix: prefix})
	if err != nil {
		return nil, err
	}
	var metas []*ObjectMeta
	if err := json.Unmarshal(data, &metas); err != nil {
		return nil, fmt.Errorf("unmarshaling []*ObjectMeta: %w", err)
	}
	return metas, nil
}

// ---- Bucket operations ----

// PutBucketMeta stores S3 bucket metadata via the remote metadata service.
func (c *GRPCClient) PutBucketMeta(ctx context.Context, meta *BucketMeta) error {
	_, err := c.exec(ctx, "PutBucketMeta", meta)
	return err
}

// GetBucketMeta retrieves S3 bucket metadata by name.
func (c *GRPCClient) GetBucketMeta(ctx context.Context, name string) (*BucketMeta, error) {
	data, err := c.exec(ctx, "GetBucketMeta", struct {
		Name string `json:"name"`
	}{Name: name})
	if err != nil {
		return nil, err
	}
	var meta BucketMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling BucketMeta: %w", err)
	}
	return &meta, nil
}

// DeleteBucketMeta removes S3 bucket metadata by name.
func (c *GRPCClient) DeleteBucketMeta(ctx context.Context, name string) error {
	_, err := c.exec(ctx, "DeleteBucketMeta", struct {
		Name string `json:"name"`
	}{Name: name})
	return err
}

// ListBucketMetas returns all S3 bucket metadata entries.
func (c *GRPCClient) ListBucketMetas(ctx context.Context) ([]*BucketMeta, error) {
	data, err := c.exec(ctx, "ListBucketMetas", nil)
	if err != nil {
		return nil, err
	}
	var metas []*BucketMeta
	if err := json.Unmarshal(data, &metas); err != nil {
		return nil, fmt.Errorf("unmarshaling []*BucketMeta: %w", err)
	}
	return metas, nil
}

// ---- Multipart operations ----

// PutMultipartUpload stores multipart upload metadata via the remote
// metadata service.
func (c *GRPCClient) PutMultipartUpload(ctx context.Context, mu *MultipartUpload) error {
	_, err := c.exec(ctx, "PutMultipartUpload", mu)
	return err
}

// GetMultipartUpload retrieves multipart upload metadata by upload ID.
func (c *GRPCClient) GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	data, err := c.exec(ctx, "GetMultipartUpload", struct {
		UploadID string `json:"uploadID"`
	}{UploadID: uploadID})
	if err != nil {
		return nil, err
	}
	var mu MultipartUpload
	if err := json.Unmarshal(data, &mu); err != nil {
		return nil, fmt.Errorf("unmarshaling MultipartUpload: %w", err)
	}
	return &mu, nil
}

// DeleteMultipartUpload removes multipart upload metadata by upload ID.
func (c *GRPCClient) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	_, err := c.exec(ctx, "DeleteMultipartUpload", struct {
		UploadID string `json:"uploadID"`
	}{UploadID: uploadID})
	return err
}

// ---- Inode operations ----

// CreateInode stores inode metadata via the remote metadata service.
func (c *GRPCClient) CreateInode(ctx context.Context, meta *InodeMeta) error {
	_, err := c.exec(ctx, "CreateInode", meta)
	return err
}

// GetInode retrieves inode metadata by inode number.
func (c *GRPCClient) GetInode(ctx context.Context, ino uint64) (*InodeMeta, error) {
	data, err := c.exec(ctx, "GetInode", struct {
		Ino uint64 `json:"ino"`
	}{Ino: ino})
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling InodeMeta: %w", err)
	}
	return &meta, nil
}

// UpdateInode updates inode metadata via the remote metadata service.
func (c *GRPCClient) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	_, err := c.exec(ctx, "UpdateInode", meta)
	return err
}

// DeleteInode removes inode metadata by inode number.
func (c *GRPCClient) DeleteInode(ctx context.Context, ino uint64) error {
	_, err := c.exec(ctx, "DeleteInode", struct {
		Ino uint64 `json:"ino"`
	}{Ino: ino})
	return err
}

// ---- Directory entry operations ----

// CreateDirEntry stores a directory entry via the remote metadata service.
func (c *GRPCClient) CreateDirEntry(ctx context.Context, parentIno uint64, entry *DirEntry) error {
	_, err := c.exec(ctx, "CreateDirEntry", dirEntryArgs{
		ParentIno: parentIno,
		Entry:     *entry,
	})
	return err
}

// DeleteDirEntry removes a directory entry via the remote metadata service.
func (c *GRPCClient) DeleteDirEntry(ctx context.Context, parentIno uint64, name string) error {
	_, err := c.exec(ctx, "DeleteDirEntry", dirLookupArgs{
		ParentIno: parentIno,
		Name:      name,
	})
	return err
}

// LookupDirEntry retrieves a directory entry by parent inode and name.
func (c *GRPCClient) LookupDirEntry(ctx context.Context, parentIno uint64, name string) (*DirEntry, error) {
	data, err := c.exec(ctx, "LookupDirEntry", dirLookupArgs{
		ParentIno: parentIno,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}
	var entry DirEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("unmarshaling DirEntry: %w", err)
	}
	return &entry, nil
}

// ListDirectory returns all directory entries for the given parent inode.
func (c *GRPCClient) ListDirectory(ctx context.Context, parentIno uint64) ([]*DirEntry, error) {
	data, err := c.exec(ctx, "ListDirectory", dirListArgs{
		ParentIno: parentIno,
	})
	if err != nil {
		return nil, err
	}
	var entries []*DirEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("unmarshaling []*DirEntry: %w", err)
	}
	return entries, nil
}
