package metadata

import (
	"context"
	"fmt"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GRPCClient is a metadata store client that communicates with the
// metadata service over gRPC. It implements the same method set as
// RaftStore so that other components (CSI driver, S3 gateway, filer)
// can use it as a drop-in replacement when they run in a separate
// process.
type GRPCClient struct {
	client pb.MetadataServiceClient
	conn   *grpc.ClientConn
	addr   string
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
	return &GRPCClient{
		client: pb.NewMetadataServiceClient(conn),
		conn:   conn,
		addr:   addr,
	}, nil
}

// Addr returns the address this client is connected to.
func (c *GRPCClient) Addr() string {
	return c.addr
}

// Close shuts down the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ---- Volume operations ----

// PutVolumeMeta stores volume metadata via the remote metadata service.
func (c *GRPCClient) PutVolumeMeta(ctx context.Context, meta *VolumeMeta) error {
	_, err := c.client.PutVolumeMeta(ctx, &pb.PutVolumeMetaRequest{
		Meta: VolumeMetaToProto(meta),
	})
	return err
}

// GetVolumeMeta retrieves volume metadata by volume ID.
func (c *GRPCClient) GetVolumeMeta(ctx context.Context, volumeID string) (*VolumeMeta, error) {
	resp, err := c.client.GetVolumeMeta(ctx, &pb.GetVolumeMetaRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		return nil, err
	}
	return VolumeMetaFromProto(resp.Meta), nil
}

// DeleteVolumeMeta removes volume metadata by volume ID.
func (c *GRPCClient) DeleteVolumeMeta(ctx context.Context, volumeID string) error {
	_, err := c.client.DeleteVolumeMeta(ctx, &pb.DeleteVolumeMetaRequest{
		VolumeId: volumeID,
	})
	return err
}

// ListVolumesMeta returns all volume metadata entries.
func (c *GRPCClient) ListVolumesMeta(ctx context.Context) ([]*VolumeMeta, error) {
	resp, err := c.client.ListVolumesMeta(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	metas := make([]*VolumeMeta, len(resp.Metas))
	for i, m := range resp.Metas {
		metas[i] = VolumeMetaFromProto(m)
	}
	return metas, nil
}

// ---- Placement operations ----

// PutPlacementMap stores a placement map via the remote metadata service.
func (c *GRPCClient) PutPlacementMap(ctx context.Context, pm *PlacementMap) error {
	_, err := c.client.PutPlacementMap(ctx, &pb.PutPlacementMapRequest{
		PlacementMap: PlacementMapToProto(pm),
	})
	return err
}

// GetPlacementMap retrieves a placement map by chunk ID.
func (c *GRPCClient) GetPlacementMap(ctx context.Context, chunkID string) (*PlacementMap, error) {
	resp, err := c.client.GetPlacementMap(ctx, &pb.GetPlacementMapRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return nil, err
	}
	return PlacementMapFromProto(resp.PlacementMap), nil
}

// ListPlacementMaps returns all placement map entries.
func (c *GRPCClient) ListPlacementMaps(ctx context.Context) ([]*PlacementMap, error) {
	resp, err := c.client.ListPlacementMaps(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	pms := make([]*PlacementMap, len(resp.PlacementMaps))
	for i, pm := range resp.PlacementMaps {
		pms[i] = PlacementMapFromProto(pm)
	}
	return pms, nil
}

// DeletePlacementMap removes a placement map entry by chunk ID.
func (c *GRPCClient) DeletePlacementMap(ctx context.Context, chunkID string) error {
	_, err := c.client.DeletePlacementMap(ctx, &pb.DeletePlacementMapRequest{
		ChunkId: chunkID,
	})
	return err
}

// ---- Shard placement operations ----

// PutShardPlacement stores a shard placement entry via the remote metadata service.
// TODO(#134): Wire to gRPC MetadataService when ShardPlacement RPCs are added to
// the protobuf definition. Until then, EC shard distribution is only supported
// when the CSI controller talks to the RaftStore directly (single-process mode).
func (c *GRPCClient) PutShardPlacement(_ context.Context, _ *ShardPlacement) error {
	return fmt.Errorf("shard placement gRPC not yet wired: EC requires direct RaftStore access")
}

// GetShardPlacements retrieves all shard placements for a chunk.
// TODO(#134): Wire to gRPC MetadataService when ShardPlacement RPCs are added.
func (c *GRPCClient) GetShardPlacements(_ context.Context, _ string) ([]*ShardPlacement, error) {
	return nil, fmt.Errorf("shard placement gRPC not yet wired: EC requires direct RaftStore access")
}

// DeleteShardPlacement removes a shard placement entry.
// TODO(#134): Wire to gRPC MetadataService when ShardPlacement RPCs are added.
func (c *GRPCClient) DeleteShardPlacement(_ context.Context, _ string, _ int) error {
	return fmt.Errorf("shard placement gRPC not yet wired: EC requires direct RaftStore access")
}

// ---- Object operations ----

// PutObjectMeta stores object metadata via the remote metadata service.
func (c *GRPCClient) PutObjectMeta(ctx context.Context, meta *ObjectMeta) error {
	_, err := c.client.PutObjectMeta(ctx, &pb.PutObjectMetaRequest{
		Meta: ObjectMetaToProto(meta),
	})
	return err
}

// GetObjectMeta retrieves object metadata by bucket and key.
func (c *GRPCClient) GetObjectMeta(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	resp, err := c.client.GetObjectMeta(ctx, &pb.GetObjectMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return nil, err
	}
	return ObjectMetaFromProto(resp.Meta), nil
}

// DeleteObjectMeta removes object metadata by bucket and key.
func (c *GRPCClient) DeleteObjectMeta(ctx context.Context, bucket, key string) error {
	_, err := c.client.DeleteObjectMeta(ctx, &pb.DeleteObjectMetaRequest{
		Bucket: bucket,
		Key:    key,
	})
	return err
}

// ListObjectMetas returns all objects in the given bucket whose key
// starts with prefix.
func (c *GRPCClient) ListObjectMetas(ctx context.Context, bucket, prefix string) ([]*ObjectMeta, error) {
	resp, err := c.client.ListObjectMetas(ctx, &pb.ListObjectMetasRequest{
		Bucket: bucket,
		Prefix: prefix,
	})
	if err != nil {
		return nil, err
	}
	metas := make([]*ObjectMeta, len(resp.Metas))
	for i, m := range resp.Metas {
		metas[i] = ObjectMetaFromProto(m)
	}
	return metas, nil
}

// ---- Bucket operations ----

// PutBucketMeta stores S3 bucket metadata via the remote metadata service.
func (c *GRPCClient) PutBucketMeta(ctx context.Context, meta *BucketMeta) error {
	_, err := c.client.PutBucketMeta(ctx, &pb.PutBucketMetaRequest{
		Meta: BucketMetaToProto(meta),
	})
	return err
}

// GetBucketMeta retrieves S3 bucket metadata by name.
func (c *GRPCClient) GetBucketMeta(ctx context.Context, name string) (*BucketMeta, error) {
	resp, err := c.client.GetBucketMeta(ctx, &pb.GetBucketMetaRequest{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return BucketMetaFromProto(resp.Meta), nil
}

// DeleteBucketMeta removes S3 bucket metadata by name.
func (c *GRPCClient) DeleteBucketMeta(ctx context.Context, name string) error {
	_, err := c.client.DeleteBucketMeta(ctx, &pb.DeleteBucketMetaRequest{
		Name: name,
	})
	return err
}

// ListBucketMetas returns all S3 bucket metadata entries.
func (c *GRPCClient) ListBucketMetas(ctx context.Context) ([]*BucketMeta, error) {
	resp, err := c.client.ListBucketMetas(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	metas := make([]*BucketMeta, len(resp.Metas))
	for i, m := range resp.Metas {
		metas[i] = BucketMetaFromProto(m)
	}
	return metas, nil
}

// ---- Multipart operations ----

// PutMultipartUpload stores multipart upload metadata via the remote
// metadata service.
func (c *GRPCClient) PutMultipartUpload(ctx context.Context, mu *MultipartUpload) error {
	_, err := c.client.PutMultipartUpload(ctx, &pb.PutMultipartUploadRequest{
		Upload: MultipartUploadToProto(mu),
	})
	return err
}

// GetMultipartUpload retrieves multipart upload metadata by upload ID.
func (c *GRPCClient) GetMultipartUpload(ctx context.Context, uploadID string) (*MultipartUpload, error) {
	resp, err := c.client.GetMultipartUpload(ctx, &pb.GetMultipartUploadRequest{
		UploadId: uploadID,
	})
	if err != nil {
		return nil, err
	}
	return MultipartUploadFromProto(resp.Upload), nil
}

// DeleteMultipartUpload removes multipart upload metadata by upload ID.
func (c *GRPCClient) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	_, err := c.client.DeleteMultipartUpload(ctx, &pb.DeleteMultipartUploadRequest{
		UploadId: uploadID,
	})
	return err
}

// ---- Snapshot operations ----

// PutSnapshot stores snapshot metadata via the remote metadata service.
func (c *GRPCClient) PutSnapshot(ctx context.Context, meta *SnapshotMeta) error {
	_, err := c.client.PutSnapshot(ctx, &pb.PutSnapshotRequest{
		Meta: SnapshotMetaToProto(meta),
	})
	return err
}

// GetSnapshot retrieves snapshot metadata by snapshot ID.
func (c *GRPCClient) GetSnapshot(ctx context.Context, snapshotID string) (*SnapshotMeta, error) {
	resp, err := c.client.GetSnapshot(ctx, &pb.GetSnapshotRequest{
		SnapshotId: snapshotID,
	})
	if err != nil {
		return nil, err
	}
	return SnapshotMetaFromProto(resp.Meta), nil
}

// DeleteSnapshot removes snapshot metadata by snapshot ID.
func (c *GRPCClient) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	_, err := c.client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{
		SnapshotId: snapshotID,
	})
	return err
}

// ListSnapshots returns all snapshot metadata entries.
func (c *GRPCClient) ListSnapshots(ctx context.Context) ([]*SnapshotMeta, error) {
	resp, err := c.client.ListSnapshots(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	metas := make([]*SnapshotMeta, len(resp.Metas))
	for i, m := range resp.Metas {
		metas[i] = SnapshotMetaFromProto(m)
	}
	return metas, nil
}

// ---- Inode operations ----

// CreateInode stores inode metadata via the remote metadata service.
func (c *GRPCClient) CreateInode(ctx context.Context, meta *InodeMeta) error {
	_, err := c.client.CreateInode(ctx, &pb.CreateInodeRequest{
		Meta: InodeMetaToProto(meta),
	})
	return err
}

// GetInode retrieves inode metadata by inode number.
func (c *GRPCClient) GetInode(ctx context.Context, ino uint64) (*InodeMeta, error) {
	resp, err := c.client.GetInode(ctx, &pb.GetInodeRequest{
		Ino: ino,
	})
	if err != nil {
		return nil, err
	}
	return InodeMetaFromProto(resp.Meta), nil
}

// UpdateInode updates inode metadata via the remote metadata service.
func (c *GRPCClient) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	_, err := c.client.UpdateInode(ctx, &pb.UpdateInodeRequest{
		Meta: InodeMetaToProto(meta),
	})
	return err
}

// DeleteInode removes inode metadata by inode number.
func (c *GRPCClient) DeleteInode(ctx context.Context, ino uint64) error {
	_, err := c.client.DeleteInode(ctx, &pb.DeleteInodeRequest{
		Ino: ino,
	})
	return err
}

// ---- Directory entry operations ----

// CreateDirEntry stores a directory entry via the remote metadata service.
func (c *GRPCClient) CreateDirEntry(ctx context.Context, parentIno uint64, entry *DirEntry) error {
	_, err := c.client.CreateDirEntry(ctx, &pb.CreateDirEntryRequest{
		ParentIno: parentIno,
		Entry:     DirEntryToProto(entry),
	})
	return err
}

// DeleteDirEntry removes a directory entry via the remote metadata service.
func (c *GRPCClient) DeleteDirEntry(ctx context.Context, parentIno uint64, name string) error {
	_, err := c.client.DeleteDirEntry(ctx, &pb.DeleteDirEntryRequest{
		ParentIno: parentIno,
		Name:      name,
	})
	return err
}

// LookupDirEntry retrieves a directory entry by parent inode and name.
func (c *GRPCClient) LookupDirEntry(ctx context.Context, parentIno uint64, name string) (*DirEntry, error) {
	resp, err := c.client.LookupDirEntry(ctx, &pb.LookupDirEntryRequest{
		ParentIno: parentIno,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}
	return DirEntryFromProto(resp.Entry), nil
}

// ListDirectory returns all directory entries for the given parent inode.
func (c *GRPCClient) ListDirectory(ctx context.Context, parentIno uint64) ([]*DirEntry, error) {
	resp, err := c.client.ListDirectory(ctx, &pb.ListDirectoryRequest{
		ParentIno: parentIno,
	})
	if err != nil {
		return nil, err
	}
	entries := make([]*DirEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = DirEntryFromProto(e)
	}
	return entries, nil
}

// ---- Node registration operations ----

// PutNodeMeta stores or updates node metadata via the remote metadata service.
func (c *GRPCClient) PutNodeMeta(ctx context.Context, meta *NodeMeta) error {
	_, err := c.client.PutNodeMeta(ctx, &pb.PutNodeMetaRequest{
		Meta: NodeMetaToProto(meta),
	})
	return err
}

// GetNodeMeta retrieves node metadata by node ID.
func (c *GRPCClient) GetNodeMeta(ctx context.Context, nodeID string) (*NodeMeta, error) {
	resp, err := c.client.GetNodeMeta(ctx, &pb.GetNodeMetaRequest{
		NodeId: nodeID,
	})
	if err != nil {
		return nil, err
	}
	return NodeMetaFromProto(resp.Meta), nil
}

// DeleteNodeMeta removes node metadata by node ID.
func (c *GRPCClient) DeleteNodeMeta(ctx context.Context, nodeID string) error {
	_, err := c.client.DeleteNodeMeta(ctx, &pb.DeleteNodeMetaRequest{
		NodeId: nodeID,
	})
	return err
}

// ListNodeMetas returns all registered storage nodes.
func (c *GRPCClient) ListNodeMetas(ctx context.Context) ([]*NodeMeta, error) {
	resp, err := c.client.ListNodeMetas(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	metas := make([]*NodeMeta, len(resp.Metas))
	for i, m := range resp.Metas {
		metas[i] = NodeMetaFromProto(m)
	}
	return metas, nil
}

// ---- Lock lease operations ----

// AcquireLock attempts to acquire a distributed file lock lease.
func (c *GRPCClient) AcquireLock(ctx context.Context, args *AcquireLockArgs) (*AcquireLockResult, error) {
	resp, err := c.client.AcquireLock(ctx, &pb.AcquireLockRequest{
		Owner:    args.Owner,
		VolumeId: args.VolumeID,
		Ino:      args.Ino,
		Start:    args.Start,
		End:      args.End,
		Type:     lockTypeToProto(args.Type),
		Ttl:      DurationToProto(args.TTL),
		FilerId:  args.FilerID,
	})
	if err != nil {
		return nil, err
	}
	return &AcquireLockResult{
		LeaseID:          resp.LeaseId,
		ExpiresAt:        resp.ExpiresAt,
		ConflictingOwner: resp.ConflictingOwner,
	}, nil
}

// RenewLock extends the expiration time of an existing lock lease.
func (c *GRPCClient) RenewLock(ctx context.Context, args *RenewLockArgs) (*LockLease, error) {
	resp, err := c.client.RenewLock(ctx, &pb.RenewLockRequest{
		LeaseId: args.LeaseID,
		Ttl:     DurationToProto(args.TTL),
	})
	if err != nil {
		return nil, err
	}
	return LockLeaseFromProto(resp.Lease), nil
}

// ReleaseLock releases a lock lease.
func (c *GRPCClient) ReleaseLock(ctx context.Context, args *ReleaseLockArgs) error {
	_, err := c.client.ReleaseLock(ctx, &pb.ReleaseLockRequest{
		LeaseId: args.LeaseID,
		Owner:   args.Owner,
	})
	return err
}

// TestLock checks if a lock could be acquired without actually acquiring it.
func (c *GRPCClient) TestLock(ctx context.Context, args *TestLockArgs) (*LockLease, error) {
	resp, err := c.client.TestLock(ctx, &pb.TestLockRequest{
		VolumeId: args.VolumeID,
		Ino:      args.Ino,
		Start:    args.Start,
		End:      args.End,
		Type:     lockTypeToProto(args.Type),
	})
	if err != nil {
		return nil, err
	}
	return LockLeaseFromProto(resp.ConflictingLock), nil
}

// GetLock retrieves a lock lease by ID.
func (c *GRPCClient) GetLock(ctx context.Context, leaseID string) (*LockLease, error) {
	resp, err := c.client.GetLock(ctx, &pb.GetLockRequest{
		LeaseId: leaseID,
	})
	if err != nil {
		return nil, err
	}
	return LockLeaseFromProto(resp.Lease), nil
}

// ListLocks returns all active lock leases, optionally filtered by volume ID.
func (c *GRPCClient) ListLocks(ctx context.Context, volumeID string) ([]*LockLease, error) {
	resp, err := c.client.ListLocks(ctx, &pb.ListLocksRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		return nil, err
	}
	locks := make([]*LockLease, len(resp.Locks))
	for i, l := range resp.Locks {
		locks[i] = LockLeaseFromProto(l)
	}
	return locks, nil
}

// CleanupExpiredLocks removes expired lock leases.
func (c *GRPCClient) CleanupExpiredLocks(ctx context.Context) (int, error) {
	resp, err := c.client.CleanupExpiredLocks(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, err
	}
	return int(resp.Cleaned), nil
}

// ---- Inode counter operations ----

// AllocateIno atomically allocates the next inode number via the remote metadata service.
func (c *GRPCClient) AllocateIno(ctx context.Context) (uint64, error) {
	resp, err := c.client.AllocateIno(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, err
	}
	return resp.Ino, nil
}

// ---- Volume ownership operations ----

// SetVolumeOwner stores or updates volume ownership via the remote metadata service.
func (c *GRPCClient) SetVolumeOwner(ctx context.Context, ownership *VolumeOwnership) error {
	_, err := c.client.SetVolumeOwner(ctx, &pb.SetVolumeOwnerRequest{
		Ownership: VolumeOwnershipToProto(ownership),
	})
	return err
}

// GetVolumeOwner retrieves volume ownership by volume ID.
func (c *GRPCClient) GetVolumeOwner(ctx context.Context, volumeID string) (*VolumeOwnership, error) {
	resp, err := c.client.GetVolumeOwner(ctx, &pb.GetVolumeOwnerRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		return nil, err
	}
	return VolumeOwnershipFromProto(resp.Ownership), nil
}

// RequestOwnership attempts to claim volume ownership via the remote metadata service.
func (c *GRPCClient) RequestOwnership(ctx context.Context, volumeID, requesterAddr string) (bool, uint64, error) {
	resp, err := c.client.RequestOwnership(ctx, &pb.RequestOwnershipRequest{
		VolumeId:      volumeID,
		RequesterAddr: requesterAddr,
	})
	if err != nil {
		return false, 0, err
	}
	return resp.Granted, resp.Generation, nil
}
