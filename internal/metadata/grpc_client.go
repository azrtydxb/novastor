package metadata

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// GRPCClient is a metadata store client that communicates with the
// metadata service over gRPC. It implements the same method set as
// RaftStore so that other components (CSI driver, S3 gateway, filer)
// can use it as a drop-in replacement when they run in a separate
// process.
//
// When connecting via a Kubernetes headless service, gRPC's dns:///
// resolver discovers all meta pod IPs. Combined with round_robin
// load balancing, each RPC call goes to a different backend. Write
// operations that hit a Raft follower return codes.Unavailable; the
// client retries automatically, and round_robin ensures the next
// attempt hits a different pod (hopefully the leader).
type GRPCClient struct {
	mu     sync.Mutex
	client pb.MetadataServiceClient
	conn   *grpc.ClientConn
	addr   string
	opts   []grpc.DialOption
}

// NewGRPCClient wraps an existing gRPC client connection.
func NewGRPCClient(conn *grpc.ClientConn) *GRPCClient {
	return &GRPCClient{
		client: pb.NewMetadataServiceClient(conn),
		conn:   conn,
	}
}

// Dial creates a new GRPCClient connected to the given address.
// For Kubernetes headless services, it uses dns:/// resolution with
// round_robin load balancing to discover all meta pods and distribute
// RPCs across them. Write operations that hit a follower are retried
// automatically on the next pod in the rotation.
func Dial(addr string, opts ...grpc.DialOption) (*GRPCClient, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Use dns:/// resolver for multi-endpoint discovery (headless services).
	target := addr
	if !strings.Contains(addr, "://") {
		target = "dns:///" + addr
	}

	// Round-robin across all resolved endpoints.
	// gRPC's built-in retry policy automatically retries UNAVAILABLE RPCs
	// on a DIFFERENT subchannel, guaranteeing that each retry hits a
	// different meta pod. This is critical for Raft leader discovery:
	// a write that lands on a follower returns UNAVAILABLE and gRPC
	// transparently retries on another pod (hopefully the leader).
	serviceConfig := `{
		"loadBalancingConfig": [{"round_robin": {}}],
		"methodConfig": [{
			"name": [{"service": "metadata.MetadataService"}],
			"retryPolicy": {
				"maxAttempts": 5,
				"initialBackoff": "0.1s",
				"maxBackoff": "1s",
				"backoffMultiplier": 2.0,
				"retryableStatusCodes": ["UNAVAILABLE"]
			}
		}]
	}`
	opts = append(opts,
		grpc.WithDefaultServiceConfig(serviceConfig),
	)

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("dialing metadata service at %s: %w", addr, err)
	}
	c := &GRPCClient{
		conn: conn,
		addr: addr,
		opts: opts,
	}
	c.client = pb.NewMetadataServiceClient(conn)
	return c, nil
}

// retryOnUnavailable calls fn and, if it returns codes.Unavailable,
// retries once more as a safety net. The primary retry mechanism is
// gRPC's built-in retry policy (configured in the service config),
// which guarantees each retry attempt goes to a different subchannel.
// This application-level retry only kicks in if gRPC-level retries
// are exhausted (e.g., all meta pods are followers during a leader
// election).
func (c *GRPCClient) retryOnUnavailable(fn func() error) error {
	const maxRetries = 2
	const retryDelay = 500 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unavailable {
			return err
		}
		if attempt < maxRetries {
			time.Sleep(retryDelay)
		}
	}
	return fmt.Errorf("metadata service unavailable after %d retries (leader not found)", maxRetries)
}

// Addr returns the address this client is connected to.
func (c *GRPCClient) Addr() string {
	return c.addr
}

// Close shuts down the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ---- Volume operations ----

// PutVolumeMeta stores volume metadata via the remote metadata service.
func (c *GRPCClient) PutVolumeMeta(ctx context.Context, meta *VolumeMeta) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutVolumeMeta(ctx, &pb.PutVolumeMetaRequest{
			Meta: VolumeMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteVolumeMeta(ctx, &pb.DeleteVolumeMetaRequest{
			VolumeId: volumeID,
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutPlacementMap(ctx, &pb.PutPlacementMapRequest{
			PlacementMap: PlacementMapToProto(pm),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeletePlacementMap(ctx, &pb.DeletePlacementMapRequest{
			ChunkId: chunkID,
		})
		return err
	})
}

// ---- Shard placement operations (erasure coding) ----

// PutShardPlacement stores a shard placement entry via the remote metadata service.
func (c *GRPCClient) PutShardPlacement(ctx context.Context, sp *ShardPlacement) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutShardPlacement(ctx, &pb.PutShardPlacementRequest{
			Placement: ShardPlacementToProto(sp),
		})
		return err
	})
}

// GetShardPlacements retrieves all shard placements for a chunk.
func (c *GRPCClient) GetShardPlacements(ctx context.Context, chunkID string) ([]*ShardPlacement, error) {
	resp, err := c.client.GetShardPlacements(ctx, &pb.GetShardPlacementsRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return nil, err
	}
	placements := make([]*ShardPlacement, len(resp.Placements))
	for i, sp := range resp.Placements {
		placements[i] = ShardPlacementFromProto(sp)
	}
	return placements, nil
}

// DeleteShardPlacement removes a shard placement entry.
func (c *GRPCClient) DeleteShardPlacement(ctx context.Context, chunkID string, shardIndex int) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteShardPlacement(ctx, &pb.DeleteShardPlacementRequest{
			ChunkId:    chunkID,
			ShardIndex: int32(shardIndex),
		})
		return err
	})
}

// ---- Object operations ----

// PutObjectMeta stores object metadata via the remote metadata service.
func (c *GRPCClient) PutObjectMeta(ctx context.Context, meta *ObjectMeta) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutObjectMeta(ctx, &pb.PutObjectMetaRequest{
			Meta: ObjectMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteObjectMeta(ctx, &pb.DeleteObjectMetaRequest{
			Bucket: bucket,
			Key:    key,
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutBucketMeta(ctx, &pb.PutBucketMetaRequest{
			Meta: BucketMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteBucketMeta(ctx, &pb.DeleteBucketMetaRequest{
			Name: name,
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutMultipartUpload(ctx, &pb.PutMultipartUploadRequest{
			Upload: MultipartUploadToProto(mu),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteMultipartUpload(ctx, &pb.DeleteMultipartUploadRequest{
			UploadId: uploadID,
		})
		return err
	})
}

// ---- Snapshot operations ----

// PutSnapshot stores snapshot metadata via the remote metadata service.
func (c *GRPCClient) PutSnapshot(ctx context.Context, meta *SnapshotMeta) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutSnapshot(ctx, &pb.PutSnapshotRequest{
			Meta: SnapshotMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{
			SnapshotId: snapshotID,
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.CreateInode(ctx, &pb.CreateInodeRequest{
			Meta: InodeMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.UpdateInode(ctx, &pb.UpdateInodeRequest{
			Meta: InodeMetaToProto(meta),
		})
		return err
	})
}

// DeleteInode removes inode metadata by inode number.
func (c *GRPCClient) DeleteInode(ctx context.Context, ino uint64) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteInode(ctx, &pb.DeleteInodeRequest{
			Ino: ino,
		})
		return err
	})
}

// ---- Directory entry operations ----

// CreateDirEntry stores a directory entry via the remote metadata service.
func (c *GRPCClient) CreateDirEntry(ctx context.Context, parentIno uint64, entry *DirEntry) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.CreateDirEntry(ctx, &pb.CreateDirEntryRequest{
			ParentIno: parentIno,
			Entry:     DirEntryToProto(entry),
		})
		return err
	})
}

// DeleteDirEntry removes a directory entry via the remote metadata service.
func (c *GRPCClient) DeleteDirEntry(ctx context.Context, parentIno uint64, name string) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteDirEntry(ctx, &pb.DeleteDirEntryRequest{
			ParentIno: parentIno,
			Name:      name,
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.PutNodeMeta(ctx, &pb.PutNodeMetaRequest{
			Meta: NodeMetaToProto(meta),
		})
		return err
	})
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
	return c.retryOnUnavailable(func() error {
		_, err := c.client.DeleteNodeMeta(ctx, &pb.DeleteNodeMetaRequest{
			NodeId: nodeID,
		})
		return err
	})
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
	var result *AcquireLockResult
	err := c.retryOnUnavailable(func() error {
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
			return err
		}
		result = &AcquireLockResult{
			LeaseID:          resp.LeaseId,
			ExpiresAt:        resp.ExpiresAt,
			ConflictingOwner: resp.ConflictingOwner,
		}
		return nil
	})
	return result, err
}

// RenewLock extends the expiration time of an existing lock lease.
func (c *GRPCClient) RenewLock(ctx context.Context, args *RenewLockArgs) (*LockLease, error) {
	var result *LockLease
	err := c.retryOnUnavailable(func() error {
		resp, err := c.client.RenewLock(ctx, &pb.RenewLockRequest{
			LeaseId: args.LeaseID,
			Ttl:     DurationToProto(args.TTL),
		})
		if err != nil {
			return err
		}
		result = LockLeaseFromProto(resp.Lease)
		return nil
	})
	return result, err
}

// ReleaseLock releases a lock lease.
func (c *GRPCClient) ReleaseLock(ctx context.Context, args *ReleaseLockArgs) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.ReleaseLock(ctx, &pb.ReleaseLockRequest{
			LeaseId: args.LeaseID,
			Owner:   args.Owner,
		})
		return err
	})
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
	var cleaned int
	err := c.retryOnUnavailable(func() error {
		resp, err := c.client.CleanupExpiredLocks(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		cleaned = int(resp.Cleaned)
		return nil
	})
	return cleaned, err
}

// ---- Inode counter operations ----

// AllocateIno atomically allocates the next inode number via the remote metadata service.
func (c *GRPCClient) AllocateIno(ctx context.Context) (uint64, error) {
	var ino uint64
	err := c.retryOnUnavailable(func() error {
		resp, err := c.client.AllocateIno(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		ino = resp.Ino
		return nil
	})
	return ino, err
}

// ---- Volume ownership operations ----

// SetVolumeOwner stores or updates volume ownership via the remote metadata service.
func (c *GRPCClient) SetVolumeOwner(ctx context.Context, ownership *VolumeOwnership) error {
	return c.retryOnUnavailable(func() error {
		_, err := c.client.SetVolumeOwner(ctx, &pb.SetVolumeOwnerRequest{
			Ownership: VolumeOwnershipToProto(ownership),
		})
		return err
	})
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
	var granted bool
	var generation uint64
	err := c.retryOnUnavailable(func() error {
		resp, err := c.client.RequestOwnership(ctx, &pb.RequestOwnershipRequest{
			VolumeId:      volumeID,
			RequesterAddr: requesterAddr,
		})
		if err != nil {
			return err
		}
		granted = resp.Granted
		generation = resp.Generation
		return nil
	})
	return granted, generation, err
}
