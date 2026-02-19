package metadata

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// setupGRPCTest spins up a single-node RaftStore, wraps it in a
// GRPCServer, starts a gRPC listener on a random port, and returns
// a connected GRPCClient along with a cleanup function.
func setupGRPCTest(t *testing.T) (*GRPCClient, func()) {
	t.Helper()

	// Create the backing RaftStore.
	store, storeCleanup := setupTestStore(t)

	// Create a gRPC server and register the metadata service.
	srv := grpc.NewServer()
	NewGRPCServer(store).Register(srv)

	// Listen on a random port.
	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		storeCleanup()
		t.Fatalf("listen: %v", err)
	}

	// Serve in background.
	go func() {
		_ = srv.Serve(lis)
	}()

	// Connect a client.
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		storeCleanup()
		t.Fatalf("dial: %v", err)
	}

	client := NewGRPCClient(conn)

	cleanup := func() {
		client.Close()
		srv.Stop()
		storeCleanup()
	}
	return client, cleanup
}

func TestGRPC_VolumeMeta_RoundTrip(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	meta := &VolumeMeta{
		VolumeID:  "vol-grpc-1",
		Pool:      "ssd-pool",
		SizeBytes: 10 * 1024 * 1024 * 1024,
		ChunkIDs:  []string{"c1", "c2", "c3"},
	}

	// Put.
	if err := client.PutVolumeMeta(ctx, meta); err != nil {
		t.Fatalf("PutVolumeMeta: %v", err)
	}

	// Get.
	got, err := client.GetVolumeMeta(ctx, "vol-grpc-1")
	if err != nil {
		t.Fatalf("GetVolumeMeta: %v", err)
	}
	if got.VolumeID != "vol-grpc-1" || got.Pool != "ssd-pool" {
		t.Errorf("field mismatch: %+v", got)
	}
	if got.SizeBytes != 10*1024*1024*1024 {
		t.Errorf("sizeBytes mismatch: %d", got.SizeBytes)
	}
	if len(got.ChunkIDs) != 3 {
		t.Errorf("expected 3 chunkIDs, got %d", len(got.ChunkIDs))
	}

	// Delete.
	if err := client.DeleteVolumeMeta(ctx, "vol-grpc-1"); err != nil {
		t.Fatalf("DeleteVolumeMeta: %v", err)
	}
	_, err = client.GetVolumeMeta(ctx, "vol-grpc-1")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestGRPC_ObjectMeta_RoundTrip(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	meta := &ObjectMeta{
		Bucket:      "photos",
		Key:         "vacation/beach.jpg",
		Size:        54321,
		ETag:        "abc",
		ContentType: "image/jpeg",
		UserMeta:    map[string]string{"author": "pascal"},
		ChunkIDs:    []string{"ch1", "ch2"},
		ModTime:     1700000000000000000,
	}

	// Put.
	if err := client.PutObjectMeta(ctx, meta); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}

	// Get.
	got, err := client.GetObjectMeta(ctx, "photos", "vacation/beach.jpg")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if got.Bucket != "photos" || got.Key != "vacation/beach.jpg" {
		t.Errorf("bucket/key mismatch: %+v", got)
	}
	if got.Size != 54321 || got.ETag != "abc" || got.ContentType != "image/jpeg" {
		t.Errorf("field mismatch: %+v", got)
	}
	if got.UserMeta["author"] != "pascal" {
		t.Errorf("userMeta mismatch: %v", got.UserMeta)
	}

	// List.
	list, err := client.ListObjectMetas(ctx, "photos", "vacation/")
	if err != nil {
		t.Fatalf("ListObjectMetas: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 object, got %d", len(list))
	}
	if list[0].Key != "vacation/beach.jpg" {
		t.Errorf("list key mismatch: %s", list[0].Key)
	}

	// Delete.
	if err := client.DeleteObjectMeta(ctx, "photos", "vacation/beach.jpg"); err != nil {
		t.Fatalf("DeleteObjectMeta: %v", err)
	}
	_, err = client.GetObjectMeta(ctx, "photos", "vacation/beach.jpg")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestGRPC_BucketMeta_RoundTrip(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	buckets := []*BucketMeta{
		{Name: "alpha", CreationDate: 1700000000000000000, Versioning: "enabled", Owner: "admin"},
		{Name: "bravo", CreationDate: 1700000001000000000, Owner: "user1"},
	}

	for _, b := range buckets {
		if err := client.PutBucketMeta(ctx, b); err != nil {
			t.Fatalf("PutBucketMeta(%s): %v", b.Name, err)
		}
	}

	// Get.
	got, err := client.GetBucketMeta(ctx, "alpha")
	if err != nil {
		t.Fatalf("GetBucketMeta: %v", err)
	}
	if got.Name != "alpha" || got.Versioning != "enabled" || got.Owner != "admin" {
		t.Errorf("field mismatch: %+v", got)
	}

	// List.
	list, err := client.ListBucketMetas(ctx)
	if err != nil {
		t.Fatalf("ListBucketMetas: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(list))
	}

	// Delete.
	if err := client.DeleteBucketMeta(ctx, "bravo"); err != nil {
		t.Fatalf("DeleteBucketMeta: %v", err)
	}
	list2, err := client.ListBucketMetas(ctx)
	if err != nil {
		t.Fatalf("ListBucketMetas after delete: %v", err)
	}
	if len(list2) != 1 {
		t.Errorf("expected 1 bucket after delete, got %d", len(list2))
	}
}

func TestGRPC_InodeMeta_RoundTrip(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	meta := &InodeMeta{
		Ino:       1000,
		Type:      InodeTypeFile,
		Size:      8192,
		Mode:      0644,
		UID:       1000,
		GID:       1000,
		LinkCount: 1,
		ChunkIDs:  []string{"ca", "cb"},
		Xattrs:    map[string]string{"user.tag": "test"},
		ATime:     1700000000000000000,
		MTime:     1700000000000000000,
		CTime:     1700000000000000000,
	}

	// Create.
	if err := client.CreateInode(ctx, meta); err != nil {
		t.Fatalf("CreateInode: %v", err)
	}

	// Get.
	got, err := client.GetInode(ctx, 1000)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if got.Ino != 1000 || got.Type != InodeTypeFile || got.Size != 8192 {
		t.Errorf("field mismatch: %+v", got)
	}
	if got.Mode != 0644 || got.UID != 1000 || got.LinkCount != 1 {
		t.Errorf("permission mismatch: %+v", got)
	}
	if len(got.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunkIDs, got %d", len(got.ChunkIDs))
	}
	if got.Xattrs["user.tag"] != "test" {
		t.Errorf("xattrs mismatch: %v", got.Xattrs)
	}

	// Update.
	meta.Size = 16384
	meta.MTime = 1700000001000000000
	if err := client.UpdateInode(ctx, meta); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	got2, err := client.GetInode(ctx, 1000)
	if err != nil {
		t.Fatalf("GetInode after update: %v", err)
	}
	if got2.Size != 16384 {
		t.Errorf("expected size 16384, got %d", got2.Size)
	}

	// Delete.
	if err := client.DeleteInode(ctx, 1000); err != nil {
		t.Fatalf("DeleteInode: %v", err)
	}
	_, err = client.GetInode(ctx, 1000)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestGRPC_DirEntry_RoundTrip(t *testing.T) {
	client, cleanup := setupGRPCTest(t)
	defer cleanup()

	ctx := context.Background()

	entries := []*DirEntry{
		{Name: "file1.txt", Ino: 10, Type: InodeTypeFile},
		{Name: "file2.txt", Ino: 11, Type: InodeTypeFile},
		{Name: "subdir", Ino: 12, Type: InodeTypeDir},
	}
	for _, e := range entries {
		if err := client.CreateDirEntry(ctx, 1, e); err != nil {
			t.Fatalf("CreateDirEntry(%s): %v", e.Name, err)
		}
	}

	// Also add an entry under a different parent.
	if err := client.CreateDirEntry(ctx, 12, &DirEntry{Name: "nested.txt", Ino: 20, Type: InodeTypeFile}); err != nil {
		t.Fatalf("CreateDirEntry(nested): %v", err)
	}

	// Lookup.
	got, err := client.LookupDirEntry(ctx, 1, "file1.txt")
	if err != nil {
		t.Fatalf("LookupDirEntry: %v", err)
	}
	if got.Name != "file1.txt" || got.Ino != 10 || got.Type != InodeTypeFile {
		t.Errorf("lookup mismatch: %+v", got)
	}

	// List parent 1.
	list, err := client.ListDirectory(ctx, 1)
	if err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 entries under parent 1, got %d", len(list))
	}

	// List parent 12.
	list12, err := client.ListDirectory(ctx, 12)
	if err != nil {
		t.Fatalf("ListDirectory(12): %v", err)
	}
	if len(list12) != 1 {
		t.Fatalf("expected 1 entry under parent 12, got %d", len(list12))
	}

	// Delete an entry.
	if err := client.DeleteDirEntry(ctx, 1, "file2.txt"); err != nil {
		t.Fatalf("DeleteDirEntry: %v", err)
	}
	_, err = client.LookupDirEntry(ctx, 1, "file2.txt")
	if err == nil {
		t.Error("expected error after delete")
	}

	// List again — should have 2 entries now.
	listAfter, err := client.ListDirectory(ctx, 1)
	if err != nil {
		t.Fatalf("ListDirectory after delete: %v", err)
	}
	if len(listAfter) != 2 {
		t.Errorf("expected 2 entries after delete, got %d", len(listAfter))
	}
}
