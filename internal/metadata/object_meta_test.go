package metadata

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
)

// applyToFSM is a test helper that marshals an fsmOp and applies it to the FSM
// via a synthetic raft.Log, returning any error from Apply.
func applyToFSM(t *testing.T, f MetadataFSM, op *fsmOp) {
	t.Helper()
	data, err := proto.Marshal(&pb.FsmOp{Op: op.Op, Bucket: op.Bucket, Key: op.Key, Value: op.Value})
	if err != nil {
		t.Fatalf("marshal fsmOp: %v", err)
	}
	resp := f.Apply(&raft.Log{Data: data})
	if resp != nil {
		if e, ok := resp.(error); ok {
			t.Fatalf("FSM Apply returned error: %v", e)
		}
	}
}

func TestFSM_GetAll(t *testing.T) {
	f := NewFSM()

	// GetAll on a non-existent bucket returns nil, nil.
	got, err := f.GetAll("no-such-bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing bucket, got %v", got)
	}

	// Insert some entries into volumes bucket.
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketVolumes, Key: "a", Value: []byte("va")})
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketVolumes, Key: "b", Value: []byte("vb")})

	all, err := f.GetAll(bucketVolumes)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}
	if string(all["a"]) != "va" || string(all["b"]) != "vb" {
		t.Errorf("unexpected entries: %v", all)
	}

	// Verify the returned map is a copy (mutating it doesn't affect FSM).
	all["a"] = []byte("mutated")
	orig, _ := f.Get(bucketVolumes, "a")
	if string(orig) == "mutated" {
		t.Error("GetAll returned reference to internal data instead of copy")
	}
}

func TestFSM_ObjectMeta_PutGet(t *testing.T) {
	f := NewFSM()

	meta := &ObjectMeta{
		Bucket:      "my-bucket",
		Key:         "photos/cat.jpg",
		Size:        12345,
		ETag:        "abc123",
		ContentType: "image/jpeg",
		UserMeta:    map[string]string{"author": "pascal"},
		ChunkIDs:    []string{"c1", "c2"},
		VersionID:   "v1",
		ModTime:     1700000000000000000,
	}
	data, _ := json.Marshal(meta)
	key := objectKey(meta.Bucket, meta.Key)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketObjects, Key: key, Value: data})

	raw, err := f.Get(bucketObjects, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got ObjectMeta
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Bucket != "my-bucket" || got.Key != "photos/cat.jpg" {
		t.Errorf("unexpected object meta: %+v", got)
	}
	if got.Size != 12345 || got.ETag != "abc123" || got.ContentType != "image/jpeg" {
		t.Errorf("field mismatch: %+v", got)
	}
	if len(got.ChunkIDs) != 2 || got.ChunkIDs[0] != "c1" {
		t.Errorf("chunkIDs mismatch: %v", got.ChunkIDs)
	}
	if got.UserMeta["author"] != "pascal" {
		t.Errorf("userMeta mismatch: %v", got.UserMeta)
	}
	if got.VersionID != "v1" || got.ModTime != 1700000000000000000 {
		t.Errorf("version/modTime mismatch: %+v", got)
	}
}

func TestFSM_ObjectMeta_Delete(t *testing.T) {
	f := NewFSM()

	meta := &ObjectMeta{Bucket: "b", Key: "k", Size: 1}
	data, _ := json.Marshal(meta)
	key := objectKey("b", "k")
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketObjects, Key: key, Value: data})

	// Verify it exists.
	if _, err := f.Get(bucketObjects, key); err != nil {
		t.Fatalf("expected object to exist: %v", err)
	}

	// Delete.
	applyToFSM(t, f, &fsmOp{Op: opDelete, Bucket: bucketObjects, Key: key})

	// Verify it's gone.
	_, err := f.Get(bucketObjects, key)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFSM_ObjectMeta_ListWithPrefix(t *testing.T) {
	f := NewFSM()

	objects := []*ObjectMeta{
		{Bucket: "photos", Key: "2024/jan/a.jpg", Size: 100},
		{Bucket: "photos", Key: "2024/jan/b.jpg", Size: 200},
		{Bucket: "photos", Key: "2024/feb/c.jpg", Size: 300},
		{Bucket: "photos", Key: "2023/dec/d.jpg", Size: 400},
		{Bucket: "other-bucket", Key: "2024/jan/e.jpg", Size: 500},
	}
	for _, o := range objects {
		data, _ := json.Marshal(o)
		applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketObjects, Key: objectKey(o.Bucket, o.Key), Value: data})
	}

	all, err := f.GetAll(bucketObjects)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(all) != 5 {
		t.Fatalf("expected 5 objects, got %d", len(all))
	}

	// Simulate ListObjectMetas prefix filter for bucket=photos, prefix=2024/jan/
	prefix := "photos/2024/jan/"
	var matched []string
	for k := range all {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			matched = append(matched, k)
		}
	}
	sort.Strings(matched)
	if len(matched) != 2 {
		t.Fatalf("expected 2 matches for prefix %q, got %d: %v", prefix, len(matched), matched)
	}

	// Prefix for all of 2024 in photos bucket.
	prefix2024 := "photos/2024/"
	var matched2024 []string
	for k := range all {
		if len(k) >= len(prefix2024) && k[:len(prefix2024)] == prefix2024 {
			matched2024 = append(matched2024, k)
		}
	}
	if len(matched2024) != 3 {
		t.Errorf("expected 3 matches for prefix %q, got %d", prefix2024, len(matched2024))
	}
}

func TestFSM_BucketMeta_PutGet(t *testing.T) {
	f := NewFSM()

	meta := &BucketMeta{
		Name:         "my-bucket",
		CreationDate: 1700000000000000000,
		Versioning:   "enabled",
		Owner:        "pascal",
	}
	data, _ := json.Marshal(meta)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketBuckets, Key: meta.Name, Value: data})

	raw, err := f.Get(bucketBuckets, "my-bucket")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got BucketMeta
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "my-bucket" || got.Owner != "pascal" || got.Versioning != "enabled" {
		t.Errorf("unexpected bucket meta: %+v", got)
	}
	if got.CreationDate != 1700000000000000000 {
		t.Errorf("creationDate mismatch: %d", got.CreationDate)
	}
}

func TestFSM_BucketMeta_Delete(t *testing.T) {
	f := NewFSM()

	meta := &BucketMeta{Name: "deleteme", Owner: "test"}
	data, _ := json.Marshal(meta)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketBuckets, Key: meta.Name, Value: data})

	applyToFSM(t, f, &fsmOp{Op: opDelete, Bucket: bucketBuckets, Key: "deleteme"})

	_, err := f.Get(bucketBuckets, "deleteme")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFSM_BucketMeta_ListAll(t *testing.T) {
	f := NewFSM()

	buckets := []*BucketMeta{
		{Name: "alpha", Owner: "user1"},
		{Name: "bravo", Owner: "user2"},
		{Name: "charlie", Owner: "user1"},
	}
	for _, b := range buckets {
		data, _ := json.Marshal(b)
		applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketBuckets, Key: b.Name, Value: data})
	}

	all, err := f.GetAll(bucketBuckets)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(all))
	}

	// Verify each can be deserialized.
	names := make(map[string]bool)
	for _, v := range all {
		var m BucketMeta
		if err := json.Unmarshal(v, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		names[m.Name] = true
	}
	for _, expected := range []string{"alpha", "bravo", "charlie"} {
		if !names[expected] {
			t.Errorf("missing bucket %q", expected)
		}
	}
}

func TestFSM_MultipartUpload_PutGet(t *testing.T) {
	f := NewFSM()

	mu := &MultipartUpload{
		UploadID:     "upload-abc",
		Bucket:       "my-bucket",
		Key:          "large-file.bin",
		CreationDate: 1700000000000000000,
		Parts: []MultipartPart{
			{PartNumber: 1, Size: 5 * 1024 * 1024, ETag: "p1hash", ChunkIDs: []string{"c1", "c2"}},
			{PartNumber: 2, Size: 3 * 1024 * 1024, ETag: "p2hash", ChunkIDs: []string{"c3"}},
		},
	}
	data, _ := json.Marshal(mu)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketMultipart, Key: mu.UploadID, Value: data})

	raw, err := f.Get(bucketMultipart, "upload-abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got MultipartUpload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.UploadID != "upload-abc" || got.Bucket != "my-bucket" || got.Key != "large-file.bin" {
		t.Errorf("unexpected multipart upload: %+v", got)
	}
	if len(got.Parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(got.Parts))
	}
	if got.Parts[0].PartNumber != 1 || got.Parts[0].Size != 5*1024*1024 {
		t.Errorf("part 0 mismatch: %+v", got.Parts[0])
	}
	if len(got.Parts[0].ChunkIDs) != 2 || got.Parts[0].ChunkIDs[0] != "c1" {
		t.Errorf("part 0 chunkIDs mismatch: %v", got.Parts[0].ChunkIDs)
	}
}

func TestFSM_MultipartUpload_Delete(t *testing.T) {
	f := NewFSM()

	mu := &MultipartUpload{UploadID: "del-upload", Bucket: "b", Key: "k"}
	data, _ := json.Marshal(mu)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketMultipart, Key: mu.UploadID, Value: data})

	applyToFSM(t, f, &fsmOp{Op: opDelete, Bucket: bucketMultipart, Key: "del-upload"})

	_, err := f.Get(bucketMultipart, "del-upload")
	if err == nil {
		t.Error("expected error after delete")
	}
}

// TestRaftStore_ObjectMeta_RoundTrip tests the full RaftStore methods for object metadata.
func TestRaftStore_ObjectMeta_RoundTrip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	meta := &ObjectMeta{
		Bucket:      "test-bucket",
		Key:         "docs/readme.md",
		Size:        4096,
		ETag:        "etag123",
		ContentType: "text/markdown",
		UserMeta:    map[string]string{"project": "novastor"},
		ChunkIDs:    []string{"ch1", "ch2", "ch3"},
		ModTime:     1700000000000000000,
	}

	if err := store.PutObjectMeta(ctx, meta); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}
	got, err := store.GetObjectMeta(ctx, "test-bucket", "docs/readme.md")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if got.Size != 4096 || got.ETag != "etag123" || got.ContentType != "text/markdown" {
		t.Errorf("field mismatch: %+v", got)
	}
	if len(got.ChunkIDs) != 3 {
		t.Errorf("expected 3 chunkIDs, got %d", len(got.ChunkIDs))
	}
}

func TestRaftStore_ObjectMeta_Delete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	meta := &ObjectMeta{Bucket: "b", Key: "k", Size: 1}
	_ = store.PutObjectMeta(ctx, meta)

	if err := store.DeleteObjectMeta(ctx, "b", "k"); err != nil {
		t.Fatalf("DeleteObjectMeta: %v", err)
	}
	_, err := store.GetObjectMeta(ctx, "b", "k")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestRaftStore_ObjectMeta_List(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	objects := []*ObjectMeta{
		{Bucket: "mybucket", Key: "a/1.txt", Size: 10},
		{Bucket: "mybucket", Key: "a/2.txt", Size: 20},
		{Bucket: "mybucket", Key: "b/3.txt", Size: 30},
		{Bucket: "other", Key: "a/4.txt", Size: 40},
	}
	for _, o := range objects {
		if err := store.PutObjectMeta(ctx, o); err != nil {
			t.Fatalf("PutObjectMeta: %v", err)
		}
	}

	list, err := store.ListObjectMetas(ctx, "mybucket", "a/")
	if err != nil {
		t.Fatalf("ListObjectMetas: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2, got %d", len(list))
	}

	// Empty prefix returns all in bucket.
	all, err := store.ListObjectMetas(ctx, "mybucket", "")
	if err != nil {
		t.Fatalf("ListObjectMetas all: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 objects in mybucket, got %d", len(all))
	}
}

func TestRaftStore_BucketMeta_RoundTrip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	meta := &BucketMeta{
		Name:         "production",
		CreationDate: 1700000000000000000,
		Versioning:   "enabled",
		Owner:        "admin",
	}
	if err := store.PutBucketMeta(ctx, meta); err != nil {
		t.Fatalf("PutBucketMeta: %v", err)
	}
	got, err := store.GetBucketMeta(ctx, "production")
	if err != nil {
		t.Fatalf("GetBucketMeta: %v", err)
	}
	if got.Name != "production" || got.Versioning != "enabled" || got.Owner != "admin" {
		t.Errorf("mismatch: %+v", got)
	}
}

func TestRaftStore_BucketMeta_Delete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	_ = store.PutBucketMeta(ctx, &BucketMeta{Name: "tmp", Owner: "test"})
	if err := store.DeleteBucketMeta(ctx, "tmp"); err != nil {
		t.Fatalf("DeleteBucketMeta: %v", err)
	}
	_, err := store.GetBucketMeta(ctx, "tmp")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestRaftStore_BucketMeta_List(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	names := []string{"alpha", "bravo", "charlie"}
	for _, n := range names {
		if err := store.PutBucketMeta(ctx, &BucketMeta{Name: n, Owner: "test"}); err != nil {
			t.Fatalf("PutBucketMeta: %v", err)
		}
	}
	list, err := store.ListBucketMetas(ctx)
	if err != nil {
		t.Fatalf("ListBucketMetas: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3, got %d", len(list))
	}
	gotNames := make(map[string]bool)
	for _, b := range list {
		gotNames[b.Name] = true
	}
	for _, n := range names {
		if !gotNames[n] {
			t.Errorf("missing bucket %q", n)
		}
	}
}

func TestRaftStore_MultipartUpload_RoundTrip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	mu := &MultipartUpload{
		UploadID:     "mpu-001",
		Bucket:       "data",
		Key:          "bigfile.tar.gz",
		CreationDate: 1700000000000000000,
		Parts: []MultipartPart{
			{PartNumber: 1, Size: 1024, ETag: "e1", ChunkIDs: []string{"c1"}},
		},
	}
	if err := store.PutMultipartUpload(ctx, mu); err != nil {
		t.Fatalf("PutMultipartUpload: %v", err)
	}
	got, err := store.GetMultipartUpload(ctx, "mpu-001")
	if err != nil {
		t.Fatalf("GetMultipartUpload: %v", err)
	}
	if got.UploadID != "mpu-001" || got.Bucket != "data" || got.Key != "bigfile.tar.gz" {
		t.Errorf("mismatch: %+v", got)
	}
	if len(got.Parts) != 1 || got.Parts[0].PartNumber != 1 {
		t.Errorf("parts mismatch: %+v", got.Parts)
	}
}

func TestRaftStore_MultipartUpload_Delete(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := t.Context()
	_ = store.PutMultipartUpload(ctx, &MultipartUpload{UploadID: "del-mpu", Bucket: "b", Key: "k"})
	if err := store.DeleteMultipartUpload(ctx, "del-mpu"); err != nil {
		t.Fatalf("DeleteMultipartUpload: %v", err)
	}
	_, err := store.GetMultipartUpload(ctx, "del-mpu")
	if err == nil {
		t.Error("expected error after delete")
	}
}
