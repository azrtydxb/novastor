package metadata

import (
	"context"
	"encoding/json"
	"testing"
)

func TestFSM_InodeMeta_CreateGet(t *testing.T) {
	f := NewFSM()

	meta := &InodeMeta{
		Ino:       100,
		Type:      InodeTypeFile,
		Size:      4096,
		Mode:      0644,
		UID:       1000,
		GID:       1000,
		LinkCount: 1,
		ChunkIDs:  []string{"c1", "c2"},
		Xattrs:    map[string]string{"user.mime": "text/plain"},
		ATime:     1700000000000000000,
		MTime:     1700000000000000000,
		CTime:     1700000000000000000,
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(meta.Ino), Value: data})

	raw, err := f.Get(bucketInodes, inodeKey(100))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got InodeMeta
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Ino != 100 || got.Type != InodeTypeFile || got.Size != 4096 {
		t.Errorf("basic fields mismatch: %+v", got)
	}
	if got.Mode != 0644 || got.UID != 1000 || got.GID != 1000 || got.LinkCount != 1 {
		t.Errorf("permission fields mismatch: %+v", got)
	}
	if len(got.ChunkIDs) != 2 || got.ChunkIDs[0] != "c1" {
		t.Errorf("chunkIDs mismatch: %v", got.ChunkIDs)
	}
	if got.Xattrs["user.mime"] != "text/plain" {
		t.Errorf("xattrs mismatch: %v", got.Xattrs)
	}
	if got.ATime != 1700000000000000000 {
		t.Errorf("atime mismatch: %d", got.ATime)
	}
}

func TestFSM_InodeMeta_Update(t *testing.T) {
	f := NewFSM()

	meta := &InodeMeta{Ino: 200, Type: InodeTypeFile, Size: 100, Mode: 0644, LinkCount: 1}
	data, _ := json.Marshal(meta)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(200), Value: data})

	// Update size and mtime.
	meta.Size = 500
	meta.MTime = 1700000001000000000
	data, _ = json.Marshal(meta)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(200), Value: data})

	raw, err := f.Get(bucketInodes, inodeKey(200))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got InodeMeta
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Size != 500 {
		t.Errorf("expected size 500, got %d", got.Size)
	}
	if got.MTime != 1700000001000000000 {
		t.Errorf("mtime not updated: %d", got.MTime)
	}
}

func TestFSM_InodeMeta_Delete(t *testing.T) {
	f := NewFSM()

	meta := &InodeMeta{Ino: 300, Type: InodeTypeDir, Mode: 0755, LinkCount: 2}
	data, _ := json.Marshal(meta)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketInodes, Key: inodeKey(300), Value: data})

	// Verify it exists.
	if _, err := f.Get(bucketInodes, inodeKey(300)); err != nil {
		t.Fatalf("expected inode to exist: %v", err)
	}

	// Delete.
	applyToFSM(t, f, &fsmOp{Op: opDelete, Bucket: bucketInodes, Key: inodeKey(300)})

	// Verify it is gone.
	_, err := f.Get(bucketInodes, inodeKey(300))
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFSM_DirEntry_CreateLookup(t *testing.T) {
	f := NewFSM()

	entry := &DirEntry{Name: "hello.txt", Ino: 50, Type: InodeTypeFile}
	data, _ := json.Marshal(entry)
	parentIno := uint64(1)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketDirents, Key: direntKey(parentIno, entry.Name), Value: data})

	raw, err := f.Get(bucketDirents, direntKey(1, "hello.txt"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got DirEntry
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Name != "hello.txt" || got.Ino != 50 || got.Type != InodeTypeFile {
		t.Errorf("dir entry mismatch: %+v", got)
	}
}

func TestFSM_DirEntry_Delete(t *testing.T) {
	f := NewFSM()

	entry := &DirEntry{Name: "remove_me", Ino: 60, Type: InodeTypeFile}
	data, _ := json.Marshal(entry)
	applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketDirents, Key: direntKey(1, entry.Name), Value: data})

	applyToFSM(t, f, &fsmOp{Op: opDelete, Bucket: bucketDirents, Key: direntKey(1, "remove_me")})

	_, err := f.Get(bucketDirents, direntKey(1, "remove_me"))
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestFSM_DirEntry_ListDirectory(t *testing.T) {
	f := NewFSM()

	entries := []struct {
		parentIno uint64
		entry     *DirEntry
	}{
		{1, &DirEntry{Name: "a.txt", Ino: 10, Type: InodeTypeFile}},
		{1, &DirEntry{Name: "b.txt", Ino: 11, Type: InodeTypeFile}},
		{1, &DirEntry{Name: "subdir", Ino: 12, Type: InodeTypeDir}},
		{12, &DirEntry{Name: "c.txt", Ino: 13, Type: InodeTypeFile}},
		{99, &DirEntry{Name: "other.txt", Ino: 14, Type: InodeTypeFile}},
	}
	for _, e := range entries {
		data, _ := json.Marshal(e.entry)
		applyToFSM(t, f, &fsmOp{Op: opPut, Bucket: bucketDirents, Key: direntKey(e.parentIno, e.entry.Name), Value: data})
	}

	// List parent inode 1 — should have 3 entries.
	all, err := f.GetAll(bucketDirents)
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	prefix := direntPrefix(1)
	var matched []*DirEntry
	for k, v := range all {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			var de DirEntry
			if err := json.Unmarshal(v, &de); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			matched = append(matched, &de)
		}
	}
	if len(matched) != 3 {
		t.Fatalf("expected 3 entries for parent 1, got %d", len(matched))
	}

	// List parent inode 12 — should have 1 entry.
	prefix12 := direntPrefix(12)
	var matched12 []*DirEntry
	for k, v := range all {
		if len(k) >= len(prefix12) && k[:len(prefix12)] == prefix12 {
			var de DirEntry
			if err := json.Unmarshal(v, &de); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			matched12 = append(matched12, &de)
		}
	}
	if len(matched12) != 1 {
		t.Fatalf("expected 1 entry for parent 12, got %d", len(matched12))
	}
	if matched12[0].Name != "c.txt" {
		t.Errorf("expected c.txt, got %s", matched12[0].Name)
	}
}

func TestRaftStore_InodeMeta_RoundTrip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	meta := &InodeMeta{
		Ino:       500,
		Type:      InodeTypeFile,
		Size:      8192,
		Mode:      0644,
		UID:       1000,
		GID:       1000,
		LinkCount: 1,
		ChunkIDs:  []string{"chunk-a", "chunk-b"},
		Xattrs:    map[string]string{"user.tag": "important"},
		ATime:     1700000000000000000,
		MTime:     1700000000000000000,
		CTime:     1700000000000000000,
	}

	if err := store.CreateInode(ctx, meta); err != nil {
		t.Fatalf("CreateInode: %v", err)
	}

	got, err := store.GetInode(ctx, 500)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if got.Ino != 500 || got.Type != InodeTypeFile || got.Size != 8192 {
		t.Errorf("field mismatch: %+v", got)
	}
	if got.Mode != 0644 || got.UID != 1000 || got.LinkCount != 1 {
		t.Errorf("permission mismatch: %+v", got)
	}
	if len(got.ChunkIDs) != 2 {
		t.Errorf("expected 2 chunkIDs, got %d", len(got.ChunkIDs))
	}
	if got.Xattrs["user.tag"] != "important" {
		t.Errorf("xattrs mismatch: %v", got.Xattrs)
	}

	// Update.
	meta.Size = 16384
	meta.MTime = 1700000001000000000
	if err := store.UpdateInode(ctx, meta); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	got2, err := store.GetInode(ctx, 500)
	if err != nil {
		t.Fatalf("GetInode after update: %v", err)
	}
	if got2.Size != 16384 {
		t.Errorf("expected updated size 16384, got %d", got2.Size)
	}

	// Delete.
	if err := store.DeleteInode(ctx, 500); err != nil {
		t.Fatalf("DeleteInode: %v", err)
	}
	_, err = store.GetInode(ctx, 500)
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestRaftStore_DirEntry_RoundTrip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create directory entries under parent inode 1.
	entries := []*DirEntry{
		{Name: "file1.txt", Ino: 10, Type: InodeTypeFile},
		{Name: "file2.txt", Ino: 11, Type: InodeTypeFile},
		{Name: "subdir", Ino: 12, Type: InodeTypeDir},
	}
	for _, e := range entries {
		if err := store.CreateDirEntry(ctx, 1, e); err != nil {
			t.Fatalf("CreateDirEntry(%s): %v", e.Name, err)
		}
	}

	// Also add an entry under a different parent.
	if err := store.CreateDirEntry(ctx, 12, &DirEntry{Name: "nested.txt", Ino: 20, Type: InodeTypeFile}); err != nil {
		t.Fatalf("CreateDirEntry(nested): %v", err)
	}

	// Lookup.
	got, err := store.LookupDirEntry(ctx, 1, "file1.txt")
	if err != nil {
		t.Fatalf("LookupDirEntry: %v", err)
	}
	if got.Name != "file1.txt" || got.Ino != 10 || got.Type != InodeTypeFile {
		t.Errorf("lookup mismatch: %+v", got)
	}

	// List parent 1.
	list, err := store.ListDirectory(ctx, 1)
	if err != nil {
		t.Fatalf("ListDirectory: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 entries under parent 1, got %d", len(list))
	}

	// List parent 12.
	list12, err := store.ListDirectory(ctx, 12)
	if err != nil {
		t.Fatalf("ListDirectory(12): %v", err)
	}
	if len(list12) != 1 {
		t.Fatalf("expected 1 entry under parent 12, got %d", len(list12))
	}

	// Delete an entry.
	if err := store.DeleteDirEntry(ctx, 1, "file2.txt"); err != nil {
		t.Fatalf("DeleteDirEntry: %v", err)
	}
	_, err = store.LookupDirEntry(ctx, 1, "file2.txt")
	if err == nil {
		t.Error("expected error after delete")
	}

	// List again — should have 2 entries now.
	listAfter, err := store.ListDirectory(ctx, 1)
	if err != nil {
		t.Fatalf("ListDirectory after delete: %v", err)
	}
	if len(listAfter) != 2 {
		t.Errorf("expected 2 entries after delete, got %d", len(listAfter))
	}
}
