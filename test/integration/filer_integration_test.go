//go:build integration

package integration

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/chunk"
	"github.com/azrtydxb/novastor/internal/filer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// setupFilerWithDeps creates an in-process metadata store, chunk agent, and
// filer.FileSystem. Returns the FileSystem for testing. Cleanup is registered
// via t.Cleanup.
func setupFilerWithDeps(t *testing.T) *filer.FileSystem {
	t.Helper()

	// Start metadata service.
	_, metaClient := setupMetadataService(t)

	// Start chunk agent.
	chunkDir := t.TempDir()
	localStore, err := chunk.NewLocalStore(chunkDir)
	if err != nil {
		t.Fatalf("NewLocalStore failed: %v", err)
	}

	chunkLis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for chunk server: %v", err)
	}
	chunkAddr := chunkLis.Addr().String()

	chunkSrv := grpc.NewServer()
	chunkServer := agent.NewChunkServer(localStore)
	chunkServer.Register(chunkSrv)
	t.Cleanup(func() { chunkSrv.GracefulStop() })
	go func() {
		_ = chunkSrv.Serve(chunkLis)
	}()

	agentClient, err := agent.Dial(
		chunkAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial chunk server: %v", err)
	}
	t.Cleanup(func() { agentClient.Close() })

	// Create filer adapters.
	metaAdapter := filer.NewMetadataAdapter(metaClient)
	chunkAdapter := filer.NewChunkAdapter(agentClient)

	// Create FileSystem (this initializes the root inode).
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)

	return fs
}

func TestFiler_CreateWriteReadFile(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create a file in the root directory.
	fileMeta, err := fs.Create(ctx, filer.RootIno, "hello.txt", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if fileMeta.Ino == 0 {
		t.Fatal("expected non-zero inode number")
	}
	if fileMeta.Type != filer.TypeFile {
		t.Errorf("expected type 'file', got %q", fileMeta.Type)
	}

	// Write data to the file.
	testData := []byte("Hello, NovaStor Filer!")
	n, err := fs.Write(ctx, fileMeta.Ino, 0, testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected %d bytes written, got %d", len(testData), n)
	}

	// Read data back.
	readData, err := fs.Read(ctx, fileMeta.Ino, 0, int64(len(testData)))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Errorf("read data mismatch: expected %q, got %q", testData, readData)
	}
}

func TestFiler_CreateWriteReadLargeFile(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	fileMeta, err := fs.Create(ctx, filer.RootIno, "large.bin", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write a moderately sized file. We stay under the default gRPC message
	// size limit (4MB) per chunk to avoid transport errors in integration tests.
	largeData := make([]byte, 2*1024*1024) // 2 MiB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := fs.Write(ctx, fileMeta.Ino, 0, largeData)
	if err != nil {
		t.Fatalf("Write (large) failed: %v", err)
	}
	if n != len(largeData) {
		t.Errorf("expected %d bytes written, got %d", len(largeData), n)
	}

	// Read back and verify.
	readData, err := fs.Read(ctx, fileMeta.Ino, 0, int64(len(largeData)))
	if err != nil {
		t.Fatalf("Read (large) failed: %v", err)
	}
	if !bytes.Equal(largeData, readData) {
		t.Error("large file data mismatch after write/read round-trip")
	}

	// Verify partial read.
	partial, err := fs.Read(ctx, fileMeta.Ino, 1024, 2048)
	if err != nil {
		t.Fatalf("Partial read failed: %v", err)
	}
	if !bytes.Equal(largeData[1024:1024+2048], partial) {
		t.Error("partial read data mismatch")
	}
}

func TestFiler_DirectoryOperations(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create a directory.
	dirMeta, err := fs.Mkdir(ctx, filer.RootIno, "mydir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if dirMeta.Type != filer.TypeDir {
		t.Errorf("expected type 'dir', got %q", dirMeta.Type)
	}

	// Create a nested directory.
	subDirMeta, err := fs.Mkdir(ctx, dirMeta.Ino, "subdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir (nested) failed: %v", err)
	}

	// ReadDir on root should show "mydir".
	entries, err := fs.ReadDir(ctx, filer.RootIno)
	if err != nil {
		t.Fatalf("ReadDir(root) failed: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Name == "mydir" && e.Ino == dirMeta.Ino {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'mydir' in root directory listing")
	}

	// ReadDir on mydir should show "subdir".
	entries, err = fs.ReadDir(ctx, dirMeta.Ino)
	if err != nil {
		t.Fatalf("ReadDir(mydir) failed: %v", err)
	}
	found = false
	for _, e := range entries {
		if e.Name == "subdir" && e.Ino == subDirMeta.Ino {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'subdir' in mydir directory listing")
	}

	// Rmdir on empty subdir.
	if err := fs.Rmdir(ctx, dirMeta.Ino, "subdir"); err != nil {
		t.Fatalf("Rmdir(subdir) failed: %v", err)
	}

	// Verify subdir is gone.
	entries, err = fs.ReadDir(ctx, dirMeta.Ino)
	if err != nil {
		t.Fatalf("ReadDir after rmdir failed: %v", err)
	}
	for _, e := range entries {
		if e.Name == "subdir" {
			t.Error("subdir should not exist after rmdir")
		}
	}
}

func TestFiler_FileRename(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create a file.
	fileMeta, err := fs.Create(ctx, filer.RootIno, "original.txt", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data.
	testData := []byte("rename test data")
	if _, err := fs.Write(ctx, fileMeta.Ino, 0, testData); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create a target directory.
	targetDir, err := fs.Mkdir(ctx, filer.RootIno, "target", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	// Rename the file.
	if err := fs.Rename(ctx, filer.RootIno, "original.txt", targetDir.Ino, "renamed.txt"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify file is in new location.
	movedMeta, err := fs.Lookup(ctx, targetDir.Ino, "renamed.txt")
	if err != nil {
		t.Fatalf("Lookup after rename failed: %v", err)
	}
	if movedMeta.Ino != fileMeta.Ino {
		t.Errorf("inode number changed after rename: expected %d, got %d", fileMeta.Ino, movedMeta.Ino)
	}

	// Verify data is intact after rename.
	readData, err := fs.Read(ctx, movedMeta.Ino, 0, int64(len(testData)))
	if err != nil {
		t.Fatalf("Read after rename failed: %v", err)
	}
	if !bytes.Equal(testData, readData) {
		t.Error("data mismatch after rename")
	}

	// Verify old location no longer exists.
	_, err = fs.Lookup(ctx, filer.RootIno, "original.txt")
	if err == nil {
		t.Error("expected error looking up old filename after rename")
	}
}

func TestFiler_SymlinkAndReadlink(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create a regular file.
	fileMeta, err := fs.Create(ctx, filer.RootIno, "real-file.txt", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write some data.
	if _, err := fs.Write(ctx, fileMeta.Ino, 0, []byte("real content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Create a symlink.
	symlinkMeta, err := fs.Symlink(ctx, filer.RootIno, "link-to-file", "real-file.txt")
	if err != nil {
		t.Fatalf("Symlink failed: %v", err)
	}
	if symlinkMeta.Type != filer.TypeSymlink {
		t.Errorf("expected type 'symlink', got %q", symlinkMeta.Type)
	}
	if symlinkMeta.Mode != 0777 {
		t.Errorf("expected mode 0777, got %o", symlinkMeta.Mode)
	}

	// Readlink.
	target, err := fs.Readlink(ctx, symlinkMeta.Ino)
	if err != nil {
		t.Fatalf("Readlink failed: %v", err)
	}
	if target != "real-file.txt" {
		t.Errorf("expected target 'real-file.txt', got %q", target)
	}

	// Verify symlink appears in directory listing.
	entries, err := fs.ReadDir(ctx, filer.RootIno)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	foundFile := false
	foundLink := false
	for _, e := range entries {
		if e.Name == "real-file.txt" {
			foundFile = true
		}
		if e.Name == "link-to-file" && e.Type == filer.TypeSymlink {
			foundLink = true
		}
	}
	if !foundFile {
		t.Error("expected 'real-file.txt' in directory listing")
	}
	if !foundLink {
		t.Error("expected 'link-to-file' symlink in directory listing")
	}
}

func TestFiler_UnlinkFile(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create and write a file.
	fileMeta, err := fs.Create(ctx, filer.RootIno, "to-delete.txt", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := fs.Write(ctx, fileMeta.Ino, 0, []byte("delete me")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Unlink the file.
	if err := fs.Unlink(ctx, filer.RootIno, "to-delete.txt"); err != nil {
		t.Fatalf("Unlink failed: %v", err)
	}

	// Verify it is gone from directory listing.
	entries, err := fs.ReadDir(ctx, filer.RootIno)
	if err != nil {
		t.Fatalf("ReadDir after unlink failed: %v", err)
	}
	for _, e := range entries {
		if e.Name == "to-delete.txt" {
			t.Error("file should not exist after unlink")
		}
	}

	// Verify lookup fails.
	_, err = fs.Lookup(ctx, filer.RootIno, "to-delete.txt")
	if err == nil {
		t.Error("expected error looking up unlinked file")
	}
}

func TestFiler_Stat(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Stat the root inode.
	rootMeta, err := fs.Stat(ctx, filer.RootIno)
	if err != nil {
		t.Fatalf("Stat(root) failed: %v", err)
	}
	if rootMeta.Ino != filer.RootIno {
		t.Errorf("expected root ino %d, got %d", filer.RootIno, rootMeta.Ino)
	}
	if rootMeta.Type != filer.TypeDir {
		t.Errorf("expected root type 'dir', got %q", rootMeta.Type)
	}

	// Create a file and stat it.
	fileMeta, err := fs.Create(ctx, filer.RootIno, "stat-test.txt", 0644)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Write data to get a non-zero size.
	testData := []byte("stat test data")
	if _, err := fs.Write(ctx, fileMeta.Ino, 0, testData); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Stat after write.
	statMeta, err := fs.Stat(ctx, fileMeta.Ino)
	if err != nil {
		t.Fatalf("Stat(file) failed: %v", err)
	}
	if statMeta.Size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), statMeta.Size)
	}
	if statMeta.Mode != 0644 {
		t.Errorf("expected mode 0644, got %o", statMeta.Mode)
	}
}

func TestFiler_RmdirNonEmpty(t *testing.T) {
	fs := setupFilerWithDeps(t)
	ctx := context.Background()

	// Create a directory with a file in it.
	dirMeta, err := fs.Mkdir(ctx, filer.RootIno, "non-empty-dir", 0755)
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}
	if _, err := fs.Create(ctx, dirMeta.Ino, "child.txt", 0644); err != nil {
		t.Fatalf("Create child failed: %v", err)
	}

	// Rmdir should fail because directory is not empty.
	err = fs.Rmdir(ctx, filer.RootIno, "non-empty-dir")
	if err == nil {
		t.Error("expected error when rmdir on non-empty directory")
	}
}
