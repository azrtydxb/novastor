//go:build e2e

package e2e

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/filer"
)

// rpcRequest is the JSON-RPC request format expected by the NFS server.
type rpcRequest struct {
	Method string      `json:"method"`
	ID     uint64      `json:"id"`
	Params interface{} `json:"params"`
}

// rpcResponse is the JSON-RPC response format returned by the NFS server.
type rpcResponse struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// nfsClient wraps a TCP connection to the NFS server for sending JSON-RPC requests.
type nfsClient struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	nextID  uint64
}

// newNFSClient connects to the NFS server at the given address.
func newNFSClient(t *testing.T, addr string) *nfsClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect to NFS server at %s: %v", addr, err)
	}
	return &nfsClient{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
		nextID:  1,
	}
}

// call sends an RPC request and returns the response.
func (c *nfsClient) call(t *testing.T, method string, params interface{}) rpcResponse {
	t.Helper()
	id := c.nextID
	c.nextID++

	req := rpcRequest{Method: method, ID: id, Params: params}
	if err := c.encoder.Encode(req); err != nil {
		t.Fatalf("Failed to send RPC request %q: %v", method, err)
	}

	var resp rpcResponse
	if err := c.decoder.Decode(&resp); err != nil {
		t.Fatalf("Failed to read RPC response for %q: %v", method, err)
	}

	if resp.ID != id {
		t.Fatalf("RPC response ID mismatch: expected %d, got %d", id, resp.ID)
	}

	return resp
}

// close terminates the connection.
func (c *nfsClient) close() {
	c.conn.Close()
}

// TestFileStorage_NFSOperations tests the NFS filer end-to-end:
//  1. Start metadata + chunk services
//  2. Start NFS server with real adapters
//  3. Connect to NFS server, send JSON-RPC
//  4. Mkdir, create file, write, read, readdir
//  5. Verify operations through the full stack
func TestFileStorage_NFSOperations(t *testing.T) {
	// Set up cluster.
	tc := newTestCluster(t)
	defer tc.Close()

	// Create filer adapters backed by the real metadata and chunk services.
	metaAdapter := filer.NewMetadataAdapter(tc.metaClient)
	chunkAdapter := filer.NewChunkAdapter(tc.agentClient)

	// Create the filesystem layer (initializes root inode).
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)

	// Create the NFS server with a lock manager.
	locker := filer.NewLockManager()
	nfsServer := filer.NewNFSServer(fs, locker)

	// Start the NFS server on a random port.
	nfsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for NFS server: %v", err)
	}
	nfsAddr := nfsLis.Addr().String()
	nfsLis.Close() // Close so that NFSServer.Serve can bind it.

	go func() {
		if err := nfsServer.Serve(nfsAddr); err != nil {
			// Server stopped, expected during cleanup.
		}
	}()
	defer nfsServer.Stop()

	// Wait briefly for server to start listening.
	waitForTCP(t, nfsAddr)

	// Connect a JSON-RPC client to the NFS server.
	client := newNFSClient(t, nfsAddr)
	defer client.close()

	// Step 1: Stat the root directory (inode 1).
	t.Log("Stat root directory (inode 1)...")
	resp := client.call(t, "Stat", map[string]interface{}{"ino": 1})
	if resp.Error != "" {
		t.Fatalf("Stat root failed: %s", resp.Error)
	}
	var rootMeta filer.InodeMeta
	if err := json.Unmarshal(resp.Result, &rootMeta); err != nil {
		t.Fatalf("Failed to unmarshal root inode: %v", err)
	}
	if rootMeta.Ino != 1 {
		t.Errorf("Expected root inode 1, got %d", rootMeta.Ino)
	}
	if rootMeta.Type != filer.TypeDir {
		t.Errorf("Expected root type 'dir', got %q", rootMeta.Type)
	}
	t.Logf("Root inode verified: ino=%d, type=%s", rootMeta.Ino, rootMeta.Type)

	// Step 2: Mkdir in root.
	t.Log("Creating directory 'testdir' in root...")
	resp = client.call(t, "Mkdir", map[string]interface{}{
		"parentIno": 1,
		"name":      "testdir",
		"mode":      0755,
	})
	if resp.Error != "" {
		t.Fatalf("Mkdir failed: %s", resp.Error)
	}
	var dirMeta filer.InodeMeta
	if err := json.Unmarshal(resp.Result, &dirMeta); err != nil {
		t.Fatalf("Failed to unmarshal dir inode: %v", err)
	}
	if dirMeta.Type != filer.TypeDir {
		t.Errorf("Expected dir type, got %q", dirMeta.Type)
	}
	dirIno := dirMeta.Ino
	t.Logf("Directory created: ino=%d", dirIno)

	// Step 3: Create a file inside testdir.
	t.Log("Creating file 'hello.txt' in testdir...")
	resp = client.call(t, "Create", map[string]interface{}{
		"parentIno": dirIno,
		"name":      "hello.txt",
		"mode":      0644,
	})
	if resp.Error != "" {
		t.Fatalf("Create failed: %s", resp.Error)
	}
	var fileMeta filer.InodeMeta
	if err := json.Unmarshal(resp.Result, &fileMeta); err != nil {
		t.Fatalf("Failed to unmarshal file inode: %v", err)
	}
	if fileMeta.Type != filer.TypeFile {
		t.Errorf("Expected file type, got %q", fileMeta.Type)
	}
	fileIno := fileMeta.Ino
	t.Logf("File created: ino=%d", fileIno)

	// Step 4: Write data to the file.
	testContent := []byte("Hello from NovaStor NFS E2E test!")
	t.Logf("Writing %d bytes to file ino=%d...", len(testContent), fileIno)
	resp = client.call(t, "Write", map[string]interface{}{
		"ino":    fileIno,
		"offset": 0,
		"data":   testContent,
	})
	if resp.Error != "" {
		t.Fatalf("Write failed: %s", resp.Error)
	}
	var writeRes struct {
		BytesWritten int `json:"bytesWritten"`
	}
	if err := json.Unmarshal(resp.Result, &writeRes); err != nil {
		t.Fatalf("Failed to unmarshal write result: %v", err)
	}
	if writeRes.BytesWritten != len(testContent) {
		t.Errorf("Expected %d bytes written, got %d", len(testContent), writeRes.BytesWritten)
	}
	t.Logf("Write verified: %d bytes written", writeRes.BytesWritten)

	// Step 5: Read data back from the file.
	t.Logf("Reading from file ino=%d...", fileIno)
	resp = client.call(t, "Read", map[string]interface{}{
		"ino":    fileIno,
		"offset": 0,
		"length": 1024,
	})
	if resp.Error != "" {
		t.Fatalf("Read failed: %s", resp.Error)
	}
	var readRes struct {
		Data []byte `json:"data"`
	}
	if err := json.Unmarshal(resp.Result, &readRes); err != nil {
		t.Fatalf("Failed to unmarshal read result: %v", err)
	}
	if string(readRes.Data) != string(testContent) {
		t.Errorf("Read data mismatch: expected %q, got %q", testContent, readRes.Data)
	}
	t.Logf("Read verified: %d bytes", len(readRes.Data))

	// Step 6: ReadDir on testdir.
	t.Logf("ReadDir on testdir ino=%d...", dirIno)
	resp = client.call(t, "ReadDir", map[string]interface{}{
		"ino": dirIno,
	})
	if resp.Error != "" {
		t.Fatalf("ReadDir failed: %s", resp.Error)
	}
	var entries []filer.DirEntry
	if err := json.Unmarshal(resp.Result, &entries); err != nil {
		t.Fatalf("Failed to unmarshal readdir result: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry in testdir, got %d", len(entries))
	} else if entries[0].Name != "hello.txt" {
		t.Errorf("Expected entry name 'hello.txt', got %q", entries[0].Name)
	}
	t.Logf("ReadDir verified: %d entries", len(entries))

	// Step 7: Lookup the file in testdir.
	t.Log("Lookup 'hello.txt' in testdir...")
	resp = client.call(t, "Lookup", map[string]interface{}{
		"parentIno": dirIno,
		"name":      "hello.txt",
	})
	if resp.Error != "" {
		t.Fatalf("Lookup failed: %s", resp.Error)
	}
	var lookupMeta filer.InodeMeta
	if err := json.Unmarshal(resp.Result, &lookupMeta); err != nil {
		t.Fatalf("Failed to unmarshal lookup result: %v", err)
	}
	if lookupMeta.Ino != fileIno {
		t.Errorf("Lookup ino mismatch: expected %d, got %d", fileIno, lookupMeta.Ino)
	}
	t.Logf("Lookup verified: ino=%d", lookupMeta.Ino)

	// Step 8: ReadDir on root to verify testdir exists.
	t.Log("ReadDir on root...")
	resp = client.call(t, "ReadDir", map[string]interface{}{
		"ino": 1,
	})
	if resp.Error != "" {
		t.Fatalf("ReadDir root failed: %s", resp.Error)
	}
	var rootEntries []filer.DirEntry
	if err := json.Unmarshal(resp.Result, &rootEntries); err != nil {
		t.Fatalf("Failed to unmarshal root readdir result: %v", err)
	}
	if len(rootEntries) != 1 {
		t.Errorf("Expected 1 entry in root, got %d", len(rootEntries))
	} else if rootEntries[0].Name != "testdir" {
		t.Errorf("Expected entry name 'testdir', got %q", rootEntries[0].Name)
	}
	t.Logf("Root readdir verified: %d entries", len(rootEntries))

	// Step 9: Unlink the file.
	t.Log("Unlinking 'hello.txt' from testdir...")
	resp = client.call(t, "Unlink", map[string]interface{}{
		"parentIno": dirIno,
		"name":      "hello.txt",
	})
	if resp.Error != "" {
		t.Fatalf("Unlink failed: %s", resp.Error)
	}
	t.Log("Unlink verified")

	// Step 10: Verify file is gone.
	resp = client.call(t, "ReadDir", map[string]interface{}{
		"ino": dirIno,
	})
	if resp.Error != "" {
		t.Fatalf("ReadDir after unlink failed: %s", resp.Error)
	}
	var afterUnlink []filer.DirEntry
	if err := json.Unmarshal(resp.Result, &afterUnlink); err != nil {
		t.Fatalf("Failed to unmarshal readdir result: %v", err)
	}
	if len(afterUnlink) != 0 {
		t.Errorf("Expected 0 entries after unlink, got %d", len(afterUnlink))
	}
	t.Log("File removal verified via ReadDir")

	// Step 11: Rmdir testdir.
	t.Log("Removing directory 'testdir'...")
	resp = client.call(t, "Rmdir", map[string]interface{}{
		"parentIno": 1,
		"name":      "testdir",
	})
	if resp.Error != "" {
		t.Fatalf("Rmdir failed: %s", resp.Error)
	}
	t.Log("Rmdir verified")

	// Step 12: Verify root is empty.
	resp = client.call(t, "ReadDir", map[string]interface{}{
		"ino": 1,
	})
	if resp.Error != "" {
		t.Fatalf("ReadDir root after rmdir failed: %s", resp.Error)
	}
	var finalEntries []filer.DirEntry
	if err := json.Unmarshal(resp.Result, &finalEntries); err != nil {
		t.Fatalf("Failed to unmarshal root readdir result: %v", err)
	}
	if len(finalEntries) != 0 {
		t.Errorf("Expected 0 entries in root after cleanup, got %d", len(finalEntries))
	}
	t.Log("Full NFS lifecycle test completed successfully")
}

// TestFileStorage_SymlinkOperations tests symlink creation and readlink.
func TestFileStorage_SymlinkOperations(t *testing.T) {
	tc := newTestCluster(t)
	defer tc.Close()

	metaAdapter := filer.NewMetadataAdapter(tc.metaClient)
	chunkAdapter := filer.NewChunkAdapter(tc.agentClient)
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)
	locker := filer.NewLockManager()
	nfsServer := filer.NewNFSServer(fs, locker)

	nfsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for NFS server: %v", err)
	}
	nfsAddr := nfsLis.Addr().String()
	nfsLis.Close()

	go func() {
		if err := nfsServer.Serve(nfsAddr); err != nil {
			// expected
		}
	}()
	defer nfsServer.Stop()

	waitForTCP(t, nfsAddr)
	client := newNFSClient(t, nfsAddr)
	defer client.close()

	// Create a symlink in root.
	resp := client.call(t, "Symlink", map[string]interface{}{
		"parentIno": 1,
		"name":      "mylink",
		"target":    "/some/target/path",
	})
	if resp.Error != "" {
		t.Fatalf("Symlink failed: %s", resp.Error)
	}
	var symlinkMeta filer.InodeMeta
	if err := json.Unmarshal(resp.Result, &symlinkMeta); err != nil {
		t.Fatalf("Failed to unmarshal symlink result: %v", err)
	}
	if symlinkMeta.Type != filer.TypeSymlink {
		t.Errorf("Expected symlink type, got %q", symlinkMeta.Type)
	}
	t.Logf("Symlink created: ino=%d", symlinkMeta.Ino)

	// Readlink.
	resp = client.call(t, "Readlink", map[string]interface{}{
		"ino": symlinkMeta.Ino,
	})
	if resp.Error != "" {
		t.Fatalf("Readlink failed: %s", resp.Error)
	}
	var readlinkRes struct {
		Target string `json:"target"`
	}
	if err := json.Unmarshal(resp.Result, &readlinkRes); err != nil {
		t.Fatalf("Failed to unmarshal readlink result: %v", err)
	}
	if readlinkRes.Target != "/some/target/path" {
		t.Errorf("Expected target '/some/target/path', got %q", readlinkRes.Target)
	}
	t.Log("Readlink verified")
}

// waitForTCP polls until a TCP connection can be established to the given address.
func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Timed out waiting for TCP server at %s", addr)
}
