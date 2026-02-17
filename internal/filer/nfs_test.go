package filer

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"
)

// stubNFSHandler implements NFSHandler with minimal stubs for testing.
type stubNFSHandler struct{}

func (s *stubNFSHandler) Stat(_ context.Context, ino uint64) (*InodeMeta, error) {
	return &InodeMeta{Ino: ino, Type: TypeDir, Mode: 0755}, nil
}

func (s *stubNFSHandler) Lookup(_ context.Context, _ uint64, name string) (*InodeMeta, error) {
	return &InodeMeta{Ino: 10, Type: TypeFile, Mode: 0644}, nil
}

func (s *stubNFSHandler) Mkdir(_ context.Context, _ uint64, name string, mode uint32) (*InodeMeta, error) {
	return &InodeMeta{Ino: 100, Type: TypeDir, Mode: mode}, nil
}

func (s *stubNFSHandler) Create(_ context.Context, _ uint64, name string, mode uint32) (*InodeMeta, error) {
	return &InodeMeta{Ino: 101, Type: TypeFile, Mode: mode}, nil
}

func (s *stubNFSHandler) Unlink(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Rmdir(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) ReadDir(_ context.Context, _ uint64) ([]*DirEntry, error) {
	return []*DirEntry{
		{Name: "file1.txt", Ino: 10, Type: TypeFile},
		{Name: "subdir", Ino: 11, Type: TypeDir},
	}, nil
}

func (s *stubNFSHandler) Read(_ context.Context, _ uint64, _, _ int64) ([]byte, error) {
	return []byte("hello"), nil
}

func (s *stubNFSHandler) Write(_ context.Context, _ uint64, _ int64, data []byte) (int, error) {
	return len(data), nil
}

func (s *stubNFSHandler) Rename(_ context.Context, _ uint64, _ string, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Symlink(_ context.Context, _ uint64, _ string, target string) (*InodeMeta, error) {
	return &InodeMeta{Ino: 200, Type: TypeSymlink, Target: target}, nil
}

func (s *stubNFSHandler) Readlink(_ context.Context, _ uint64) (string, error) {
	return "/some/target", nil
}

// waitForAddr polls until the server has a non-nil address or the timeout expires.
func waitForAddr(srv *NFSServer, timeout time.Duration) net.Addr {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != nil {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// rpcCall sends a JSON-RPC request and decodes the response.
func rpcCall(t *testing.T, conn net.Conn, method string, id uint64, params interface{}) rpcResponse {
	t.Helper()

	paramsJSON, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("failed to marshal params: %v", err)
	}

	req := rpcRequest{
		Method: method,
		ID:     id,
		Params: paramsJSON,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	var resp rpcResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	return resp
}

func startTestServer(t *testing.T) (*NFSServer, net.Addr) {
	t.Helper()
	srv := NewNFSServer(&stubNFSHandler{}, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	return srv, addr
}

func TestNFSServer_StartStop(t *testing.T) {
	srv := NewNFSServer(&stubNFSHandler{}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	// Verify we can connect.
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("expected to connect to NFS server at %s: %v", addr, err)
	}
	conn.Close()

	// Stop the server.
	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected error stopping server: %v", err)
	}

	// The Serve goroutine should return.
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Serve returned (expected after stop): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
}

func TestNFSServer_AcceptConnection(t *testing.T) {
	srv := NewNFSServer(&stubNFSHandler{}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	// Dial the server.
	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to NFS server: %v", err)
	}

	// Write some data and close; the server should handle this gracefully.
	_, err = conn.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}
	conn.Close()

	// Brief pause to let the server process the connection.
	time.Sleep(50 * time.Millisecond)

	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected error stopping server: %v", err)
	}

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
}

func TestNFSServer_Stat_RPC(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	resp := rpcCall(t, conn, "Stat", 1, statParams{Ino: 1})

	if resp.ID != 1 {
		t.Errorf("expected response ID 1, got %d", resp.ID)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Result == nil {
		t.Fatal("expected non-nil result")
	}

	// Decode the result into InodeMeta.
	resultJSON, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}
	var meta InodeMeta
	if err := json.Unmarshal(resultJSON, &meta); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if meta.Ino != 1 {
		t.Errorf("expected ino 1, got %d", meta.Ino)
	}
	if meta.Type != TypeDir {
		t.Errorf("expected type dir, got %s", meta.Type)
	}
}

func TestNFSServer_Mkdir_RPC(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	resp := rpcCall(t, conn, "Mkdir", 2, mkdirParams{
		ParentIno: 1,
		Name:      "testdir",
		Mode:      0755,
	})

	if resp.ID != 2 {
		t.Errorf("expected response ID 2, got %d", resp.ID)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Result == nil {
		t.Fatal("expected non-nil result")
	}

	resultJSON, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}
	var meta InodeMeta
	if err := json.Unmarshal(resultJSON, &meta); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if meta.Ino != 100 {
		t.Errorf("expected ino 100, got %d", meta.Ino)
	}
	if meta.Type != TypeDir {
		t.Errorf("expected type dir, got %s", meta.Type)
	}
}

func TestNFSServer_ReadDir_RPC(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	resp := rpcCall(t, conn, "ReadDir", 3, readDirParams{Ino: 1})

	if resp.ID != 3 {
		t.Errorf("expected response ID 3, got %d", resp.ID)
	}
	if resp.Error != "" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	if resp.Result == nil {
		t.Fatal("expected non-nil result")
	}

	resultJSON, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("failed to marshal result: %v", err)
	}
	var entries []*DirEntry
	if err := json.Unmarshal(resultJSON, &entries); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Name != "file1.txt" {
		t.Errorf("expected first entry name 'file1.txt', got %q", entries[0].Name)
	}
	if entries[1].Name != "subdir" {
		t.Errorf("expected second entry name 'subdir', got %q", entries[1].Name)
	}
}

func TestNFSServer_UnknownMethod_RPC(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	resp := rpcCall(t, conn, "NonExistent", 99, struct{}{})

	if resp.ID != 99 {
		t.Errorf("expected response ID 99, got %d", resp.ID)
	}
	if resp.Error == "" {
		t.Fatal("expected error for unknown method")
	}
}

func TestNFSServer_MultipleRPCs(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send multiple RPC calls on the same connection.
	resp1 := rpcCall(t, conn, "Stat", 1, statParams{Ino: 1})
	if resp1.Error != "" {
		t.Fatalf("Stat error: %s", resp1.Error)
	}

	resp2 := rpcCall(t, conn, "ReadDir", 2, readDirParams{Ino: 1})
	if resp2.Error != "" {
		t.Fatalf("ReadDir error: %s", resp2.Error)
	}

	resp3 := rpcCall(t, conn, "Mkdir", 3, mkdirParams{ParentIno: 1, Name: "newdir", Mode: 0755})
	if resp3.Error != "" {
		t.Fatalf("Mkdir error: %s", resp3.Error)
	}
}
