package filer

import (
	"context"
	"net"
	"testing"
	"time"
)

// stubNFSHandler implements NFSHandler with minimal stubs for testing.
type stubNFSHandler struct{}

func (s *stubNFSHandler) Stat(_ context.Context, _ uint64) (*InodeMeta, error) {
	return &InodeMeta{Ino: 1, Type: TypeDir}, nil
}

func (s *stubNFSHandler) Lookup(_ context.Context, _ uint64, _ string) (*InodeMeta, error) {
	return nil, nil
}

func (s *stubNFSHandler) Mkdir(_ context.Context, _ uint64, _ string, _ uint32) (*InodeMeta, error) {
	return nil, nil
}

func (s *stubNFSHandler) Create(_ context.Context, _ uint64, _ string, _ uint32) (*InodeMeta, error) {
	return nil, nil
}

func (s *stubNFSHandler) Unlink(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Rmdir(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) ReadDir(_ context.Context, _ uint64) ([]*DirEntry, error) {
	return nil, nil
}

func (s *stubNFSHandler) Read(_ context.Context, _ uint64, _, _ int64) ([]byte, error) {
	return nil, nil
}

func (s *stubNFSHandler) Write(_ context.Context, _ uint64, _ int64, _ []byte) (int, error) {
	return 0, nil
}

func (s *stubNFSHandler) Rename(_ context.Context, _ uint64, _ string, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Symlink(_ context.Context, _ uint64, _ string, _ string) (*InodeMeta, error) {
	return nil, nil
}

func (s *stubNFSHandler) Readlink(_ context.Context, _ uint64) (string, error) {
	return "", nil
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
