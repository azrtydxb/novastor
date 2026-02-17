package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// NFSHandler is the interface that a filesystem must implement for NFS serving.
type NFSHandler interface {
	Stat(ctx context.Context, ino uint64) (*InodeMeta, error)
	Lookup(ctx context.Context, parentIno uint64, name string) (*InodeMeta, error)
	Mkdir(ctx context.Context, parentIno uint64, name string, mode uint32) (*InodeMeta, error)
	Create(ctx context.Context, parentIno uint64, name string, mode uint32) (*InodeMeta, error)
	Unlink(ctx context.Context, parentIno uint64, name string) error
	Rmdir(ctx context.Context, parentIno uint64, name string) error
	ReadDir(ctx context.Context, ino uint64) ([]*DirEntry, error)
	Read(ctx context.Context, ino uint64, offset, length int64) ([]byte, error)
	Write(ctx context.Context, ino uint64, offset int64, data []byte) (int, error)
	Rename(ctx context.Context, oldParent uint64, oldName string, newParent uint64, newName string) error
	Symlink(ctx context.Context, parentIno uint64, name string, target string) (*InodeMeta, error)
	Readlink(ctx context.Context, ino uint64) (string, error)
}

// NFSServer wraps an NFSHandler and serves NFS over TCP.
type NFSServer struct {
	handler  NFSHandler
	locker   *LockManager
	mu       sync.Mutex
	listener net.Listener
	closed   bool
}

// NewNFSServer creates a new NFSServer with the given handler and lock manager.
func NewNFSServer(handler NFSHandler, locker *LockManager) *NFSServer {
	return &NFSServer{handler: handler, locker: locker}
}

// Addr returns the listener's address, or nil if not listening.
func (s *NFSServer) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// Serve listens on the given address and handles NFS connections.
// In Phase 4, this is a simplified implementation that accepts TCP connections
// and processes a basic RPC protocol. Full NFS v4.1 compound operations
// will be implemented when the go-nfs library integration is finalized.
func (s *NFSServer) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.closed = false
	s.mu.Unlock()

	log.Printf("NFS server listening on %s", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			wasClosed := s.closed
			s.mu.Unlock()
			if wasClosed {
				return nil
			}
			return fmt.Errorf("accepting connection: %w", err)
		}
		go s.handleConnection(conn)
	}
}

// handleConnection processes a single NFS client connection.
func (s *NFSServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("NFS connection from %s", conn.RemoteAddr())
	// Phase 4 stub: real NFS RPC handling will be implemented with go-nfs.
	// For now, we accept the connection and wait for it to close.
	io.Copy(io.Discard, conn)
}

// Stop gracefully stops the NFS server.
func (s *NFSServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil && !s.closed {
		s.closed = true
		return s.listener.Close()
	}
	return nil
}

// errClosed is a sentinel used to detect closed listener errors.
var errClosed = errors.New("use of closed network connection")
