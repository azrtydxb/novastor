package filer

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/internal/logging"
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
	Truncate(ctx context.Context, ino uint64, size int64) error
	Link(ctx context.Context, targetIno uint64, parentIno uint64, name string) (*InodeMeta, error)
}

// NFSServer wraps an NFSHandler and serves NFS v3 over TCP using the ONC/RPC
// protocol (RFC 5531) with record marking (RFC 5531 section 11). Standard NFS
// clients (e.g., Linux `mount -t nfs`) can mount the export.
//
// The server implements both the MOUNT v3 program (100005) and the NFS v3
// program (100003) on the same TCP port, which is the common approach for
// user-space NFS servers.
type NFSServer struct {
	handler    NFSHandler
	locker     *LockManager
	handles    *handleManager
	dispatcher *rpcDispatcher
	mu         sync.Mutex
	listener   net.Listener
	closed     bool
	exportPath string
}

// NewNFSServer creates a new NFSServer with the given handler and lock manager.
// The server supports NFS v3 and MOUNT v3 protocols over a single TCP listener.
func NewNFSServer(handler NFSHandler, locker *LockManager) *NFSServer {
	handles := newHandleManager()
	dispatcher := newRPCDispatcher()

	mountH := newMountHandler(handles, "/")
	nfsH := newNFSV3Handler(handler, handles, locker)

	dispatcher.register(mountH)
	dispatcher.register(nfsH)

	return &NFSServer{
		handler:    handler,
		locker:     locker,
		handles:    handles,
		dispatcher: dispatcher,
		exportPath: "/",
	}
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

// Serve listens on the given address and handles NFS v3 connections using
// ONC/RPC record marking over TCP.
func (s *NFSServer) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}

	s.mu.Lock()
	s.listener = ln
	s.closed = false
	s.mu.Unlock()

	logging.L.Info("NFS v3 server listening",
		zap.String("addr", ln.Addr().String()),
		zap.String("export", s.exportPath),
	)

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

// handleConnection processes a single NFS client connection using ONC/RPC
// record marking framing.
func (s *NFSServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().String()
	logging.L.Debug("nfs: new connection", zap.String("client", clientAddr))

	for {
		// Read a complete ONC/RPC record (with record marking).
		record, err := readRecord(conn)
		if err != nil {
			// Connection closed or read error — normal during shutdown.
			logging.L.Debug("nfs: connection closed", zap.String("client", clientAddr))
			return
		}

		// Dispatch the RPC call.
		reply, err := s.dispatcher.dispatch(record, conn)
		if err != nil {
			logging.L.Warn("nfs: dispatch error",
				zap.String("client", clientAddr),
				zap.Error(err),
			)
			continue
		}

		if reply == nil {
			continue
		}

		// Send the reply with record marking.
		if err := writeRecord(conn, reply); err != nil {
			logging.L.Debug("nfs: write error", zap.String("client", clientAddr))
			return
		}
	}
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
