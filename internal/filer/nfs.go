package filer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// rpcRequest represents an incoming JSON-RPC request over the NFS TCP connection.
type rpcRequest struct {
	Method string          `json:"method"`
	ID     uint64          `json:"id"`
	Params json.RawMessage `json:"params"`
}

// rpcResponse represents an outgoing JSON-RPC response.
type rpcResponse struct {
	ID     uint64      `json:"id"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// Parameter types for RPC methods.

type statParams struct {
	Ino uint64 `json:"ino"`
}

type lookupParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
}

type mkdirParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type createParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
	Mode      uint32 `json:"mode"`
}

type unlinkParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
}

type rmdirParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
}

type readDirParams struct {
	Ino uint64 `json:"ino"`
}

type readParams struct {
	Ino    uint64 `json:"ino"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
}

type writeParams struct {
	Ino    uint64 `json:"ino"`
	Offset int64  `json:"offset"`
	Data   []byte `json:"data"`
}

type renameParams struct {
	OldParent uint64 `json:"oldParent"`
	OldName   string `json:"oldName"`
	NewParent uint64 `json:"newParent"`
	NewName   string `json:"newName"`
}

type symlinkParams struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
	Target    string `json:"target"`
}

type readlinkParams struct {
	Ino uint64 `json:"ino"`
}

// writeResult is the result type for Write operations.
type writeResult struct {
	BytesWritten int `json:"bytesWritten"`
}

// readResult is the result type for Read operations.
type readResult struct {
	Data []byte `json:"data"`
}

// readlinkResult is the result type for Readlink operations.
type readlinkResult struct {
	Target string `json:"target"`
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

// handleConnection processes a single NFS client connection using JSON-RPC.
func (s *NFSServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var req rpcRequest
		if err := decoder.Decode(&req); err != nil {
			return // connection closed or malformed input
		}
		resp := s.handleRPC(req)
		if err := encoder.Encode(resp); err != nil {
			return
		}
	}
}

// handleRPC dispatches an RPC request to the appropriate NFSHandler method.
func (s *NFSServer) handleRPC(req rpcRequest) rpcResponse {
	ctx := context.Background()

	switch req.Method {
	case "Stat":
		var p statParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.Stat(ctx, p.Ino)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Lookup":
		var p lookupParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.Lookup(ctx, p.ParentIno, p.Name)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Mkdir":
		var p mkdirParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.Mkdir(ctx, p.ParentIno, p.Name, p.Mode)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Create":
		var p createParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.Create(ctx, p.ParentIno, p.Name, p.Mode)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Unlink":
		var p unlinkParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		if err := s.handler.Unlink(ctx, p.ParentIno, p.Name); err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: "ok"}

	case "Rmdir":
		var p rmdirParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		if err := s.handler.Rmdir(ctx, p.ParentIno, p.Name); err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: "ok"}

	case "ReadDir":
		var p readDirParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.ReadDir(ctx, p.Ino)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Read":
		var p readParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		data, err := s.handler.Read(ctx, p.Ino, p.Offset, p.Length)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: readResult{Data: data}}

	case "Write":
		var p writeParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		n, err := s.handler.Write(ctx, p.Ino, p.Offset, p.Data)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: writeResult{BytesWritten: n}}

	case "Rename":
		var p renameParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		if err := s.handler.Rename(ctx, p.OldParent, p.OldName, p.NewParent, p.NewName); err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: "ok"}

	case "Symlink":
		var p symlinkParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		result, err := s.handler.Symlink(ctx, p.ParentIno, p.Name, p.Target)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: result}

	case "Readlink":
		var p readlinkParams
		if err := json.Unmarshal(req.Params, &p); err != nil {
			return rpcResponse{ID: req.ID, Error: fmt.Sprintf("invalid params: %v", err)}
		}
		target, err := s.handler.Readlink(ctx, p.Ino)
		if err != nil {
			return rpcResponse{ID: req.ID, Error: err.Error()}
		}
		return rpcResponse{ID: req.ID, Result: readlinkResult{Target: target}}

	default:
		return rpcResponse{ID: req.ID, Error: fmt.Sprintf("unknown method: %s", req.Method)}
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

// errClosed is a sentinel used to detect closed listener errors.
var errClosed = errors.New("use of closed network connection")
