// Package memchunk is an in-memory ChunkService for Go integration and e2e
// tests. Production chunk I/O is served by the Rust SPDK data-plane (the
// agent.ChunkServer only bridges to it), which Go tests cannot start, so the
// access layers (S3, filer, agent client) run against this stand-in over a
// real gRPC socket.
package memchunk

import (
	"context"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/azrtydxb/novastor/api/proto/chunk"
)

// Server is an in-memory pb.ChunkServiceServer.
type Server struct {
	pb.UnimplementedChunkServiceServer

	mu     sync.Mutex
	chunks map[string]entry
}

type entry struct {
	data     []byte
	checksum uint32
}

// New returns an empty Server.
func New() *Server { return &Server{chunks: map[string]entry{}} }

// Register adds the ChunkService to srv.
func (s *Server) Register(srv *grpc.Server) { pb.RegisterChunkServiceServer(srv, s) }

// PutChunk assembles the streamed fragments and stores the chunk.
func (s *Server) PutChunk(stream grpc.ClientStreamingServer[pb.PutChunkRequest, pb.PutChunkResponse]) error {
	var (
		id       string
		checksum uint32
		data     []byte
	)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if id == "" {
			id, checksum = req.GetChunkId(), req.GetChecksum()
			if id == "" {
				return status.Error(codes.InvalidArgument, "first message must contain chunk_id")
			}
		}
		data = append(data, req.GetData()...)
	}
	if len(data) == 0 {
		return status.Error(codes.InvalidArgument, "no chunk data received")
	}
	s.mu.Lock()
	s.chunks[id] = entry{data: data, checksum: checksum}
	s.mu.Unlock()
	return stream.SendAndClose(&pb.PutChunkResponse{ChunkId: id, BytesWritten: int64(len(data))})
}

// GetChunk streams a stored chunk back.
func (s *Server) GetChunk(req *pb.GetChunkRequest, stream grpc.ServerStreamingServer[pb.GetChunkResponse]) error {
	s.mu.Lock()
	c, ok := s.chunks[req.GetChunkId()]
	s.mu.Unlock()
	if !ok {
		return status.Error(codes.NotFound, "chunk not found")
	}
	return stream.Send(&pb.GetChunkResponse{ChunkId: req.GetChunkId(), Checksum: c.checksum, Data: c.data})
}

// DeleteChunk removes a chunk (idempotent).
func (s *Server) DeleteChunk(_ context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	s.mu.Lock()
	delete(s.chunks, req.GetChunkId())
	s.mu.Unlock()
	return &pb.DeleteChunkResponse{}, nil
}

// HasChunk reports whether a chunk exists.
func (s *Server) HasChunk(_ context.Context, req *pb.HasChunkRequest) (*pb.HasChunkResponse, error) {
	s.mu.Lock()
	_, ok := s.chunks[req.GetChunkId()]
	s.mu.Unlock()
	return &pb.HasChunkResponse{Exists: ok}, nil
}
