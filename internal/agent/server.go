package agent

import (
	"context"
	"io"

	pb "github.com/azrtydxb/novastor/api/proto/chunk"
	"github.com/azrtydxb/novastor/internal/chunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamFragmentSize is the maximum number of bytes sent per gRPC
// streaming message when returning chunk data.
const streamFragmentSize = 1 * 1024 * 1024 // 1 MiB

// ChunkServer implements the pb.ChunkServiceServer interface backed by a
// chunk.Store.
type ChunkServer struct {
	pb.UnimplementedChunkServiceServer
	store chunk.Store
}

// NewChunkServer returns a ChunkServer that delegates storage operations to
// the provided chunk.Store.
func NewChunkServer(store chunk.Store) *ChunkServer {
	return &ChunkServer{store: store}
}

// Register adds the ChunkService to the given gRPC server.
func (s *ChunkServer) Register(srv *grpc.Server) {
	pb.RegisterChunkServiceServer(srv, s)
}

// PutChunk receives a client-streaming request carrying chunk data. The
// first message must contain chunk_id and checksum. Data from all messages
// is concatenated to form the complete chunk, which is then written to the
// store.
func (s *ChunkServer) PutChunk(stream grpc.ClientStreamingServer[pb.PutChunkRequest, pb.PutChunkResponse]) error {
	var (
		chunkID  string
		checksum uint32
		data     []byte
		gotFirst bool
	)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receiving stream: %v", err)
		}

		if !gotFirst {
			chunkID = msg.GetChunkId()
			checksum = msg.GetChecksum()
			if chunkID == "" {
				return status.Error(codes.InvalidArgument, "first message must contain chunk_id")
			}
			gotFirst = true
		}

		data = append(data, msg.GetData()...)
	}

	if !gotFirst {
		return status.Error(codes.InvalidArgument, "no messages received")
	}

	c := &chunk.Chunk{
		ID:       chunk.ChunkID(chunkID),
		Data:     data,
		Checksum: checksum,
	}

	if err := c.VerifyChecksum(); err != nil {
		return status.Errorf(codes.DataLoss, "checksum verification failed: %v", err)
	}

	ctx := stream.Context()
	if err := s.store.Put(ctx, c); err != nil {
		return status.Errorf(codes.Internal, "storing chunk: %v", err)
	}

	return stream.SendAndClose(&pb.PutChunkResponse{
		ChunkId:      chunkID,
		BytesWritten: int64(len(data)),
	})
}

// GetChunk streams the requested chunk back to the caller. The first
// response message includes chunk_id and checksum alongside the first
// data fragment; subsequent messages carry remaining fragments.
func (s *ChunkServer) GetChunk(req *pb.GetChunkRequest, stream grpc.ServerStreamingServer[pb.GetChunkResponse]) error {
	id := req.GetChunkId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	ctx := stream.Context()
	c, err := s.store.Get(ctx, chunk.ChunkID(id))
	if err != nil {
		return status.Errorf(codes.NotFound, "chunk %s: %v", id, err)
	}

	data := c.Data
	for offset := 0; offset < len(data); offset += streamFragmentSize {
		end := offset + streamFragmentSize
		if end > len(data) {
			end = len(data)
		}
		resp := &pb.GetChunkResponse{
			Data: data[offset:end],
		}
		if offset == 0 {
			resp.ChunkId = string(c.ID)
			resp.Checksum = c.Checksum
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "sending stream: %v", err)
		}
	}

	// Handle zero-length data: still send at least one message with metadata.
	if len(data) == 0 {
		if err := stream.Send(&pb.GetChunkResponse{
			ChunkId:  string(c.ID),
			Checksum: c.Checksum,
		}); err != nil {
			return status.Errorf(codes.Internal, "sending stream: %v", err)
		}
	}

	return nil
}

// DeleteChunk removes a chunk from the store.
func (s *ChunkServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	id := req.GetChunkId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	if err := s.store.Delete(ctx, chunk.ChunkID(id)); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting chunk %s: %v", id, err)
	}

	return &pb.DeleteChunkResponse{}, nil
}

// HasChunk reports whether a chunk exists in the store.
func (s *ChunkServer) HasChunk(ctx context.Context, req *pb.HasChunkRequest) (*pb.HasChunkResponse, error) {
	id := req.GetChunkId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	exists, err := s.store.Has(ctx, chunk.ChunkID(id))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "checking chunk %s: %v", id, err)
	}

	return &pb.HasChunkResponse{Exists: exists}, nil
}
