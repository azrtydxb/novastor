package agent

import (
	"context"
	"fmt"
	"io"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/azrtydxb/novastor/api/proto/chunk"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/spdk"
)

// ChunkServer implements the ChunkService gRPC server by bridging calls to the
// Rust SPDK data-plane via JSON-RPC. This ensures all chunk I/O (from S3, Filer,
// or any other access layer) flows through the Rust dataplane, never through Go.
type ChunkServer struct {
	pb.UnimplementedChunkServiceServer

	spdkClient *spdk.Client
	bdevName   string
}

// NewChunkServer creates a ChunkServer that routes chunk operations to the SPDK
// data-plane. bdevName is the chunk store bdev (same as used by SPDKTargetServer).
func NewChunkServer(spdkClient *spdk.Client, bdevName string) *ChunkServer {
	return &ChunkServer{
		spdkClient: spdkClient,
		bdevName:   bdevName,
	}
}

// Register adds the ChunkService to the given gRPC server.
func (s *ChunkServer) Register(srv *grpc.Server) {
	pb.RegisterChunkServiceServer(srv, s)
}

// PutChunk receives a stream of chunk data fragments, assembles them, and writes
// the chunk to the Rust dataplane via the chunk_write JSON-RPC method.
func (s *ChunkServer) PutChunk(stream grpc.ClientStreamingServer[pb.PutChunkRequest, pb.PutChunkResponse]) error {
	var (
		chunkID  string
		checksum uint32
		data     []byte
	)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return status.Errorf(codes.Internal, "receiving chunk stream: %v", err)
		}

		// First message must contain chunk_id and checksum.
		if chunkID == "" {
			chunkID = req.GetChunkId()
			checksum = req.GetChecksum()
			if chunkID == "" {
				return status.Error(codes.InvalidArgument, "first message must contain chunk_id")
			}
		}

		if len(req.GetData()) > 0 {
			data = append(data, req.GetData()...)
		}
	}

	if len(data) == 0 {
		return status.Error(codes.InvalidArgument, "no chunk data received")
	}

	logging.L.Debug("chunk_server: writing chunk via SPDK dataplane",
		zap.String("chunkID", chunkID),
		zap.Int("dataLen", len(data)),
		zap.Uint32("checksum", checksum),
	)

	result, err := s.spdkClient.ChunkWrite(s.bdevName, data)
	if err != nil {
		logging.L.Error("chunk_server: chunk_write failed",
			zap.String("chunkID", chunkID),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "writing chunk to dataplane: %v", err)
	}

	return stream.SendAndClose(&pb.PutChunkResponse{
		ChunkId:      result.ChunkID,
		BytesWritten: int64(len(data)),
	})
}

// GetChunk reads a chunk from the Rust dataplane via the chunk_read JSON-RPC
// method and streams it back to the caller.
func (s *ChunkServer) GetChunk(req *pb.GetChunkRequest, stream grpc.ServerStreamingServer[pb.GetChunkResponse]) error {
	chunkID := req.GetChunkId()
	if chunkID == "" {
		return status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	logging.L.Debug("chunk_server: reading chunk via SPDK dataplane",
		zap.String("chunkID", chunkID),
	)

	data, checksum, err := s.spdkClient.ChunkRead(s.bdevName, chunkID)
	if err != nil {
		logging.L.Error("chunk_server: chunk_read failed",
			zap.String("chunkID", chunkID),
			zap.Error(err),
		)
		return status.Errorf(codes.Internal, "reading chunk from dataplane: %v", err)
	}

	// Stream the data back. For chunks up to 4MB we send in a single message;
	// the gRPC max message size (default 4MB) may need tuning for larger payloads.
	const fragmentSize = 2 * 1024 * 1024 // 2MB fragments
	for offset := 0; offset < len(data); offset += fragmentSize {
		end := offset + fragmentSize
		if end > len(data) {
			end = len(data)
		}
		resp := &pb.GetChunkResponse{
			Data: data[offset:end],
		}
		// Include metadata in the first fragment.
		if offset == 0 {
			resp.ChunkId = chunkID
			resp.Checksum = checksum
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("sending chunk fragment: %w", err)
		}
	}

	return nil
}

// DeleteChunk removes a chunk from the Rust dataplane via the chunk_delete
// JSON-RPC method.
func (s *ChunkServer) DeleteChunk(ctx context.Context, req *pb.DeleteChunkRequest) (*pb.DeleteChunkResponse, error) {
	chunkID := req.GetChunkId()
	if chunkID == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	logging.L.Debug("chunk_server: deleting chunk via SPDK dataplane",
		zap.String("chunkID", chunkID),
	)

	if err := s.spdkClient.ChunkDelete(s.bdevName, chunkID); err != nil {
		logging.L.Error("chunk_server: chunk_delete failed",
			zap.String("chunkID", chunkID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "deleting chunk from dataplane: %v", err)
	}

	return &pb.DeleteChunkResponse{}, nil
}

// HasChunk checks whether a chunk exists in the Rust dataplane via the
// chunk_exists JSON-RPC method.
func (s *ChunkServer) HasChunk(ctx context.Context, req *pb.HasChunkRequest) (*pb.HasChunkResponse, error) {
	chunkID := req.GetChunkId()
	if chunkID == "" {
		return nil, status.Error(codes.InvalidArgument, "chunk_id is required")
	}

	exists, err := s.spdkClient.ChunkExists(s.bdevName, chunkID)
	if err != nil {
		logging.L.Error("chunk_server: chunk_exists failed",
			zap.String("chunkID", chunkID),
			zap.Error(err),
		)
		return nil, status.Errorf(codes.Internal, "checking chunk existence: %v", err)
	}

	return &pb.HasChunkResponse{Exists: exists}, nil
}
