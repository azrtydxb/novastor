package agent_test

import (
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"

	pb "github.com/piwi3910/novastor/api/proto/chunk"
	"github.com/piwi3910/novastor/internal/agent"
	"github.com/piwi3910/novastor/internal/chunk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testEnv bundles a running gRPC server, a connected client, and a cleanup
// function. Every test should call cleanup when done.
type testEnv struct {
	client  pb.ChunkServiceClient
	conn    *grpc.ClientConn
	server  *grpc.Server
	cleanup func()
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	dir := t.TempDir()
	store, err := chunk.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("creating local store: %v", err)
	}

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	srv := grpc.NewServer()
	chunkSrv := agent.NewChunkServer(store)
	chunkSrv.Register(srv)

	go func() {
		if serveErr := srv.Serve(lis); serveErr != nil {
			// Server stopped — expected during cleanup.
			return
		}
	}()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("dialing server: %v", err)
	}

	client := pb.NewChunkServiceClient(conn)

	return &testEnv{
		client: client,
		conn:   conn,
		server: srv,
		cleanup: func() {
			_ = conn.Close()
			srv.GracefulStop()
		},
	}
}

// makeChunk creates a deterministic chunk from the given data.
func makeChunk(data []byte) *chunk.Chunk {
	c := &chunk.Chunk{
		ID:   chunk.NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()
	return c
}

func TestPutChunk(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	data := []byte("hello, novastor chunk service")
	c := makeChunk(data)

	ctx := context.Background()
	stream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("opening PutChunk stream: %v", err)
	}

	// Send a single message with all data.
	err = stream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	})
	if err != nil {
		t.Fatalf("sending PutChunkRequest: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	if resp.GetChunkId() != string(c.ID) {
		t.Errorf("chunk_id = %q, want %q", resp.GetChunkId(), c.ID)
	}
	if resp.GetBytesWritten() != int64(len(data)) {
		t.Errorf("bytes_written = %d, want %d", resp.GetBytesWritten(), len(data))
	}
}

func TestPutChunkStreaming(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	// Create a 2 MiB chunk so we can split it into two stream messages.
	data := make([]byte, 2*1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("generating random data: %v", err)
	}
	c := makeChunk(data)

	ctx := context.Background()
	stream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("opening PutChunk stream: %v", err)
	}

	half := len(data) / 2

	// First message: metadata + first half.
	if err := stream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     data[:half],
		Checksum: c.Checksum,
	}); err != nil {
		t.Fatalf("sending first fragment: %v", err)
	}

	// Second message: second half.
	if err := stream.Send(&pb.PutChunkRequest{
		Data: data[half:],
	}); err != nil {
		t.Fatalf("sending second fragment: %v", err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	if resp.GetBytesWritten() != int64(len(data)) {
		t.Errorf("bytes_written = %d, want %d", resp.GetBytesWritten(), len(data))
	}
}

func TestGetChunk(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	data := []byte("get-chunk-test-data")
	c := makeChunk(data)

	ctx := context.Background()

	// Store chunk first via PutChunk.
	putStream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := putStream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	}); err != nil {
		t.Fatalf("sending PutChunkRequest: %v", err)
	}
	if _, err := putStream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	// Retrieve it via GetChunk.
	getStream, err := env.client.GetChunk(ctx, &pb.GetChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	var received []byte
	var chunkID string
	var checksum uint32
	first := true
	for {
		resp, recvErr := getStream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			t.Fatalf("Recv: %v", recvErr)
		}
		if first {
			chunkID = resp.GetChunkId()
			checksum = resp.GetChecksum()
			first = false
		}
		received = append(received, resp.GetData()...)
	}

	if chunkID != string(c.ID) {
		t.Errorf("chunk_id = %q, want %q", chunkID, c.ID)
	}
	if checksum != c.Checksum {
		t.Errorf("checksum = %d, want %d", checksum, c.Checksum)
	}
	if string(received) != string(data) {
		t.Errorf("data mismatch: got %d bytes, want %d bytes", len(received), len(data))
	}
}

func TestGetChunkNotFound(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()
	stream, err := env.client.GetChunk(ctx, &pb.GetChunkRequest{ChunkId: "nonexistent"})
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error for nonexistent chunk, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}

func TestDeleteChunk(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	data := []byte("delete-me")
	c := makeChunk(data)

	ctx := context.Background()

	// Put it first.
	putStream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := putStream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if _, err := putStream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	// Verify it exists.
	hasResp, err := env.client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if !hasResp.GetExists() {
		t.Fatal("expected chunk to exist before deletion")
	}

	// Delete it.
	_, err = env.client.DeleteChunk(ctx, &pb.DeleteChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}

	// Verify it no longer exists.
	hasResp, err = env.client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("HasChunk after delete: %v", err)
	}
	if hasResp.GetExists() {
		t.Error("expected chunk to not exist after deletion")
	}
}

func TestHasChunk(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	ctx := context.Background()

	// Non-existent chunk should report false.
	resp, err := env.client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: "does-not-exist"})
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if resp.GetExists() {
		t.Error("expected Exists=false for missing chunk")
	}

	// Put a chunk, then check again.
	data := []byte("has-chunk-test")
	c := makeChunk(data)

	putStream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}
	if err := putStream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum,
	}); err != nil {
		t.Fatalf("sending: %v", err)
	}
	if _, err := putStream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	resp, err = env.client.HasChunk(ctx, &pb.HasChunkRequest{ChunkId: string(c.ID)})
	if err != nil {
		t.Fatalf("HasChunk: %v", err)
	}
	if !resp.GetExists() {
		t.Error("expected Exists=true after putting chunk")
	}
}

func TestPutChunkBadChecksum(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()

	data := []byte("bad checksum test")
	c := makeChunk(data)

	ctx := context.Background()
	stream, err := env.client.PutChunk(ctx)
	if err != nil {
		t.Fatalf("PutChunk: %v", err)
	}

	// Send with wrong checksum.
	if err := stream.Send(&pb.PutChunkRequest{
		ChunkId:  string(c.ID),
		Data:     c.Data,
		Checksum: c.Checksum + 1, // corrupt
	}); err != nil {
		t.Fatalf("sending: %v", err)
	}

	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("expected error for bad checksum, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.DataLoss {
		t.Errorf("code = %v, want DataLoss", st.Code())
	}
}
