package metadata

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
	"google.golang.org/grpc"
)

// GRPCServer wraps a RaftStore and exposes it over gRPC using the
// generic MetadataService Execute RPC. Each operation name maps to
// a RaftStore method; arguments and results are JSON-encoded in the
// request/response payloads.
type GRPCServer struct {
	pb.UnimplementedMetadataServiceServer
	store *RaftStore
}

// NewGRPCServer creates a new GRPCServer backed by the given RaftStore.
func NewGRPCServer(store *RaftStore) *GRPCServer {
	return &GRPCServer{store: store}
}

// Register adds the MetadataService to a gRPC server.
func (s *GRPCServer) Register(srv *grpc.Server) {
	pb.RegisterMetadataServiceServer(srv, s)
}

// Execute dispatches a metadata operation to the underlying RaftStore.
func (s *GRPCServer) Execute(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	switch req.Operation {
	// ---- Volume operations ----
	case "PutVolumeMeta":
		return s.putVolumeMeta(ctx, req.Payload)
	case "GetVolumeMeta":
		return s.getVolumeMeta(ctx, req.Payload)
	case "DeleteVolumeMeta":
		return s.deleteVolumeMeta(ctx, req.Payload)

	// ---- Placement operations ----
	case "PutPlacementMap":
		return s.putPlacementMap(ctx, req.Payload)
	case "GetPlacementMap":
		return s.getPlacementMap(ctx, req.Payload)

	// ---- Object operations ----
	case "PutObjectMeta":
		return s.putObjectMeta(ctx, req.Payload)
	case "GetObjectMeta":
		return s.getObjectMeta(ctx, req.Payload)
	case "DeleteObjectMeta":
		return s.deleteObjectMeta(ctx, req.Payload)
	case "ListObjectMetas":
		return s.listObjectMetas(ctx, req.Payload)

	// ---- Bucket operations ----
	case "PutBucketMeta":
		return s.putBucketMeta(ctx, req.Payload)
	case "GetBucketMeta":
		return s.getBucketMeta(ctx, req.Payload)
	case "DeleteBucketMeta":
		return s.deleteBucketMeta(ctx, req.Payload)
	case "ListBucketMetas":
		return s.listBucketMetas(ctx)

	// ---- Multipart operations ----
	case "PutMultipartUpload":
		return s.putMultipartUpload(ctx, req.Payload)
	case "GetMultipartUpload":
		return s.getMultipartUpload(ctx, req.Payload)
	case "DeleteMultipartUpload":
		return s.deleteMultipartUpload(ctx, req.Payload)

	// ---- Inode operations ----
	case "CreateInode":
		return s.createInode(ctx, req.Payload)
	case "GetInode":
		return s.getInode(ctx, req.Payload)
	case "UpdateInode":
		return s.updateInode(ctx, req.Payload)
	case "DeleteInode":
		return s.deleteInode(ctx, req.Payload)

	// ---- Directory entry operations ----
	case "CreateDirEntry":
		return s.createDirEntry(ctx, req.Payload)
	case "DeleteDirEntry":
		return s.deleteDirEntry(ctx, req.Payload)
	case "LookupDirEntry":
		return s.lookupDirEntry(ctx, req.Payload)
	case "ListDirectory":
		return s.listDirectory(ctx, req.Payload)

	default:
		return &pb.MetadataResponse{Error: fmt.Sprintf("unknown operation: %s", req.Operation)}, nil
	}
}

// errResp builds a MetadataResponse carrying only an error string.
func errResp(err error) *pb.MetadataResponse {
	return &pb.MetadataResponse{Error: err.Error()}
}

// okResp builds a MetadataResponse carrying a JSON payload.
func okResp(v any) (*pb.MetadataResponse, error) {
	if v == nil {
		return &pb.MetadataResponse{}, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshaling response: %w", err)
	}
	return &pb.MetadataResponse{Payload: data}, nil
}

// ---- Volume operations ----

func (s *GRPCServer) putVolumeMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var meta VolumeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return errResp(fmt.Errorf("unmarshal VolumeMeta: %w", err)), nil
	}
	if err := s.store.PutVolumeMeta(ctx, &meta); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getVolumeMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		VolumeID string `json:"volumeID"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	meta, err := s.store.GetVolumeMeta(ctx, args.VolumeID)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(meta)
}

func (s *GRPCServer) deleteVolumeMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		VolumeID string `json:"volumeID"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	if err := s.store.DeleteVolumeMeta(ctx, args.VolumeID); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

// ---- Placement operations ----

func (s *GRPCServer) putPlacementMap(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var pm PlacementMap
	if err := json.Unmarshal(payload, &pm); err != nil {
		return errResp(fmt.Errorf("unmarshal PlacementMap: %w", err)), nil
	}
	if err := s.store.PutPlacementMap(ctx, &pm); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getPlacementMap(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		ChunkID string `json:"chunkID"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	pm, err := s.store.GetPlacementMap(ctx, args.ChunkID)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(pm)
}

// ---- Object operations ----

func (s *GRPCServer) putObjectMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var meta ObjectMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return errResp(fmt.Errorf("unmarshal ObjectMeta: %w", err)), nil
	}
	if err := s.store.PutObjectMeta(ctx, &meta); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getObjectMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	meta, err := s.store.GetObjectMeta(ctx, args.Bucket, args.Key)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(meta)
}

func (s *GRPCServer) deleteObjectMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	if err := s.store.DeleteObjectMeta(ctx, args.Bucket, args.Key); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) listObjectMetas(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Bucket string `json:"bucket"`
		Prefix string `json:"prefix"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	metas, err := s.store.ListObjectMetas(ctx, args.Bucket, args.Prefix)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(metas)
}

// ---- Bucket operations ----

func (s *GRPCServer) putBucketMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var meta BucketMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return errResp(fmt.Errorf("unmarshal BucketMeta: %w", err)), nil
	}
	if err := s.store.PutBucketMeta(ctx, &meta); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getBucketMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	meta, err := s.store.GetBucketMeta(ctx, args.Name)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(meta)
}

func (s *GRPCServer) deleteBucketMeta(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	if err := s.store.DeleteBucketMeta(ctx, args.Name); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) listBucketMetas(ctx context.Context) (*pb.MetadataResponse, error) {
	metas, err := s.store.ListBucketMetas(ctx)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(metas)
}

// ---- Multipart operations ----

func (s *GRPCServer) putMultipartUpload(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var mu MultipartUpload
	if err := json.Unmarshal(payload, &mu); err != nil {
		return errResp(fmt.Errorf("unmarshal MultipartUpload: %w", err)), nil
	}
	if err := s.store.PutMultipartUpload(ctx, &mu); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getMultipartUpload(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		UploadID string `json:"uploadID"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	mu, err := s.store.GetMultipartUpload(ctx, args.UploadID)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(mu)
}

func (s *GRPCServer) deleteMultipartUpload(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		UploadID string `json:"uploadID"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	if err := s.store.DeleteMultipartUpload(ctx, args.UploadID); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

// ---- Inode operations ----

func (s *GRPCServer) createInode(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var meta InodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return errResp(fmt.Errorf("unmarshal InodeMeta: %w", err)), nil
	}
	if err := s.store.CreateInode(ctx, &meta); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) getInode(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Ino uint64 `json:"ino"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	meta, err := s.store.GetInode(ctx, args.Ino)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(meta)
}

func (s *GRPCServer) updateInode(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var meta InodeMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return errResp(fmt.Errorf("unmarshal InodeMeta: %w", err)), nil
	}
	if err := s.store.UpdateInode(ctx, &meta); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) deleteInode(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args struct {
		Ino uint64 `json:"ino"`
	}
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal args: %w", err)), nil
	}
	if err := s.store.DeleteInode(ctx, args.Ino); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

// ---- Directory entry operations ----

// dirEntryArgs is the wire format for directory entry operations that
// carry a parent inode number and a DirEntry.
type dirEntryArgs struct {
	ParentIno uint64   `json:"parentIno"`
	Entry     DirEntry `json:"entry"`
}

// dirLookupArgs is the wire format for LookupDirEntry / DeleteDirEntry.
type dirLookupArgs struct {
	ParentIno uint64 `json:"parentIno"`
	Name      string `json:"name"`
}

// dirListArgs is the wire format for ListDirectory.
type dirListArgs struct {
	ParentIno uint64 `json:"parentIno"`
}

func (s *GRPCServer) createDirEntry(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args dirEntryArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal dirEntryArgs: %w", err)), nil
	}
	if err := s.store.CreateDirEntry(ctx, args.ParentIno, &args.Entry); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) deleteDirEntry(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args dirLookupArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal dirLookupArgs: %w", err)), nil
	}
	if err := s.store.DeleteDirEntry(ctx, args.ParentIno, args.Name); err != nil {
		return errResp(err), nil
	}
	return &pb.MetadataResponse{}, nil
}

func (s *GRPCServer) lookupDirEntry(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args dirLookupArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal dirLookupArgs: %w", err)), nil
	}
	entry, err := s.store.LookupDirEntry(ctx, args.ParentIno, args.Name)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(entry)
}

func (s *GRPCServer) listDirectory(ctx context.Context, payload []byte) (*pb.MetadataResponse, error) {
	var args dirListArgs
	if err := json.Unmarshal(payload, &args); err != nil {
		return errResp(fmt.Errorf("unmarshal dirListArgs: %w", err)), nil
	}
	entries, err := s.store.ListDirectory(ctx, args.ParentIno)
	if err != nil {
		return errResp(err), nil
	}
	return okResp(entries)
}
