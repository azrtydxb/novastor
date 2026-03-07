package metadata

import (
	"time"

	pb "github.com/azrtydxb/novastor/api/proto/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

// LockLeaseToProto converts a Go LockLease to its protobuf representation.
func LockLeaseToProto(l *LockLease) *pb.LockLease {
	if l == nil {
		return nil
	}
	return &pb.LockLease{
		LeaseId:   l.LeaseID,
		Owner:     l.Owner,
		VolumeId:  l.VolumeID,
		Ino:       l.Ino,
		Start:     l.Start,
		End:       l.End,
		Type:      lockTypeToProto(l.Type),
		ExpiresAt: l.ExpiresAt,
		FilerId:   l.FilerID,
	}
}

// LockLeaseFromProto converts a protobuf LockLease to its Go representation.
func LockLeaseFromProto(l *pb.LockLease) *LockLease {
	if l == nil {
		return nil
	}
	return &LockLease{
		LeaseID:   l.LeaseId,
		Owner:     l.Owner,
		VolumeID:  l.VolumeId,
		Ino:       l.Ino,
		Start:     l.Start,
		End:       l.End,
		Type:      lockTypeFromProto(l.Type),
		ExpiresAt: l.ExpiresAt,
		FilerID:   l.FilerId,
	}
}

// AcquireLockArgsFromProto converts a protobuf AcquireLockRequest to Go AcquireLockArgs.
func AcquireLockArgsFromProto(r *pb.AcquireLockRequest) *AcquireLockArgs {
	if r == nil {
		return nil
	}
	return &AcquireLockArgs{
		Owner:    r.Owner,
		VolumeID: r.VolumeId,
		Ino:      r.Ino,
		Start:    r.Start,
		End:      r.End,
		Type:     lockTypeFromProto(r.Type),
		TTL:      durationFromProto(r.Ttl),
		FilerID:  r.FilerId,
	}
}

// AcquireLockResultToProto converts a Go AcquireLockResult to its protobuf response.
func AcquireLockResultToProto(r *AcquireLockResult) *pb.AcquireLockResponse {
	if r == nil {
		return nil
	}
	return &pb.AcquireLockResponse{
		LeaseId:          r.LeaseID,
		ExpiresAt:        r.ExpiresAt,
		ConflictingOwner: r.ConflictingOwner,
	}
}

// RenewLockArgsFromProto converts a protobuf RenewLockRequest to Go RenewLockArgs.
func RenewLockArgsFromProto(r *pb.RenewLockRequest) *RenewLockArgs {
	if r == nil {
		return nil
	}
	return &RenewLockArgs{
		LeaseID: r.LeaseId,
		TTL:     durationFromProto(r.Ttl),
	}
}

// ReleaseLockArgsFromProto converts a protobuf ReleaseLockRequest to Go ReleaseLockArgs.
func ReleaseLockArgsFromProto(r *pb.ReleaseLockRequest) *ReleaseLockArgs {
	if r == nil {
		return nil
	}
	return &ReleaseLockArgs{
		LeaseID: r.LeaseId,
		Owner:   r.Owner,
	}
}

// TestLockArgsFromProto converts a protobuf TestLockRequest to Go TestLockArgs.
func TestLockArgsFromProto(r *pb.TestLockRequest) *TestLockArgs {
	if r == nil {
		return nil
	}
	return &TestLockArgs{
		VolumeID: r.VolumeId,
		Ino:      r.Ino,
		Start:    r.Start,
		End:      r.End,
		Type:     lockTypeFromProto(r.Type),
	}
}

// DurationToProto converts a Go time.Duration to a protobuf Duration.
func DurationToProto(d time.Duration) *durationpb.Duration {
	return durationpb.New(d)
}

func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func lockTypeToProto(t LockType) pb.LockType {
	switch t {
	case LockRead:
		return pb.LockType_LOCK_TYPE_READ
	case LockWrite:
		return pb.LockType_LOCK_TYPE_WRITE
	default:
		return pb.LockType_LOCK_TYPE_UNSPECIFIED
	}
}

func lockTypeFromProto(t pb.LockType) LockType {
	switch t {
	case pb.LockType_LOCK_TYPE_READ:
		return LockRead
	case pb.LockType_LOCK_TYPE_WRITE:
		return LockWrite
	default:
		return LockWrite // Default to exclusive lock for safety when type is unspecified.
	}
}
