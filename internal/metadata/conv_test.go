package metadata

import (
	"testing"
	"time"

	pb "github.com/piwi3910/novastor/api/proto/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestVolumeMetaRoundTrip(t *testing.T) {
	original := &VolumeMeta{
		VolumeID:  "vol-1",
		Pool:      "ssd-pool",
		SizeBytes: 10 * 1024 * 1024 * 1024,
		ChunkIDs:  []string{"c1", "c2", "c3"},
		DataProtection: &DataProtectionConfig{
			Mode: ProtectionModeReplication,
			Replication: &ReplicationProfile{
				Factor:      3,
				WriteQuorum: 2,
			},
		},
		TargetNodeID:  "node-1",
		TargetAddress: "10.0.0.1",
		TargetPort:    "4420",
		SubsystemNQN:  "nqn.2024-01.io.novastor:vol-1",
		ProtectionProfile: &ProtectionProfile{
			Mode: ProtectionModeErasureCoding,
			ErasureCoding: &ErasureCodingProfile{
				DataShards:   4,
				ParityShards: 2,
			},
		},
		ComplianceInfo: &ComplianceInfo{
			State:             ComplianceStateCompliant,
			LastCheckTime:     1700000000000000000,
			AvailableReplicas: 3,
			RequiredReplicas:  2,
			Reason:            "All replicas available",
		},
	}

	proto := VolumeMetaToProto(original)
	result := VolumeMetaFromProto(proto)

	if result.VolumeID != original.VolumeID {
		t.Errorf("VolumeID: got %q, want %q", result.VolumeID, original.VolumeID)
	}
	if result.Pool != original.Pool {
		t.Errorf("Pool: got %q, want %q", result.Pool, original.Pool)
	}
	if result.SizeBytes != original.SizeBytes {
		t.Errorf("SizeBytes: got %d, want %d", result.SizeBytes, original.SizeBytes)
	}
	if len(result.ChunkIDs) != len(original.ChunkIDs) {
		t.Errorf("ChunkIDs len: got %d, want %d", len(result.ChunkIDs), len(original.ChunkIDs))
	}
	if result.TargetNodeID != original.TargetNodeID {
		t.Errorf("TargetNodeID: got %q, want %q", result.TargetNodeID, original.TargetNodeID)
	}
	if result.SubsystemNQN != original.SubsystemNQN {
		t.Errorf("SubsystemNQN: got %q, want %q", result.SubsystemNQN, original.SubsystemNQN)
	}
	if result.DataProtection == nil {
		t.Fatal("DataProtection is nil")
	}
	if result.DataProtection.Mode != ProtectionModeReplication {
		t.Errorf("DataProtection.Mode: got %q, want %q", result.DataProtection.Mode, ProtectionModeReplication)
	}
	if result.DataProtection.Replication.Factor != 3 {
		t.Errorf("DataProtection.Replication.Factor: got %d, want 3", result.DataProtection.Replication.Factor)
	}
	if result.ProtectionProfile == nil {
		t.Fatal("ProtectionProfile is nil")
	}
	if result.ProtectionProfile.Mode != ProtectionModeErasureCoding {
		t.Errorf("ProtectionProfile.Mode: got %q, want %q", result.ProtectionProfile.Mode, ProtectionModeErasureCoding)
	}
	if result.ProtectionProfile.ErasureCoding.DataShards != 4 {
		t.Errorf("EC DataShards: got %d, want 4", result.ProtectionProfile.ErasureCoding.DataShards)
	}
	if result.ComplianceInfo == nil {
		t.Fatal("ComplianceInfo is nil")
	}
	if result.ComplianceInfo.State != ComplianceStateCompliant {
		t.Errorf("ComplianceInfo.State: got %q, want %q", result.ComplianceInfo.State, ComplianceStateCompliant)
	}
}

func TestVolumeMetaNilRoundTrip(t *testing.T) {
	if VolumeMetaToProto(nil) != nil {
		t.Error("expected nil for nil input")
	}
	if VolumeMetaFromProto(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestPlacementMapRoundTrip(t *testing.T) {
	original := &PlacementMap{
		ChunkID: "chunk-abc",
		Nodes:   []string{"node-1", "node-2", "node-3"},
	}

	proto := PlacementMapToProto(original)
	result := PlacementMapFromProto(proto)

	if result.ChunkID != original.ChunkID {
		t.Errorf("ChunkID: got %q, want %q", result.ChunkID, original.ChunkID)
	}
	if len(result.Nodes) != len(original.Nodes) {
		t.Errorf("Nodes len: got %d, want %d", len(result.Nodes), len(original.Nodes))
	}
}

func TestObjectMetaRoundTrip(t *testing.T) {
	original := &ObjectMeta{
		Bucket:      "photos",
		Key:         "vacation/beach.jpg",
		Size:        54321,
		ETag:        "abc123",
		ContentType: "image/jpeg",
		UserMeta:    map[string]string{"author": "pascal"},
		ChunkIDs:    []string{"ch1", "ch2"},
		VersionID:   "v1",
		ModTime:     1700000000000000000,
	}

	proto := ObjectMetaToProto(original)
	result := ObjectMetaFromProto(proto)

	if result.Bucket != original.Bucket {
		t.Errorf("Bucket: got %q, want %q", result.Bucket, original.Bucket)
	}
	if result.Key != original.Key {
		t.Errorf("Key: got %q, want %q", result.Key, original.Key)
	}
	if result.Size != original.Size {
		t.Errorf("Size: got %d, want %d", result.Size, original.Size)
	}
	if result.ETag != original.ETag {
		t.Errorf("ETag: got %q, want %q", result.ETag, original.ETag)
	}
	if result.ContentType != original.ContentType {
		t.Errorf("ContentType: got %q, want %q", result.ContentType, original.ContentType)
	}
	if result.UserMeta["author"] != "pascal" {
		t.Errorf("UserMeta[author]: got %q, want %q", result.UserMeta["author"], "pascal")
	}
	if result.VersionID != original.VersionID {
		t.Errorf("VersionID: got %q, want %q", result.VersionID, original.VersionID)
	}
}

func TestBucketMetaRoundTrip(t *testing.T) {
	original := &BucketMeta{
		Name:         "my-bucket",
		CreationDate: 1700000000000000000,
		Versioning:   "enabled",
		Owner:        "admin",
		MaxSize:      1024 * 1024 * 1024,
	}

	proto := BucketMetaToProto(original)
	result := BucketMetaFromProto(proto)

	if result.Name != original.Name {
		t.Errorf("Name: got %q, want %q", result.Name, original.Name)
	}
	if result.CreationDate != original.CreationDate {
		t.Errorf("CreationDate: got %d, want %d", result.CreationDate, original.CreationDate)
	}
	if result.Versioning != original.Versioning {
		t.Errorf("Versioning: got %q, want %q", result.Versioning, original.Versioning)
	}
	if result.Owner != original.Owner {
		t.Errorf("Owner: got %q, want %q", result.Owner, original.Owner)
	}
	if result.MaxSize != original.MaxSize {
		t.Errorf("MaxSize: got %d, want %d", result.MaxSize, original.MaxSize)
	}
}

func TestMultipartUploadRoundTrip(t *testing.T) {
	original := &MultipartUpload{
		UploadID:     "upload-1",
		Bucket:       "my-bucket",
		Key:          "large-file.bin",
		CreationDate: 1700000000000000000,
		Parts: []MultipartPart{
			{PartNumber: 1, Size: 5 * 1024 * 1024, ETag: "part1", ChunkIDs: []string{"c1"}},
			{PartNumber: 2, Size: 3 * 1024 * 1024, ETag: "part2", ChunkIDs: []string{"c2", "c3"}},
		},
	}

	proto := MultipartUploadToProto(original)
	result := MultipartUploadFromProto(proto)

	if result.UploadID != original.UploadID {
		t.Errorf("UploadID: got %q, want %q", result.UploadID, original.UploadID)
	}
	if len(result.Parts) != 2 {
		t.Fatalf("Parts len: got %d, want 2", len(result.Parts))
	}
	if result.Parts[0].PartNumber != 1 {
		t.Errorf("Parts[0].PartNumber: got %d, want 1", result.Parts[0].PartNumber)
	}
	if result.Parts[1].ETag != "part2" {
		t.Errorf("Parts[1].ETag: got %q, want %q", result.Parts[1].ETag, "part2")
	}
}

func TestInodeMetaRoundTrip(t *testing.T) {
	original := &InodeMeta{
		Ino:       1000,
		Type:      InodeTypeFile,
		Size:      8192,
		Mode:      0644,
		UID:       1000,
		GID:       1000,
		LinkCount: 1,
		ChunkIDs:  []string{"ca", "cb"},
		Target:    "",
		Xattrs:    map[string]string{"user.tag": "test"},
		ATime:     1700000000000000000,
		MTime:     1700000000000000000,
		CTime:     1700000000000000000,
	}

	proto := InodeMetaToProto(original)
	result := InodeMetaFromProto(proto)

	if result.Ino != original.Ino {
		t.Errorf("Ino: got %d, want %d", result.Ino, original.Ino)
	}
	if result.Type != InodeTypeFile {
		t.Errorf("Type: got %q, want %q", result.Type, InodeTypeFile)
	}
	if result.Size != original.Size {
		t.Errorf("Size: got %d, want %d", result.Size, original.Size)
	}
	if result.Mode != original.Mode {
		t.Errorf("Mode: got %o, want %o", result.Mode, original.Mode)
	}
	if result.UID != original.UID {
		t.Errorf("UID: got %d, want %d", result.UID, original.UID)
	}
	if len(result.ChunkIDs) != 2 {
		t.Errorf("ChunkIDs len: got %d, want 2", len(result.ChunkIDs))
	}
	if result.Xattrs["user.tag"] != "test" {
		t.Errorf("Xattrs[user.tag]: got %q, want %q", result.Xattrs["user.tag"], "test")
	}
}

func TestInodeMetaSymlinkRoundTrip(t *testing.T) {
	original := &InodeMeta{
		Ino:    2000,
		Type:   InodeTypeSymlink,
		Target: "/etc/hosts",
	}

	proto := InodeMetaToProto(original)
	result := InodeMetaFromProto(proto)

	if result.Type != InodeTypeSymlink {
		t.Errorf("Type: got %q, want %q", result.Type, InodeTypeSymlink)
	}
	if result.Target != "/etc/hosts" {
		t.Errorf("Target: got %q, want %q", result.Target, "/etc/hosts")
	}
}

func TestDirEntryRoundTrip(t *testing.T) {
	original := &DirEntry{
		Name: "file.txt",
		Ino:  42,
		Type: InodeTypeFile,
	}

	proto := DirEntryToProto(original)
	result := DirEntryFromProto(proto)

	if result.Name != original.Name {
		t.Errorf("Name: got %q, want %q", result.Name, original.Name)
	}
	if result.Ino != original.Ino {
		t.Errorf("Ino: got %d, want %d", result.Ino, original.Ino)
	}
	if result.Type != InodeTypeFile {
		t.Errorf("Type: got %q, want %q", result.Type, InodeTypeFile)
	}
}

func TestNodeMetaRoundTrip(t *testing.T) {
	original := &NodeMeta{
		NodeID:            "node-1",
		Address:           "10.0.0.1:9100",
		DiskCount:         4,
		TotalCapacity:     1099511627776,
		AvailableCapacity: 549755813888,
		Zone:              "us-east-1a",
		Rack:              "rack-A",
		LastHeartbeat:     1700000000,
		Status:            "ready",
	}

	proto := NodeMetaToProto(original)
	result := NodeMetaFromProto(proto)

	if result.NodeID != original.NodeID {
		t.Errorf("NodeID: got %q, want %q", result.NodeID, original.NodeID)
	}
	if result.Address != original.Address {
		t.Errorf("Address: got %q, want %q", result.Address, original.Address)
	}
	if result.DiskCount != original.DiskCount {
		t.Errorf("DiskCount: got %d, want %d", result.DiskCount, original.DiskCount)
	}
	if result.TotalCapacity != original.TotalCapacity {
		t.Errorf("TotalCapacity: got %d, want %d", result.TotalCapacity, original.TotalCapacity)
	}
	if result.Zone != original.Zone {
		t.Errorf("Zone: got %q, want %q", result.Zone, original.Zone)
	}
}

func TestSnapshotMetaRoundTrip(t *testing.T) {
	original := &SnapshotMeta{
		SnapshotID:     "snap-1",
		SourceVolumeID: "vol-1",
		SizeBytes:      1073741824,
		ChunkIDs:       []string{"c1", "c2"},
		CreationTime:   1700000000000000000,
		ReadyToUse:     true,
	}

	proto := SnapshotMetaToProto(original)
	result := SnapshotMetaFromProto(proto)

	if result.SnapshotID != original.SnapshotID {
		t.Errorf("SnapshotID: got %q, want %q", result.SnapshotID, original.SnapshotID)
	}
	if result.SourceVolumeID != original.SourceVolumeID {
		t.Errorf("SourceVolumeID: got %q, want %q", result.SourceVolumeID, original.SourceVolumeID)
	}
	if result.SizeBytes != original.SizeBytes {
		t.Errorf("SizeBytes: got %d, want %d", result.SizeBytes, original.SizeBytes)
	}
	if !result.ReadyToUse {
		t.Error("ReadyToUse: got false, want true")
	}
}

func TestLockLeaseRoundTrip(t *testing.T) {
	original := &LockLease{
		LeaseID:   "lease-1",
		Owner:     "client:host1:1234",
		VolumeID:  "vol-1",
		Ino:       100,
		Start:     0,
		End:       -1,
		Type:      LockWrite,
		ExpiresAt: 1700000000000000000,
		FilerID:   "filer-1",
	}

	proto := LockLeaseToProto(original)
	result := LockLeaseFromProto(proto)

	if result.LeaseID != original.LeaseID {
		t.Errorf("LeaseID: got %q, want %q", result.LeaseID, original.LeaseID)
	}
	if result.Owner != original.Owner {
		t.Errorf("Owner: got %q, want %q", result.Owner, original.Owner)
	}
	if result.Type != LockWrite {
		t.Errorf("Type: got %d, want %d", result.Type, LockWrite)
	}
	if result.End != -1 {
		t.Errorf("End: got %d, want -1", result.End)
	}
	if result.FilerID != "filer-1" {
		t.Errorf("FilerID: got %q, want %q", result.FilerID, "filer-1")
	}
}

func TestAcquireLockArgsRoundTrip(t *testing.T) {
	ttl := 30 * time.Second
	req := &pb.AcquireLockRequest{
		Owner:    "owner1",
		VolumeId: "vol-1",
		Ino:      100,
		Start:    0,
		End:      -1,
		Type:     pb.LockType_LOCK_TYPE_WRITE,
		Ttl:      durationpb.New(ttl),
		FilerId:  "filer-1",
	}
	args := AcquireLockArgsFromProto(req)
	if args.Owner != "owner1" {
		t.Errorf("Owner: got %q, want %q", args.Owner, "owner1")
	}
	if args.TTL != ttl {
		t.Errorf("TTL: got %v, want %v", args.TTL, ttl)
	}
	if args.Type != LockWrite {
		t.Errorf("Type: got %d, want %d", args.Type, LockWrite)
	}
	if args.End != -1 {
		t.Errorf("End: got %d, want -1", args.End)
	}
	if args.FilerID != "filer-1" {
		t.Errorf("FilerID: got %q, want %q", args.FilerID, "filer-1")
	}
}

func TestVolumeOwnershipRoundTrip(t *testing.T) {
	original := &VolumeOwnership{
		VolumeID:   "vol-1",
		OwnerAddr:  "10.0.0.1:9100",
		OwnerSince: 1700000000,
		Generation: 5,
	}

	proto := VolumeOwnershipToProto(original)
	result := VolumeOwnershipFromProto(proto)

	if result.VolumeID != original.VolumeID {
		t.Errorf("VolumeID: got %q, want %q", result.VolumeID, original.VolumeID)
	}
	if result.OwnerAddr != original.OwnerAddr {
		t.Errorf("OwnerAddr: got %q, want %q", result.OwnerAddr, original.OwnerAddr)
	}
	if result.Generation != original.Generation {
		t.Errorf("Generation: got %d, want %d", result.Generation, original.Generation)
	}
}

func TestProtectionModeEnumRoundTrip(t *testing.T) {
	tests := []ProtectionMode{
		ProtectionModeReplication,
		ProtectionModeErasureCoding,
		"",
	}
	for _, mode := range tests {
		got := protectionModeFromProto(protectionModeToProto(mode))
		if got != mode {
			t.Errorf("ProtectionMode round-trip: got %q, want %q", got, mode)
		}
	}
}

func TestComplianceStateEnumRoundTrip(t *testing.T) {
	tests := []ComplianceState{
		ComplianceStateUnknown,
		ComplianceStateCompliant,
		ComplianceStateDegraded,
		ComplianceStateNonCompliant,
		"",
	}
	for _, state := range tests {
		got := complianceStateFromProto(complianceStateToProto(state))
		if got != state {
			t.Errorf("ComplianceState round-trip: got %q, want %q", got, state)
		}
	}
}

func TestInodeTypeEnumRoundTrip(t *testing.T) {
	tests := []InodeType{
		InodeTypeFile,
		InodeTypeDir,
		InodeTypeSymlink,
		"",
	}
	for _, typ := range tests {
		got := inodeTypeFromProto(inodeTypeToProto(typ))
		if got != typ {
			t.Errorf("InodeType round-trip: got %q, want %q", got, typ)
		}
	}
}

func TestLockTypeEnumRoundTrip(t *testing.T) {
	tests := []struct {
		input LockType
		want  LockType
	}{
		{LockRead, LockRead},
		{LockWrite, LockWrite},
	}
	for _, tc := range tests {
		got := lockTypeFromProto(lockTypeToProto(tc.input))
		if got != tc.want {
			t.Errorf("LockType round-trip: got %d, want %d", got, tc.want)
		}
	}
}

func TestRenewLockArgsFromProto(t *testing.T) {
	ttl := 60 * time.Second
	req := &pb.RenewLockRequest{
		LeaseId: "lease-1",
		Ttl:     durationpb.New(ttl),
	}
	args := RenewLockArgsFromProto(req)
	if args.LeaseID != "lease-1" {
		t.Errorf("LeaseID: got %q, want %q", args.LeaseID, "lease-1")
	}
	if args.TTL != ttl {
		t.Errorf("TTL: got %v, want %v", args.TTL, ttl)
	}
}

func TestReleaseLockArgsFromProto(t *testing.T) {
	req := &pb.ReleaseLockRequest{
		LeaseId: "lease-1",
		Owner:   "owner1",
	}
	args := ReleaseLockArgsFromProto(req)
	if args.LeaseID != "lease-1" {
		t.Errorf("LeaseID: got %q, want %q", args.LeaseID, "lease-1")
	}
	if args.Owner != "owner1" {
		t.Errorf("Owner: got %q, want %q", args.Owner, "owner1")
	}
}

func TestTestLockArgsFromProto(t *testing.T) {
	req := &pb.TestLockRequest{
		VolumeId: "vol-1",
		Ino:      100,
		Start:    0,
		End:      -1,
		Type:     pb.LockType_LOCK_TYPE_READ,
	}
	args := TestLockArgsFromProto(req)
	if args.VolumeID != "vol-1" {
		t.Errorf("VolumeID: got %q, want %q", args.VolumeID, "vol-1")
	}
	if args.Type != LockRead {
		t.Errorf("Type: got %d, want %d", args.Type, LockRead)
	}
}
