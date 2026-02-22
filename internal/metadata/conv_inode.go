package metadata

import (
	pb "github.com/piwi3910/novastor/api/proto/metadata"
)

// InodeMetaToProto converts a Go InodeMeta to its protobuf representation.
func InodeMetaToProto(m *InodeMeta) *pb.InodeMeta {
	if m == nil {
		return nil
	}
	return &pb.InodeMeta{
		Ino:       m.Ino,
		Type:      inodeTypeToProto(m.Type),
		Size:      m.Size,
		Mode:      m.Mode,
		Uid:       m.UID,
		Gid:       m.GID,
		LinkCount: m.LinkCount,
		ChunkIds:  m.ChunkIDs,
		Target:    m.Target,
		Xattrs:    m.Xattrs,
		Atime:     m.ATime,
		Mtime:     m.MTime,
		Ctime:     m.CTime,
	}
}

// InodeMetaFromProto converts a protobuf InodeMeta to its Go representation.
func InodeMetaFromProto(m *pb.InodeMeta) *InodeMeta {
	if m == nil {
		return nil
	}
	return &InodeMeta{
		Ino:       m.Ino,
		Type:      inodeTypeFromProto(m.Type),
		Size:      m.Size,
		Mode:      m.Mode,
		UID:       m.Uid,
		GID:       m.Gid,
		LinkCount: m.LinkCount,
		ChunkIDs:  m.ChunkIds,
		Target:    m.Target,
		Xattrs:    m.Xattrs,
		ATime:     m.Atime,
		MTime:     m.Mtime,
		CTime:     m.Ctime,
	}
}

// DirEntryToProto converts a Go DirEntry to its protobuf representation.
func DirEntryToProto(d *DirEntry) *pb.DirEntry {
	if d == nil {
		return nil
	}
	return &pb.DirEntry{
		Name: d.Name,
		Ino:  d.Ino,
		Type: inodeTypeToProto(d.Type),
	}
}

// DirEntryFromProto converts a protobuf DirEntry to its Go representation.
func DirEntryFromProto(d *pb.DirEntry) *DirEntry {
	if d == nil {
		return nil
	}
	return &DirEntry{
		Name: d.Name,
		Ino:  d.Ino,
		Type: inodeTypeFromProto(d.Type),
	}
}

func inodeTypeToProto(t InodeType) pb.InodeType {
	switch t {
	case InodeTypeFile:
		return pb.InodeType_INODE_TYPE_FILE
	case InodeTypeDir:
		return pb.InodeType_INODE_TYPE_DIR
	case InodeTypeSymlink:
		return pb.InodeType_INODE_TYPE_SYMLINK
	default:
		return pb.InodeType_INODE_TYPE_UNSPECIFIED
	}
}

func inodeTypeFromProto(t pb.InodeType) InodeType {
	switch t {
	case pb.InodeType_INODE_TYPE_FILE:
		return InodeTypeFile
	case pb.InodeType_INODE_TYPE_DIR:
		return InodeTypeDir
	case pb.InodeType_INODE_TYPE_SYMLINK:
		return InodeTypeSymlink
	default:
		return ""
	}
}
