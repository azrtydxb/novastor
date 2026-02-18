package filer

import (
	"context"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/piwi3910/novastor/internal/logging"
)

// FUSEConfig holds configuration for the FUSE filesystem mount.
type FUSEConfig struct {
	// MountPoint is the directory where the filesystem will be mounted.
	MountPoint string
	// AllowOther enables the allow_other mount option (allows other users to access).
	AllowOther bool
	// FsName specifies the filesystem name shown in mount options.
	FsName string
	// Debug enables debug logging from the FUSE library.
	Debug bool
}

// DefaultFUSEConfig returns a configuration with sensible defaults.
func DefaultFUSEConfig(mountPoint string) *FUSEConfig {
	return &FUSEConfig{
		MountPoint: mountPoint,
		AllowOther: false,
		FsName:     "novastor",
		Debug:      false,
	}
}

// FUSEServer manages a FUSE filesystem mount.
type FUSEServer struct {
	fs      *FileSystem
	config  *FUSEConfig
	server  *fuse.Server
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	mounted bool
}

// NewFUSEServer creates a new FUSE server that wraps the given FileSystem.
func NewFUSEServer(filesystem *FileSystem, config *FUSEConfig) *FUSEServer {
	if config == nil {
		config = DefaultFUSEConfig("/mnt/novastor")
	}
	return &FUSEServer{
		fs:     filesystem,
		config: config,
		done:   make(chan struct{}),
	}
}

// Serve mounts and serves the FUSE filesystem. This call blocks until
// the filesystem is unmounted or an error occurs.
func (s *FUSEServer) Serve() error {
	s.mu.Lock()
	if s.mounted {
		s.mu.Unlock()
		return fmt.Errorf("already mounted")
	}
	s.mu.Unlock()

	// Ensure mount point exists.
	if err := os.MkdirAll(s.config.MountPoint, 0755); err != nil {
		return fmt.Errorf("creating mount point: %w", err)
	}

	// Create the root node.
	rootNode := &fuseRootNode{fs: s.fs}

	// Mount options.
	entryTimeout := time.Second
	attrTimeout := time.Second
	options := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: s.config.AllowOther,
			FsName:     s.config.FsName,
			Name:       s.config.FsName,
			Debug:      s.config.Debug,
		},
		EntryTimeout: &entryTimeout,
		AttrTimeout:  &attrTimeout,
	}

	// Mount the filesystem.
	server, err := fs.Mount(s.config.MountPoint, rootNode, options)
	if err != nil {
		return fmt.Errorf("mount failed: %w", err)
	}

	s.mu.Lock()
	s.server = server
	s.mounted = true
	s.mu.Unlock()

	logging.L.Info("FUSE filesystem mounted",
		zap.String("mountpoint", s.config.MountPoint),
		zap.String("fsname", s.config.FsName),
	)

	// Wait for the server to finish.
	server.Wait()

	close(s.done)
	return nil
}

// Stop unmounts the FUSE filesystem.
func (s *FUSEServer) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.mounted {
		return nil
	}

	s.once.Do(func() {
		if s.server != nil {
			_ = s.server.Unmount()
		}
		s.mounted = false
		logging.L.Info("FUSE filesystem unmounted",
			zap.String("mountpoint", s.config.MountPoint),
		)
	})

	return nil
}

// Mounted returns true if the filesystem is currently mounted.
func (s *FUSEServer) Mounted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mounted
}

// fuseRootNode represents the root directory of the FUSE filesystem.
type fuseRootNode struct {
	fs   *FileSystem
	fs.Inode
}

var _ = (fs.InodeEmbedder)((*fuseRootNode)(nil))
var _ = (fs.NodeGetattrer)((*fuseRootNode)(nil))
var _ = (fs.NodeReaddirer)((*fuseRootNode)(nil))
var _ = (fs.NodeLookuper)((*fuseRootNode)(nil))
var _ = (fs.NodeMkdirer)((*fuseRootNode)(nil))
var _ = (fs.NodeUnlinker)((*fuseRootNode)(nil))
var _ = (fs.NodeRmdirer)((*fuseRootNode)(nil))
var _ = (fs.NodeRenamer)((*fuseRootNode)(nil))
var _ = (fs.NodeSymlinker)((*fuseRootNode)(nil))
var _ = (fs.NodeCreater)((*fuseRootNode)(nil))

// Getattr returns attributes for the root directory.
func (n *fuseRootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	now := time.Now()
	out.Ino = RootIno
	out.Size = 0
	out.Blocks = 0
	out.Atime = uint64(now.Unix())
	out.Mtime = uint64(now.Unix())
	out.Ctime = uint64(now.Unix())
	out.Mode = syscall.S_IFDIR | 0755
	out.Nlink = 2
	out.Uid = 0
	out.Gid = 0
	out.Rdev = 0
	out.Blksize = 4096
	return 0
}

// Readdir reads the root directory entries.
func (n *fuseRootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(ctx, RootIno)
	if err != nil {
		return nil, syscall.EIO
	}

	dirents := make([]fuse.DirEntry, 0, len(entries)+2)
	dirents = append(dirents, fuse.DirEntry{
		Ino:  RootIno,
		Name: ".",
		Mode: syscall.S_IFDIR,
	})
	dirents = append(dirents, fuse.DirEntry{
		Ino:  RootIno,
		Name: "..",
		Mode: syscall.S_IFDIR,
	})

	for _, e := range entries {
		var mode uint32
		switch e.Type {
		case TypeDir:
			mode = syscall.S_IFDIR
		case TypeFile:
			mode = syscall.S_IFREG
		case TypeSymlink:
			mode = syscall.S_IFLNK
		}
		dirents = append(dirents, fuse.DirEntry{
			Ino:  e.Ino,
			Name: e.Name,
			Mode: mode,
		})
	}

	return fs.NewListDirStream(dirents), 0
}

// Lookup looks up a directory entry in the root directory.
func (n *fuseRootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." || name == ".." {
		now := time.Now()
		out.Ino = RootIno
		out.Mode = syscall.S_IFDIR | 0755
		out.Size = 0
		out.Atime = uint64(now.Unix())
		out.Mtime = uint64(now.Unix())
		out.Ctime = uint64(now.Unix())
		return nil, 0
	}

	meta, err := n.fs.Lookup(ctx, RootIno, name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	fillEntryOut(out, meta)

	var stable fs.StableAttr
	stable.Ino = meta.Ino
	stable.Gen = 1

	var childNode fs.InodeEmbedder
	switch meta.Type {
	case TypeDir:
		childNode = &fuseDirNode{fs: n.fs, ino: meta.Ino}
	case TypeFile:
		childNode = &fuseFileNode{fs: n.fs, ino: meta.Ino}
	case TypeSymlink:
		childNode = &fuseSymlinkNode{fs: n.fs, ino: meta.Ino, target: meta.Target}
	default:
		return nil, syscall.EIO
	}

	return n.NewInode(ctx, childNode, stable), 0
}

// Mkdir creates a new directory in the root.
func (n *fuseRootNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	meta, err := n.fs.Mkdir(ctx, RootIno, name, mode&0777)
	if err != nil {
		return nil, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	childNode := &fuseDirNode{fs: n.fs, ino: meta.Ino}

	return n.NewInode(ctx, childNode, stable), 0
}

// Create creates a new file in the root.
func (n *fuseRootNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	meta, err := n.fs.Create(ctx, RootIno, name, mode&0777)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	fileNode := &fuseFileNode{fs: n.fs, ino: meta.Ino}
	inode := n.NewInode(ctx, fileNode, stable)
	handle := &fuseFileHandle{}

	return inode, handle, 0, 0
}

// Unlink removes a file from the root.
func (n *fuseRootNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.fs.Unlink(ctx, RootIno, name); err != nil {
		return syscall.EIO
	}
	return 0
}

// Rmdir removes a directory from the root.
func (n *fuseRootNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.fs.Rmdir(ctx, RootIno, name); err != nil {
		return syscall.EIO
	}
	return 0
}

// Rename renames a file or directory in the root.
func (n *fuseRootNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// For simplicity, only support rename within root.
	if err := n.fs.Rename(ctx, RootIno, name, RootIno, newName); err != nil {
		return syscall.EIO
	}
	return 0
}

// Symlink creates a symbolic link in the root.
func (n *fuseRootNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	meta, err := n.fs.Symlink(ctx, RootIno, name, target)
	if err != nil {
		return nil, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	linkNode := &fuseSymlinkNode{fs: n.fs, ino: meta.Ino, target: meta.Target}

	return n.NewInode(ctx, linkNode, stable), 0
}

// fuseDirNode represents a directory in the FUSE filesystem.
type fuseDirNode struct {
	fs  *FileSystem
	ino uint64
	mu  sync.Mutex
	fs.Inode
}

var _ = (fs.InodeEmbedder)((*fuseDirNode)(nil))
var _ = (fs.NodeGetattrer)((*fuseDirNode)(nil))
var _ = (fs.NodeSetattrer)((*fuseDirNode)(nil))
var _ = (fs.NodeReaddirer)((*fuseDirNode)(nil))
var _ = (fs.NodeLookuper)((*fuseDirNode)(nil))
var _ = (fs.NodeMkdirer)((*fuseDirNode)(nil))
var _ = (fs.NodeUnlinker)((*fuseDirNode)(nil))
var _ = (fs.NodeRmdirer)((*fuseDirNode)(nil))
var _ = (fs.NodeRenamer)((*fuseDirNode)(nil))
var _ = (fs.NodeSymlinker)((*fuseDirNode)(nil))
var _ = (fs.NodeCreater)((*fuseDirNode)(nil))

// Getattr returns file attributes.
func (n *fuseDirNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.fs.Stat(ctx, n.ino)
	if err != nil {
		return syscall.EIO
	}

	fillAttrOut(out, meta)
	return 0
}

// Setattr sets file attributes.
func (n *fuseDirNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.fs.Stat(ctx, n.ino)
	if err != nil {
		return syscall.EIO
	}

	// Update mode.
	if in.Valid&fuse.FATTR_MODE != 0 {
		if mode, ok := in.GetMode(); ok {
			meta.Mode = mode & 07777
		}
	}
	// Update UID/GID.
	if in.Valid&fuse.FATTR_UID != 0 {
		if uid, ok := in.GetUID(); ok {
			meta.UID = uid
		}
	}
	if in.Valid&fuse.FATTR_GID != 0 {
		if gid, ok := in.GetGID(); ok {
			meta.GID = gid
		}
	}
	// Update timestamps.
	if in.Valid&fuse.FATTR_ATIME != 0 {
		if atime, ok := in.GetATime(); ok {
			meta.ATime = atime.UnixNano()
		}
	}
	if in.Valid&fuse.FATTR_MTIME != 0 {
		if mtime, ok := in.GetMTime(); ok {
			meta.MTime = mtime.UnixNano()
		}
	}
	if in.Valid&fuse.FATTR_CTIME != 0 {
		if ctime, ok := in.GetCTime(); ok {
			meta.CTime = ctime.UnixNano()
		}
	}

	fillAttrOut(out, meta)
	return 0
}

// Readdir reads directory entries.
func (n *fuseDirNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.fs.ReadDir(ctx, n.ino)
	if err != nil {
		return nil, syscall.EIO
	}

	dirents := make([]fuse.DirEntry, 0, len(entries)+2)
	dirents = append(dirents, fuse.DirEntry{
		Ino:  n.ino,
		Name: ".",
		Mode: syscall.S_IFDIR,
	})
	dirents = append(dirents, fuse.DirEntry{
		Ino:  n.ino,
		Name: "..",
		Mode: syscall.S_IFDIR,
	})

	for _, e := range entries {
		var mode uint32
		switch e.Type {
		case TypeDir:
			mode = syscall.S_IFDIR
		case TypeFile:
			mode = syscall.S_IFREG
		case TypeSymlink:
			mode = syscall.S_IFLNK
		}
		dirents = append(dirents, fuse.DirEntry{
			Ino:  e.Ino,
			Name: e.Name,
			Mode: mode,
		})
	}

	return fs.NewListDirStream(dirents), 0
}

// Lookup looks up a directory entry by name.
func (n *fuseDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if name == "." || name == ".." {
		meta, _ := n.fs.Stat(ctx, n.ino)
		fillEntryOut(out, meta)
		return nil, 0
	}

	meta, err := n.fs.Lookup(ctx, n.ino, name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	fillEntryOut(out, meta)

	var stable fs.StableAttr
	stable.Ino = meta.Ino
	stable.Gen = 1

	var childNode fs.InodeEmbedder
	switch meta.Type {
	case TypeDir:
		childNode = &fuseDirNode{fs: n.fs, ino: meta.Ino}
	case TypeFile:
		childNode = &fuseFileNode{fs: n.fs, ino: meta.Ino}
	case TypeSymlink:
		childNode = &fuseSymlinkNode{fs: n.fs, ino: meta.Ino, target: meta.Target}
	default:
		return nil, syscall.EIO
	}

	return n.NewInode(ctx, childNode, stable), 0
}

// Mkdir creates a new directory.
func (n *fuseDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	meta, err := n.fs.Mkdir(ctx, n.ino, name, mode&0777)
	if err != nil {
		return nil, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	childNode := &fuseDirNode{fs: n.fs, ino: meta.Ino}

	return n.NewInode(ctx, childNode, stable), 0
}

// Unlink removes a file.
func (n *fuseDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := n.fs.Unlink(ctx, n.ino, name); err != nil {
		return syscall.EIO
	}
	return 0
}

// Rmdir removes a directory.
func (n *fuseDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	if err := n.fs.Rmdir(ctx, n.ino, name); err != nil {
		return syscall.EIO
	}
	return 0
}

// Rename renames a file or directory.
func (n *fuseDirNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// For simplicity, only support rename within the same parent.
	if err := n.fs.Rename(ctx, n.ino, name, n.ino, newName); err != nil {
		return syscall.EIO
	}
	return 0
}

// Symlink creates a symbolic link.
func (n *fuseDirNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	meta, err := n.fs.Symlink(ctx, n.ino, name, target)
	if err != nil {
		return nil, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	linkNode := &fuseSymlinkNode{fs: n.fs, ino: meta.Ino, target: meta.Target}

	return n.NewInode(ctx, linkNode, stable), 0
}

// Create creates a new file.
func (n *fuseDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	meta, err := n.fs.Create(ctx, n.ino, name, mode&0777)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	fillEntryOut(out, meta)

	stable := fs.StableAttr{
		Ino: meta.Ino,
		Gen: 1,
	}
	fileNode := &fuseFileNode{fs: n.fs, ino: meta.Ino}
	inode := n.NewInode(ctx, fileNode, stable)
	handle := &fuseFileHandle{}

	return inode, handle, 0, 0
}

// fuseFileNode represents a regular file in the FUSE filesystem.
type fuseFileNode struct {
	fs  *FileSystem
	ino uint64
	mu  sync.Mutex
	fs.Inode
}

var _ = (fs.InodeEmbedder)((*fuseFileNode)(nil))
var _ = (fs.NodeGetattrer)((*fuseFileNode)(nil))
var _ = (fs.NodeSetattrer)((*fuseFileNode)(nil))
var _ = (fs.NodeOpener)((*fuseFileNode)(nil))
var _ = (fs.NodeReader)((*fuseFileNode)(nil))
var _ = (fs.NodeWriter)((*fuseFileNode)(nil))
var _ = (fs.NodeFsyncer)((*fuseFileNode)(nil))

// Getattr returns file attributes.
func (n *fuseFileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.fs.Stat(ctx, n.ino)
	if err != nil {
		return syscall.EIO
	}

	fillAttrOut(out, meta)
	return 0
}

// Setattr sets file attributes.
func (n *fuseFileNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.fs.Stat(ctx, n.ino)
	if err != nil {
		return syscall.EIO
	}

	// Handle size changes (truncate).
	if in.Valid&fuse.FATTR_SIZE != 0 {
		if size, ok := in.GetSize(); ok && size == 0 {
			// Truncate to zero.
			_, err = n.fs.Write(ctx, n.ino, 0, []byte{})
		} else if size, ok := in.GetSize(); ok && int64(size) < meta.Size {
			// Truncate by reading partial content and rewriting.
			data, err := n.fs.Read(ctx, n.ino, 0, int64(size))
			if err != nil {
				return syscall.EIO
			}
			_, err = n.fs.Write(ctx, n.ino, 0, data)
			if err != nil {
				return syscall.EIO
			}
			meta.Size = int64(size)
		} else if size, ok := in.GetSize(); ok {
			meta.Size = int64(size)
		}
	}

	// Update mode.
	if in.Valid&fuse.FATTR_MODE != 0 {
		if mode, ok := in.GetMode(); ok {
			meta.Mode = mode & 07777
		}
	}
	// Update UID/GID.
	if in.Valid&fuse.FATTR_UID != 0 {
		if uid, ok := in.GetUID(); ok {
			meta.UID = uid
		}
	}
	if in.Valid&fuse.FATTR_GID != 0 {
		if gid, ok := in.GetGID(); ok {
			meta.GID = gid
		}
	}
	// Update timestamps.
	if in.Valid&fuse.FATTR_ATIME != 0 {
		if atime, ok := in.GetATime(); ok {
			meta.ATime = atime.UnixNano()
		}
	}
	if in.Valid&fuse.FATTR_MTIME != 0 {
		if mtime, ok := in.GetMTime(); ok {
			meta.MTime = mtime.UnixNano()
		}
	}
	if in.Valid&fuse.FATTR_CTIME != 0 {
		if ctime, ok := in.GetCTime(); ok {
			meta.CTime = ctime.UnixNano()
		}
	}

	fillAttrOut(out, meta)
	return 0
}

// Open opens a file.
func (n *fuseFileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	handle := &fuseFileHandle{}
	return handle, 0, 0
}

// Read reads data from a file.
func (n *fuseFileNode) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	data, err := n.fs.Read(ctx, n.ino, off, int64(len(dest)))
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(data), 0
}

// Write writes data to a file.
func (n *fuseFileNode) Write(ctx context.Context, f fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	nwritten, err := n.fs.Write(ctx, n.ino, off, data)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(nwritten), 0
}

// Fsync ensures file data is written to stable storage.
func (n *fuseFileNode) Fsync(ctx context.Context, f fs.FileHandle, flags uint32) syscall.Errno {
	return 0
}

// fuseFileHandle represents an open file handle.
type fuseFileHandle struct {
	mu sync.Mutex
}

var _ = (fs.FileHandle)((*fuseFileHandle)(nil))
var _ = (fs.FileReleaser)((*fuseFileHandle)(nil))

// Release closes the file handle.
func (h *fuseFileHandle) Release(ctx context.Context) syscall.Errno {
	return 0
}

// fuseSymlinkNode represents a symbolic link in the FUSE filesystem.
type fuseSymlinkNode struct {
	fs     *FileSystem
	ino    uint64
	target string
	fs.Inode
}

var _ = (fs.InodeEmbedder)((*fuseSymlinkNode)(nil))
var _ = (fs.NodeGetattrer)((*fuseSymlinkNode)(nil))
var _ = (fs.NodeReadlinker)((*fuseSymlinkNode)(nil))

// Getattr returns file attributes.
func (n *fuseSymlinkNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	meta, err := n.fs.Stat(ctx, n.ino)
	if err != nil {
		return syscall.EIO
	}

	fillAttrOut(out, meta)
	return 0
}

// Readlink reads the target of a symbolic link.
func (n *fuseSymlinkNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	return []byte(n.target), 0
}

// fillAttrOut populates a fuse AttrOut from an InodeMeta.
func fillAttrOut(out *fuse.AttrOut, meta *InodeMeta) {
	out.Ino = meta.Ino
	out.Size = uint64(meta.Size)
	out.Blocks = uint64((meta.Size + 511) / 512)
	out.Atime = uint64(meta.ATime / 1e9)
	out.Mtime = uint64(meta.MTime / 1e9)
	out.Ctime = uint64(meta.CTime / 1e9)
	out.Mode = meta.Mode
	out.Nlink = meta.LinkCount
	out.Uid = meta.UID
	out.Gid = meta.GID
	out.Rdev = 0
	out.Blksize = 4096

	// Set file type bits based on inode type.
	switch meta.Type {
	case TypeDir:
		out.Mode |= syscall.S_IFDIR
	case TypeFile:
		out.Mode |= syscall.S_IFREG
	case TypeSymlink:
		out.Mode |= syscall.S_IFLNK
	}
}

// fillEntryOut populates a fuse EntryOut from an InodeMeta.
func fillEntryOut(out *fuse.EntryOut, meta *InodeMeta) {
	out.Ino = meta.Ino
	out.Size = uint64(meta.Size)
	out.Blocks = uint64((meta.Size + 511) / 512)
	out.Atime = uint64(meta.ATime / 1e9)
	out.Mtime = uint64(meta.MTime / 1e9)
	out.Ctime = uint64(meta.CTime / 1e9)
	out.Mode = meta.Mode
	out.Nlink = meta.LinkCount
	out.Uid = meta.UID
	out.Gid = meta.GID
	out.Rdev = 0
	out.Blksize = 4096

	// Set file type bits based on inode type.
	switch meta.Type {
	case TypeDir:
		out.Mode |= syscall.S_IFDIR
	case TypeFile:
		out.Mode |= syscall.S_IFREG
	case TypeSymlink:
		out.Mode |= syscall.S_IFLNK
	}

	out.NodeId = meta.Ino
	out.Generation = 1
	out.EntryValid = 1.0
	out.AttrValid = 1.0
}
