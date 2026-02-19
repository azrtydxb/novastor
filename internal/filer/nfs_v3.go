package filer

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metrics"
)

// NFS v3 protocol constants (RFC 1813).
const (
	nfsProg    uint32 = 100003
	nfsVersion uint32 = 3

	// NFS v3 procedures.
	nfsProcNull        uint32 = 0
	nfsProcGetattr     uint32 = 1
	nfsProcSetattr     uint32 = 2
	nfsProcLookup      uint32 = 3
	nfsProcAccess      uint32 = 4
	nfsProcReadlink    uint32 = 5
	nfsProcRead        uint32 = 6
	nfsProcWrite       uint32 = 7
	nfsProcCreate      uint32 = 8
	nfsProcMkdir       uint32 = 9
	nfsProcSymlink     uint32 = 10
	nfsProcMknod       uint32 = 11
	nfsProcRemove      uint32 = 12
	nfsProcRmdir       uint32 = 13
	nfsProcRename      uint32 = 14
	nfsProcLink        uint32 = 15
	nfsProcReadDir     uint32 = 16
	nfsProcReadDirPlus uint32 = 17
	nfsProcFsStat      uint32 = 18
	nfsProcFsInfo      uint32 = 19
	nfsProcPathConf    uint32 = 20
	nfsProcCommit      uint32 = 21

	// NFS v3 status codes (nfsstat3).
	nfs3OK             uint32 = 0
	nfs3ErrPerm        uint32 = 1
	nfs3ErrNoEnt       uint32 = 2
	nfs3ErrIO          uint32 = 5
	nfs3ErrNXIO        uint32 = 6
	nfs3ErrAcces       uint32 = 13
	nfs3ErrExist       uint32 = 17
	nfs3ErrXDev        uint32 = 18
	nfs3ErrNoDev       uint32 = 19
	nfs3ErrNotDir      uint32 = 20
	nfs3ErrIsDir       uint32 = 21
	nfs3ErrInval       uint32 = 22
	nfs3ErrFBig        uint32 = 27
	nfs3ErrNoSpc       uint32 = 28
	nfs3ErrROFS        uint32 = 30
	nfs3ErrNameTooLong uint32 = 63
	nfs3ErrNotEmpty    uint32 = 66
	nfs3ErrStale       uint32 = 70
	nfs3ErrBadHandle   uint32 = 10001
	nfs3ErrServerfault uint32 = 10006

	// NFS v3 file types (ftype3).
	nf3Reg  uint32 = 1
	nf3Dir  uint32 = 2
	nf3Blk  uint32 = 3
	nf3Chr  uint32 = 4
	nf3Lnk  uint32 = 5
	nf3Sock uint32 = 6
	nf3FIFO uint32 = 7

	// ACCESS3 check bits.
	access3Read    uint32 = 0x0001
	access3Lookup  uint32 = 0x0002
	access3Modify  uint32 = 0x0004
	access3Extend  uint32 = 0x0008
	access3Delete  uint32 = 0x0010
	access3Execute uint32 = 0x0020

	// WRITE3 stable/unstable constants.
	writeUnstable uint32 = 0
	writeDataSync uint32 = 1
	writeFileSync uint32 = 2

	// CREATE3 mode constants.
	createUnchecked uint32 = 0
	createGuarded   uint32 = 1
	createExclusive uint32 = 2

	// Maximum sizes for NFS v3.
	maxNFSData     uint32 = 1048576 // 1 MiB max read/write
	maxNFSName     uint32 = 255
	maxNFSPath     uint32 = 4096
	maxNFSFileSize uint64 = 1 << 62 // ~4 EiB
)

// nfsV3Handler implements the NFS v3 program.
type nfsV3Handler struct {
	fs            NFSHandler
	handles       *handleManager
	locker        *LockManager
	writeVerifier [8]byte // stable across server lifetime, changes on restart
}

func newNFSV3Handler(fs NFSHandler, handles *handleManager, locker *LockManager) *nfsV3Handler {
	h := &nfsV3Handler{
		fs:      fs,
		handles: handles,
		locker:  locker,
	}
	// Generate write verifier from current time (changes on each server restart).
	now := time.Now().UnixNano()
	for i := 0; i < 8; i++ {
		h.writeVerifier[i] = byte(now >> (i * 8))
	}
	return h
}

func (n *nfsV3Handler) Program() uint32 { return nfsProg }
func (n *nfsV3Handler) Version() uint32 { return nfsVersion }

func (n *nfsV3Handler) HandleProc(proc uint32, _ uint32, payload []byte, _ net.Conn) ([]byte, error) {
	ctx := context.Background()
	start := time.Now()

	// Map procedure ID to operation name for metrics.
	opName := nfsProcName(proc)
	defer func() {
		metrics.NFSOpsTotal.WithLabelValues(opName).Inc()
		metrics.NFSOpDuration.WithLabelValues(opName).Observe(time.Since(start).Seconds())
	}()

	switch proc {
	case nfsProcNull:
		return nil, nil
	case nfsProcGetattr:
		return n.handleGetattr(ctx, payload)
	case nfsProcSetattr:
		return n.handleSetattr(ctx, payload)
	case nfsProcLookup:
		return n.handleLookup(ctx, payload)
	case nfsProcAccess:
		return n.handleAccess(ctx, payload)
	case nfsProcReadlink:
		return n.handleReadlink(ctx, payload)
	case nfsProcRead:
		return n.handleRead(ctx, payload)
	case nfsProcWrite:
		return n.handleWrite(ctx, payload)
	case nfsProcCreate:
		return n.handleCreate(ctx, payload)
	case nfsProcMkdir:
		return n.handleMkdir(ctx, payload)
	case nfsProcSymlink:
		return n.handleSymlink(ctx, payload)
	case nfsProcMknod:
		return n.handleMknod(ctx, payload)
	case nfsProcRemove:
		return n.handleRemove(ctx, payload)
	case nfsProcRmdir:
		return n.handleRmdir(ctx, payload)
	case nfsProcRename:
		return n.handleRename(ctx, payload)
	case nfsProcLink:
		return n.handleLink(ctx, payload)
	case nfsProcReadDir:
		return n.handleReadDir(ctx, payload)
	case nfsProcReadDirPlus:
		return n.handleReadDirPlus(ctx, payload)
	case nfsProcFsStat:
		return n.handleFsStat(ctx, payload)
	case nfsProcFsInfo:
		return n.handleFsInfo(ctx, payload)
	case nfsProcPathConf:
		return n.handlePathConf(ctx, payload)
	case nfsProcCommit:
		return n.handleCommit(ctx, payload)
	default:
		logging.L.Warn("nfs: unknown procedure", zap.Uint32("proc", proc))
		return nil, nil
	}
}

// nfsProcName maps NFS procedure IDs to operation names for metrics.
func nfsProcName(proc uint32) string {
	switch proc {
	case nfsProcNull:
		return "null"
	case nfsProcGetattr:
		return "getattr"
	case nfsProcSetattr:
		return "setattr"
	case nfsProcLookup:
		return "lookup"
	case nfsProcAccess:
		return "access"
	case nfsProcReadlink:
		return "readlink"
	case nfsProcRead:
		return "read"
	case nfsProcWrite:
		return "write"
	case nfsProcCreate:
		return "create"
	case nfsProcMkdir:
		return "mkdir"
	case nfsProcSymlink:
		return "symlink"
	case nfsProcMknod:
		return "mknod"
	case nfsProcRemove:
		return "remove"
	case nfsProcRmdir:
		return "rmdir"
	case nfsProcRename:
		return "rename"
	case nfsProcLink:
		return "link"
	case nfsProcReadDir:
		return "readdir"
	case nfsProcReadDirPlus:
		return "readdirplus"
	case nfsProcFsStat:
		return "fsstat"
	case nfsProcFsInfo:
		return "fsinfo"
	case nfsProcPathConf:
		return "pathconf"
	case nfsProcCommit:
		return "commit"
	default:
		return "unknown"
	}
}

// resolveHandle decodes a file handle from XDR and resolves it to an inode number.
func (n *nfsV3Handler) resolveHandle(r *xdrReader) (uint64, []byte, error) {
	handle, err := r.readOpaque()
	if err != nil {
		return 0, nil, err
	}
	ino, ok := n.handles.lookupHandle(handle)
	if !ok {
		// Try to recover the inode from the handle itself (server restart case).
		ino = inodeFromHandle(handle)
		if ino == 0 {
			return 0, handle, errStaleHandle
		}
		// Re-register the handle.
		n.handles.getOrCreateHandle(ino)
	}
	return ino, handle, nil
}

// errStaleHandle is a sentinel error for stale file handles.
var errStaleHandle = &nfsError{status: nfs3ErrStale}

type nfsError struct {
	status uint32
}

func (e *nfsError) Error() string {
	return "nfs error"
}

// inodeToFtype3 converts our InodeType to NFS v3 ftype3.
func inodeToFtype3(t InodeType) uint32 {
	switch t {
	case TypeFile:
		return nf3Reg
	case TypeDir:
		return nf3Dir
	case TypeSymlink:
		return nf3Lnk
	default:
		return nf3Reg
	}
}

// writeFattr3 writes NFS v3 file attributes (fattr3) for an inode.
func (n *nfsV3Handler) writeFattr3(w *xdrWriter, meta *InodeMeta) {
	// ftype3
	w.writeUint32(inodeToFtype3(meta.Type))
	// mode3 (uint32)
	w.writeUint32(meta.Mode)
	// nlink (uint32)
	w.writeUint32(meta.LinkCount)
	// uid3 (uint32)
	w.writeUint32(meta.UID)
	// gid3 (uint32)
	w.writeUint32(meta.GID)
	// size3 (uint64)
	w.writeUint64(uint64(meta.Size))
	// used3 (uint64) - disk space used; approximate as size
	w.writeUint64(uint64(meta.Size))
	// specdata3 (2 x uint32) - for block/char devices
	w.writeUint32(0)
	w.writeUint32(0)
	// fsid3 (uint64)
	w.writeUint64(1) // single filesystem
	// fileid3 (uint64) - inode number
	w.writeUint64(meta.Ino)
	// atime (nfstime3: seconds + nseconds)
	w.writeUint32(uint32(meta.ATime / 1e9))
	w.writeUint32(uint32(meta.ATime % 1e9))
	// mtime
	w.writeUint32(uint32(meta.MTime / 1e9))
	w.writeUint32(uint32(meta.MTime % 1e9))
	// ctime
	w.writeUint32(uint32(meta.CTime / 1e9))
	w.writeUint32(uint32(meta.CTime % 1e9))
}

// writePostOpAttr writes post_op_attr (optional attributes after an operation).
func (n *nfsV3Handler) writePostOpAttr(w *xdrWriter, meta *InodeMeta) {
	if meta == nil {
		w.writeBool(false) // attributes_follow = false
		return
	}
	w.writeBool(true) // attributes_follow = true
	n.writeFattr3(w, meta)
}

// writeWcc writes weak cache consistency data (wcc_data).
func (n *nfsV3Handler) writeWcc(w *xdrWriter, before *InodeMeta, after *InodeMeta) {
	// pre_op_attr (wcc_attr)
	if before != nil {
		w.writeBool(true) // attributes_follow
		w.writeUint64(uint64(before.Size))
		w.writeUint32(uint32(before.MTime / 1e9))
		w.writeUint32(uint32(before.MTime % 1e9))
		w.writeUint32(uint32(before.CTime / 1e9))
		w.writeUint32(uint32(before.CTime % 1e9))
	} else {
		w.writeBool(false)
	}
	// post_op_attr
	n.writePostOpAttr(w, after)
}

// nfsErrorReply builds a simple error reply with status and optional post-op attributes.
func nfsErrorReply(status uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(status)
	w.writeBool(false) // no post_op_attr
	return w.Bytes()
}

// nfsErrorReplyWcc builds an error reply with wcc_data.
func nfsErrorReplyWcc(status uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(status)
	// wcc_data: no pre_op_attr, no post_op_attr
	w.writeBool(false)
	w.writeBool(false)
	return w.Bytes()
}

// readSattr3 reads an NFS v3 sattr3 (settable attributes) from the reader.
// Returns mode, uid, gid, size pointers (nil if not set) and atime/mtime.
func readSattr3(r *xdrReader) (mode *uint32, uid *uint32, gid *uint32, size *uint64, err error) {
	// set_mode3
	setMode, err := r.readBool()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if setMode {
		m, err := r.readUint32()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		mode = &m
	}
	// set_uid3
	setUID, err := r.readBool()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if setUID {
		u, err := r.readUint32()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		uid = &u
	}
	// set_gid3
	setGID, err := r.readBool()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if setGID {
		g, err := r.readUint32()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		gid = &g
	}
	// set_size3
	setSize, err := r.readBool()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if setSize {
		s, err := r.readUint64()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		size = &s
	}
	// set_atime (enum: DONT_CHANGE=0, SET_TO_SERVER_TIME=1, SET_TO_CLIENT_TIME=2)
	atimeHow, err := r.readUint32()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if atimeHow == 2 {
		// Read client-provided time.
		_, _ = r.readUint32() // seconds
		_, _ = r.readUint32() // nseconds
	}
	// set_mtime
	mtimeHow, err := r.readUint32()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if mtimeHow == 2 {
		_, _ = r.readUint32()
		_, _ = r.readUint32()
	}

	return mode, uid, gid, size, nil
}

// ---- NFS v3 procedure implementations ----

func (n *nfsV3Handler) handleGetattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}

	meta, err := n.fs.Stat(ctx, ino)
	if err != nil {
		return nfsErrorReply(nfs3ErrIO), nil
	}

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeFattr3(w, meta)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleSetattr(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}

	meta, err := n.fs.Stat(ctx, ino)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrIO), nil
	}

	before := *meta

	// Read sattr3.
	mode, uid, gid, size, sErr := readSattr3(r)
	if sErr != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	// Handle size truncation first, before applying other attributes.
	// This must be done for regular files only.
	if size != nil {
		if meta.Type != TypeFile {
			w := newXDRWriter()
			w.writeUint32(nfs3ErrInval)
			n.writeWcc(w, &before, meta)
			return w.Bytes(), nil
		}
		if err := n.fs.Truncate(ctx, ino, int64(*size)); err != nil {
			logging.L.Warn("nfs: truncate failed",
				zap.Uint64("ino", ino),
				zap.Int64("size", int64(*size)),
				zap.Error(err),
			)
			w := newXDRWriter()
			w.writeUint32(nfs3ErrIO)
			n.writeWcc(w, &before, meta)
			return w.Bytes(), nil
		}
		// Refresh metadata after truncate to get updated size.
		meta, err = n.fs.Stat(ctx, ino)
		if err != nil {
			return nfsErrorReplyWcc(nfs3ErrIO), nil
		}
	}

	// Apply other attributes. We support mode, uid, gid.
	if mode != nil {
		meta.Mode = *mode
	}
	if uid != nil {
		meta.UID = *uid
	}
	if gid != nil {
		meta.GID = *gid
	}
	meta.CTime = time.Now().UnixNano()

	// Skip guard check (sattrguard3).
	// The guard is an optional pre-op ctime check; we accept all setattr calls.

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, &before, meta)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleLookup(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}

	// Handle special "." and ".." entries.
	var meta *InodeMeta
	switch name {
	case ".":
		meta, err = n.fs.Stat(ctx, parentIno)
	case "..":
		// We don't track parent pointers, so ".." from root returns root.
		// For non-root directories, this is a simplification.
		meta, err = n.fs.Stat(ctx, RootIno)
	default:
		meta, err = n.fs.Lookup(ctx, parentIno, name)
	}
	if err != nil {
		w := newXDRWriter()
		w.writeUint32(nfs3ErrNoEnt)
		// post_op_attr for directory
		dirMeta, _ := n.fs.Stat(ctx, parentIno)
		n.writePostOpAttr(w, dirMeta)
		return w.Bytes(), nil
	}

	childHandle := n.handles.getOrCreateHandle(meta.Ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	// object file handle
	w.writeOpaque(childHandle)
	// obj_attributes (post_op_attr)
	n.writePostOpAttr(w, meta)
	// dir_attributes (post_op_attr)
	dirMeta, _ := n.fs.Stat(ctx, parentIno)
	n.writePostOpAttr(w, dirMeta)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleAccess(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}
	accessRequested, err := r.readUint32()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}

	meta, err := n.fs.Stat(ctx, ino)
	if err != nil {
		return nfsErrorReply(nfs3ErrIO), nil
	}

	// Grant all requested access (NovaStor does not enforce POSIX permissions at the NFS layer).
	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)
	w.writeUint32(accessRequested)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleReadlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}

	target, err := n.fs.Readlink(ctx, ino)
	if err != nil {
		meta, _ := n.fs.Stat(ctx, ino)
		w := newXDRWriter()
		w.writeUint32(nfs3ErrInval)
		n.writePostOpAttr(w, meta)
		return w.Bytes(), nil
	}

	meta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)
	w.writeString(target)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleRead(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}
	offset, err := r.readUint64()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	count, err := r.readUint32()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	if count > maxNFSData {
		count = maxNFSData
	}

	data, err := n.fs.Read(ctx, ino, int64(offset), int64(count))
	if err != nil {
		meta, _ := n.fs.Stat(ctx, ino)
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writePostOpAttr(w, meta)
		return w.Bytes(), nil
	}

	meta, _ := n.fs.Stat(ctx, ino)

	// Determine EOF.
	eof := false
	if meta != nil && int64(offset)+int64(len(data)) >= meta.Size {
		eof = true
	}
	if data == nil {
		data = []byte{}
		eof = true
	}

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)
	w.writeUint32(uint32(len(data))) // count
	w.writeBool(eof)                 // eof
	w.writeOpaque(data)              // data
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleWrite(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	offset, err := r.readUint64()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}
	count, err := r.readUint32()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}
	stable, err := r.readUint32()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}
	_ = stable

	data, err := r.readOpaque()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	// Use the smaller of count and actual data length.
	if uint32(len(data)) < count {
		count = uint32(len(data))
	}
	writeData := data[:count]

	beforeMeta, _ := n.fs.Stat(ctx, ino)
	var before *InodeMeta
	if beforeMeta != nil {
		cp := *beforeMeta
		before = &cp
	}

	written, err := n.fs.Write(ctx, ino, int64(offset), writeData)
	if err != nil {
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writeWcc(w, before, beforeMeta)
		return w.Bytes(), nil
	}

	afterMeta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, before, afterMeta)
	w.writeUint32(uint32(written)) // count
	w.writeUint32(writeFileSync)   // committed (always FILE_SYNC)
	w.writeFixedOpaque(n.writeVerifier[:])
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleCreate(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	// createhow3: read the mode discriminant.
	createMode, err := r.readUint32()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	var fileMode uint32 = 0644
	if createMode == createUnchecked || createMode == createGuarded {
		// Read sattr3 to get mode.
		mode, _, _, _, sErr := readSattr3(r)
		if sErr == nil && mode != nil {
			fileMode = *mode
		}
	}
	// For EXCLUSIVE mode, there's a createverf3 (8 bytes) which we skip.

	// Check if file already exists.
	if createMode == createGuarded {
		if _, lookErr := n.fs.Lookup(ctx, parentIno, name); lookErr == nil {
			return nfsErrorReplyWcc(nfs3ErrExist), nil
		}
	}

	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var beforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		beforeCp = &cp
	}

	meta, err := n.fs.Create(ctx, parentIno, name, fileMode)
	if err != nil {
		logging.L.Error("nfs: create failed", zap.String("name", name), zap.Error(err))
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writeWcc(w, beforeCp, dirBefore)
		return w.Bytes(), nil
	}

	childHandle := n.handles.getOrCreateHandle(meta.Ino)
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	// post_op_fh3 (obj handle): value follows
	w.writeBool(true)
	w.writeOpaque(childHandle)
	// post_op_attr (obj attributes)
	n.writePostOpAttr(w, meta)
	// dir_wcc
	n.writeWcc(w, beforeCp, dirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleMkdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	// Read sattr3.
	var dirMode uint32 = 0755
	mode, _, _, _, sErr := readSattr3(r)
	if sErr == nil && mode != nil {
		dirMode = *mode
	}

	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var beforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		beforeCp = &cp
	}

	meta, err := n.fs.Mkdir(ctx, parentIno, name, dirMode)
	if err != nil {
		logging.L.Error("nfs: mkdir failed", zap.String("name", name), zap.Error(err))
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writeWcc(w, beforeCp, dirBefore)
		return w.Bytes(), nil
	}

	childHandle := n.handles.getOrCreateHandle(meta.Ino)
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	// post_op_fh3
	w.writeBool(true)
	w.writeOpaque(childHandle)
	// post_op_attr
	n.writePostOpAttr(w, meta)
	// dir_wcc
	n.writeWcc(w, beforeCp, dirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleSymlink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	// Read symlink attributes (sattr3) - mostly ignored for symlinks.
	_, _, _, _, _ = readSattr3(r)

	// Read symlink target (symlink_data).
	target, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var beforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		beforeCp = &cp
	}

	meta, err := n.fs.Symlink(ctx, parentIno, name, target)
	if err != nil {
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writeWcc(w, beforeCp, dirBefore)
		return w.Bytes(), nil
	}

	childHandle := n.handles.getOrCreateHandle(meta.Ino)
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	w.writeBool(true)
	w.writeOpaque(childHandle)
	n.writePostOpAttr(w, meta)
	n.writeWcc(w, beforeCp, dirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleMknod(_ context.Context, _ []byte) ([]byte, error) {
	// MKNOD is for creating special device files; not supported.
	return nfsErrorReplyWcc(nfs3ErrNotDir), nil
}

func (n *nfsV3Handler) handleRemove(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var beforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		beforeCp = &cp
	}

	// Look up the entry before removing, so we can clean up the handle.
	entry, _ := n.fs.Lookup(ctx, parentIno, name)

	if err := n.fs.Unlink(ctx, parentIno, name); err != nil {
		w := newXDRWriter()
		w.writeUint32(nfs3ErrNoEnt)
		n.writeWcc(w, beforeCp, dirBefore)
		return w.Bytes(), nil
	}

	if entry != nil {
		n.handles.removeHandle(entry.Ino)
	}
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, beforeCp, dirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleRmdir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}
	name, err := r.readString()
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrInval), nil
	}

	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var beforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		beforeCp = &cp
	}

	entry, _ := n.fs.Lookup(ctx, parentIno, name)

	if err := n.fs.Rmdir(ctx, parentIno, name); err != nil {
		// Determine appropriate error code.
		status := nfs3ErrIO
		if entry == nil {
			status = nfs3ErrNoEnt
		}
		w := newXDRWriter()
		w.writeUint32(status)
		n.writeWcc(w, beforeCp, dirBefore)
		return w.Bytes(), nil
	}

	if entry != nil {
		n.handles.removeHandle(entry.Ino)
	}
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, beforeCp, dirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleRename(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	fromDirIno, _, err := n.resolveHandle(r)
	if err != nil {
		return n.renameErrorReply(nfs3ErrStale), nil
	}
	fromName, err := r.readString()
	if err != nil {
		return n.renameErrorReply(nfs3ErrInval), nil
	}
	toDirIno, _, err := n.resolveHandle(r)
	if err != nil {
		return n.renameErrorReply(nfs3ErrStale), nil
	}
	toName, err := r.readString()
	if err != nil {
		return n.renameErrorReply(nfs3ErrInval), nil
	}

	fromDirBefore, _ := n.fs.Stat(ctx, fromDirIno)
	toDirBefore, _ := n.fs.Stat(ctx, toDirIno)

	var fromBeforeCp, toBeforeCp *InodeMeta
	if fromDirBefore != nil {
		cp := *fromDirBefore
		fromBeforeCp = &cp
	}
	if toDirBefore != nil {
		cp := *toDirBefore
		toBeforeCp = &cp
	}

	if err := n.fs.Rename(ctx, fromDirIno, fromName, toDirIno, toName); err != nil {
		w := newXDRWriter()
		w.writeUint32(nfs3ErrIO)
		n.writeWcc(w, fromBeforeCp, fromDirBefore)
		n.writeWcc(w, toBeforeCp, toDirBefore)
		return w.Bytes(), nil
	}

	fromDirAfter, _ := n.fs.Stat(ctx, fromDirIno)
	toDirAfter, _ := n.fs.Stat(ctx, toDirIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, fromBeforeCp, fromDirAfter)
	n.writeWcc(w, toBeforeCp, toDirAfter)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) renameErrorReply(status uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(status)
	// fromdir_wcc
	w.writeBool(false)
	w.writeBool(false)
	// todir_wcc
	w.writeBool(false)
	w.writeBool(false)
	return w.Bytes()
}

func (n *nfsV3Handler) handleLink(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	// LINK3args: file (target file handle), dir (directory handle), linkname (string)
	targetIno, _, err := n.resolveHandle(r)
	if err != nil {
		return n.linkErrorReply(nfs3ErrStale), nil
	}

	parentIno, _, err := n.resolveHandle(r)
	if err != nil {
		return n.linkErrorReply(nfs3ErrStale), nil
	}

	linkName, err := r.readString()
	if err != nil {
		return n.linkErrorReply(nfs3ErrInval), nil
	}

	// Get target metadata to verify it exists and is a regular file.
	targetMeta, err := n.fs.Stat(ctx, targetIno)
	if err != nil {
		return n.linkErrorReply(nfs3ErrNoEnt), nil
	}

	// Hard links are only supported for regular files.
	if targetMeta.Type != TypeFile {
		return n.linkErrorReply(nfs3ErrXDev), nil
	}

	// Get parent directory metadata before the operation.
	dirBefore, _ := n.fs.Stat(ctx, parentIno)
	var dirBeforeCp *InodeMeta
	if dirBefore != nil {
		cp := *dirBefore
		dirBeforeCp = &cp
	}

	// Check if the link name already exists. According to NFS v3 spec,
	// we need to remove the existing entry first if it exists.
	if existing, _ := n.fs.Lookup(ctx, parentIno, linkName); existing != nil {
		// Remove the existing entry.
		if err := n.fs.Unlink(ctx, parentIno, linkName); err != nil {
			return n.linkErrorReply(nfs3ErrIO), nil
		}
	}

	// Create the hard link.
	meta, err := n.fs.Link(ctx, targetIno, parentIno, linkName)
	if err != nil {
		logging.L.Error("nfs: link failed", zap.String("name", linkName), zap.Error(err))
		return n.linkErrorReply(nfs3ErrIO), nil
	}

	// Get updated parent directory metadata.
	dirAfter, _ := n.fs.Stat(ctx, parentIno)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	// post_op_attr for the target file (attributes after link)
	n.writePostOpAttr(w, meta)
	// dir_wcc (weak cache consistency data for the directory)
	n.writeWcc(w, dirBeforeCp, dirAfter)
	return w.Bytes(), nil
}

// linkErrorReply builds an error reply for the LINK procedure with status and empty attributes.
func (n *nfsV3Handler) linkErrorReply(status uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(status)
	// post_op_attr (no attributes)
	w.writeBool(false)
	// dir_wcc (no pre_op_attr, no post_op_attr)
	w.writeBool(false)
	w.writeBool(false)
	return w.Bytes()
}

func (n *nfsV3Handler) handleReadDir(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}
	cookie, err := r.readUint64()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	// cookieverf3 (8 bytes)
	cookieVerf, err := r.readFixedOpaque(8)
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	_ = cookieVerf
	dirCount, err := r.readUint32()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	_ = dirCount

	entries, err := n.fs.ReadDir(ctx, ino)
	if err != nil {
		return nfsErrorReply(nfs3ErrIO), nil
	}

	dirMeta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, dirMeta)
	// cookieverf3 (8 zero bytes for our simple implementation)
	w.writeFixedOpaque(make([]byte, 8))

	// Emit entries starting from cookie offset.
	// Cookie is 1-based index (0 means start from beginning).
	startIdx := int(cookie)

	// Add synthetic "." and ".." entries at cookies 0 and 1.
	syntheticEntries := []struct {
		name string
		ino  uint64
	}{
		{".", ino},
		{"..", RootIno},
	}

	for i := startIdx; i < len(syntheticEntries); i++ {
		se := syntheticEntries[i]
		w.writeBool(true) // value follows
		w.writeUint64(se.ino)
		w.writeString(se.name)
		w.writeUint64(uint64(i + 1)) // cookie for next entry
	}

	realStart := startIdx - len(syntheticEntries)
	if realStart < 0 {
		realStart = 0
	}

	for i := realStart; i < len(entries); i++ {
		e := entries[i]
		w.writeBool(true) // value follows
		w.writeUint64(e.Ino)
		w.writeString(e.Name)
		w.writeUint64(uint64(len(syntheticEntries) + i + 1)) // cookie
	}

	w.writeBool(false) // no more entries
	w.writeBool(true)  // eof
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleReadDirPlus(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}
	cookie, err := r.readUint64()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	cookieVerf, err := r.readFixedOpaque(8)
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	_ = cookieVerf
	dirCount, err := r.readUint32()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	_ = dirCount
	maxCount, err := r.readUint32()
	if err != nil {
		return nfsErrorReply(nfs3ErrInval), nil
	}
	_ = maxCount

	entries, err := n.fs.ReadDir(ctx, ino)
	if err != nil {
		return nfsErrorReply(nfs3ErrIO), nil
	}

	dirMeta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, dirMeta)
	w.writeFixedOpaque(make([]byte, 8)) // cookieverf3

	startIdx := int(cookie)

	// Synthetic "." and ".." entries.
	type synthEntry struct {
		name string
		ino  uint64
	}
	synthEntries := []synthEntry{
		{".", ino},
		{"..", RootIno},
	}

	for i := startIdx; i < len(synthEntries); i++ {
		se := synthEntries[i]
		entryMeta, _ := n.fs.Stat(ctx, se.ino)
		w.writeBool(true) // value follows
		w.writeUint64(se.ino)
		w.writeString(se.name)
		w.writeUint64(uint64(i + 1)) // cookie
		// name_attributes (post_op_attr)
		n.writePostOpAttr(w, entryMeta)
		// name_handle (post_op_fh3)
		fh := n.handles.getOrCreateHandle(se.ino)
		w.writeBool(true)
		w.writeOpaque(fh)
	}

	realStart := startIdx - len(synthEntries)
	if realStart < 0 {
		realStart = 0
	}

	for i := realStart; i < len(entries); i++ {
		e := entries[i]
		entryMeta, _ := n.fs.Stat(ctx, e.Ino)
		w.writeBool(true) // value follows
		w.writeUint64(e.Ino)
		w.writeString(e.Name)
		w.writeUint64(uint64(len(synthEntries) + i + 1)) // cookie
		// name_attributes
		n.writePostOpAttr(w, entryMeta)
		// name_handle
		fh := n.handles.getOrCreateHandle(e.Ino)
		w.writeBool(true)
		w.writeOpaque(fh)
	}

	w.writeBool(false) // no more entries
	w.writeBool(true)  // eof
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleFsStat(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}

	meta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)

	// Total, free, available bytes (report large values - chunk storage is distributed).
	var totalBytes uint64 = 1 << 50 // ~1 PiB
	var freeBytes uint64 = 1 << 49  // ~512 TiB
	w.writeUint64(totalBytes)       // tbytes
	w.writeUint64(freeBytes)        // fbytes
	w.writeUint64(freeBytes)        // abytes (available)
	// Total, free, available files.
	var totalFiles uint64 = 1 << 40
	var freeFiles uint64 = 1 << 39
	w.writeUint64(totalFiles) // tfiles
	w.writeUint64(freeFiles)  // ffiles
	w.writeUint64(freeFiles)  // afiles
	// invarsec (seconds of invariance; 0 = volatile)
	w.writeUint32(0)
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleFsInfo(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}

	meta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)
	w.writeUint32(maxNFSData)     // rtmax
	w.writeUint32(maxNFSData)     // rtpref
	w.writeUint32(8)              // rtmult (suggested read multiple)
	w.writeUint32(maxNFSData)     // wtmax
	w.writeUint32(maxNFSData)     // wtpref
	w.writeUint32(8)              // wtmult
	w.writeUint32(maxNFSData)     // dtpref (readdir)
	w.writeUint64(maxNFSFileSize) // maxfilesize
	// time_delta (smallest time granularity: 1 nanosecond)
	w.writeUint32(0) // seconds
	w.writeUint32(1) // nseconds
	// properties bitmask: FSF3_LINK | FSF3_SYMLINK | FSF3_HOMOGENEOUS | FSF3_CANSETTIME
	w.writeUint32(0x001B) // 0x0001|0x0002|0x0008|0x0010
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handlePathConf(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReply(nfs3ErrStale), nil
	}

	meta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writePostOpAttr(w, meta)
	w.writeUint32(32767) // linkmax
	w.writeUint32(255)   // name_max
	w.writeBool(true)    // no_trunc
	w.writeBool(false)   // chown_restricted
	w.writeBool(true)    // case_insensitive = false (case_preserving)
	w.writeBool(true)    // case_preserving
	return w.Bytes(), nil
}

func (n *nfsV3Handler) handleCommit(ctx context.Context, payload []byte) ([]byte, error) {
	r := newXDRReader(payload)
	ino, _, err := n.resolveHandle(r)
	if err != nil {
		return nfsErrorReplyWcc(nfs3ErrStale), nil
	}

	// offset and count
	_, _ = r.readUint64()
	_, _ = r.readUint32()

	meta, _ := n.fs.Stat(ctx, ino)

	w := newXDRWriter()
	w.writeUint32(nfs3OK)
	n.writeWcc(w, nil, meta)
	w.writeFixedOpaque(n.writeVerifier[:])
	return w.Bytes(), nil
}
