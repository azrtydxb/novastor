package filer

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// stubNFSHandler implements NFSHandler with minimal stubs for testing.
type stubNFSHandler struct{}

func (s *stubNFSHandler) Stat(_ context.Context, ino uint64) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	// Root inode (1) is a directory, others are files.
	if ino == RootIno {
		return &InodeMeta{Ino: ino, Type: TypeDir, Mode: 0755, LinkCount: 2, ATime: now, MTime: now, CTime: now}, nil
	}
	return &InodeMeta{Ino: ino, Type: TypeFile, Mode: 0644, LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (s *stubNFSHandler) Lookup(_ context.Context, _ uint64, _ string) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: 10, Type: TypeFile, Mode: 0644, LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (s *stubNFSHandler) Mkdir(_ context.Context, _ uint64, _ string, mode uint32) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: 100, Type: TypeDir, Mode: mode, LinkCount: 2, ATime: now, MTime: now, CTime: now}, nil
}

func (s *stubNFSHandler) Create(_ context.Context, _ uint64, _ string, mode uint32) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: 101, Type: TypeFile, Mode: mode, LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (s *stubNFSHandler) Unlink(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Rmdir(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) ReadDir(_ context.Context, _ uint64) ([]*DirEntry, error) {
	return []*DirEntry{
		{Name: "file1.txt", Ino: 10, Type: TypeFile},
		{Name: "subdir", Ino: 11, Type: TypeDir},
	}, nil
}

func (s *stubNFSHandler) Read(_ context.Context, _ uint64, _ int64, length int64) ([]byte, error) {
	data := []byte("hello")
	if int64(len(data)) > length {
		data = data[:length]
	}
	return data, nil
}

func (s *stubNFSHandler) Write(_ context.Context, _ uint64, _ int64, data []byte) (int, error) {
	return len(data), nil
}

func (s *stubNFSHandler) Rename(_ context.Context, _ uint64, _ string, _ uint64, _ string) error {
	return nil
}

func (s *stubNFSHandler) Symlink(_ context.Context, _ uint64, _ string, target string) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: 200, Type: TypeSymlink, Target: target, Mode: 0777, LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (s *stubNFSHandler) Readlink(_ context.Context, _ uint64) (string, error) {
	return "/some/target", nil
}

func (s *stubNFSHandler) Truncate(_ context.Context, _ uint64, _ int64) error {
	return nil
}

func (s *stubNFSHandler) Link(_ context.Context, targetIno uint64, _ uint64, _ string) (*InodeMeta, error) {
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: targetIno, Type: TypeFile, Mode: 0644, LinkCount: 2, ATime: now, MTime: now, CTime: now}, nil
}

// waitForAddr polls until the server has a non-nil address or the timeout expires.
func waitForAddr(srv *NFSServer, timeout time.Duration) net.Addr {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != nil {
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil
}

// buildRPCCall constructs a complete ONC/RPC call message for testing.
func buildRPCCall(xid, prog, vers, proc uint32, payload []byte) []byte {
	w := newXDRWriter()
	w.writeUint32(xid)
	w.writeUint32(rpcCall)    // msg_type = CALL
	w.writeUint32(rpcVersion) // rpc_vers = 2
	w.writeUint32(prog)
	w.writeUint32(vers)
	w.writeUint32(proc)
	// AUTH_NONE credential
	w.writeUint32(authNone)
	w.writeOpaque(nil) // empty cred body
	// AUTH_NONE verifier
	w.writeUint32(authNone)
	w.writeOpaque(nil) // empty verf body
	// procedure-specific payload
	if payload != nil {
		w.buf = append(w.buf, payload...)
	}
	return w.Bytes()
}

// sendRPCRecord wraps data in ONC/RPC record marking and sends it.
func sendRPCRecord(t *testing.T, conn net.Conn, data []byte) {
	t.Helper()
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data))|0x80000000)
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("failed to write record header: %v", err)
	}
	if _, err := conn.Write(data); err != nil {
		t.Fatalf("failed to write record data: %v", err)
	}
}

// recvRPCRecord reads a single ONC/RPC record from the connection.
func recvRPCRecord(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	var result []byte
	for {
		var hdr [4]byte
		if _, err := readFull(conn, hdr[:]); err != nil {
			t.Fatalf("failed to read record header: %v", err)
		}
		marker := binary.BigEndian.Uint32(hdr[:])
		lastFragment := (marker & 0x80000000) != 0
		length := marker & 0x7FFFFFFF

		frag := make([]byte, length)
		if _, err := readFull(conn, frag); err != nil {
			t.Fatalf("failed to read record fragment: %v", err)
		}
		result = append(result, frag...)
		if lastFragment {
			return result
		}
	}
}

// readFull reads exactly len(buf) bytes from r.
func readFull(r net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func startTestServer(t *testing.T) (*NFSServer, net.Addr) {
	t.Helper()
	srv := NewNFSServer(&stubNFSHandler{}, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	return srv, addr
}

func TestNFSServer_StartStop(t *testing.T) {
	srv := NewNFSServer(&stubNFSHandler{}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("expected to connect to NFS server at %s: %v", addr, err)
	}
	conn.Close()

	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected error stopping server: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("Serve returned (expected after stop): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
}

func TestNFSServer_AcceptConnection(t *testing.T) {
	srv := NewNFSServer(&stubNFSHandler{}, nil)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect to NFS server: %v", err)
	}
	conn.Close()

	time.Sleep(50 * time.Millisecond)

	if err := srv.Stop(); err != nil {
		t.Fatalf("unexpected error stopping server: %v", err)
	}

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
}

func TestNFSServer_NullProc(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// NFS NULL procedure (procedure 0).
	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcNull, nil)
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	xid, _ := r.readUint32()
	if xid != 1 {
		t.Errorf("expected xid 1, got %d", xid)
	}
	msgType, _ := r.readUint32()
	if msgType != rpcReply {
		t.Errorf("expected REPLY (1), got %d", msgType)
	}
	replyStat, _ := r.readUint32()
	if replyStat != msgAccepted {
		t.Errorf("expected MSG_ACCEPTED (0), got %d", replyStat)
	}
}

func TestNFSServer_MountExport(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// MOUNT EXPORT procedure.
	call := buildRPCCall(2, mountProg, mountVersion, mountProcExport, nil)
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	xid, _ := r.readUint32()
	if xid != 2 {
		t.Errorf("expected xid 2, got %d", xid)
	}
	// Skip msg_type, reply_stat, verifier, accept_stat.
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	// Export list: first bool = value_follows.
	follows, _ := r.readBool()
	if !follows {
		t.Fatal("expected export entry")
	}
	exportPath, _ := r.readString()
	if exportPath != "/" {
		t.Errorf("expected export path '/', got %q", exportPath)
	}
}

func TestNFSServer_MountMnt(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Build MNT payload: directory path string.
	pw := newXDRWriter()
	pw.writeString("/")
	call := buildRPCCall(3, mountProg, mountVersion, mountProcMnt, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	// MNT reply: status.
	mntStatus, _ := r.readUint32()
	if mntStatus != mntOK {
		t.Fatalf("expected MNT3_OK (0), got %d", mntStatus)
	}

	// File handle.
	fh, _ := r.readOpaque()
	if len(fh) != nfsHandleSize {
		t.Errorf("expected handle size %d, got %d", nfsHandleSize, len(fh))
	}
}

func TestNFSServer_Getattr(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Get root handle.
	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build GETATTR payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	call := buildRPCCall(4, nfsProg, nfsVersion, nfsProcGetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	// GETATTR reply: status.
	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// fattr3: ftype3.
	ftype, _ := r.readUint32()
	if ftype != nf3Dir {
		t.Errorf("expected NF3DIR (%d), got %d", nf3Dir, ftype)
	}

	// mode.
	mode, _ := r.readUint32()
	if mode != 0755 {
		t.Errorf("expected mode 0755, got %o", mode)
	}
}

func TestNFSServer_Lookup(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build LOOKUP payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("file1.txt")
	call := buildRPCCall(5, nfsProg, nfsVersion, nfsProcLookup, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Object file handle.
	objFH, _ := r.readOpaque()
	if len(objFH) != nfsHandleSize {
		t.Errorf("expected handle size %d, got %d", nfsHandleSize, len(objFH))
	}
}

func TestNFSServer_ReadDir(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build READDIR payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeUint64(0)                    // cookie (start from beginning)
	pw.writeFixedOpaque(make([]byte, 8)) // cookieverf
	pw.writeUint32(4096)                 // dircount
	call := buildRPCCall(6, nfsProg, nfsVersion, nfsProcReadDir, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Skip post_op_attr.
	attrFollows, _ := r.readBool()
	if attrFollows {
		// Skip fattr3 (21 fields: ftype + mode + nlink + uid + gid + size + used +
		// specdata(2) + fsid + fileid + atime(2) + mtime(2) + ctime(2) = 84 bytes).
		for i := 0; i < 21; i++ {
			r.readUint32()
		}
	}

	// cookieverf3 (8 bytes).
	r.readFixedOpaque(8)

	// Count entries.
	var names []string
	for {
		follows, _ := r.readBool()
		if !follows {
			break
		}
		// fileid3.
		r.readUint64()
		// filename3.
		name, _ := r.readString()
		names = append(names, name)
		// cookie3.
		r.readUint64()
	}

	// Should have ".", "..", "file1.txt", "subdir" = 4 entries.
	if len(names) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(names), names)
	}
	if names[0] != "." {
		t.Errorf("expected first entry '.', got %q", names[0])
	}
	if names[1] != ".." {
		t.Errorf("expected second entry '..', got %q", names[1])
	}
	if names[2] != "file1.txt" {
		t.Errorf("expected third entry 'file1.txt', got %q", names[2])
	}
	if names[3] != "subdir" {
		t.Errorf("expected fourth entry 'subdir', got %q", names[3])
	}
}

func TestNFSServer_Create(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build CREATE payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("newfile.txt")
	pw.writeUint32(createUnchecked) // createhow3 = UNCHECKED
	// sattr3.
	pw.writeBool(true)   // set_mode
	pw.writeUint32(0644) // mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(false)  // set_size
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	call := buildRPCCall(7, nfsProg, nfsVersion, nfsProcCreate, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// post_op_fh3: handle follows.
	handleFollows, _ := r.readBool()
	if !handleFollows {
		t.Fatal("expected file handle in create response")
	}
	fh, _ := r.readOpaque()
	if len(fh) != nfsHandleSize {
		t.Errorf("expected handle size %d, got %d", nfsHandleSize, len(fh))
	}
}

func TestNFSServer_ReadWrite(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// WRITE.
	writeData := []byte("hello NFS v3")
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeUint64(0)                      // offset
	pw.writeUint32(uint32(len(writeData))) // count
	pw.writeUint32(writeFileSync)          // stable
	pw.writeOpaque(writeData)              // data
	call := buildRPCCall(8, nfsProg, nfsVersion, nfsProcWrite, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("write: expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("write: expected NFS3_OK, got %d", nfsStatus)
	}

	// READ.
	pw2 := newXDRWriter()
	pw2.writeOpaque(rootHandle)
	pw2.writeUint64(0)    // offset
	pw2.writeUint32(1024) // count
	call2 := buildRPCCall(9, nfsProg, nfsVersion, nfsProcRead, pw2.Bytes())
	sendRPCRecord(t, conn, call2)
	reply2 := recvRPCRecord(t, conn)

	r2 := newXDRReader(reply2)
	r2.readUint32() // xid
	r2.readUint32() // msg_type
	r2.readUint32() // reply_stat
	r2.readUint32() // verifier flavor
	r2.readOpaque() // verifier body
	acceptStat2, _ := r2.readUint32()
	if acceptStat2 != acceptSuccess {
		t.Fatalf("read: expected SUCCESS, got %d", acceptStat2)
	}

	nfsStatus2, _ := r2.readUint32()
	if nfsStatus2 != nfs3OK {
		t.Fatalf("read: expected NFS3_OK, got %d", nfsStatus2)
	}
}

func TestNFSServer_FsInfo(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	call := buildRPCCall(10, nfsProg, nfsVersion, nfsProcFsInfo, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}
}

func TestNFSServer_FsStat(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	call := buildRPCCall(11, nfsProg, nfsVersion, nfsProcFsStat, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}
}

func TestNFSServer_MountNull(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	call := buildRPCCall(12, mountProg, mountVersion, mountProcNull, nil)
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	xid, _ := r.readUint32()
	if xid != 12 {
		t.Errorf("expected xid 12, got %d", xid)
	}
}

func TestNFSServer_ProgUnavail(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send a call to a non-existent program (99999).
	call := buildRPCCall(20, 99999, 1, 0, nil)
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptProgUnavail {
		t.Errorf("expected PROG_UNAVAIL (%d), got %d", acceptProgUnavail, acceptStat)
	}
}

func TestNFSServer_ProgMismatch(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send NFS program with wrong version (99).
	call := buildRPCCall(21, nfsProg, 99, 0, nil)
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptProgMismatch {
		t.Errorf("expected PROG_MISMATCH (%d), got %d", acceptProgMismatch, acceptStat)
	}
}

func TestNFSServer_MultipleRPCs(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Send multiple RPCs on the same connection.
	for i := uint32(1); i <= 5; i++ {
		pw := newXDRWriter()
		pw.writeOpaque(rootHandle)
		call := buildRPCCall(i, nfsProg, nfsVersion, nfsProcGetattr, pw.Bytes())
		sendRPCRecord(t, conn, call)
		reply := recvRPCRecord(t, conn)

		r := newXDRReader(reply)
		xid, _ := r.readUint32()
		if xid != i {
			t.Errorf("iteration %d: expected xid %d, got %d", i, i, xid)
		}
	}
}

func TestNFSServer_Mkdir(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("testdir")
	// sattr3
	pw.writeBool(true)   // set_mode
	pw.writeUint32(0755) // mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(false)  // set_size
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	call := buildRPCCall(13, nfsProg, nfsVersion, nfsProcMkdir, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// post_op_fh3: handle follows.
	handleFollows, _ := r.readBool()
	if !handleFollows {
		t.Fatal("expected directory handle in mkdir response")
	}
	fh, _ := r.readOpaque()
	if len(fh) != nfsHandleSize {
		t.Errorf("expected handle size %d, got %d", nfsHandleSize, len(fh))
	}
}

func TestNFSServer_PathConf(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	call := buildRPCCall(14, nfsProg, nfsVersion, nfsProcPathConf, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}
}

func TestNFSServer_Access(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeUint32(access3Read | access3Lookup | access3Modify | access3Extend | access3Delete | access3Execute)
	call := buildRPCCall(15, nfsProg, nfsVersion, nfsProcAccess, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}
}

func TestNFSServer_Setattr(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build SETATTR payload: handle + sattr3.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	// sattr3
	pw.writeBool(true)   // set_mode
	pw.writeUint32(0755) // mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(false)  // set_size
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3 - skip (not validated in our implementation)

	call := buildRPCCall(16, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Check wcc_data was returned
	preOpFollows, _ := r.readBool()
	postOpFollows, _ := r.readBool()
	if !preOpFollows || !postOpFollows {
		t.Logf("wcc_data: pre_op_follows=%v, post_op_follows=%v", preOpFollows, postOpFollows)
	}
}

func TestNFSServer_Remove(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build REMOVE payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("file-to-remove.txt")
	call := buildRPCCall(17, nfsProg, nfsVersion, nfsProcRemove, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK && nfsStatus != nfs3ErrNoEnt {
		t.Fatalf("expected NFS3_OK or NFS3_ERR_NOENT, got %d", nfsStatus)
	}
}

func TestNFSServer_Rmdir(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build RMDIR payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("dir-to-remove")
	call := buildRPCCall(18, nfsProg, nfsVersion, nfsProcRmdir, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK && nfsStatus != nfs3ErrNoEnt {
		t.Fatalf("expected NFS3_OK or NFS3_ERR_NOENT, got %d", nfsStatus)
	}
}

func TestNFSServer_Rename(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build RENAME payload: fromdir + fromname + todir + toname.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)    // fromdir
	pw.writeString("oldname.txt") // fromname
	pw.writeOpaque(rootHandle)    // todir
	pw.writeString("newname.txt") // toname
	call := buildRPCCall(19, nfsProg, nfsVersion, nfsProcRename, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK && nfsStatus != nfs3ErrNoEnt {
		t.Fatalf("expected NFS3_OK or NFS3_ERR_NOENT, got %d", nfsStatus)
	}

	// Should have two wcc_data responses (fromdir and todir)
}

func TestNFSServer_Symlink(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build SYMLINK payload: dirname + name + sattr3 + target.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeString("link-name")
	// sattr3 (mostly ignored for symlinks)
	pw.writeBool(false) // set_mode
	pw.writeBool(false) // set_uid
	pw.writeBool(false) // set_gid
	pw.writeBool(false) // set_size
	pw.writeUint32(0)   // set_atime
	pw.writeUint32(0)   // set_mtime
	// symlink data
	pw.writeString("/target/path")
	call := buildRPCCall(20, nfsProg, nfsVersion, nfsProcSymlink, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// post_op_fh3: handle follows
	handleFollows, _ := r.readBool()
	if !handleFollows {
		t.Fatal("expected file handle in symlink response")
	}
}

func TestNFSServer_Readlink(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build READLINK payload: handle (use root handle for test)
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	call := buildRPCCall(21, nfsProg, nfsVersion, nfsProcReadlink, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	// May get NFS3_ERR_INVAL if root is not a symlink
	if nfsStatus != nfs3OK && nfsStatus != nfs3ErrInval {
		t.Fatalf("expected NFS3_OK or NFS3_ERR_INVAL, got %d", nfsStatus)
	}
}

func TestNFSServer_ReadDirPlus(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build READDIRPLUS payload.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeUint64(0)                    // cookie (start from beginning)
	pw.writeFixedOpaque(make([]byte, 8)) // cookieverf
	pw.writeUint32(4096)                 // dircount
	pw.writeUint32(4096)                 // maxcount
	call := buildRPCCall(22, nfsProg, nfsVersion, nfsProcReadDirPlus, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Skip post_op_attr
	attrFollows, _ := r.readBool()
	if attrFollows {
		// Skip fattr3 (21 fields)
		for i := 0; i < 21; i++ {
			r.readUint32()
		}
	}

	// cookieverf3 (8 bytes)
	r.readFixedOpaque(8)

	// Count entries with full attributes
	var names []string
	for {
		follows, _ := r.readBool()
		if !follows {
			break
		}
		// fileid3
		r.readUint64()
		// filename3
		name, _ := r.readString()
		names = append(names, name)
		// cookie3
		r.readUint64()
		// name_attributes (post_op_attr)
		attrFollows, _ := r.readBool()
		if attrFollows {
			for i := 0; i < 21; i++ {
				r.readUint32()
			}
		}
		// name_handle (post_op_fh3)
		handleFollows, _ := r.readBool()
		if handleFollows {
			r.readOpaque()
		}
	}

	// Should have ".", "..", "file1.txt", "subdir" = 4 entries.
	if len(names) != 4 {
		t.Fatalf("expected 4 entries, got %d: %v", len(names), names)
	}
	if names[0] != "." {
		t.Errorf("expected first entry '.', got %q", names[0])
	}
	if names[1] != ".." {
		t.Errorf("expected second entry '..', got %q", names[1])
	}
}

func TestNFSServer_Commit(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := srv.handles.getOrCreateHandle(RootIno)

	// Build COMMIT payload: handle + offset + count.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	pw.writeUint64(0)    // offset
	pw.writeUint32(1024) // count
	call := buildRPCCall(23, nfsProg, nfsVersion, nfsProcCommit, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// wcc_data should follow
	// Then 8-byte write verifier
	_, _ = r.readFixedOpaque(8)
}

func TestNFSServer_Mknod(t *testing.T) {
	_, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootHandle := newHandleManager()
	rootHandle.getOrCreateHandle(RootIno)
	rootFH := rootHandle.getOrCreateHandle(RootIno)

	// Build MKNOD payload - should return error as we don't support device files.
	pw := newXDRWriter()
	pw.writeOpaque(rootFH)
	pw.writeString("dev-node")
	pw.writeUint32(1) // ftype3: NF3REG
	// sattr3 would follow, but we just want to test that the procedure is handled
	call := buildRPCCall(24, nfsProg, nfsVersion, nfsProcMknod, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	// MKNOD is not supported, should return an error
	if nfsStatus == nfs3OK {
		t.Error("expected error for MKNOD (not supported), got NFS3_OK")
	}
}

func TestNFSServer_Link(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootFH := srv.handles.getOrCreateHandle(RootIno)
	// Use a non-root inode (file)
	fileIno := uint64(100)
	fileFH := srv.handles.getOrCreateHandle(fileIno)

	// Build LINK payload - should succeed now that hard links are supported.
	pw := newXDRWriter()
	pw.writeOpaque(fileFH) // target file handle (file, not directory)
	pw.writeOpaque(rootFH) // dir handle
	pw.writeString("link-name")
	call := buildRPCCall(25, nfsProg, nfsVersion, nfsProcLink, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// post_op_attr for target file
	attrFollows, _ := r.readBool()
	if !attrFollows {
		t.Error("expected post_op_attr in link response")
	}

	// dir_wcc_data for the directory
	preOpFollows, _ := r.readBool()
	postOpFollows, _ := r.readBool()
	if !preOpFollows || !postOpFollows {
		t.Logf("dir_wcc_data: pre_op_follows=%v, post_op_follows=%v", preOpFollows, postOpFollows)
	}
}

func TestNFSServer_LinkToFile(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a file first
	rootFH := srv.handles.getOrCreateHandle(RootIno)
	fileIno := uint64(123)
	fileFH := srv.handles.getOrCreateHandle(fileIno)

	// Build LINK payload to create a hard link to the file
	pw := newXDRWriter()
	pw.writeOpaque(fileFH) // target file handle
	pw.writeOpaque(rootFH) // dir handle (root)
	pw.writeString("hardlink-to-file")
	call := buildRPCCall(30, nfsProg, nfsVersion, nfsProcLink, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK for file hard link, got %d", nfsStatus)
	}

	// Verify post_op_attr contains file attributes
	attrFollows, _ := r.readBool()
	if !attrFollows {
		t.Fatal("expected post_op_attr in link response")
	}

	// Skip fattr3 fields (21 fields)
	for i := 0; i < 21; i++ {
		r.readUint32()
	}

	// dir_wcc_data
	preOpFollows, _ := r.readBool()
	postOpFollows, _ := r.readBool()
	t.Logf("dir_wcc_data: pre_op_follows=%v, post_op_follows=%v", preOpFollows, postOpFollows)
}

func TestNFSServer_LinkNameTooLong(t *testing.T) {
	srv, addr := startTestServer(t)

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rootFH := srv.handles.getOrCreateHandle(RootIno)

	// Create a link name that exceeds NFS_MAXNAME (255 bytes)
	longName := make([]byte, 256)
	for i := range longName {
		longName[i] = 'a'
	}

	pw := newXDRWriter()
	pw.writeOpaque(rootFH)
	pw.writeOpaque(rootFH)
	// Write a long string - the XDR writer should handle this
	// but we'll test the server's behavior
	// For this test, we'll use a normal name since XDR handles string encoding
	pw.writeString(string(longName))
	call := buildRPCCall(31, nfsProg, nfsVersion, nfsProcLink, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	// Server should either accept it or return NFS3_ERR_NAMETOOLONG
	nfsStatus, _ := r.readUint32()
	t.Logf("Link with long name returned status %d", nfsStatus)
}

// TestNFSServer_AllProcedureDispatches verifies that all 22 NFS v3 procedures
// are handled correctly (either implemented or intentionally unsupported).
func TestNFSServer_AllProcedureDispatches(t *testing.T) {
	_, addr := startTestServer(t)

	// Procedures 0-21 should all be recognized
	for proc := uint32(0); proc <= 21; proc++ {
		conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
		if err != nil {
			t.Fatalf("proc %d: failed to connect: %v", proc, err)
		}

		rootHandle := newHandleManager()
		rootHandle.getOrCreateHandle(RootIno)
		rootFH := rootHandle.getOrCreateHandle(RootIno)

		// Build minimal valid payload for each procedure type
		var payload []byte
		switch proc {
		case nfsProcNull:
			payload = nil
		case nfsProcGetattr, nfsProcAccess, nfsProcReadlink, nfsProcFsStat, nfsProcFsInfo, nfsProcPathConf, nfsProcCommit:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			if proc == nfsProcAccess {
				pw.writeUint32(0x3F) // all access bits
			}
			if proc == nfsProcCommit {
				pw.writeUint64(0)
				pw.writeUint32(0)
			}
			payload = pw.Bytes()
		case nfsProcSetattr:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeBool(false) // set_mode
			pw.writeBool(false) // set_uid
			pw.writeBool(false) // set_gid
			pw.writeBool(false) // set_size
			pw.writeUint32(0)   // set_atime
			pw.writeUint32(0)   // set_mtime
			payload = pw.Bytes()
		case nfsProcLookup, nfsProcRemove, nfsProcRmdir:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeString("test")
			payload = pw.Bytes()
		case nfsProcRead:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeUint64(0)
			pw.writeUint32(1024)
			payload = pw.Bytes()
		case nfsProcWrite:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeUint64(0)
			pw.writeUint32(0)
			pw.writeUint32(writeFileSync)
			pw.writeOpaque([]byte{})
			payload = pw.Bytes()
		case nfsProcCreate, nfsProcMkdir, nfsProcSymlink:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeString("test")
			// Minimal sattr3
			pw.writeBool(false)
			pw.writeBool(false)
			pw.writeBool(false)
			pw.writeBool(false)
			pw.writeUint32(0)
			pw.writeUint32(0)
			if proc == nfsProcSymlink {
				pw.writeString("target")
			} else if proc == nfsProcCreate {
				pw.writeUint32(createUnchecked)
			}
			payload = pw.Bytes()
		case nfsProcMknod, nfsProcLink:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeString("test")
			payload = pw.Bytes()
		case nfsProcRename:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeString("old")
			pw.writeOpaque(rootFH)
			pw.writeString("new")
			payload = pw.Bytes()
		case nfsProcReadDir, nfsProcReadDirPlus:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			pw.writeUint64(0)
			pw.writeFixedOpaque(make([]byte, 8))
			pw.writeUint32(4096)
			if proc == nfsProcReadDirPlus {
				pw.writeUint32(4096)
			}
			payload = pw.Bytes()
		default:
			pw := newXDRWriter()
			pw.writeOpaque(rootFH)
			payload = pw.Bytes()
		}

		call := buildRPCCall(proc, nfsProg, nfsVersion, proc, payload)
		sendRPCRecord(t, conn, call)
		reply := recvRPCRecord(t, conn)

		// Verify we get a valid reply (not a dropped connection)
		r := newXDRReader(reply)
		xid, _ := r.readUint32()
		if xid != proc {
			conn.Close()
			t.Errorf("proc %d: expected xid %d, got %d", proc, proc, xid)
			continue
		}

		msgType, _ := r.readUint32()
		if msgType != rpcReply {
			conn.Close()
			t.Errorf("proc %d: expected REPLY (1), got %d", proc, msgType)
			continue
		}

		conn.Close()
	}
}

// truncationTestHandler implements NFSHandler with a real in-memory file
// that supports truncation for testing.
type truncationTestHandler struct {
	mu       sync.Mutex
	files    map[uint64][]byte
	nextIno  uint64
	rootMeta *InodeMeta
}

func newTruncationTestHandler() *truncationTestHandler {
	now := time.Now().UnixNano()
	return &truncationTestHandler{
		files: make(map[uint64][]byte),
		nextIno: 2,
		rootMeta: &InodeMeta{Ino: 1, Type: TypeDir, Mode: 0755, LinkCount: 2, ATime: now, MTime: now, CTime: now},
	}
}

func (h *truncationTestHandler) Stat(_ context.Context, ino uint64) (*InodeMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ino == 1 {
		return h.rootMeta, nil
	}
	data, ok := h.files[ino]
	if !ok {
		return nil, fmt.Errorf("no such inode")
	}
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: ino, Type: TypeFile, Mode: 0644, Size: int64(len(data)), LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (h *truncationTestHandler) Lookup(_ context.Context, parentIno uint64, name string) (*InodeMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if parentIno != 1 {
		return nil, fmt.Errorf("not found")
	}
	// For testing, name "file1" maps to inode 2, "file2" to inode 3
	ino := uint64(0)
	if name == "file1" {
		ino = 2
	} else if name == "file2" {
		ino = 3
	}
	if ino == 0 {
		return nil, fmt.Errorf("not found")
	}
	data, ok := h.files[ino]
	if !ok {
		// Create empty file on lookup
		h.files[ino] = []byte{}
		data = []byte{}
	}
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: ino, Type: TypeFile, Mode: 0644, Size: int64(len(data)), LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (h *truncationTestHandler) Mkdir(_ context.Context, _ uint64, _ string, _ uint32) (*InodeMeta, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *truncationTestHandler) Create(_ context.Context, _ uint64, _ string, _ uint32) (*InodeMeta, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ino := h.nextIno
	h.nextIno++
	h.files[ino] = []byte{}
	now := time.Now().UnixNano()
	return &InodeMeta{Ino: ino, Type: TypeFile, Mode: 0644, Size: 0, LinkCount: 1, ATime: now, MTime: now, CTime: now}, nil
}

func (h *truncationTestHandler) Unlink(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (h *truncationTestHandler) Rmdir(_ context.Context, _ uint64, _ string) error {
	return nil
}

func (h *truncationTestHandler) ReadDir(_ context.Context, _ uint64) ([]*DirEntry, error) {
	return []*DirEntry{}, nil
}

func (h *truncationTestHandler) Read(_ context.Context, ino uint64, offset, length int64) ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	data, ok := h.files[ino]
	if !ok {
		return nil, fmt.Errorf("no such file")
	}
	if offset >= int64(len(data)) {
		return []byte{}, nil
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[offset:end], nil
}

func (h *truncationTestHandler) Write(_ context.Context, ino uint64, offset int64, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fileData, ok := h.files[ino]
	if !ok {
		return 0, fmt.Errorf("no such file")
	}
	// Extend file if needed
	newSize := offset + int64(len(data))
	if newSize > int64(len(fileData)) {
		newData := make([]byte, newSize)
		copy(newData, fileData)
		fileData = newData
	}
	copy(fileData[offset:], data)
	h.files[ino] = fileData
	return len(data), nil
}

func (h *truncationTestHandler) Rename(_ context.Context, _ uint64, _ string, _ uint64, _ string) error {
	return nil
}

func (h *truncationTestHandler) Symlink(_ context.Context, _ uint64, _ string, _ string) (*InodeMeta, error) {
	return nil, fmt.Errorf("not implemented")
}

func (h *truncationTestHandler) Readlink(_ context.Context, _ uint64) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (h *truncationTestHandler) Truncate(_ context.Context, ino uint64, size int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.files[ino]
	if !ok {
		return fmt.Errorf("no such file")
	}
	if size < 0 {
		return fmt.Errorf("invalid size")
	}
	newData := make([]byte, size)
	copy(newData, h.files[ino])
	h.files[ino] = newData
	return nil
}

// TestNFSServer_SetattrTruncateToZero tests truncating a file to size 0.
func TestNFSServer_SetattrTruncateToZero(t *testing.T) {
	handler := newTruncationTestHandler()
	// Pre-populate a file with content
	handler.files[2] = []byte("hello world, this is some data")

	srv := NewNFSServer(handler, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	fileHandle := srv.handles.getOrCreateHandle(2)

	// Build SETATTR payload to truncate to size 0.
	pw := newXDRWriter()
	pw.writeOpaque(fileHandle)
	// sattr3
	pw.writeBool(false)  // set_mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(true)   // set_size = true
	pw.writeUint64(0)    // new size = 0
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3 - check flag (false = no check)
	pw.writeBool(false)

	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Verify the file was truncated
	data, ok := handler.files[2]
	if !ok {
		t.Fatal("file was deleted")
	}
	if len(data) != 0 {
		t.Errorf("expected file size 0, got %d", len(data))
	}
}

// TestNFSServer_SetattrTruncateSmaller tests truncating a file to a smaller size.
func TestNFSServer_SetattrTruncateSmaller(t *testing.T) {
	handler := newTruncationTestHandler()
	// Pre-populate a file with content
	handler.files[2] = []byte("hello world, this is some data") // 31 bytes

	srv := NewNFSServer(handler, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	fileHandle := srv.handles.getOrCreateHandle(2)

	// Build SETATTR payload to truncate to size 5.
	pw := newXDRWriter()
	pw.writeOpaque(fileHandle)
	// sattr3
	pw.writeBool(false)  // set_mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(true)   // set_size = true
	pw.writeUint64(5)    // new size = 5
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3
	pw.writeBool(false)

	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Verify the file was truncated to 5 bytes
	data, ok := handler.files[2]
	if !ok {
		t.Fatal("file was deleted")
	}
	if len(data) != 5 {
		t.Errorf("expected file size 5, got %d", len(data))
	}
	expected := "hello"
	if string(data) != expected {
		t.Errorf("expected content %q, got %q", expected, string(data))
	}
}

// TestNFSServer_SetattrTruncateLarger tests truncating a file to a larger size (extending with zeros).
func TestNFSServer_SetattrTruncateLarger(t *testing.T) {
	handler := newTruncationTestHandler()
	// Pre-populate a file with content
	handler.files[2] = []byte("hello") // 5 bytes

	srv := NewNFSServer(handler, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	fileHandle := srv.handles.getOrCreateHandle(2)

	// Build SETATTR payload to extend to size 10.
	pw := newXDRWriter()
	pw.writeOpaque(fileHandle)
	// sattr3
	pw.writeBool(false)  // set_mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(true)   // set_size = true
	pw.writeUint64(10)   // new size = 10
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3
	pw.writeBool(false)

	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Verify the file was extended to 10 bytes
	data, ok := handler.files[2]
	if !ok {
		t.Fatal("file was deleted")
	}
	if len(data) != 10 {
		t.Errorf("expected file size 10, got %d", len(data))
	}
	// First 5 bytes should be "hello", rest should be zeros
	if string(data[:5]) != "hello" {
		t.Errorf("expected first 5 bytes to be 'hello', got %q", string(data[:5]))
	}
	for i := 5; i < 10; i++ {
		if data[i] != 0 {
			t.Errorf("expected byte %d to be 0, got %d", i, data[i])
		}
	}
}

// TestNFSServer_SetattrTruncateWithMode tests truncating a file while also changing mode.
func TestNFSServer_SetattrTruncateWithMode(t *testing.T) {
	handler := newTruncationTestHandler()
	handler.files[2] = []byte("hello world") // 11 bytes

	srv := NewNFSServer(handler, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	fileHandle := srv.handles.getOrCreateHandle(2)

	// Build SETATTR payload to truncate to size 0 and change mode.
	pw := newXDRWriter()
	pw.writeOpaque(fileHandle)
	// sattr3
	pw.writeBool(true)   // set_mode = true
	pw.writeUint32(0600) // new mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(true)   // set_size = true
	pw.writeUint64(0)    // new size = 0
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3
	pw.writeBool(false)

	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3OK {
		t.Fatalf("expected NFS3_OK, got %d", nfsStatus)
	}

	// Verify the file was truncated
	data, ok := handler.files[2]
	if !ok {
		t.Fatal("file was deleted")
	}
	if len(data) != 0 {
		t.Errorf("expected file size 0, got %d", len(data))
	}
}

// TestNFSServer_SetattrTruncateDirectory tests that truncating a directory returns an error.
func TestNFSServer_SetattrTruncateDirectory(t *testing.T) {
	handler := newTruncationTestHandler()

	srv := NewNFSServer(handler, nil)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve("127.0.0.1:0")
	}()

	addr := waitForAddr(srv, 2*time.Second)
	if addr == nil {
		t.Fatal("server did not start listening within timeout")
	}
	t.Cleanup(func() {
		srv.Stop()
		<-errCh
	})

	conn, err := net.DialTimeout("tcp", addr.String(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Use root handle (a directory)
	rootHandle := srv.handles.getOrCreateHandle(1)

	// Build SETATTR payload to attempt truncating a directory.
	pw := newXDRWriter()
	pw.writeOpaque(rootHandle)
	// sattr3
	pw.writeBool(false)  // set_mode
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(true)   // set_size = true
	pw.writeUint64(0)    // new size = 0
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	// sattrguard3
	pw.writeBool(false)

	call := buildRPCCall(1, nfsProg, nfsVersion, nfsProcSetattr, pw.Bytes())
	sendRPCRecord(t, conn, call)
	reply := recvRPCRecord(t, conn)

	r := newXDRReader(reply)
	r.readUint32() // xid
	r.readUint32() // msg_type
	r.readUint32() // reply_stat
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat, _ := r.readUint32()
	if acceptStat != acceptSuccess {
		t.Fatalf("expected SUCCESS, got %d", acceptStat)
	}

	nfsStatus, _ := r.readUint32()
	if nfsStatus != nfs3ErrInval {
		t.Fatalf("expected NFS3_ERR_INVAL for directory truncate, got %d", nfsStatus)
	}
}
