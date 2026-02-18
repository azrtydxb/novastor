//go:build e2e

package e2e

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/piwi3910/novastor/internal/filer"
)

// ONC/RPC and NFS v3 protocol constants replicated from internal/filer for the
// E2E test (the internal helpers are not exported).
const (
	// ONC/RPC message types.
	e2eRPCCall    uint32 = 0
	e2eRPCReply   uint32 = 1
	e2eRPCVersion uint32 = 2

	// ONC/RPC accept stat.
	e2eMsgAccepted    uint32 = 0
	e2eAcceptSuccess  uint32 = 0
	e2eAuthNone       uint32 = 0

	// MOUNT v3 program.
	e2eMountProg    uint32 = 100005
	e2eMountVersion uint32 = 3
	e2eMountProcMnt uint32 = 1

	// MOUNT v3 status.
	e2eMntOK uint32 = 0

	// NFS v3 program.
	e2eNFSProg    uint32 = 100003
	e2eNFSVersion uint32 = 3

	// NFS v3 procedures.
	e2eNFSProcGetattr  uint32 = 1
	e2eNFSProcLookup   uint32 = 3
	e2eNFSProcRead     uint32 = 6
	e2eNFSProcWrite    uint32 = 7
	e2eNFSProcCreate   uint32 = 8
	e2eNFSProcMkdir    uint32 = 9
	e2eNFSProcRemove   uint32 = 12
	e2eNFSProcRmdir    uint32 = 13
	e2eNFSProcReadDir  uint32 = 16

	// NFS v3 status codes.
	e2eNFS3OK uint32 = 0

	// NFS v3 file types (ftype3).
	e2eNF3Reg uint32 = 1
	e2eNF3Dir uint32 = 2

	// WRITE3 stable_how.
	e2eWriteFileSync uint32 = 2

	// CREATE3 createhow3.
	e2eCreateUnchecked uint32 = 0

	// NFS handle size (matches internal/filer).
	e2eNFSHandleSize = 32
)

// ---------------------------------------------------------------------------
// XDR writer (local copy of internal/filer.xdrWriter)
// ---------------------------------------------------------------------------

type e2eXDRWriter struct {
	buf []byte
}

func newE2EXDRWriter() *e2eXDRWriter {
	return &e2eXDRWriter{buf: make([]byte, 0, 4096)}
}

func (w *e2eXDRWriter) bytes() []byte { return w.buf }

func (w *e2eXDRWriter) writeUint32(v uint32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	w.buf = append(w.buf, b...)
}

func (w *e2eXDRWriter) writeUint64(v uint64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	w.buf = append(w.buf, b...)
}

func (w *e2eXDRWriter) writeBool(v bool) {
	if v {
		w.writeUint32(1)
	} else {
		w.writeUint32(0)
	}
}

func (w *e2eXDRWriter) writeOpaque(data []byte) {
	w.writeUint32(uint32(len(data)))
	w.buf = append(w.buf, data...)
	pad := (4 - len(data)%4) % 4
	for i := 0; i < pad; i++ {
		w.buf = append(w.buf, 0)
	}
}

func (w *e2eXDRWriter) writeFixedOpaque(data []byte) {
	w.buf = append(w.buf, data...)
	pad := (4 - len(data)%4) % 4
	for i := 0; i < pad; i++ {
		w.buf = append(w.buf, 0)
	}
}

func (w *e2eXDRWriter) writeString(s string) {
	w.writeOpaque([]byte(s))
}

// ---------------------------------------------------------------------------
// XDR reader (local copy of internal/filer.xdrReader)
// ---------------------------------------------------------------------------

type e2eXDRReader struct {
	data []byte
	pos  int
}

func newE2EXDRReader(data []byte) *e2eXDRReader {
	return &e2eXDRReader{data: data}
}

func (r *e2eXDRReader) readUint32() uint32 {
	v := binary.BigEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return v
}

func (r *e2eXDRReader) readUint64() uint64 {
	v := binary.BigEndian.Uint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return v
}

func (r *e2eXDRReader) readBool() bool {
	return r.readUint32() != 0
}

func (r *e2eXDRReader) readOpaque() []byte {
	length := r.readUint32()
	result := make([]byte, length)
	copy(result, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	pad := (4 - int(length)%4) % 4
	r.pos += pad
	return result
}

func (r *e2eXDRReader) readFixedOpaque(length int) []byte {
	result := make([]byte, length)
	copy(result, r.data[r.pos:r.pos+length])
	r.pos += length
	pad := (4 - length%4) % 4
	r.pos += pad
	return result
}

func (r *e2eXDRReader) readString() string {
	return string(r.readOpaque())
}

// skipFattr3 skips a complete fattr3 structure in the XDR stream.
// fattr3 consists of 21 uint32-sized fields:
//
//	ftype3(1) + mode(1) + nlink(1) + uid(1) + gid(1) + size(2) + used(2) +
//	specdata(2) + fsid(2) + fileid(2) + atime(2) + mtime(2) + ctime(2) = 21.
func (r *e2eXDRReader) skipFattr3() {
	r.pos += 21 * 4
}

// skipPostOpAttr skips an optional post_op_attr (bool + maybe fattr3).
func (r *e2eXDRReader) skipPostOpAttr() {
	if r.readBool() {
		r.skipFattr3()
	}
}

// skipWcc skips a wcc_data structure (pre_op_attr + post_op_attr).
func (r *e2eXDRReader) skipWcc() {
	// pre_op_attr: bool + optional pre_op_attr (size = uint64 = 2 x uint32,
	// mtime = 2 x uint32, ctime = 2 x uint32 => 24 bytes if present).
	if r.readBool() {
		r.pos += 24 // size(8) + mtime(8) + ctime(8)
	}
	// post_op_attr.
	r.skipPostOpAttr()
}

// ---------------------------------------------------------------------------
// ONC/RPC helpers
// ---------------------------------------------------------------------------

// buildE2ERPCCall constructs a complete ONC/RPC CALL message.
func buildE2ERPCCall(xid, prog, vers, proc uint32, payload []byte) []byte {
	w := newE2EXDRWriter()
	w.writeUint32(xid)
	w.writeUint32(e2eRPCCall)
	w.writeUint32(e2eRPCVersion)
	w.writeUint32(prog)
	w.writeUint32(vers)
	w.writeUint32(proc)
	// AUTH_NONE credential.
	w.writeUint32(e2eAuthNone)
	w.writeOpaque(nil)
	// AUTH_NONE verifier.
	w.writeUint32(e2eAuthNone)
	w.writeOpaque(nil)
	if payload != nil {
		w.buf = append(w.buf, payload...)
	}
	return w.bytes()
}

// sendE2ERPCRecord wraps data in ONC/RPC record marking and sends it.
func sendE2ERPCRecord(t *testing.T, conn net.Conn, data []byte) {
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

// recvE2ERPCRecord reads a single ONC/RPC record from the connection.
func recvE2ERPCRecord(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("failed to set read deadline: %v", err)
	}
	defer func() {
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			t.Fatalf("failed to clear read deadline: %v", err)
		}
	}()

	var result []byte
	for {
		var hdr [4]byte
		if _, err := readE2EFull(conn, hdr[:]); err != nil {
			t.Fatalf("failed to read record header: %v", err)
		}
		marker := binary.BigEndian.Uint32(hdr[:])
		lastFragment := (marker & 0x80000000) != 0
		length := marker & 0x7FFFFFFF

		frag := make([]byte, length)
		if _, err := readE2EFull(conn, frag); err != nil {
			t.Fatalf("failed to read record fragment: %v", err)
		}
		result = append(result, frag...)
		if lastFragment {
			return result
		}
	}
}

// readE2EFull reads exactly len(buf) bytes from conn.
func readE2EFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// RPC reply parsing helpers
// ---------------------------------------------------------------------------

// parseReplyHeader reads the ONC/RPC reply header (xid, msg_type, reply_stat,
// verifier, accept_stat) and returns the accept_stat. It fatals on any
// unexpected values.
func parseReplyHeader(t *testing.T, r *e2eXDRReader, expectedXID uint32) {
	t.Helper()
	xid := r.readUint32()
	if xid != expectedXID {
		t.Fatalf("xid mismatch: expected %d, got %d", expectedXID, xid)
	}
	msgType := r.readUint32()
	if msgType != e2eRPCReply {
		t.Fatalf("expected REPLY (1), got %d", msgType)
	}
	replyStat := r.readUint32()
	if replyStat != e2eMsgAccepted {
		t.Fatalf("expected MSG_ACCEPTED (0), got %d", replyStat)
	}
	// Skip verifier (flavor + opaque body).
	r.readUint32() // verifier flavor
	r.readOpaque() // verifier body
	acceptStat := r.readUint32()
	if acceptStat != e2eAcceptSuccess {
		t.Fatalf("expected SUCCESS (0), got %d", acceptStat)
	}
}

// ---------------------------------------------------------------------------
// NFS v3 RPC call helpers
// ---------------------------------------------------------------------------

// e2eNFSClient wraps a TCP connection to the NFS server for sending ONC/RPC
// encoded NFS v3 and MOUNT v3 requests.
type e2eNFSClient struct {
	conn   net.Conn
	nextID uint32
}

func newE2ENFSClient(t *testing.T, addr string) *e2eNFSClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to NFS server at %s: %v", addr, err)
	}
	return &e2eNFSClient{conn: conn, nextID: 1}
}

func (c *e2eNFSClient) close() {
	c.conn.Close()
}

func (c *e2eNFSClient) nextXID() uint32 {
	id := c.nextID
	c.nextID++
	return id
}

// mount sends a MOUNT MNT request for "/" and returns the root file handle.
func (c *e2eNFSClient) mount(t *testing.T) []byte {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeString("/")
	call := buildE2ERPCCall(xid, e2eMountProg, e2eMountVersion, e2eMountProcMnt, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	mntStatus := r.readUint32()
	if mntStatus != e2eMntOK {
		t.Fatalf("MOUNT MNT failed: status %d", mntStatus)
	}

	fh := r.readOpaque()
	if len(fh) != e2eNFSHandleSize {
		t.Fatalf("expected root handle size %d, got %d", e2eNFSHandleSize, len(fh))
	}
	t.Logf("MOUNT MNT successful, root handle acquired (%d bytes)", len(fh))
	return fh
}

// getattr sends NFS GETATTR and returns (ftype3, mode).
func (c *e2eNFSClient) getattr(t *testing.T, fh []byte) (ftype uint32, mode uint32) {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(fh)
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcGetattr, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("GETATTR failed: NFS status %d", nfsStatus)
	}

	ftype = r.readUint32()
	mode = r.readUint32()
	return ftype, mode
}

// lookup sends NFS LOOKUP and returns the object file handle.
func (c *e2eNFSClient) lookup(t *testing.T, dirFH []byte, name string) []byte {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeString(name)
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcLookup, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("LOOKUP %q failed: NFS status %d", name, nfsStatus)
	}

	objFH := r.readOpaque()
	if len(objFH) != e2eNFSHandleSize {
		t.Fatalf("LOOKUP %q: expected handle size %d, got %d", name, e2eNFSHandleSize, len(objFH))
	}
	return objFH
}

// create sends NFS CREATE (UNCHECKED) and returns the new file handle.
func (c *e2eNFSClient) create(t *testing.T, dirFH []byte, name string, mode uint32) []byte {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeString(name)
	pw.writeUint32(e2eCreateUnchecked) // createhow3
	// sattr3
	pw.writeBool(true)   // set_mode
	pw.writeUint32(mode) // mode value
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(false)  // set_size
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcCreate, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("CREATE %q failed: NFS status %d", name, nfsStatus)
	}

	// post_op_fh3: handle_follows bool + handle.
	handleFollows := r.readBool()
	if !handleFollows {
		t.Fatalf("CREATE %q: expected file handle in response", name)
	}
	fh := r.readOpaque()
	if len(fh) != e2eNFSHandleSize {
		t.Fatalf("CREATE %q: expected handle size %d, got %d", name, e2eNFSHandleSize, len(fh))
	}
	return fh
}

// writeFile sends NFS WRITE and returns the number of bytes written.
func (c *e2eNFSClient) writeFile(t *testing.T, fh []byte, offset uint64, data []byte) uint32 {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(fh)
	pw.writeUint64(offset)
	pw.writeUint32(uint32(len(data))) // count
	pw.writeUint32(e2eWriteFileSync)  // stable = FILE_SYNC
	pw.writeOpaque(data)              // data
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcWrite, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("WRITE failed: NFS status %d", nfsStatus)
	}

	// WRITE3resok: wcc_data + count + committed + writeverf.
	r.skipWcc()
	count := r.readUint32()
	return count
}

// readFile sends NFS READ and returns the data read.
func (c *e2eNFSClient) readFile(t *testing.T, fh []byte, offset uint64, count uint32) []byte {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(fh)
	pw.writeUint64(offset)
	pw.writeUint32(count)
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcRead, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("READ failed: NFS status %d", nfsStatus)
	}

	// READ3resok: post_op_attr + count + eof + data.
	r.skipPostOpAttr()
	_ = r.readUint32() // count
	_ = r.readBool()   // eof
	data := r.readOpaque()
	return data
}

// mkdir sends NFS MKDIR and returns the new directory handle.
func (c *e2eNFSClient) mkdir(t *testing.T, dirFH []byte, name string, mode uint32) []byte {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeString(name)
	// sattr3
	pw.writeBool(true)   // set_mode
	pw.writeUint32(mode) // mode value
	pw.writeBool(false)  // set_uid
	pw.writeBool(false)  // set_gid
	pw.writeBool(false)  // set_size
	pw.writeUint32(0)    // set_atime = DONT_CHANGE
	pw.writeUint32(0)    // set_mtime = DONT_CHANGE
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcMkdir, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("MKDIR %q failed: NFS status %d", name, nfsStatus)
	}

	// post_op_fh3.
	handleFollows := r.readBool()
	if !handleFollows {
		t.Fatalf("MKDIR %q: expected directory handle in response", name)
	}
	fh := r.readOpaque()
	if len(fh) != e2eNFSHandleSize {
		t.Fatalf("MKDIR %q: expected handle size %d, got %d", name, e2eNFSHandleSize, len(fh))
	}
	return fh
}

// readdir sends NFS READDIR and returns the list of entry names.
func (c *e2eNFSClient) readdir(t *testing.T, dirFH []byte) []string {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeUint64(0)                    // cookie
	pw.writeFixedOpaque(make([]byte, 8)) // cookieverf
	pw.writeUint32(4096)                 // dircount
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcReadDir, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("READDIR failed: NFS status %d", nfsStatus)
	}

	// post_op_attr.
	r.skipPostOpAttr()

	// cookieverf3 (8 bytes fixed).
	r.readFixedOpaque(8)

	// Read directory entries.
	var names []string
	for {
		follows := r.readBool()
		if !follows {
			break
		}
		_ = r.readUint64()        // fileid
		name := r.readString()    // filename
		names = append(names, name)
		_ = r.readUint64()        // cookie
	}
	return names
}

// remove sends NFS REMOVE (for files).
func (c *e2eNFSClient) remove(t *testing.T, dirFH []byte, name string) {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeString(name)
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcRemove, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("REMOVE %q failed: NFS status %d", name, nfsStatus)
	}
}

// rmdir sends NFS RMDIR.
func (c *e2eNFSClient) rmdir(t *testing.T, dirFH []byte, name string) {
	t.Helper()
	xid := c.nextXID()

	pw := newE2EXDRWriter()
	pw.writeOpaque(dirFH)
	pw.writeString(name)
	call := buildE2ERPCCall(xid, e2eNFSProg, e2eNFSVersion, e2eNFSProcRmdir, pw.bytes())
	sendE2ERPCRecord(t, c.conn, call)
	reply := recvE2ERPCRecord(t, c.conn)

	r := newE2EXDRReader(reply)
	parseReplyHeader(t, r, xid)

	nfsStatus := r.readUint32()
	if nfsStatus != e2eNFS3OK {
		t.Fatalf("RMDIR %q failed: NFS status %d", name, nfsStatus)
	}
}

// ---------------------------------------------------------------------------
// E2E Tests
// ---------------------------------------------------------------------------

// TestFileStorage_NFSOperations tests the NFS filer end-to-end using the real
// ONC/RPC + XDR wire protocol (NFS v3):
//  1. Start metadata + chunk services
//  2. Start NFS server with real adapters
//  3. Connect via TCP, send ONC/RPC framed messages
//  4. MOUNT MNT to get root handle
//  5. GETATTR, MKDIR, CREATE, WRITE, READ, READDIR, LOOKUP, REMOVE, RMDIR
//  6. Verify operations through the full stack
func TestFileStorage_NFSOperations(t *testing.T) {
	// Set up cluster.
	tc := newTestCluster(t)
	defer tc.Close()

	// Create filer adapters backed by the real metadata and chunk services.
	metaAdapter := filer.NewMetadataAdapter(tc.metaClient)
	chunkAdapter := filer.NewChunkAdapter(tc.agentClient)

	// Create the filesystem layer (initializes root inode).
	fs := filer.NewFileSystem(metaAdapter, chunkAdapter)

	// Create the NFS server with a lock manager.
	locker := filer.NewLockManager()
	nfsServer := filer.NewNFSServer(fs, locker)

	// Start the NFS server on a random port.
	nfsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen for NFS server: %v", err)
	}
	nfsAddr := nfsLis.Addr().String()
	nfsLis.Close() // Close so that NFSServer.Serve can bind it.

	go func() {
		if err := nfsServer.Serve(nfsAddr); err != nil {
			// Server stopped, expected during cleanup.
		}
	}()
	defer nfsServer.Stop()

	// Wait briefly for server to start listening.
	waitForTCP(t, nfsAddr)

	// Connect an ONC/RPC client to the NFS server.
	client := newE2ENFSClient(t, nfsAddr)
	defer client.close()

	// Step 1: MOUNT MNT to get root file handle.
	t.Log("MOUNT MNT to acquire root handle...")
	rootFH := client.mount(t)

	// Step 2: GETATTR on root directory.
	t.Log("GETATTR on root directory...")
	ftype, mode := client.getattr(t, rootFH)
	if ftype != e2eNF3Dir {
		t.Errorf("Expected root ftype NF3DIR (%d), got %d", e2eNF3Dir, ftype)
	}
	if mode != 0755 {
		t.Errorf("Expected root mode 0755, got %o", mode)
	}
	t.Logf("Root verified: ftype=%d, mode=%o", ftype, mode)

	// Step 3: MKDIR in root.
	t.Log("MKDIR 'testdir' in root...")
	dirFH := client.mkdir(t, rootFH, "testdir", 0755)
	t.Logf("Directory created, handle acquired (%d bytes)", len(dirFH))

	// Step 4: GETATTR on newly created directory.
	t.Log("GETATTR on testdir...")
	dirFtype, dirMode := client.getattr(t, dirFH)
	if dirFtype != e2eNF3Dir {
		t.Errorf("Expected testdir ftype NF3DIR (%d), got %d", e2eNF3Dir, dirFtype)
	}
	if dirMode != 0755 {
		t.Errorf("Expected testdir mode 0755, got %o", dirMode)
	}
	t.Logf("testdir verified: ftype=%d, mode=%o", dirFtype, dirMode)

	// Step 5: CREATE a file inside testdir.
	t.Log("CREATE 'hello.txt' in testdir...")
	fileFH := client.create(t, dirFH, "hello.txt", 0644)
	t.Logf("File created, handle acquired (%d bytes)", len(fileFH))

	// Step 6: WRITE data to the file.
	testContent := []byte("Hello from NovaStor NFS v3 E2E test!")
	t.Logf("WRITE %d bytes to hello.txt...", len(testContent))
	written := client.writeFile(t, fileFH, 0, testContent)
	if written != uint32(len(testContent)) {
		t.Errorf("Expected %d bytes written, got %d", len(testContent), written)
	}
	t.Logf("WRITE verified: %d bytes", written)

	// Step 7: READ data back from the file.
	t.Log("READ from hello.txt...")
	readData := client.readFile(t, fileFH, 0, 1024)
	if string(readData) != string(testContent) {
		t.Errorf("READ data mismatch: expected %q, got %q", testContent, readData)
	}
	t.Logf("READ verified: %d bytes, content matches", len(readData))

	// Step 8: READDIR on testdir.
	t.Log("READDIR on testdir...")
	entries := client.readdir(t, dirFH)
	// Expect ".", "..", "hello.txt".
	found := false
	for _, name := range entries {
		if name == "hello.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("Expected 'hello.txt' in READDIR entries, got: %v", entries)
	}
	t.Logf("READDIR verified: %d entries: %v", len(entries), entries)

	// Step 9: LOOKUP the file in testdir.
	t.Log("LOOKUP 'hello.txt' in testdir...")
	lookupFH := client.lookup(t, dirFH, "hello.txt")
	t.Logf("LOOKUP verified: handle acquired (%d bytes)", len(lookupFH))

	// Step 10: READDIR on root to verify testdir exists.
	t.Log("READDIR on root...")
	rootEntries := client.readdir(t, rootFH)
	foundDir := false
	for _, name := range rootEntries {
		if name == "testdir" {
			foundDir = true
		}
	}
	if !foundDir {
		t.Errorf("Expected 'testdir' in root READDIR entries, got: %v", rootEntries)
	}
	t.Logf("Root READDIR verified: %d entries: %v", len(rootEntries), rootEntries)

	// Step 11: REMOVE the file.
	t.Log("REMOVE 'hello.txt' from testdir...")
	client.remove(t, dirFH, "hello.txt")
	t.Log("REMOVE verified")

	// Step 12: Verify file is gone via READDIR.
	entries = client.readdir(t, dirFH)
	for _, name := range entries {
		if name == "hello.txt" {
			t.Errorf("Expected 'hello.txt' to be removed, but still in READDIR: %v", entries)
		}
	}
	t.Logf("File removal verified via READDIR: %v", entries)

	// Step 13: RMDIR testdir.
	t.Log("RMDIR 'testdir' from root...")
	client.rmdir(t, rootFH, "testdir")
	t.Log("RMDIR verified")

	// Step 14: Verify root is clean.
	rootEntries = client.readdir(t, rootFH)
	for _, name := range rootEntries {
		if name != "." && name != ".." {
			t.Errorf("Expected empty root (besides . and ..), got entry: %q", name)
		}
	}
	t.Logf("Root cleanup verified via READDIR: %v", rootEntries)
	t.Log("Full NFS v3 lifecycle test completed successfully")
}

// waitForTCP polls until a TCP connection can be established to the given address.
func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Timed out waiting for TCP server at %s", addr)
}
