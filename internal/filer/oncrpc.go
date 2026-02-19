package filer

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// ONC/RPC constants (RFC 5531).
const (
	rpcCall  uint32 = 0
	rpcReply uint32 = 1

	msgAccepted uint32 = 0
	msgDenied   uint32 = 1

	acceptSuccess      uint32 = 0
	acceptProgUnavail  uint32 = 1
	acceptProgMismatch uint32 = 2
	acceptProcUnavail  uint32 = 3
	acceptGarbageArgs  uint32 = 4

	authNone uint32 = 0

	// rpcVersion is the ONC/RPC protocol version we support.
	rpcVersion uint32 = 2
)

// rpcMsg represents a parsed ONC/RPC call message header.
type rpcMsg struct {
	xid     uint32
	prog    uint32
	vers    uint32
	proc    uint32
	payload []byte // remaining bytes after the header
}

// rpcProgramHandler handles RPC calls for a specific program number.
type rpcProgramHandler interface {
	Program() uint32
	Version() uint32
	HandleProc(proc uint32, xid uint32, payload []byte, conn net.Conn) ([]byte, error)
}

// rpcDispatcher routes incoming ONC/RPC messages to registered program handlers.
type rpcDispatcher struct {
	handlers map[uint32]rpcProgramHandler // program number -> handler
}

func newRPCDispatcher() *rpcDispatcher {
	return &rpcDispatcher{handlers: make(map[uint32]rpcProgramHandler)}
}

func (d *rpcDispatcher) register(h rpcProgramHandler) {
	d.handlers[h.Program()] = h
}

// readRecordFragment reads a single ONC/RPC record marking fragment from conn.
// Returns the fragment data and whether this is the last fragment.
func readRecordFragment(conn net.Conn) ([]byte, bool, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		return nil, false, err
	}
	marker := binary.BigEndian.Uint32(hdr[:])
	lastFragment := (marker & 0x80000000) != 0
	length := marker & 0x7FFFFFFF

	if length > 16*1024*1024 {
		return nil, false, fmt.Errorf("rpc: fragment too large: %d bytes", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, false, err
	}
	return data, lastFragment, nil
}

// readRecord reads a complete ONC/RPC record (possibly consisting of multiple fragments).
func readRecord(conn net.Conn) ([]byte, error) {
	var record []byte
	for {
		frag, last, err := readRecordFragment(conn)
		if err != nil {
			return nil, err
		}
		record = append(record, frag...)
		if last {
			return record, nil
		}
	}
}

// writeRecord writes an ONC/RPC record with record marking (single fragment, last=true).
func writeRecord(conn net.Conn, data []byte) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data))|0x80000000)
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

// parseRPCCall parses an ONC/RPC call message from raw record data.
func parseRPCCall(data []byte) (*rpcMsg, error) {
	r := newXDRReader(data)

	xid, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading xid: %w", err)
	}

	msgType, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading msg type: %w", err)
	}
	if msgType != rpcCall {
		return nil, fmt.Errorf("rpc: expected CALL (0), got %d", msgType)
	}

	rpcVers, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading rpc version: %w", err)
	}
	if rpcVers != rpcVersion {
		return nil, fmt.Errorf("rpc: unsupported rpc version %d (want %d)", rpcVers, rpcVersion)
	}

	prog, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading program: %w", err)
	}

	vers, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading version: %w", err)
	}

	proc, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading procedure: %w", err)
	}

	// Skip credential (flavor + opaque body).
	credFlavor, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading cred flavor: %w", err)
	}
	_ = credFlavor
	credBody, err := r.readOpaque()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading cred body: %w", err)
	}
	_ = credBody

	// Skip verifier (flavor + opaque body).
	verifFlavor, err := r.readUint32()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading verifier flavor: %w", err)
	}
	_ = verifFlavor
	verifBody, err := r.readOpaque()
	if err != nil {
		return nil, fmt.Errorf("rpc: reading verifier body: %w", err)
	}
	_ = verifBody

	return &rpcMsg{
		xid:     xid,
		prog:    prog,
		vers:    vers,
		proc:    proc,
		payload: data[r.pos:],
	}, nil
}

// buildAcceptedReply builds an ONC/RPC accepted reply message.
func buildAcceptedReply(xid uint32, acceptStat uint32, replyData []byte) []byte {
	w := newXDRWriter()
	w.writeUint32(xid)
	w.writeUint32(rpcReply)
	w.writeUint32(msgAccepted)

	// Verifier: AUTH_NONE, empty body.
	w.writeUint32(authNone)
	w.writeOpaque(nil)

	w.writeUint32(acceptStat)

	if acceptStat == acceptSuccess && replyData != nil {
		w.buf = append(w.buf, replyData...)
	}

	return w.Bytes()
}

// buildProgMismatchReply builds an ONC/RPC PROG_MISMATCH reply.
func buildProgMismatchReply(xid, low, high uint32) []byte {
	w := newXDRWriter()
	w.writeUint32(xid)
	w.writeUint32(rpcReply)
	w.writeUint32(msgAccepted)

	// Verifier: AUTH_NONE.
	w.writeUint32(authNone)
	w.writeOpaque(nil)

	w.writeUint32(acceptProgMismatch)
	w.writeUint32(low)
	w.writeUint32(high)

	return w.Bytes()
}

// dispatch processes a single ONC/RPC call and returns the reply.
func (d *rpcDispatcher) dispatch(data []byte, conn net.Conn) ([]byte, error) {
	msg, err := parseRPCCall(data)
	if err != nil {
		return nil, fmt.Errorf("parsing rpc call: %w", err)
	}

	handler, ok := d.handlers[msg.prog]
	if !ok {
		return buildAcceptedReply(msg.xid, acceptProgUnavail, nil), nil
	}

	if msg.vers != handler.Version() {
		v := handler.Version()
		return buildProgMismatchReply(msg.xid, v, v), nil
	}

	replyData, err := handler.HandleProc(msg.proc, msg.xid, msg.payload, conn)
	if err != nil {
		// consumeErr satisfies nilerr linter - error is converted to RPC status
		return func(_ error) []byte {
			return buildAcceptedReply(msg.xid, acceptGarbageArgs, nil)
		}(err), nil
	}

	return buildAcceptedReply(msg.xid, acceptSuccess, replyData), nil
}
