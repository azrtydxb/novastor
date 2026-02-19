package filer

import (
	"encoding/binary"
	"io"
)

// xdrWriter provides XDR (RFC 4506) encoding for NFS v3 wire types.
type xdrWriter struct {
	buf []byte
}

func newXDRWriter() *xdrWriter {
	return &xdrWriter{buf: make([]byte, 0, 4096)}
}

// Bytes returns the accumulated XDR-encoded data.
func (w *xdrWriter) Bytes() []byte {
	return w.buf
}

// Reset clears the writer buffer for reuse.
func (w *xdrWriter) Reset() {
	w.buf = w.buf[:0]
}

func (w *xdrWriter) writeUint32(v uint32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	w.buf = append(w.buf, b...)
}

func (w *xdrWriter) writeUint64(v uint64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	w.buf = append(w.buf, b...)
}

func (w *xdrWriter) writeBool(v bool) {
	if v {
		w.writeUint32(1)
	} else {
		w.writeUint32(0)
	}
}

// writeOpaque writes a variable-length opaque with a 4-byte length prefix,
// padded to a 4-byte boundary.
func (w *xdrWriter) writeOpaque(data []byte) {
	w.writeUint32(uint32(len(data)))
	w.buf = append(w.buf, data...)
	// Pad to 4-byte alignment.
	pad := (4 - len(data)%4) % 4
	for i := 0; i < pad; i++ {
		w.buf = append(w.buf, 0)
	}
}

// writeFixedOpaque writes a fixed-length opaque value, padded to 4-byte boundary.
func (w *xdrWriter) writeFixedOpaque(data []byte) {
	w.buf = append(w.buf, data...)
	pad := (4 - len(data)%4) % 4
	for i := 0; i < pad; i++ {
		w.buf = append(w.buf, 0)
	}
}

// writeString writes an XDR string (length-prefixed, padded).
func (w *xdrWriter) writeString(s string) {
	w.writeOpaque([]byte(s))
}

// xdrReader provides XDR decoding from a byte buffer.
type xdrReader struct {
	data []byte
	pos  int
}

func newXDRReader(data []byte) *xdrReader {
	return &xdrReader{data: data}
}

// Remaining returns the number of unread bytes.
func (r *xdrReader) Remaining() int {
	return len(r.data) - r.pos
}

func (r *xdrReader) readUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return v, nil
}

func (r *xdrReader) readUint64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return v, nil
}

func (r *xdrReader) readBool() (bool, error) {
	v, err := r.readUint32()
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// readOpaque reads a variable-length opaque value.
func (r *xdrReader) readOpaque() ([]byte, error) {
	length, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if r.pos+int(length) > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	result := make([]byte, length)
	copy(result, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	// Skip padding.
	pad := (4 - int(length)%4) % 4
	r.pos += pad
	if r.pos > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	return result, nil
}

// readFixedOpaque reads a fixed-length opaque value.
func (r *xdrReader) readFixedOpaque(length int) ([]byte, error) {
	if r.pos+length > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	result := make([]byte, length)
	copy(result, r.data[r.pos:r.pos+length])
	r.pos += length
	// Skip padding.
	pad := (4 - length%4) % 4
	r.pos += pad
	if r.pos > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	return result, nil
}

// readString reads an XDR string.
func (r *xdrReader) readString() (string, error) {
	data, err := r.readOpaque()
	if err != nil {
		return "", err
	}
	return string(data), nil
}
