package chunk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestNewChunkID(t *testing.T) {
	data := []byte("hello world")
	id := NewChunkID(data)
	expected := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expected[:])
	if string(id) != expectedHex {
		t.Errorf("NewChunkID = %s, want %s", id, expectedHex)
	}
}

func TestNewChunkID_DifferentData(t *testing.T) {
	id1 := NewChunkID([]byte("data1"))
	id2 := NewChunkID([]byte("data2"))
	if id1 == id2 {
		t.Error("different data should produce different chunk IDs")
	}
}

func TestNewChunkID_SameData(t *testing.T) {
	data := []byte("same data")
	id1 := NewChunkID(data)
	id2 := NewChunkID(data)
	if id1 != id2 {
		t.Error("same data should produce same chunk ID")
	}
}

func TestChunkChecksum(t *testing.T) {
	data := []byte("test data for checksum")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	if err := c.VerifyChecksum(); err != nil {
		t.Errorf("valid checksum should verify: %v", err)
	}
}

func TestChunkChecksum_Corrupted(t *testing.T) {
	data := []byte("test data for checksum")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	c.Data = []byte("corrupted data")
	if err := c.VerifyChecksum(); err == nil {
		t.Error("corrupted data should fail checksum verification")
	}
}

func TestChunkSize(t *testing.T) {
	if ChunkSize != 4*1024*1024 {
		t.Errorf("ChunkSize = %d, want %d", ChunkSize, 4*1024*1024)
	}
}

func TestSplitData(t *testing.T) {
	tests := []struct {
		name       string
		dataLen    int
		wantChunks int
	}{
		{"empty", 0, 0},
		{"one byte", 1, 1},
		{"exact chunk", ChunkSize, 1},
		{"chunk plus one", ChunkSize + 1, 2},
		{"two chunks", ChunkSize * 2, 2},
		{"two and a half", ChunkSize*2 + ChunkSize/2, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.dataLen)
			for i := range data {
				data[i] = byte(i % 256)
			}
			chunks := SplitData(data)
			if len(chunks) != tt.wantChunks {
				t.Errorf("SplitData(%d bytes) = %d chunks, want %d", tt.dataLen, len(chunks), tt.wantChunks)
			}
			if tt.dataLen > 0 {
				var reassembled []byte
				for _, c := range chunks {
					reassembled = append(reassembled, c.Data...)
				}
				if !bytes.Equal(reassembled, data) {
					t.Error("reassembled data does not match original")
				}
			}
		})
	}
}
