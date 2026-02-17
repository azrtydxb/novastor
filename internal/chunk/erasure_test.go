package chunk

import (
	"bytes"
	"testing"
)

func TestErasureCoder_EncodeDecodeNoFailure(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatalf("NewErasureCoder failed: %v", err)
	}
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if len(shards) != 6 {
		t.Fatalf("expected 6 shards, got %d", len(shards))
	}
	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if !bytes.Equal(decoded, data) {
		t.Error("decoded data does not match original")
	}
}

func TestErasureCoder_RecoverFromTwoShardLoss(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, ChunkSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}
	shards[0] = nil
	shards[3] = nil
	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode with 2 lost shards failed: %v", err)
	}
	if !bytes.Equal(decoded, data) {
		t.Error("recovered data does not match original after 2 shard loss")
	}
}

func TestErasureCoder_TooManyLostShards(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, ChunkSize)
	shards, _ := ec.Encode(data)
	shards[0] = nil
	shards[1] = nil
	shards[2] = nil
	_, err = ec.Decode(shards)
	if err == nil {
		t.Error("Decode should fail with 3 lost shards (parity=2)")
	}
}

func TestErasureCoder_SmallData(t *testing.T) {
	ec, err := NewErasureCoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("small")
	shards, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("Encode small data failed: %v", err)
	}
	decoded, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode small data failed: %v", err)
	}
	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded = %q, want %q", decoded, data)
	}
}

func TestNewErasureCoder_InvalidParams(t *testing.T) {
	tests := []struct {
		name   string
		data   int
		parity int
	}{
		{"zero data", 0, 2},
		{"zero parity", 4, 0},
		{"negative data", -1, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewErasureCoder(tt.data, tt.parity)
			if err == nil {
				t.Error("expected error for invalid params")
			}
		})
	}
}
