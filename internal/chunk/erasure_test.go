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

func TestShardID(t *testing.T) {
	tests := []struct {
		name       string
		chunkID    string
		shardIndex int
		want       ChunkID
	}{
		{"first shard", "vol-abc-chunk-0001", 0, "vol-abc-chunk-0001:shard:0"},
		{"last shard", "vol-abc-chunk-0001", 5, "vol-abc-chunk-0001:shard:5"},
		{"uuid chunk", "550e8400-e29b-41d4-a716-446655440000-chunk-0042", 3, "550e8400-e29b-41d4-a716-446655440000-chunk-0042:shard:3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShardID(tt.chunkID, tt.shardIndex)
			if got != tt.want {
				t.Errorf("ShardID(%q, %d) = %q, want %q", tt.chunkID, tt.shardIndex, got, tt.want)
			}
		})
	}
}

func TestParseShardID(t *testing.T) {
	tests := []struct {
		name      string
		id        ChunkID
		wantChunk string
		wantIndex int
		wantErr   bool
	}{
		{"valid shard 0", "vol-abc-chunk-0001:shard:0", "vol-abc-chunk-0001", 0, false},
		{"valid shard 5", "vol-abc-chunk-0001:shard:5", "vol-abc-chunk-0001", 5, false},
		{"not a shard", "vol-abc-chunk-0001", "", 0, true},
		{"empty chunk ID", ":shard:0", "", 0, true},
		{"invalid index", "vol-abc-chunk-0001:shard:abc", "", 0, true},
		{"negative index", "vol-abc-chunk-0001:shard:-1", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunkID, shardIndex, err := ParseShardID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseShardID(%q) error = %v, wantErr %v", tt.id, err, tt.wantErr)
			}
			if err == nil {
				if chunkID != tt.wantChunk {
					t.Errorf("chunkID = %q, want %q", chunkID, tt.wantChunk)
				}
				if shardIndex != tt.wantIndex {
					t.Errorf("shardIndex = %d, want %d", shardIndex, tt.wantIndex)
				}
			}
		})
	}
}

func TestShardID_Roundtrip(t *testing.T) {
	chunkID := "vol-abc-chunk-0001"
	for i := range 6 {
		sid := ShardID(chunkID, i)
		gotChunk, gotIndex, err := ParseShardID(sid)
		if err != nil {
			t.Fatalf("ParseShardID(ShardID(%q, %d)) failed: %v", chunkID, i, err)
		}
		if gotChunk != chunkID || gotIndex != i {
			t.Errorf("roundtrip failed: got (%q, %d), want (%q, %d)", gotChunk, gotIndex, chunkID, i)
		}
	}
}

func TestIsShardID(t *testing.T) {
	tests := []struct {
		id   ChunkID
		want bool
	}{
		{"vol-abc-chunk-0001:shard:0", true},
		{"vol-abc-chunk-0001:shard:5", true},
		{"vol-abc-chunk-0001", false},
		{"plain-chunk-id", false},
	}
	for _, tt := range tests {
		if got := IsShardID(tt.id); got != tt.want {
			t.Errorf("IsShardID(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}

func TestShardID_PanicsOnSeparatorInChunkID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when chunkID contains :shard: separator")
		}
	}()
	ShardID("bad:shard:id", 0)
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
