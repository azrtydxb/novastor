package chunk

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
)

func newTestKey(t *testing.T) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating random key: %v", err)
	}
	return key
}

func newTestLocalStore(t *testing.T) *LocalStore {
	t.Helper()
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("creating local store: %v", err)
	}
	return store
}

func TestEncryptedStore_PutGet(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	enc, err := NewEncryptedStore(inner, key)
	if err != nil {
		t.Fatalf("creating encrypted store: %v", err)
	}

	data := []byte("hello, encrypted world!")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	ctx := context.Background()

	if err := enc.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := enc.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !bytes.Equal(got.Data, data) {
		t.Errorf("data mismatch: got %q, want %q", got.Data, data)
	}

	if got.Checksum != c.Checksum {
		t.Errorf("checksum mismatch: got %d, want %d", got.Checksum, c.Checksum)
	}
}

func TestEncryptedStore_Has(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	enc, err := NewEncryptedStore(inner, key)
	if err != nil {
		t.Fatalf("creating encrypted store: %v", err)
	}

	data := []byte("test has")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	ctx := context.Background()

	ok, err := enc.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has before Put: %v", err)
	}
	if ok {
		t.Error("Has returned true before Put")
	}

	if err := enc.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ok, err = enc.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has after Put: %v", err)
	}
	if !ok {
		t.Error("Has returned false after Put")
	}
}

func TestEncryptedStore_Delete(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	enc, err := NewEncryptedStore(inner, key)
	if err != nil {
		t.Fatalf("creating encrypted store: %v", err)
	}

	data := []byte("test delete")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	ctx := context.Background()

	if err := enc.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := enc.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ok, err := enc.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has after Delete: %v", err)
	}
	if ok {
		t.Error("Has returned true after Delete")
	}
}

func TestEncryptedStore_List(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	enc, err := NewEncryptedStore(inner, key)
	if err != nil {
		t.Fatalf("creating encrypted store: %v", err)
	}

	ctx := context.Background()

	ids, err := enc.List(ctx)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(ids))
	}

	data1 := []byte("chunk one")
	c1 := &Chunk{ID: NewChunkID(data1), Data: data1}
	c1.Checksum = c1.ComputeChecksum()

	data2 := []byte("chunk two")
	c2 := &Chunk{ID: NewChunkID(data2), Data: data2}
	c2.Checksum = c2.ComputeChecksum()

	if err := enc.Put(ctx, c1); err != nil {
		t.Fatalf("Put c1: %v", err)
	}
	if err := enc.Put(ctx, c2); err != nil {
		t.Fatalf("Put c2: %v", err)
	}

	ids, err = enc.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(ids))
	}
}

func TestEncryptedStore_WrongKey(t *testing.T) {
	inner := newTestLocalStore(t)
	keyA := newTestKey(t)
	keyB := newTestKey(t)

	encA, err := NewEncryptedStore(inner, keyA)
	if err != nil {
		t.Fatalf("creating encrypted store A: %v", err)
	}

	data := []byte("secret data")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	ctx := context.Background()

	if err := encA.Put(ctx, c); err != nil {
		t.Fatalf("Put with key A: %v", err)
	}

	encB, err := NewEncryptedStore(inner, keyB)
	if err != nil {
		t.Fatalf("creating encrypted store B: %v", err)
	}

	_, err = encB.Get(ctx, c.ID)
	if err == nil {
		t.Fatal("expected error decrypting with wrong key, got nil")
	}
}

func TestEncryptedStore_RandomNonce(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	enc, err := NewEncryptedStore(inner, key)
	if err != nil {
		t.Fatalf("creating encrypted store: %v", err)
	}

	data := []byte("same data for both")

	c1 := &Chunk{
		ID:   ChunkID("chunk-a"),
		Data: data,
	}
	c1.Checksum = c1.ComputeChecksum()

	c2 := &Chunk{
		ID:   ChunkID("chunk-b"),
		Data: data,
	}
	c2.Checksum = c2.ComputeChecksum()

	ctx := context.Background()

	if err := enc.Put(ctx, c1); err != nil {
		t.Fatalf("Put c1: %v", err)
	}
	if err := enc.Put(ctx, c2); err != nil {
		t.Fatalf("Put c2: %v", err)
	}

	raw1, err := inner.Get(ctx, ChunkID("chunk-a"))
	if err != nil {
		t.Fatalf("inner Get chunk-a: %v", err)
	}
	raw2, err := inner.Get(ctx, ChunkID("chunk-b"))
	if err != nil {
		t.Fatalf("inner Get chunk-b: %v", err)
	}

	if bytes.Equal(raw1.Data, raw2.Data) {
		t.Error("encrypting same plaintext produced identical ciphertexts; nonce should differ")
	}
}
