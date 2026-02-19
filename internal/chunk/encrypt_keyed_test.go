package chunk

import (
	"bytes"
	"context"
	"testing"
)

func TestKeyedEncryptedStore_PutGet(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)

	data := []byte("hello, keyed encrypted world!")
	c := &Chunk{
		ID:   NewChunkID(data),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	ctx := context.Background()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, c.ID)
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

func TestKeyedEncryptedStore_KeyRotation(t *testing.T) {
	inner := newTestLocalStore(t)
	master := newTestKey(t)
	km, err := NewDerivedKeyManager(master)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	// Write with key v1.
	data1 := []byte("written with v1 key")
	c1 := &Chunk{
		ID:   ChunkID("chunk-v1"),
		Data: data1,
	}
	c1.Checksum = c1.ComputeChecksum()

	if err := store.Put(ctx, c1); err != nil {
		t.Fatalf("Put v1: %v", err)
	}

	// Rotate to v2.
	newKeyID, err := km.RotateKey()
	if err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if newKeyID != "v2" {
		t.Fatalf("expected v2, got %q", newKeyID)
	}

	// Write with key v2.
	data2 := []byte("written with v2 key")
	c2 := &Chunk{
		ID:   ChunkID("chunk-v2"),
		Data: data2,
	}
	c2.Checksum = c2.ComputeChecksum()

	if err := store.Put(ctx, c2); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	// Read both chunks back — both must decrypt successfully.
	got1, err := store.Get(ctx, ChunkID("chunk-v1"))
	if err != nil {
		t.Fatalf("Get chunk-v1: %v", err)
	}
	if !bytes.Equal(got1.Data, data1) {
		t.Errorf("chunk-v1 data mismatch: got %q, want %q", got1.Data, data1)
	}

	got2, err := store.Get(ctx, ChunkID("chunk-v2"))
	if err != nil {
		t.Fatalf("Get chunk-v2: %v", err)
	}
	if !bytes.Equal(got2.Data, data2) {
		t.Errorf("chunk-v2 data mismatch: got %q, want %q", got2.Data, data2)
	}
}

func TestKeyedEncryptedStore_InvalidKeyID(t *testing.T) {
	inner := newTestLocalStore(t)
	masterA := newTestKey(t)
	masterB := newTestKey(t)

	kmA, err := NewDerivedKeyManager(masterA)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager A: %v", err)
	}
	kmB, err := NewStaticKeyManager(masterB)
	if err != nil {
		t.Fatalf("NewStaticKeyManager B: %v", err)
	}

	storeA := NewKeyedEncryptedStore(inner, kmA)
	ctx := context.Background()

	// Write with manager A (key ID "v1").
	data := []byte("secret payload")
	c := &Chunk{
		ID:   ChunkID("chunk-cross"),
		Data: data,
	}
	c.Checksum = c.ComputeChecksum()

	if err := storeA.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Try to read with manager B which does not know key ID "v1".
	storeB := NewKeyedEncryptedStore(inner, kmB)
	_, err = storeB.Get(ctx, ChunkID("chunk-cross"))
	if err == nil {
		t.Fatal("expected error when reading with wrong key manager")
	}
}

func TestKeyedEncryptedStore_Has(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	data := []byte("has test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	ok, err := store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has before Put: %v", err)
	}
	if ok {
		t.Error("Has returned true before Put")
	}

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ok, err = store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has after Put: %v", err)
	}
	if !ok {
		t.Error("Has returned false after Put")
	}
}

func TestKeyedEncryptedStore_Delete(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	data := []byte("delete test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := store.Delete(ctx, c.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ok, err := store.Has(ctx, c.ID)
	if err != nil {
		t.Fatalf("Has after Delete: %v", err)
	}
	if ok {
		t.Error("Has returned true after Delete")
	}
}

func TestKeyedEncryptedStore_List(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(ids))
	}

	data := []byte("list test")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ids, err = store.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(ids))
	}
}

func TestKeyedEncryptedStore_DataEncryptedAtRest(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	data := []byte("plaintext that should not appear in stored data")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()

	if err := store.Put(ctx, c); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Read raw data from the inner store — it should be encrypted.
	raw, err := inner.Get(ctx, c.ID)
	if err != nil {
		t.Fatalf("inner Get: %v", err)
	}
	if bytes.Contains(raw.Data, data) {
		t.Error("plaintext found in stored data; encryption not applied")
	}
}

func TestKeyedEncryptedStore_MultipleRotations(t *testing.T) {
	inner := newTestLocalStore(t)
	master := newTestKey(t)
	km, err := NewDerivedKeyManager(master)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	type entry struct {
		id   ChunkID
		data []byte
	}

	var entries []entry

	// Write chunks across 5 key versions.
	for i := 0; i < 5; i++ {
		if i > 0 {
			if _, err := km.RotateKey(); err != nil {
				t.Fatalf("RotateKey %d: %v", i, err)
			}
		}
		data := []byte("data for version " + string(rune('0'+i)))
		id := ChunkID("chunk-" + string(rune('0'+i)))
		c := &Chunk{ID: id, Data: data}
		c.Checksum = c.ComputeChecksum()

		if err := store.Put(ctx, c); err != nil {
			t.Fatalf("Put chunk-%d: %v", i, err)
		}
		entries = append(entries, entry{id: id, data: data})
	}

	// All chunks must be readable.
	for i, e := range entries {
		got, err := store.Get(ctx, e.id)
		if err != nil {
			t.Fatalf("Get chunk-%d: %v", i, err)
		}
		if !bytes.Equal(got.Data, e.data) {
			t.Errorf("chunk-%d data mismatch", i)
		}
	}
}

func TestKeyedEncryptedStore_ImplementsCapacityStore(_ *testing.T) {
	var _ CapacityStore = &KeyedEncryptedStore{}
}

func TestKeyedEncryptedStore_Stats(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	// KeyedEncryptedStore should delegate Stats to inner LocalStore.
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.TotalBytes <= 0 {
		t.Errorf("TotalBytes = %d, want > 0", stats.TotalBytes)
	}
}

func TestKeyedEncryptedStore_ImplementsChunkMetaStore(_ *testing.T) {
	var _ ChunkMetaStore = &KeyedEncryptedStore{}
}

func TestKeyedEncryptedStore_GetMeta(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	data := []byte("test metadata")
	c := &Chunk{ID: NewChunkID(data), Data: data}
	c.Checksum = c.ComputeChecksum()
	_ = store.Put(ctx, c)

	// GetMeta should delegate to inner LocalStore.
	meta, err := store.GetMeta(ctx, c.ID)
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}
	if meta.ID != c.ID {
		t.Errorf("GetMeta ID = %s, want %s", meta.ID, c.ID)
	}
}

func TestKeyedEncryptedStore_ImplementsHealthCheckStore(_ *testing.T) {
	var _ HealthCheckStore = &KeyedEncryptedStore{}
}

func TestKeyedEncryptedStore_HealthCheck(t *testing.T) {
	inner := newTestLocalStore(t)
	key := newTestKey(t)
	km, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	store := NewKeyedEncryptedStore(inner, km)
	ctx := context.Background()

	// HealthCheck should delegate to inner LocalStore.
	if err := store.HealthCheck(ctx); err != nil {
		t.Errorf("HealthCheck failed: %v", err)
	}
}
