package chunk

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// EncryptedStore wraps a Store and transparently encrypts/decrypts chunk data.
type EncryptedStore struct {
	inner Store
	aead  cipher.AEAD
}

// NewEncryptedStore creates a new EncryptedStore with the given 32-byte AES-256 key.
func NewEncryptedStore(inner Store, key []byte) (*EncryptedStore, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	return &EncryptedStore{inner: inner, aead: aead}, nil
}

// Put encrypts the chunk data and delegates to the inner store.
func (s *EncryptedStore) Put(ctx context.Context, c *Chunk) error {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("generating nonce: %w", err)
	}

	ciphertext := s.aead.Seal(nonce, nonce, c.Data, nil)

	encrypted := &Chunk{
		ID:   c.ID,
		Data: ciphertext,
	}
	encrypted.Checksum = encrypted.ComputeChecksum()

	return s.inner.Put(ctx, encrypted)
}

// Get retrieves a chunk from the inner store and decrypts its data.
func (s *EncryptedStore) Get(ctx context.Context, id ChunkID) (*Chunk, error) {
	c, err := s.inner.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	nonceSize := s.aead.NonceSize()
	if len(c.Data) < nonceSize {
		return nil, fmt.Errorf("chunk %s: encrypted data too short", id)
	}

	nonce := c.Data[:nonceSize]
	ciphertext := c.Data[nonceSize:]

	plaintext, err := s.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypting chunk %s: %w", id, err)
	}

	c.Data = plaintext
	c.Checksum = c.ComputeChecksum()
	return c, nil
}

// Delete delegates to the inner store.
func (s *EncryptedStore) Delete(ctx context.Context, id ChunkID) error {
	return s.inner.Delete(ctx, id)
}

// Has delegates to the inner store.
func (s *EncryptedStore) Has(ctx context.Context, id ChunkID) (bool, error) {
	return s.inner.Has(ctx, id)
}

// List delegates to the inner store.
func (s *EncryptedStore) List(ctx context.Context) ([]ChunkID, error) {
	return s.inner.List(ctx)
}

// Stats delegates to the inner store if it implements CapacityStore.
func (s *EncryptedStore) Stats(ctx context.Context) (*StoreStats, error) {
	if cs, ok := s.inner.(CapacityStore); ok {
		return cs.Stats(ctx)
	}
	return nil, fmt.Errorf("inner store does not support capacity stats")
}

// GetMeta delegates to the inner store if it implements ChunkMetaStore.
func (s *EncryptedStore) GetMeta(ctx context.Context, id ChunkID) (*ChunkMeta, error) {
	if cms, ok := s.inner.(ChunkMetaStore); ok {
		return cms.GetMeta(ctx, id)
	}
	return nil, fmt.Errorf("inner store does not support metadata retrieval")
}

// HealthCheck delegates to the inner store if it implements HealthCheckStore.
func (s *EncryptedStore) HealthCheck(ctx context.Context) error {
	if hcs, ok := s.inner.(HealthCheckStore); ok {
		return hcs.HealthCheck(ctx)
	}
	return fmt.Errorf("inner store does not support health checks")
}
