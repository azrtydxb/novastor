package chunk

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// KeyedEncryptedStore wraps a Store and transparently encrypts/decrypts chunk
// data using a KeyManager. Each encrypted chunk is prefixed with the key ID so
// that decryption can look up the correct key, enabling seamless key rotation.
//
// Encrypted data format:
//
//	[1 byte key-ID length][key-ID bytes][nonce (12 bytes for AES-GCM)][ciphertext+tag]
type KeyedEncryptedStore struct {
	inner      Store
	keyManager KeyManager
}

// NewKeyedEncryptedStore creates a new KeyedEncryptedStore that uses km for key
// management and delegates storage to inner.
func NewKeyedEncryptedStore(inner Store, km KeyManager) *KeyedEncryptedStore {
	return &KeyedEncryptedStore{inner: inner, keyManager: km}
}

// buildAEAD constructs an AES-256-GCM AEAD from a raw 32-byte key.
func buildAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	return aead, nil
}

// Put encrypts the chunk data with the current key and stores it. The key ID
// is prepended to the encrypted payload so that Get can identify which key to
// use for decryption.
func (s *KeyedEncryptedStore) Put(ctx context.Context, c *Chunk) error {
	keyID, key, err := s.keyManager.CurrentKey()
	if err != nil {
		return fmt.Errorf("obtaining current encryption key: %w", err)
	}

	if len(keyID) > 255 {
		return fmt.Errorf("key ID too long (%d bytes, max 255)", len(keyID))
	}

	aead, err := buildAEAD(key)
	if err != nil {
		return fmt.Errorf("building AEAD for key %q: %w", keyID, err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return fmt.Errorf("generating nonce: %w", err)
	}

	ciphertext := aead.Seal(nonce, nonce, c.Data, nil)

	// Build the envelope: [1 byte keyID len][keyID][nonce+ciphertext]
	keyIDBytes := []byte(keyID)
	envelope := make([]byte, 0, 1+len(keyIDBytes)+len(ciphertext))
	envelope = append(envelope, byte(len(keyIDBytes)))
	envelope = append(envelope, keyIDBytes...)
	envelope = append(envelope, ciphertext...)

	encrypted := &Chunk{
		ID:   c.ID,
		Data: envelope,
	}
	encrypted.Checksum = encrypted.ComputeChecksum()

	return s.inner.Put(ctx, encrypted)
}

// Get retrieves a chunk, reads the key ID prefix, fetches the corresponding
// key, and decrypts the data.
func (s *KeyedEncryptedStore) Get(ctx context.Context, id ChunkID) (*Chunk, error) {
	c, err := s.inner.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if len(c.Data) < 1 {
		return nil, fmt.Errorf("chunk %s: encrypted envelope too short", id)
	}

	keyIDLen := int(c.Data[0])
	if len(c.Data) < 1+keyIDLen {
		return nil, fmt.Errorf("chunk %s: encrypted envelope truncated (key ID)", id)
	}

	keyID := string(c.Data[1 : 1+keyIDLen])
	payload := c.Data[1+keyIDLen:]

	key, err := s.keyManager.KeyByID(keyID)
	if err != nil {
		return nil, fmt.Errorf("chunk %s: looking up key %q: %w", id, keyID, err)
	}

	aead, err := buildAEAD(key)
	if err != nil {
		return nil, fmt.Errorf("chunk %s: building AEAD for key %q: %w", id, keyID, err)
	}

	nonceSize := aead.NonceSize()
	if len(payload) < nonceSize {
		return nil, fmt.Errorf("chunk %s: encrypted payload too short for nonce", id)
	}

	nonce := payload[:nonceSize]
	ciphertext := payload[nonceSize:]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypting chunk %s: %w", id, err)
	}

	c.Data = plaintext
	c.Checksum = c.ComputeChecksum()
	return c, nil
}

// Delete delegates to the inner store.
func (s *KeyedEncryptedStore) Delete(ctx context.Context, id ChunkID) error {
	return s.inner.Delete(ctx, id)
}

// Has delegates to the inner store.
func (s *KeyedEncryptedStore) Has(ctx context.Context, id ChunkID) (bool, error) {
	return s.inner.Has(ctx, id)
}

// List delegates to the inner store.
func (s *KeyedEncryptedStore) List(ctx context.Context) ([]ChunkID, error) {
	return s.inner.List(ctx)
}

// Stats delegates to the inner store if it implements CapacityStore.
func (s *KeyedEncryptedStore) Stats(ctx context.Context) (*StoreStats, error) {
	if cs, ok := s.inner.(CapacityStore); ok {
		return cs.Stats(ctx)
	}
	return nil, fmt.Errorf("inner store does not support capacity stats")
}

// GetMeta delegates to the inner store if it implements ChunkMetaStore.
func (s *KeyedEncryptedStore) GetMeta(ctx context.Context, id ChunkID) (*ChunkMeta, error) {
	if cms, ok := s.inner.(ChunkMetaStore); ok {
		return cms.GetMeta(ctx, id)
	}
	return nil, fmt.Errorf("inner store does not support metadata retrieval")
}

// HealthCheck delegates to the inner store if it implements HealthCheckStore.
func (s *KeyedEncryptedStore) HealthCheck(ctx context.Context) error {
	if hcs, ok := s.inner.(HealthCheckStore); ok {
		return hcs.HealthCheck(ctx)
	}
	return fmt.Errorf("inner store does not support health checks")
}
