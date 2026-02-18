package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/crypto/hkdf"
)

// KeyManager manages encryption keys for chunk storage.
type KeyManager interface {
	// CurrentKey returns the current active encryption key and its ID.
	CurrentKey() (keyID string, key []byte, err error)
	// KeyByID returns the encryption key for the given key ID.
	KeyByID(keyID string) ([]byte, error)
	// RotateKey generates a new key and makes it the current active key.
	RotateKey() (newKeyID string, err error)
}

// ---------------------------------------------------------------------------
// StaticKeyManager
// ---------------------------------------------------------------------------

// StaticKeyManager wraps a single key with a fixed ID. Suitable for simple
// deployments where key rotation is not required.
type StaticKeyManager struct {
	keyID string
	key   []byte
}

// NewStaticKeyManager creates a StaticKeyManager from a raw 32-byte key.
// The key ID is derived deterministically from the key content (hex-encoded
// SHA-256 prefix).
func NewStaticKeyManager(key []byte) (*StaticKeyManager, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("static key must be 32 bytes, got %d", len(key))
	}
	h := sha256.Sum256(key)
	keyID := hex.EncodeToString(h[:8])
	keyCopy := make([]byte, 32)
	copy(keyCopy, key)
	return &StaticKeyManager{keyID: keyID, key: keyCopy}, nil
}

// CurrentKey returns the single static key and its ID.
func (m *StaticKeyManager) CurrentKey() (string, []byte, error) {
	return m.keyID, m.key, nil
}

// KeyByID returns the key only if the requested ID matches the static key ID.
func (m *StaticKeyManager) KeyByID(keyID string) ([]byte, error) {
	if keyID != m.keyID {
		return nil, fmt.Errorf("unknown key ID %q (static manager only has %q)", keyID, m.keyID)
	}
	return m.key, nil
}

// RotateKey returns an error because a static key manager does not support rotation.
func (m *StaticKeyManager) RotateKey() (string, error) {
	return "", fmt.Errorf("static key manager does not support key rotation")
}

// ---------------------------------------------------------------------------
// DerivedKeyManager
// ---------------------------------------------------------------------------

// DerivedKeyManager derives per-key-ID keys from a master key using HKDF
// (SHA-256). Key versions are labelled "v1", "v2", etc.
type DerivedKeyManager struct {
	mu        sync.RWMutex
	masterKey []byte
	version   int
}

// NewDerivedKeyManager creates a DerivedKeyManager with the given 32-byte
// master key. The initial current version is "v1".
func NewDerivedKeyManager(masterKey []byte) (*DerivedKeyManager, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes, got %d", len(masterKey))
	}
	mk := make([]byte, 32)
	copy(mk, masterKey)
	return &DerivedKeyManager{masterKey: mk, version: 1}, nil
}

// deriveKey derives a 32-byte key from the master key and the given key ID
// using HKDF-SHA256.
func (m *DerivedKeyManager) deriveKey(keyID string) ([]byte, error) {
	reader := hkdf.New(sha256.New, m.masterKey, []byte(keyID), []byte("novastor-chunk-encryption"))
	derived := make([]byte, 32)
	if _, err := io.ReadFull(reader, derived); err != nil {
		return nil, fmt.Errorf("deriving key for %q: %w", keyID, err)
	}
	return derived, nil
}

// CurrentKey returns the derived key for the current version.
func (m *DerivedKeyManager) CurrentKey() (string, []byte, error) {
	m.mu.RLock()
	v := m.version
	m.mu.RUnlock()
	keyID := fmt.Sprintf("v%d", v)
	key, err := m.deriveKey(keyID)
	if err != nil {
		return "", nil, err
	}
	return keyID, key, nil
}

// KeyByID derives and returns the key for an arbitrary key ID.
func (m *DerivedKeyManager) KeyByID(keyID string) ([]byte, error) {
	return m.deriveKey(keyID)
}

// RotateKey increments the version counter and returns the new version label.
func (m *DerivedKeyManager) RotateKey() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.version++
	return fmt.Sprintf("v%d", m.version), nil
}

// ---------------------------------------------------------------------------
// FileKeyManager
// ---------------------------------------------------------------------------

// FileKeyManager loads encryption keys from a directory where each file
// contains a raw 32-byte key and the filename is the key ID. This is designed
// for Kubernetes Secret volume mounts. The most recently modified file is
// treated as the current key.
type FileKeyManager struct {
	mu     sync.RWMutex
	keyDir string
	keys   map[string][]byte
	order  []string // key IDs sorted by mod time, last element is current
}

// NewFileKeyManager scans keyDir for key files and loads them into memory.
// Each file must contain exactly 32 bytes of raw key material.
func NewFileKeyManager(keyDir string) (*FileKeyManager, error) {
	info, err := os.Stat(keyDir)
	if err != nil {
		return nil, fmt.Errorf("accessing key directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("key path %q is not a directory", keyDir)
	}

	m := &FileKeyManager{
		keyDir: keyDir,
		keys:   make(map[string][]byte),
	}
	if err := m.reload(); err != nil {
		return nil, err
	}
	if len(m.keys) == 0 {
		return nil, fmt.Errorf("no key files found in %q", keyDir)
	}
	return m, nil
}

type keyFileEntry struct {
	name    string
	modTime int64
}

// reload scans the key directory and loads all key files.
func (m *FileKeyManager) reload() error {
	entries, err := os.ReadDir(m.keyDir)
	if err != nil {
		return fmt.Errorf("reading key directory: %w", err)
	}

	var files []keyFileEntry
	keys := make(map[string][]byte)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Skip hidden files and common Kubernetes mount artifacts.
		if strings.HasPrefix(e.Name(), ".") || strings.HasPrefix(e.Name(), "..") {
			continue
		}

		path := filepath.Join(m.keyDir, e.Name())
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return fmt.Errorf("reading key file %q: %w", e.Name(), readErr)
		}
		if len(data) != 32 {
			return fmt.Errorf("key file %q must contain exactly 32 bytes, got %d", e.Name(), len(data))
		}

		info, infoErr := e.Info()
		if infoErr != nil {
			return fmt.Errorf("stat key file %q: %w", e.Name(), infoErr)
		}

		keyCopy := make([]byte, 32)
		copy(keyCopy, data)
		keys[e.Name()] = keyCopy
		files = append(files, keyFileEntry{name: e.Name(), modTime: info.ModTime().UnixNano()})
	}

	// Sort by modification time ascending; the last entry is the most recent.
	sort.Slice(files, func(i, j int) bool {
		if files[i].modTime == files[j].modTime {
			return files[i].name < files[j].name
		}
		return files[i].modTime < files[j].modTime
	})

	order := make([]string, len(files))
	for i, f := range files {
		order[i] = f.name
	}

	m.mu.Lock()
	m.keys = keys
	m.order = order
	m.mu.Unlock()

	return nil
}

// CurrentKey returns the most recently modified key and its ID (filename).
func (m *FileKeyManager) CurrentKey() (string, []byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.order) == 0 {
		return "", nil, fmt.Errorf("no keys loaded")
	}
	keyID := m.order[len(m.order)-1]
	return keyID, m.keys[keyID], nil
}

// KeyByID returns the key for the given key ID (filename).
func (m *FileKeyManager) KeyByID(keyID string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key, ok := m.keys[keyID]
	if !ok {
		return nil, fmt.Errorf("key %q not found in key directory", keyID)
	}
	return key, nil
}

// RotateKey returns an error because file-based key rotation must be performed
// externally (e.g., by updating the Kubernetes Secret).
func (m *FileKeyManager) RotateKey() (string, error) {
	return "", fmt.Errorf("file key manager does not support programmatic rotation; update the key directory externally")
}

// Reload re-scans the key directory, picking up any new or removed keys.
func (m *FileKeyManager) Reload() error {
	return m.reload()
}

// keyVersionNumber extracts the numeric suffix from a version string like "v3".
// Returns -1 if the string does not match the pattern.
func keyVersionNumber(s string) int {
	if !strings.HasPrefix(s, "v") {
		return -1
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil {
		return -1
	}
	return n
}
