package chunk

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// StaticKeyManager tests
// ---------------------------------------------------------------------------

func TestStaticKeyManager_CurrentKey(t *testing.T) {
	key := newTestKey(t)
	m, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	keyID, got, err := m.CurrentKey()
	if err != nil {
		t.Fatalf("CurrentKey: %v", err)
	}
	if keyID == "" {
		t.Error("expected non-empty key ID")
	}
	if !bytes.Equal(got, key) {
		t.Error("returned key does not match input key")
	}
}

func TestStaticKeyManager_KeyByID(t *testing.T) {
	key := newTestKey(t)
	m, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	keyID, _, _ := m.CurrentKey()

	t.Run("valid ID", func(t *testing.T) {
		got, err := m.KeyByID(keyID)
		if err != nil {
			t.Fatalf("KeyByID: %v", err)
		}
		if !bytes.Equal(got, key) {
			t.Error("returned key does not match")
		}
	})

	t.Run("invalid ID", func(t *testing.T) {
		_, err := m.KeyByID("nonexistent")
		if err == nil {
			t.Fatal("expected error for unknown key ID")
		}
	})
}

func TestStaticKeyManager_RotateKey(t *testing.T) {
	key := newTestKey(t)
	m, err := NewStaticKeyManager(key)
	if err != nil {
		t.Fatalf("NewStaticKeyManager: %v", err)
	}

	_, err = m.RotateKey()
	if err == nil {
		t.Fatal("expected error from RotateKey on static manager")
	}
}

func TestStaticKeyManager_InvalidKeyLength(t *testing.T) {
	_, err := NewStaticKeyManager([]byte("too-short"))
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

// ---------------------------------------------------------------------------
// DerivedKeyManager tests
// ---------------------------------------------------------------------------

func TestDerivedKeyManager_CurrentKey(t *testing.T) {
	master := newTestKey(t)
	m, err := NewDerivedKeyManager(master)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager: %v", err)
	}

	keyID, key, err := m.CurrentKey()
	if err != nil {
		t.Fatalf("CurrentKey: %v", err)
	}
	if keyID != "v1" {
		t.Errorf("expected key ID %q, got %q", "v1", keyID)
	}
	if len(key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(key))
	}
}

func TestDerivedKeyManager_KeyByID(t *testing.T) {
	master := newTestKey(t)
	m, err := NewDerivedKeyManager(master)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager: %v", err)
	}

	k1, err := m.KeyByID("v1")
	if err != nil {
		t.Fatalf("KeyByID v1: %v", err)
	}

	k2, err := m.KeyByID("v2")
	if err != nil {
		t.Fatalf("KeyByID v2: %v", err)
	}

	if bytes.Equal(k1, k2) {
		t.Error("different key IDs should produce different derived keys")
	}

	// Same key ID should produce the same key.
	k1Again, err := m.KeyByID("v1")
	if err != nil {
		t.Fatalf("KeyByID v1 again: %v", err)
	}
	if !bytes.Equal(k1, k1Again) {
		t.Error("same key ID should produce identical derived keys")
	}
}

func TestDerivedKeyManager_RotateKey(t *testing.T) {
	master := newTestKey(t)
	m, err := NewDerivedKeyManager(master)
	if err != nil {
		t.Fatalf("NewDerivedKeyManager: %v", err)
	}

	newKeyID, err := m.RotateKey()
	if err != nil {
		t.Fatalf("RotateKey: %v", err)
	}
	if newKeyID != "v2" {
		t.Errorf("expected %q, got %q", "v2", newKeyID)
	}

	keyID, _, err := m.CurrentKey()
	if err != nil {
		t.Fatalf("CurrentKey after rotate: %v", err)
	}
	if keyID != "v2" {
		t.Errorf("current key after rotate: expected %q, got %q", "v2", keyID)
	}

	// Second rotation.
	newKeyID, err = m.RotateKey()
	if err != nil {
		t.Fatalf("RotateKey 2: %v", err)
	}
	if newKeyID != "v3" {
		t.Errorf("expected %q, got %q", "v3", newKeyID)
	}
}

func TestDerivedKeyManager_InvalidKeyLength(t *testing.T) {
	_, err := NewDerivedKeyManager([]byte("short"))
	if err == nil {
		t.Fatal("expected error for short master key")
	}
}

// ---------------------------------------------------------------------------
// FileKeyManager tests
// ---------------------------------------------------------------------------

func writeKeyFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating key: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, key, 0o600); err != nil {
		t.Fatalf("writing key file: %v", err)
	}
	return key
}

func TestFileKeyManager_CurrentKey(t *testing.T) {
	dir := t.TempDir()

	writeKeyFile(t, dir, "key-old")
	// Ensure distinct modification times.
	time.Sleep(50 * time.Millisecond)
	newestKey := writeKeyFile(t, dir, "key-new")

	m, err := NewFileKeyManager(dir)
	if err != nil {
		t.Fatalf("NewFileKeyManager: %v", err)
	}

	keyID, key, err := m.CurrentKey()
	if err != nil {
		t.Fatalf("CurrentKey: %v", err)
	}
	if keyID != "key-new" {
		t.Errorf("expected current key ID %q, got %q", "key-new", keyID)
	}
	if !bytes.Equal(key, newestKey) {
		t.Error("current key content does not match newest file")
	}
}

func TestFileKeyManager_KeyByID(t *testing.T) {
	dir := t.TempDir()

	oldKey := writeKeyFile(t, dir, "key-alpha")
	time.Sleep(50 * time.Millisecond)
	writeKeyFile(t, dir, "key-beta")

	m, err := NewFileKeyManager(dir)
	if err != nil {
		t.Fatalf("NewFileKeyManager: %v", err)
	}

	t.Run("existing key", func(t *testing.T) {
		got, err := m.KeyByID("key-alpha")
		if err != nil {
			t.Fatalf("KeyByID: %v", err)
		}
		if !bytes.Equal(got, oldKey) {
			t.Error("returned key does not match file content")
		}
	})

	t.Run("missing key", func(t *testing.T) {
		_, err := m.KeyByID("nonexistent")
		if err == nil {
			t.Fatal("expected error for missing key ID")
		}
	})
}

func TestFileKeyManager_RotateKey(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "key-only")

	m, err := NewFileKeyManager(dir)
	if err != nil {
		t.Fatalf("NewFileKeyManager: %v", err)
	}

	_, err = m.RotateKey()
	if err == nil {
		t.Fatal("expected error from RotateKey on file manager")
	}
}

func TestFileKeyManager_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	_, err := NewFileKeyManager(dir)
	if err == nil {
		t.Fatal("expected error for empty key directory")
	}
}

func TestFileKeyManager_InvalidKeyLength(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bad-key"), []byte("too-short"), 0o600); err != nil {
		t.Fatalf("writing bad key: %v", err)
	}

	_, err := NewFileKeyManager(dir)
	if err == nil {
		t.Fatal("expected error for invalid key file length")
	}
}

func TestFileKeyManager_NotADirectory(t *testing.T) {
	f, err := os.CreateTemp("", "keymanager-test-*")
	if err != nil {
		t.Fatalf("creating temp file: %v", err)
	}
	f.Close()
	defer os.Remove(f.Name())

	_, err = NewFileKeyManager(f.Name())
	if err == nil {
		t.Fatal("expected error when path is not a directory")
	}
}

func TestFileKeyManager_SkipsHiddenFiles(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "visible-key")
	// Write a hidden file that should be ignored.
	if err := os.WriteFile(filepath.Join(dir, ".hidden"), make([]byte, 32), 0o600); err != nil {
		t.Fatalf("writing hidden file: %v", err)
	}

	m, err := NewFileKeyManager(dir)
	if err != nil {
		t.Fatalf("NewFileKeyManager: %v", err)
	}

	_, err = m.KeyByID(".hidden")
	if err == nil {
		t.Error("hidden file should have been skipped")
	}
}

func TestFileKeyManager_Reload(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "initial-key")

	m, err := NewFileKeyManager(dir)
	if err != nil {
		t.Fatalf("NewFileKeyManager: %v", err)
	}

	// Add a new key file.
	time.Sleep(50 * time.Millisecond)
	newKey := writeKeyFile(t, dir, "rotated-key")

	if err := m.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}

	keyID, key, err := m.CurrentKey()
	if err != nil {
		t.Fatalf("CurrentKey after reload: %v", err)
	}
	if keyID != "rotated-key" {
		t.Errorf("expected %q after reload, got %q", "rotated-key", keyID)
	}
	if !bytes.Equal(key, newKey) {
		t.Error("reloaded key content does not match")
	}
}

// ---------------------------------------------------------------------------
// keyVersionNumber helper tests
// ---------------------------------------------------------------------------

func TestKeyVersionNumber(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"v1", 1},
		{"v10", 10},
		{"v0", 0},
		{"abc", -1},
		{"v", -1},
		{"vx", -1},
		{"", -1},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := keyVersionNumber(tt.input)
			if got != tt.want {
				t.Errorf("keyVersionNumber(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
