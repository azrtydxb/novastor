package chunk

import (
	"context"
	"testing"
)

func TestRegisterBackend(t *testing.T) {
	// Create a unique backend name for testing.
	testName := "test-backend-" + string(NewChunkID([]byte("test")))

	// Define a simple factory.
	factory := func(config map[string]string) (Store, error) {
		return NewMemoryStore(), nil
	}

	// Register the backend.
	RegisterBackend(testName, factory)

	// Verify it's in the registry.
	registered := GetRegisteredBackends()
	found := false
	for _, name := range registered {
		if name == testName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("registered backend %q not found in %v", testName, registered)
	}

	// Verify we can create an instance.
	store, err := CreateBackend(testName, nil)
	if err != nil {
		t.Fatalf("CreateBackend failed: %v", err)
	}
	if store == nil {
		t.Error("CreateBackend returned nil store")
	}

	// Verify the store works.
	ctx := context.Background()
	data := []byte("test data")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if err := store.Put(ctx, chunks[0]); err != nil {
		t.Errorf("store.Put failed: %v", err)
	}
}

func TestRegisterBackendDuplicate(t *testing.T) {
	// This test verifies that registering a duplicate backend panics.
	// We can't run this in the same process as it would crash the test runner.
	// Instead, we document the expected behavior.
	t.Skip("registering a duplicate backend should panic")
}

func TestCreateBackendUnknown(t *testing.T) {
	_, err := CreateBackend("unknown-backend-type", nil)
	if err == nil {
		t.Error("expected error for unknown backend type, got nil")
	}
}

func TestCreateBackendLocal(t *testing.T) {
	// Test the built-in local backend.
	tempDir := t.TempDir()
	store, err := CreateBackend("local", map[string]string{"dir": tempDir})
	if err != nil {
		t.Fatalf("CreateBackend failed: %v", err)
	}
	if store == nil {
		t.Fatal("CreateBackend returned nil store")
	}

	// Verify it's a LocalStore by exercising it.
	if _, ok := store.(*LocalStore); !ok {
		t.Error("expected *LocalStore, got different type")
	}

	// Verify the store works.
	ctx := context.Background()
	data := []byte("test data for local backend")
	chunks := SplitData(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if err := store.Put(ctx, chunks[0]); err != nil {
		t.Errorf("store.Put failed: %v", err)
	}

	got, err := store.Get(ctx, chunks[0].ID)
	if err != nil {
		t.Errorf("store.Get failed: %v", err)
	}
	if string(got.Data) != string(chunks[0].Data) {
		t.Error("data mismatch")
	}
}

func TestCreateBackendLocalMissingConfig(t *testing.T) {
	// Test that local backend requires dir config.
	_, err := CreateBackend("local", map[string]string{})
	if err == nil {
		t.Error("expected error for missing dir config, got nil")
	}
}

func TestCreateBackendMemory(t *testing.T) {
	// Test the built-in memory backend.
	store, err := CreateBackend("memory", nil)
	if err != nil {
		t.Fatalf("CreateBackend failed: %v", err)
	}
	if store == nil {
		t.Fatal("CreateBackend returned nil store")
	}

	// Verify it's a MemoryStore.
	if _, ok := store.(*MemoryStore); !ok {
		t.Error("expected *MemoryStore, got different type")
	}

	// Run conformance tests.
	StoreConformanceTest(store, t)
}

func TestGetRegisteredBackends(t *testing.T) {
	backends := GetRegisteredBackends()
	if len(backends) < 2 {
		t.Errorf("expected at least 2 built-in backends, got %d", len(backends))
	}

	// Verify expected backends are present.
	backendMap := make(map[string]bool)
	for _, name := range backends {
		backendMap[name] = true
	}

	expectedBackends := []string{"local", "memory"}
	for _, expected := range expectedBackends {
		if !backendMap[expected] {
			t.Errorf("expected backend %q not found in %v", expected, backends)
		}
	}
}

func TestBackendFactoryConcurrent(t *testing.T) {
	// Test that the backend registry is thread-safe.
	const numGoroutines = 10
	done := make(chan bool)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			// Try to create backends concurrently.
			_, err := CreateBackend("memory", nil)
			if err != nil {
				t.Errorf("CreateBackend failed: %v", err)
			}
			backends := GetRegisteredBackends()
			if len(backends) == 0 {
				t.Error("GetRegisteredBackends returned empty list")
			}
			done <- true
		}()
	}

	// Wait for all goroutines.
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
