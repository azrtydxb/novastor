package logging

import (
	"testing"
)

func TestInitDevelopment(t *testing.T) {
	Init(false)
	if L == nil {
		t.Fatal("expected L to be non-nil after Init(false)")
	}
	if S == nil {
		t.Fatal("expected S to be non-nil after Init(false)")
	}
	// Ensure we can log without panicking.
	L.Info("development logger test")
	S.Infow("sugared logger test", "key", "value")
}

func TestInitProduction(t *testing.T) {
	Init(true)
	if L == nil {
		t.Fatal("expected L to be non-nil after Init(true)")
	}
	if S == nil {
		t.Fatal("expected S to be non-nil after Init(true)")
	}
	L.Info("production logger test")
	S.Infow("sugared logger test", "key", "value")
}

func TestSync(_ *testing.T) {
	Init(false)
	// Sync should not panic.
	Sync()
}
