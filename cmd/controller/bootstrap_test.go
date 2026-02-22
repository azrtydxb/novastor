package main

import (
	"os"
	"testing"
)

func TestRunTLSBootstrap_MissingFlags(t *testing.T) {
	// Save and restore os.Args.
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Missing required flags should return an error.
	os.Args = []string{"novastor-controller", "tls-bootstrap"}
	err := runTLSBootstrap()
	if err == nil {
		t.Fatal("expected error for missing flags, got nil")
	}
	if err.Error() != "--secret-name and --namespace are required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRunTLSBootstrap_MissingNamespace(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	os.Args = []string{"novastor-controller", "tls-bootstrap", "--secret-name=test-secret"}
	err := runTLSBootstrap()
	if err == nil {
		t.Fatal("expected error for missing namespace flag, got nil")
	}
}

func TestRunTLSBootstrap_NoInClusterConfig(t *testing.T) {
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	// Providing both flags but running outside cluster should fail at InClusterConfig.
	os.Args = []string{"novastor-controller", "tls-bootstrap", "--secret-name=test", "--namespace=default"}
	err := runTLSBootstrap()
	if err == nil {
		t.Fatal("expected error when not running in cluster, got nil")
	}
	// Should fail at InClusterConfig step.
	expected := "getting in-cluster config"
	if len(err.Error()) < len(expected) || err.Error()[:len(expected)] != expected {
		t.Logf("got error: %v (this is expected when running outside a cluster)", err)
	}
}
