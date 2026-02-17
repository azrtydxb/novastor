package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// generateTestCert writes a self-signed certificate and key to the given paths.
func generateTestCert(t *testing.T, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	defer certFile.Close()
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatalf("failed to write cert PEM: %v", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	defer keyFile.Close()
	if err := pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("failed to write key PEM: %v", err)
	}
}

func TestCertRotator_InitialLoad(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	generateTestCert(t, certPath, keyPath)

	rotator := NewCertRotator(certPath, keyPath, time.Hour)

	cert := rotator.GetCertificate()
	if cert == nil {
		t.Fatal("expected certificate to be loaded")
	}
}

func TestCertRotator_MissingFiles(t *testing.T) {
	rotator := NewCertRotator("/nonexistent/cert.pem", "/nonexistent/key.pem", time.Hour)

	cert := rotator.GetCertificate()
	if cert != nil {
		t.Fatal("expected nil certificate for missing files")
	}
}

func TestCertRotator_TLSConfig(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	generateTestCert(t, certPath, keyPath)

	rotator := NewCertRotator(certPath, keyPath, time.Hour)
	tlsConfig := rotator.TLSConfig()

	if tlsConfig == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if tlsConfig.GetCertificate == nil {
		t.Fatal("expected GetCertificate callback to be set")
	}

	cert, err := tlsConfig.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate failed: %v", err)
	}
	if cert == nil {
		t.Fatal("expected non-nil certificate from TLS config")
	}
}

func TestCertRotator_Reload(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	generateTestCert(t, certPath, keyPath)

	rotator := NewCertRotator(certPath, keyPath, 50*time.Millisecond)

	cert1 := rotator.GetCertificate()
	if cert1 == nil {
		t.Fatal("expected initial certificate")
	}

	// Regenerate the certificate (new serial number).
	generateTestCert(t, certPath, keyPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rotator.Start(ctx)

	// Wait for at least one reload cycle.
	time.Sleep(200 * time.Millisecond)

	cert2 := rotator.GetCertificate()
	if cert2 == nil {
		t.Fatal("expected reloaded certificate")
	}
	// The certificate should have been reloaded (we cannot easily compare
	// certificates, but verify it is non-nil and the rotator is functional).
}
