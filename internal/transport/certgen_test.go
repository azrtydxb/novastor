package transport

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateCA(t *testing.T) {
	certPEM, keyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode CA certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	if !cert.IsCA {
		t.Error("CA certificate IsCA should be true")
	}

	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] != "TestOrg" {
		t.Errorf("unexpected organization: %v", cert.Subject.Organization)
	}

	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		t.Fatal("failed to decode CA key PEM")
	}
	if keyBlock.Type != "EC PRIVATE KEY" {
		t.Errorf("unexpected key PEM type: %s", keyBlock.Type)
	}
}

func TestGenerateCert_Server(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	certPEM, _, err := GenerateCert(caCertPEM, caKeyPEM, "server.test", true, []string{"localhost"})
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode server certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing server certificate: %v", err)
	}

	if cert.Subject.CommonName != "server.test" {
		t.Errorf("unexpected CN: %s", cert.Subject.CommonName)
	}

	hasServerAuth := false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			hasServerAuth = true
		}
	}
	if !hasServerAuth {
		t.Error("server cert missing ExtKeyUsageServerAuth")
	}

	// Verify it's signed by the CA.
	caBlock, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(caBlock.Bytes)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("server cert verification failed: %v", err)
	}
}

func TestGenerateCert_Client(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	certPEM, _, err := GenerateCert(caCertPEM, caKeyPEM, "client.test", false, nil)
	if err != nil {
		t.Fatalf("GenerateCert: %v", err)
	}

	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("failed to decode client certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing client certificate: %v", err)
	}

	if cert.Subject.CommonName != "client.test" {
		t.Errorf("unexpected CN: %s", cert.Subject.CommonName)
	}

	hasClientAuth := false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageClientAuth {
			hasClientAuth = true
		}
	}
	if !hasClientAuth {
		t.Error("client cert missing ExtKeyUsageClientAuth")
	}

	// Verify it's signed by the CA.
	caBlock, _ := pem.Decode(caCertPEM)
	caCert, _ := x509.ParseCertificate(caBlock.Bytes)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("client cert verification failed: %v", err)
	}
}

func TestWriteCertFiles(t *testing.T) {
	certPEM, keyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	if err := WriteCertFiles(certPath, keyPath, certPEM, keyPEM); err != nil {
		t.Fatalf("WriteCertFiles: %v", err)
	}

	readCert, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatalf("reading cert file: %v", err)
	}
	block, _ := pem.Decode(readCert)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Error("cert file does not contain valid PEM CERTIFICATE")
	}

	readKey, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("reading key file: %v", err)
	}
	keyBlock, _ := pem.Decode(readKey)
	if keyBlock == nil || keyBlock.Type != "EC PRIVATE KEY" {
		t.Error("key file does not contain valid PEM EC PRIVATE KEY")
	}
}
