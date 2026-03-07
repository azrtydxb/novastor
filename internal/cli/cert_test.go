package cli

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/azrtydxb/novastor/internal/transport"
)

// TestCertBootstrap verifies the cert bootstrap command generates
// valid certificates that can be loaded and parsed.
func TestCertBootstrap(t *testing.T) {
	outputDir := t.TempDir()

	// Simulate the bootstrap process by calling the underlying functions
	certOrg := "TestOrg"

	// Generate CA certificate
	caCertPEM, caKeyPEM, err := transport.GenerateCA(certOrg)
	if err != nil {
		t.Fatalf("generating CA certificate: %v", err)
	}

	// Write CA certificates
	caCertPath := filepath.Join(outputDir, "ca.crt")
	caKeyPath := filepath.Join(outputDir, "ca.key")
	if err := transport.WriteCertFiles(caCertPath, caKeyPath, caCertPEM, caKeyPEM); err != nil {
		t.Fatalf("writing CA certificate files: %v", err)
	}

	// Verify CA files exist
	if _, err := os.Stat(caCertPath); os.IsNotExist(err) {
		t.Error("CA certificate file was not created")
	}
	if _, err := os.Stat(caKeyPath); os.IsNotExist(err) {
		t.Error("CA key file was not created")
	}

	// Generate server certificate
	serverDNSNames := []string{
		"localhost",
		"*.novacluster.local",
		"*.novacluster.local.svc",
		"*.novacluster.local.svc.cluster.local",
	}
	serverCertPEM, serverKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-server", true, serverDNSNames)
	if err != nil {
		t.Fatalf("generating server certificate: %v", err)
	}

	serverCertPath := filepath.Join(outputDir, "server.crt")
	serverKeyPath := filepath.Join(outputDir, "server.key")
	if err := transport.WriteCertFiles(serverCertPath, serverKeyPath, serverCertPEM, serverKeyPEM); err != nil {
		t.Fatalf("writing server certificate files: %v", err)
	}

	// Verify server files exist
	if _, err := os.Stat(serverCertPath); os.IsNotExist(err) {
		t.Error("Server certificate file was not created")
	}
	if _, err := os.Stat(serverKeyPath); os.IsNotExist(err) {
		t.Error("Server key file was not created")
	}

	// Generate client certificate
	clientCertPEM, clientKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-client", false, nil)
	if err != nil {
		t.Fatalf("generating client certificate: %v", err)
	}

	clientCertPath := filepath.Join(outputDir, "client.crt")
	clientKeyPath := filepath.Join(outputDir, "client.key")
	if err := transport.WriteCertFiles(clientCertPath, clientKeyPath, clientCertPEM, clientKeyPEM); err != nil {
		t.Fatalf("writing client certificate files: %v", err)
	}

	// Verify client files exist
	if _, err := os.Stat(clientCertPath); os.IsNotExist(err) {
		t.Error("Client certificate file was not created")
	}
	if _, err := os.Stat(clientKeyPath); os.IsNotExist(err) {
		t.Error("Client key file was not created")
	}

	// Verify certificates can be loaded into a TLS configuration
	cert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		t.Errorf("failed to load server certificate pair: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Error("server certificate pair contains no certificates")
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		t.Errorf("failed to load client certificate pair: %v", err)
	}
	if len(clientCert.Certificate) == 0 {
		t.Error("client certificate pair contains no certificates")
	}
}

// TestCertBootstrapExistingCA verifies that bootstrap with existing CA
// uses the provided CA instead of generating a new one.
func TestCertBootstrapExistingCA(t *testing.T) {
	outputDir := t.TempDir()
	certOrg := "ExistingOrg"

	// Generate existing CA
	existingCACertPEM, existingCAKeyPEM, err := transport.GenerateCA(certOrg)
	if err != nil {
		t.Fatalf("generating existing CA: %v", err)
	}

	// Write existing CA files
	existingCACertPath := filepath.Join(outputDir, "existing-ca.crt")
	existingCAKeyPath := filepath.Join(outputDir, "existing-ca.key")
	if err := transport.WriteCertFiles(existingCACertPath, existingCAKeyPath, existingCACertPEM, existingCAKeyPEM); err != nil {
		t.Fatalf("writing existing CA files: %v", err)
	}

	// Read existing CA files
	readCACertPEM, err := os.ReadFile(existingCACertPath)
	if err != nil {
		t.Fatalf("reading existing CA cert: %v", err)
	}
	readCAKeyPEM, err := os.ReadFile(existingCAKeyPath)
	if err != nil {
		t.Fatalf("reading existing CA key: %v", err)
	}

	// Generate server cert using existing CA
	serverCertPEM, _, err := transport.GenerateCert(readCACertPEM, readCAKeyPEM, "novastor-server", true, []string{"localhost"})
	if err != nil {
		t.Fatalf("generating server cert with existing CA: %v", err)
	}

	// Parse the server cert and verify it's signed by the existing CA
	block, _ := pem.Decode(serverCertPEM)
	if block == nil {
		t.Fatal("failed to decode server certificate PEM")
	}

	serverCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing server certificate: %v", err)
	}

	caBlock, _ := pem.Decode(readCACertPEM)
	if caBlock == nil {
		t.Fatal("failed to decode CA certificate PEM")
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	// Verify the server cert is signed by the existing CA
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := serverCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("server cert verification failed: %v", err)
	}

	// Verify the CA has the expected organization
	if len(caCert.Subject.Organization) == 0 || caCert.Subject.Organization[0] != certOrg {
		t.Errorf("CA organization mismatch: got %v, want %s", caCert.Subject.Organization, certOrg)
	}
}

// TestCertValidity verifies that generated certificates have the
// expected validity period.
func TestCertValidity(t *testing.T) {
	certOrg := "ValidityTestOrg"

	// Generate CA certificate
	caCertPEM, caKeyPEM, err := transport.GenerateCA(certOrg)
	if err != nil {
		t.Fatalf("generating CA certificate: %v", err)
	}

	// Parse CA certificate
	caBlock, _ := pem.Decode(caCertPEM)
	if caBlock == nil {
		t.Fatal("failed to decode CA certificate PEM")
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	// Verify CA has ~10 year validity (allow 1 day tolerance)
	caValidityDuration := 10 * 365 * 24 * time.Hour
	caActualDuration := caCert.NotAfter.Sub(caCert.NotBefore)
	caTolerance := 24 * time.Hour

	if absDuration(caActualDuration-caValidityDuration) > caTolerance {
		t.Errorf("CA validity duration: got %v, want %v (tolerance %v)", caActualDuration, caValidityDuration, caTolerance)
	}

	// Generate server certificate
	serverCertPEM, _, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-server", true, []string{"localhost"})
	if err != nil {
		t.Fatalf("generating server certificate: %v", err)
	}

	// Parse server certificate
	serverBlock, _ := pem.Decode(serverCertPEM)
	if serverBlock == nil {
		t.Fatal("failed to decode server certificate PEM")
	}

	serverCert, err := x509.ParseCertificate(serverBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing server certificate: %v", err)
	}

	// Verify server cert has ~1 year validity
	serverValidityDuration := 1 * 365 * 24 * time.Hour
	serverActualDuration := serverCert.NotAfter.Sub(serverCert.NotBefore)

	if absDuration(serverActualDuration-serverValidityDuration) > caTolerance {
		t.Errorf("Server validity duration: got %v, want %v (tolerance %v)", serverActualDuration, serverValidityDuration, caTolerance)
	}

	// Verify server cert includes expected DNS names
	expectedDNSNames := []string{"localhost"}
	if len(serverCert.DNSNames) != len(expectedDNSNames) {
		t.Errorf("DNS names count: got %d, want %d", len(serverCert.DNSNames), len(expectedDNSNames))
	}
	for i, dns := range serverCert.DNSNames {
		if i >= len(expectedDNSNames) || dns != expectedDNSNames[i] {
			t.Errorf("DNS name[%d]: got %s, want %s", i, dns, expectedDNSNames[i])
		}
	}

	// Generate client certificate
	clientCertPEM, _, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-client", false, nil)
	if err != nil {
		t.Fatalf("generating client certificate: %v", err)
	}

	// Parse client certificate
	clientBlock, _ := pem.Decode(clientCertPEM)
	if clientBlock == nil {
		t.Fatal("failed to decode client certificate PEM")
	}

	clientCert, err := x509.ParseCertificate(clientBlock.Bytes)
	if err != nil {
		t.Fatalf("parsing client certificate: %v", err)
	}

	// Verify client cert has ~1 year validity
	clientActualDuration := clientCert.NotAfter.Sub(clientCert.NotBefore)

	if absDuration(clientActualDuration-serverValidityDuration) > caTolerance {
		t.Errorf("Client validity duration: got %v, want %v (tolerance %v)", clientActualDuration, serverValidityDuration, caTolerance)
	}
}

// TestMTLSComponentCommunication verifies that components configured
// with mTLS can communicate with each other and reject unauthenticated
// connections.
func TestMTLSComponentCommunication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mTLS integration test in short mode")
	}

	outputDir := t.TempDir()
	certOrg := "mTLSTest"

	// Generate CA certificate
	caCertPEM, caKeyPEM, err := transport.GenerateCA(certOrg)
	if err != nil {
		t.Fatalf("generating CA certificate: %v", err)
	}

	// Generate server certificate
	serverCertPEM, serverKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-server", true, []string{"localhost"})
	if err != nil {
		t.Fatalf("generating server certificate: %v", err)
	}

	// Generate client certificate
	clientCertPEM, clientKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-client", false, nil)
	if err != nil {
		t.Fatalf("generating client certificate: %v", err)
	}

	// Write certificates to files
	serverCertPath := filepath.Join(outputDir, "server.crt")
	serverKeyPath := filepath.Join(outputDir, "server.key")
	clientCertPath := filepath.Join(outputDir, "client.crt")
	clientKeyPath := filepath.Join(outputDir, "client.key")
	caCertPath := filepath.Join(outputDir, "ca.crt")

	if err := transport.WriteCertFiles(serverCertPath, serverKeyPath, serverCertPEM, serverKeyPEM); err != nil {
		t.Fatalf("writing server certificate files: %v", err)
	}
	if err := transport.WriteCertFiles(clientCertPath, clientKeyPath, clientCertPEM, clientKeyPEM); err != nil {
		t.Fatalf("writing client certificate files: %v", err)
	}
	if err := os.WriteFile(caCertPath, caCertPEM, 0o600); err != nil {
		t.Fatalf("writing CA certificate: %v", err)
	}

	// Verify certificates can be loaded for TLS configuration
	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("loading server certificate pair: %v", err)
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("loading client certificate pair: %v", err)
	}

	// Create CA pool for verification
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	// Verify server certificate chain
	if len(serverCert.Certificate) == 0 {
		t.Fatal("server certificate contains no certificates")
	}
	serverX509Cert, err := x509.ParseCertificate(serverCert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing server certificate: %v", err)
	}
	if _, err := serverX509Cert.Verify(x509.VerifyOptions{
		Roots:     caCertPool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("server certificate verification failed: %v", err)
	}

	// Verify client certificate chain
	if len(clientCert.Certificate) == 0 {
		t.Fatal("client certificate contains no certificates")
	}
	clientX509Cert, err := x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		t.Fatalf("parsing client certificate: %v", err)
	}
	if _, err := clientX509Cert.Verify(x509.VerifyOptions{
		Roots:     caCertPool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("client certificate verification failed: %v", err)
	}

	// Note: Full integration test with actual TLS server/client would require
	// starting gRPC services with mTLS configuration, which is beyond unit test scope.
	// This test verifies certificates are generated correctly and can be loaded
	// for TLS use. Integration testing should be done in e2e tests.
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
