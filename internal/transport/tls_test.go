package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func writeTempCerts(t *testing.T, dir string, caCert, caKey []byte, cn string, isServer bool, dnsNames []string) (certPath, keyPath string) {
	t.Helper()
	certPEM, keyPEM, err := GenerateCert(caCert, caKey, cn, isServer, dnsNames)
	if err != nil {
		t.Fatalf("GenerateCert(%s): %v", cn, err)
	}
	certPath = filepath.Join(dir, cn+".crt")
	keyPath = filepath.Join(dir, cn+".key")
	if err := WriteCertFiles(certPath, keyPath, certPEM, keyPEM); err != nil {
		t.Fatalf("WriteCertFiles(%s): %v", cn, err)
	}
	return certPath, keyPath
}

func TestNewServerTLS_Success(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := WriteCertFiles(caPath, filepath.Join(dir, "ca.key"), caCertPEM, caKeyPEM); err != nil {
		t.Fatalf("writing CA: %v", err)
	}

	serverCertPath, serverKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "server", true, []string{"localhost"})

	cfg := TLSConfig{
		CACertPath: caPath,
		CertPath:   serverCertPath,
		KeyPath:    serverKeyPath,
	}

	opt, err := NewServerTLS(cfg)
	if err != nil {
		t.Fatalf("NewServerTLS: %v", err)
	}
	if opt == nil {
		t.Fatal("NewServerTLS returned nil option")
	}
}

func TestNewClientTLS_Success(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := WriteCertFiles(caPath, filepath.Join(dir, "ca.key"), caCertPEM, caKeyPEM); err != nil {
		t.Fatalf("writing CA: %v", err)
	}

	clientCertPath, clientKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "client", false, nil)

	cfg := TLSConfig{
		CACertPath: caPath,
		CertPath:   clientCertPath,
		KeyPath:    clientKeyPath,
		ServerName: "localhost",
	}

	opt, err := NewClientTLS(cfg)
	if err != nil {
		t.Fatalf("NewClientTLS: %v", err)
	}
	if opt == nil {
		t.Fatal("NewClientTLS returned nil option")
	}
}

func TestNewServerTLS_BadCert(t *testing.T) {
	cfg := TLSConfig{
		CACertPath: "/nonexistent/ca.crt",
		CertPath:   "/nonexistent/server.crt",
		KeyPath:    "/nonexistent/server.key",
	}

	_, err := NewServerTLS(cfg)
	if err == nil {
		t.Fatal("expected error for bad cert paths, got nil")
	}
}

func TestNewClientTLS_BadCA(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	clientCertPath, clientKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "client", false, nil)

	cfg := TLSConfig{
		CACertPath: "/nonexistent/ca.crt",
		CertPath:   clientCertPath,
		KeyPath:    clientKeyPath,
		ServerName: "localhost",
	}

	_, err = NewClientTLS(cfg)
	if err == nil {
		t.Fatal("expected error for bad CA path, got nil")
	}
}

func TestMTLS_RoundTrip(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := WriteCertFiles(caPath, filepath.Join(dir, "ca.key"), caCertPEM, caKeyPEM); err != nil {
		t.Fatalf("writing CA: %v", err)
	}

	serverCertPath, serverKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "server", true, []string{"localhost"})
	clientCertPath, clientKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "client", false, nil)

	// Create server with mTLS.
	serverOpt, err := NewServerTLS(TLSConfig{
		CACertPath: caPath,
		CertPath:   serverCertPath,
		KeyPath:    serverKeyPath,
	})
	if err != nil {
		t.Fatalf("NewServerTLS: %v", err)
	}

	srv := grpc.NewServer(serverOpt)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	go func() {
		_ = srv.Serve(lis) // Server stopped; expected on cleanup.
	}()
	defer srv.Stop()

	// Create client with mTLS.
	clientOpt, err := NewClientTLS(TLSConfig{
		CACertPath: caPath,
		CertPath:   clientCertPath,
		KeyPath:    clientKeyPath,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("NewClientTLS: %v", err)
	}

	addr := lis.Addr().String()
	conn, err := grpc.NewClient(
		fmt.Sprintf("dns:///%s", addr),
		clientOpt,
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check over mTLS: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("unexpected health status: %v", resp.Status)
	}
}

// TestMTLS_RejectsConnectionWithoutClientCert verifies that the server
// configured with mTLS rejects connections from clients that don't present
// a valid client certificate.
func TestMTLS_RejectsConnectionWithoutClientCert(t *testing.T) {
	caCertPEM, caKeyPEM, err := GenerateCA("TestOrg")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := WriteCertFiles(caPath, filepath.Join(dir, "ca.key"), caCertPEM, caKeyPEM); err != nil {
		t.Fatalf("writing CA: %v", err)
	}

	serverCertPath, serverKeyPath := writeTempCerts(t, dir, caCertPEM, caKeyPEM, "server", true, []string{"localhost"})

	// Create server with mTLS (requires client certificate).
	serverOpt, err := NewServerTLS(TLSConfig{
		CACertPath: caPath,
		CertPath:   serverCertPath,
		KeyPath:    serverKeyPath,
	})
	if err != nil {
		t.Fatalf("NewServerTLS: %v", err)
	}

	srv := grpc.NewServer(serverOpt)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	go func() {
		_ = srv.Serve(lis) // Server stopped; expected on cleanup.
	}()
	defer srv.Stop()

	// Try to connect with a client that does NOT present a certificate.
	// This should fail because the server requires client certificates.
	// Use TLS credentials without a client certificate.
	addr := lis.Addr().String()
	conn, err := grpc.NewClient(
		fmt.Sprintf("dns:///%s", addr),
		grpc.WithTransportCredentials(grpcNoClientCertCreds()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient (no client cert): %v", err)
	}
	defer conn.Close()

	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The connection should succeed at the TCP level, but RPC calls should fail
	// because the server requires a client certificate.
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected error when calling RPC without client certificate, got nil")
	}
	// The error should indicate a connection problem (typically "connection closed",
	// "transport is closing", or "certificate required" depending on the TLS implementation).
}

// grpcNoClientCertCreds returns TLS credentials that don't present a client certificate.
// This is used only for testing that mTLS properly rejects unauthenticated connections.
func grpcNoClientCertCreds() grpcNoClientCert {
	return grpcNoClientCert{}
}

type grpcNoClientCert struct{}

type noAuthInfo struct{}

func (noAuthInfo) AuthType() string {
	return "no-auth"
}

func (c grpcNoClientCert) ClientHandshake(ctx context.Context, _ string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	// Perform TLS handshake but without sending a client certificate.
	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true, // Skip CA verification for test purposes
		MinVersion:         tls.VersionTLS12,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, nil, err
	}
	// Return empty AuthInfo since we're not providing client certs
	return tlsConn, noAuthInfo{}, nil
}

func (c grpcNoClientCert) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return conn, noAuthInfo{}, nil
}

func (c grpcNoClientCert) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{}
}

func (c grpcNoClientCert) Clone() credentials.TransportCredentials {
	return grpcNoClientCert{}
}

func (c grpcNoClientCert) OverrideServerName(_ string) error {
	return nil
}
