package transport

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
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

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	go func() {
		if serveErr := srv.Serve(lis); serveErr != nil {
			// Server stopped; expected on cleanup.
		}
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
