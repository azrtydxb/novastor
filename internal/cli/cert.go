package cli

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/azrtydxb/novastor/internal/transport"
)

var (
	certOutputDir string
	certOrg       string
	certValidity  time.Duration
)

// certCmd represents the cert command group
var certCmd = &cobra.Command{
	Use:   "cert",
	Short: "Certificate management commands",
}

// certBootstrapCmd generates a CA, server cert, and client cert for mTLS
var certBootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Generate CA and leaf certificates for mTLS",
	Long: `Generate a self-signed CA certificate and leaf certificates for
mutual TLS authentication between NovaStor components.

This command creates:
- ca.crt, ca.key: Self-signed CA certificate (10-year validity)
- server.crt, server.key: Server certificate for gRPC endpoints (1-year validity)
- client.crt, client.key: Client certificate for gRPC clients (1-year validity)

All certificates use ECDSA P-256 keys and TLS 1.3. The server certificate
includes the cluster.local service DNS naming convention for Kubernetes
internal services.`,
	RunE: func(_ *cobra.Command, _ []string) error {
		return runCertBootstrap()
	},
}

func init() {
	certBootstrapCmd.Flags().StringVar(&certOutputDir, "output-dir", "/output", "Directory to write certificates")
	certBootstrapCmd.Flags().StringVar(&certOrg, "organization", "NovaStor", "Organization name for certificates")
	certBootstrapCmd.Flags().DurationVar(&certValidity, "validity", 365*24*time.Hour, "Leaf certificate validity period")
	certCmd.AddCommand(certBootstrapCmd)
	rootCmd.AddCommand(certCmd)
}

// runCertBootstrap generates certificates and writes them to the output directory
func runCertBootstrap() error {
	// Ensure output directory exists
	if err := os.MkdirAll(certOutputDir, 0o755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	// Generate CA certificate
	caCertPEM, caKeyPEM, err := transport.GenerateCA(certOrg)
	if err != nil {
		return fmt.Errorf("generating CA certificate: %w", err)
	}

	// Write CA certificates
	caCertPath := filepath.Join(certOutputDir, "ca.crt")
	caKeyPath := filepath.Join(certOutputDir, "ca.key")
	if err := transport.WriteCertFiles(caCertPath, caKeyPath, caCertPEM, caKeyPEM); err != nil {
		return fmt.Errorf("writing CA certificate files: %w", err)
	}
	fmt.Printf("Generated CA certificate: %s\n", caCertPath)

	// Generate server certificate (for gRPC servers: meta, agent)
	// Includes common service DNS names for Kubernetes cluster
	serverDNSNames := []string{
		"localhost",
		// Legacy wildcard SANs
		"*.novacluster.local",
		"*.novacluster.local.svc",
		"*.novacluster.local.svc.cluster.local",
		// Kubernetes service discovery SANs for novastor-system namespace
		"novastor-meta",
		"novastor-meta-headless",
		"novastor-agent",
		"*.novastor-meta-headless.novastor-system.svc.cluster.local",
		"*.novastor-system.svc.cluster.local",
	}
	serverCertPEM, serverKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-server", true, serverDNSNames)
	if err != nil {
		return fmt.Errorf("generating server certificate: %w", err)
	}

	serverCertPath := filepath.Join(certOutputDir, "server.crt")
	serverKeyPath := filepath.Join(certOutputDir, "server.key")
	if err := transport.WriteCertFiles(serverCertPath, serverKeyPath, serverCertPEM, serverKeyPEM); err != nil {
		return fmt.Errorf("writing server certificate files: %w", err)
	}
	fmt.Printf("Generated server certificate: %s\n", serverCertPath)

	// Generate client certificate (for gRPC clients: csi, filer, s3gw, controller)
	clientCertPEM, clientKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-client", false, nil)
	if err != nil {
		return fmt.Errorf("generating client certificate: %w", err)
	}

	clientCertPath := filepath.Join(certOutputDir, "client.crt")
	clientKeyPath := filepath.Join(certOutputDir, "client.key")
	if err := transport.WriteCertFiles(clientCertPath, clientKeyPath, clientCertPEM, clientKeyPEM); err != nil {
		return fmt.Errorf("writing client certificate files: %w", err)
	}
	fmt.Printf("Generated client certificate: %s\n", clientCertPath)

	// Generate Kubernetes Secret manifest
	secretManifest, err := generateSecretManifest(caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM, clientCertPEM, clientKeyPEM)
	if err != nil {
		return fmt.Errorf("generating Secret manifest: %w", err)
	}

	secretPath := filepath.Join(certOutputDir, "secret.yaml")
	if err := os.WriteFile(secretPath, []byte(secretManifest), 0o644); err != nil {
		return fmt.Errorf("writing Secret manifest: %w", err)
	}
	fmt.Printf("Generated Secret manifest: %s\n", secretPath)

	return nil
}

// generateSecretManifest creates a Kubernetes Secret YAML manifest
func generateSecretManifest(caCert, caKey, serverCert, serverKey, clientCert, clientKey []byte) (string, error) {
	var buf bytes.Buffer
	buf.WriteString("# This is an auto-generated Secret for NovaStor mTLS certificates.\n")
	buf.WriteString("# To apply: kubectl apply -f secret.yaml\n")
	buf.WriteString("---\n")
	buf.WriteString("apiVersion: v1\n")
	buf.WriteString("kind: Secret\n")
	buf.WriteString("metadata:\n")
	buf.WriteString("  name: novastor-tls-certs\n")
	buf.WriteString("  namespace: novastor\n")
	buf.WriteString("type: Opaque\n")
	buf.WriteString("data:\n")
	fmt.Fprintf(&buf, "  ca.crt: %s\n", bytesToBase64(caCert))
	fmt.Fprintf(&buf, "  ca.key: %s\n", bytesToBase64(caKey))
	fmt.Fprintf(&buf, "  server.crt: %s\n", bytesToBase64(serverCert))
	fmt.Fprintf(&buf, "  server.key: %s\n", bytesToBase64(serverKey))
	fmt.Fprintf(&buf, "  client.crt: %s\n", bytesToBase64(clientCert))
	fmt.Fprintf(&buf, "  client.key: %s\n", bytesToBase64(clientKey))
	return buf.String(), nil
}

// bytesToBase64 converts a byte slice to a base64 string for YAML embedding
func bytesToBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}
