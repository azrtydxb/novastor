// Package transport provides TLS and certificate management for NovaStor.
// This package handles mTLS configuration, certificate rotation, and
// secure transport layer setup for gRPC communication.
package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// TLSConfig holds paths to TLS certificate files.
type TLSConfig struct {
	CACertPath string
	CertPath   string
	KeyPath    string
	ServerName string
}

// NewServerTLS creates gRPC server options for mTLS.
func NewServerTLS(cfg TLSConfig) (grpc.ServerOption, error) {
	serverCert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("loading server certificate: %w", err)
	}

	caCert, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,
		MinVersion:   tls.VersionTLS13,
	}

	return grpc.Creds(credentials.NewTLS(tlsConfig)), nil
}

// NewServerTLSWithRotation creates gRPC server options with certificate rotation.
// The CA certificate is loaded from cfg.CACertPath for the trust pool, while the
// leaf certificate is served via the rotator's GetCertificate callback so that
// certificate renewals on disk are picked up without a process restart.
func NewServerTLSWithRotation(cfg TLSConfig, rotator *CertRotator) (grpc.ServerOption, error) {
	caCert, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := rotator.GetCertificate()
			if cert == nil {
				return nil, fmt.Errorf("no certificate loaded by rotator")
			}
			return cert, nil
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  certPool,
		MinVersion: tls.VersionTLS13,
	}

	return grpc.Creds(credentials.NewTLS(tlsConfig)), nil
}

// NewClientTLSWithRotation creates gRPC dial options with certificate rotation.
// The CA certificate is loaded from cfg.CACertPath for server verification, while
// the client certificate is served via the rotator's GetCertificate callback.
func NewClientTLSWithRotation(cfg TLSConfig, rotator *CertRotator) (grpc.DialOption, error) {
	caCert, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert := rotator.GetCertificate()
			if cert == nil {
				return nil, fmt.Errorf("no certificate loaded by rotator")
			}
			return cert, nil
		},
		RootCAs:    certPool,
		ServerName: cfg.ServerName,
		MinVersion: tls.VersionTLS13,
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}

// NewClientTLS creates gRPC dial options for mTLS.
func NewClientTLS(cfg TLSConfig) (grpc.DialOption, error) {
	clientCert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}

	caCert, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to add CA certificate to pool")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
		ServerName:   cfg.ServerName,
		MinVersion:   tls.VersionTLS13,
	}

	return grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), nil
}
