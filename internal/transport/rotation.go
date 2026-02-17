package transport

import (
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"
)

// CertRotator watches for certificate file changes and reloads them
// periodically. It provides a tls.Config that uses the latest certificate.
type CertRotator struct {
	certPath string
	keyPath  string
	interval time.Duration
	mu       sync.RWMutex
	cert     *tls.Certificate
}

// NewCertRotator creates a new CertRotator that loads certificates from the
// given paths and checks for updates at the specified interval.
func NewCertRotator(certPath, keyPath string, interval time.Duration) *CertRotator {
	r := &CertRotator{
		certPath: certPath,
		keyPath:  keyPath,
		interval: interval,
	}
	// Attempt an initial load.
	if err := r.reload(); err != nil {
		log.Printf("CertRotator: initial certificate load failed: %v", err)
	}
	return r
}

// reload loads (or reloads) the certificate and key from disk.
func (r *CertRotator) reload() error {
	cert, err := tls.LoadX509KeyPair(r.certPath, r.keyPath)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.cert = &cert
	r.mu.Unlock()
	return nil
}

// Start begins the background certificate rotation loop. It periodically
// checks for certificate file changes and reloads them. The loop runs until
// the context is cancelled.
func (r *CertRotator) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.reload(); err != nil {
					log.Printf("CertRotator: failed to reload certificate: %v", err)
				} else {
					log.Printf("CertRotator: certificate reloaded successfully")
				}
			}
		}
	}()
}

// GetCertificate returns the currently loaded TLS certificate.
// Returns nil if no certificate has been loaded.
func (r *CertRotator) GetCertificate() *tls.Certificate {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cert
}

// TLSConfig returns a tls.Config that uses the GetCertificate callback
// to always serve the most recently loaded certificate.
func (r *CertRotator) TLSConfig() *tls.Config {
	return &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := r.GetCertificate()
			if cert == nil {
				return nil, errNoCertificateLoaded
			}
			return cert, nil
		},
		MinVersion: tls.VersionTLS13,
	}
}

// errNoCertificateLoaded is returned when GetCertificate is called but
// no certificate has been loaded yet.
var errNoCertificateLoaded = &noCertError{}

type noCertError struct{}

func (e *noCertError) Error() string {
	return "no certificate loaded"
}
