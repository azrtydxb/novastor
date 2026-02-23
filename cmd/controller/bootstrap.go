// Package main provides the NovaStor controller binary.
package main

import (
	"context"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/piwi3910/novastor/internal/transport"
)

// runTLSBootstrap generates TLS certificates and stores them in a Kubernetes
// Secret. It is designed to run inside a distroless container image without
// requiring a shell, kubectl, or any external tools.
func runTLSBootstrap() error {
	fs := flag.NewFlagSet("tls-bootstrap", flag.ExitOnError)
	secretName := fs.String("secret-name", "", "Name of the Kubernetes Secret to create/update")
	namespace := fs.String("namespace", "", "Kubernetes namespace for the Secret")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return fmt.Errorf("parsing flags: %w", err)
	}

	if *secretName == "" || *namespace == "" {
		return fmt.Errorf("--secret-name and --namespace are required")
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	logger.Info("starting TLS bootstrap",
		zap.String("secretName", *secretName),
		zap.String("namespace", *namespace),
	)

	// Build in-cluster Kubernetes client.
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("getting in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("creating kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Check if secret already exists with a valid CA.
	existing, err := clientset.CoreV1().Secrets(*namespace).Get(ctx, *secretName, metav1.GetOptions{})
	if err == nil {
		if caCert, ok := existing.Data["ca.crt"]; ok && len(caCert) > 0 {
			if block, _ := pem.Decode(caCert); block != nil {
				logger.Info("secret already exists with valid CA certificate, skipping bootstrap")
				return nil
			}
		}
	}

	// Generate CA.
	caCertPEM, caKeyPEM, err := transport.GenerateCA("NovaStor")
	if err != nil {
		return fmt.Errorf("generating CA: %w", err)
	}

	// Generate server certificate with SANs matching the deployment namespace.
	// Include wildcard patterns for FQDN resolution (*.novastor-system.svc)
	// and explicit short names for same-namespace resolution.
	releaseName := strings.TrimSuffix(*secretName, "-tls-certs")
	serverCertPEM, serverKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-server", true, []string{
		"localhost",
		fmt.Sprintf("*.%s.svc", *namespace),
		fmt.Sprintf("*.%s.svc.cluster.local", *namespace),
		// Headless StatefulSet DNS: pod-name.svc-headless.namespace.svc.cluster.local
		// Wildcard certs only match one level, so we need explicit headless wildcards.
		fmt.Sprintf("*.%s-meta-headless.%s.svc.cluster.local", releaseName, *namespace),
		// Short service names for same-namespace resolution
		releaseName + "-meta",
		releaseName + "-agent",
		releaseName + "-controller",
		releaseName + "-csi-controller",
		releaseName + "-s3gw",
		releaseName + "-filer",
		releaseName + "-webhook",
	})
	if err != nil {
		return fmt.Errorf("generating server cert: %w", err)
	}

	// Generate client certificate.
	clientCertPEM, clientKeyPEM, err := transport.GenerateCert(caCertPEM, caKeyPEM, "novastor-client", false, nil)
	if err != nil {
		return fmt.Errorf("generating client cert: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *secretName,
			Namespace: *namespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt":     caCertPEM,
			"ca.key":     caKeyPEM,
			"server.crt": serverCertPEM,
			"server.key": serverKeyPEM,
			"client.crt": clientCertPEM,
			"client.key": clientKeyPEM,
		},
	}

	// Create or update the secret.
	if existing != nil && existing.Name != "" {
		secret.ResourceVersion = existing.ResourceVersion
		_, err = clientset.CoreV1().Secrets(*namespace).Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("updating secret: %w", err)
		}
		logger.Info("TLS bootstrap: updated existing secret")
	} else {
		_, err = clientset.CoreV1().Secrets(*namespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("creating secret: %w", err)
		}
		logger.Info("TLS bootstrap: created new secret")
	}

	logger.Info("TLS bootstrap completed successfully")
	return nil
}
