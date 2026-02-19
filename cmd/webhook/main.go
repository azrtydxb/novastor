// Package main provides the NovaStor admission webhook binary.
// The webhook injects scheduler configuration into pods that use
// NovaStor storage for data-locality aware scheduling.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metrics"
	"github.com/piwi3910/novastor/internal/webhook"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var tlsCertFile string
	var tlsKeyFile string
	var port int
	var metricsAddr string
	var healthProbeAddr string
	var kubeconfig string
	var showVersion bool

	flag.StringVar(&tlsCertFile, "tls-cert", "/etc/webhook-cert/tls.crt", "Path to TLS certificate file.")
	flag.StringVar(&tlsKeyFile, "tls-key", "/etc/webhook-cert/tls.key", "Path to TLS key file.")
	flag.IntVar(&port, "port", 9443, "Webhook server port.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file. If empty, in-cluster config is used.")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit.")

	flag.Parse()

	if showVersion {
		_, _ = fmt.Fprintf(os.Stdout, "novastor-webhook %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	// Initialize logging.
	logging.Init(false)
	logger := logging.L.Named("webhook")

	// Register Prometheus metrics.
	metrics.Register()

	logger.Info("starting novastor-webhook", zap.String("version", version), zap.String("commit", commit), zap.String("date", date))

	// Create Kubernetes client.
	k8sClient, err := createKubernetesClient(kubeconfig)
	if err != nil {
		logger.Error("unable to create Kubernetes client", zap.Error(err))
		os.Exit(1)
	}

	// Create the scheduler injector.
	injector := webhook.NewSchedulerInjector(k8sClient)

	// Create HTTP server with TLS.
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: createRouter(injector),
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// Start metrics server.
	go startMetricsServer(metricsAddr)

	// Start health probe server.
	go startHealthProbeServer(healthProbeAddr)

	// Start the webhook server in a goroutine.
	errChan := make(chan error, 1)
	go func() {
		logger.Info("webhook server listening", zap.Int("port", port), zap.String("path", "/inject-scheduler"))
		if err := server.ListenAndServeTLS(tlsCertFile, tlsKeyFile); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("webhook server error: %w", err)
		}
	}()

	// Wait for interrupt signal.
	stopCh := setupSignalHandler()
	select {
	case <-stopCh:
		logger.Info("shutting down webhook server")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("error shutting down webhook server", zap.Error(err))
		}
	case err := <-errChan:
		logger.Error("webhook server failed", zap.Error(err))
		os.Exit(1)
	}

	logger.Info("webhook server stopped")
}

// createKubernetesClient creates a Kubernetes client using either in-cluster
// configuration or the provided kubeconfig file.
func createKubernetesClient(kubeconfig string) (kubernetes.Interface, error) {
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("building kubeconfig: %w", err)
	}

	return kubernetes.NewForConfig(config)
}

// createRouter creates the HTTP router for the webhook server.
func createRouter(injector *webhook.SchedulerInjector) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/inject-scheduler", injector.ServeHTTP)
	return mux
}

// startMetricsServer starts a Prometheus metrics server.
func startMetricsServer(addr string) {
	logger := logging.L.Named("metrics")
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Info("metrics server listening", zap.String("addr", addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("metrics server error", zap.Error(err))
	}
}

// startHealthProbeServer starts a health probe server.
func startHealthProbeServer(addr string) {
	logger := logging.L.Named("health")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	logger.Info("health probe server listening", zap.String("addr", addr))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("health probe server error", zap.Error(err))
	}
}

// setupSignalHandler creates a channel that will be closed when an interrupt
// signal is received.
func setupSignalHandler() <-chan struct{} {
	stopCh := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		close(stopCh)
	}()
	return stopCh
}
