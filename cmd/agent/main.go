// Package main provides the NovaStor storage agent binary.
// The agent runs on each storage node as a management plane: it coordinates
// with the metadata service, manages NVMe-oF targets via the SPDK data plane,
// and exposes Prometheus metrics. All data-path I/O is handled by the Rust
// SPDK data-plane process.
package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	novastorev1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/agent/device"
	"github.com/azrtydxb/novastor/internal/agent/failover"
	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
	"github.com/azrtydxb/novastor/internal/transport"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// buildNodeMeta constructs current NodeMeta from SPDK data-plane stats.
func buildNodeMeta(nodeID, listenAddr, status string) *metadata.NodeMeta {
	return &metadata.NodeMeta{
		NodeID:        nodeID,
		Address:       listenAddr,
		DiskCount:     1,
		LastHeartbeat: time.Now().Unix(),
		Status:        status,
	}
}

// registerNode sends the initial NodeMeta registration and then periodically
// sends heartbeats until the context is cancelled.
func registerNode(ctx context.Context, client *metadata.GRPCClient, nodeID, listenAddr string, heartbeatInterval time.Duration) {
	// Initial registration.
	if err := client.PutNodeMeta(ctx, buildNodeMeta(nodeID, listenAddr, "ready")); err != nil {
		logging.L.Warn("node registration failed; will retry on next heartbeat", zap.Error(err))
	} else {
		logging.L.Info("node registered with metadata service", zap.String("nodeID", nodeID))
	}

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Mark node as offline on graceful shutdown.
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := client.PutNodeMeta(shutdownCtx, buildNodeMeta(nodeID, listenAddr, "offline")); err != nil {
				logging.L.Warn("failed to mark node offline on shutdown", zap.Error(err))
			}
			cancel()
			return
		case <-ticker.C:
			if err := client.PutNodeMeta(ctx, buildNodeMeta(nodeID, listenAddr, "ready")); err != nil {
				logging.L.Warn("heartbeat failed", zap.String("nodeID", nodeID), zap.Error(err))
			} else {
				logging.L.Debug("heartbeat sent", zap.String("nodeID", nodeID))
			}
		}
	}
}

func main() {
	listenAddr := flag.String("listen", ":9100", "gRPC listen address")
	hostIP := flag.String("host-ip", "", "Host IP address advertised to NVMe-oF initiators (required)")
	podIP := flag.String("pod-ip", "", "Pod IP address used for gRPC registration (pod-network routable)")
	metaAddr := flag.String("meta-addr", "localhost:7001", "Metadata service address")
	metricsAddr := flag.String("metrics-addr", ":9101", "Prometheus metrics listen address")
	gcInterval := flag.Duration("gc-interval", 1*time.Hour, "Interval between garbage collection runs")
	heartbeatInterval := flag.Duration("heartbeat-interval", 30*time.Second, "Interval between metadata heartbeats")
	nodeID := flag.String("node-id", "", "Unique node ID (defaults to hostname)")
	tlsCA := flag.String("tls-ca", "", "Path to CA certificate for mTLS")
	tlsCert := flag.String("tls-cert", "", "Path to server certificate for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to server key for mTLS")
	tlsClientCert := flag.String("tls-client-cert", "", "Path to client certificate for outbound mTLS connections to metadata service")
	tlsClientKey := flag.String("tls-client-key", "", "Path to client key for outbound mTLS connections to metadata service")
	tlsRotationInterval := flag.Duration("tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")
	dataplaneAddr := flag.String("dataplane-addr", "localhost:9500", "Address of the Rust gRPC dataplane")
	spdkBaseBdev := flag.String("spdk-base-bdev", "NVMe0n1", "SPDK bdev name used as the base device for the storage backend (e.g. NVMe0n1, Malloc0)")
	testMode := flag.Bool("test-mode", false, "Enable test mode: allows Malloc bdev auto-creation (NOT for production use)")
	flag.Parse()

	logging.Init(false)
	defer logging.Sync()

	logging.L.Info("novastor-agent starting",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.String("built", date),
	)

	// Resolve node ID from hostname if not provided.
	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logging.L.Fatal("failed to determine hostname for node ID", zap.Error(err))
		}
		*nodeID = hostname
	}

	// Register Prometheus metrics.
	metrics.Register()

	// Main context for the agent lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialise device manager for NVMe unbinding and hugepage management.
	devMgr := device.NewManager(logging.L)
	if err := devMgr.EnsureHugetlbfsMount(); err != nil {
		logging.L.Warn("hugetlbfs check failed (SPDK may not start)", zap.Error(err))
	}
	// Ensure minimum hugepages for SPDK (1024 x 2MB = 2GB).
	if hpInfo, hpErr := devMgr.EnsureHugepages(1024, ""); hpErr != nil {
		logging.L.Warn("hugepage allocation failed", zap.Error(hpErr))
	} else {
		logging.L.Info("hugepages ready",
			zap.Int("total", hpInfo.TotalPages),
			zap.Int("free", hpInfo.FreePages),
			zap.Uint64("totalBytes", hpInfo.TotalBytes),
		)
	}

	// Connect to the Rust data-plane via gRPC. SPDK is always required;
	// all data-path I/O goes through the Rust dataplane.
	logging.L.Info("connecting to Rust data-plane via gRPC", zap.String("addr", *dataplaneAddr))
	dpClient, err := dataplane.Dial(*dataplaneAddr, logging.L)
	if err != nil {
		logging.L.Fatal("failed to connect to Rust data-plane", zap.Error(err))
	}
	defer dpClient.Close()
	logging.L.Info("connected to Rust data-plane via gRPC", zap.String("addr", *dataplaneAddr))

	// Verify dataplane connectivity.
	dpVersion, dpCommit, err := dpClient.GetVersion()
	if err != nil {
		logging.L.Warn("dataplane version check failed (dataplane may not be ready yet)", zap.Error(err))
	} else {
		logging.L.Info("dataplane version",
			zap.String("version", dpVersion),
			zap.String("commit", dpCommit),
		)
	}

	// Build gRPC server options.
	var serverOpts []grpc.ServerOption
	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		rotator := transport.NewCertRotator(*tlsCert, *tlsKey, *tlsRotationInterval)
		rotator.Start(ctx)
		logging.L.Info("TLS certificate rotation enabled",
			zap.String("certPath", *tlsCert),
			zap.String("keyPath", *tlsKey),
			zap.Duration("interval", *tlsRotationInterval),
		)
		tlsOpt, tlsErr := transport.NewServerTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   *tlsCert,
			KeyPath:    *tlsKey,
		}, rotator)
		if tlsErr != nil {
			logging.L.Fatal("failed to configure TLS", zap.Error(tlsErr))
		}
		serverOpts = append(serverOpts, tlsOpt)
	}

	srv := grpc.NewServer(serverOpts...)

	// Build TLS dial options for outbound metadata connections.
	var dialOpts []grpc.DialOption
	clientCertPath := *tlsClientCert
	clientKeyPath := *tlsClientKey
	if clientCertPath == "" {
		clientCertPath = *tlsCert
	}
	if clientKeyPath == "" {
		clientKeyPath = *tlsKey
	}
	if *tlsCA != "" && clientCertPath != "" && clientKeyPath != "" {
		clientRotator := transport.NewCertRotator(clientCertPath, clientKeyPath, *tlsRotationInterval)
		clientRotator.Start(ctx)
		clientTLSOpt, clientTLSErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
			CACertPath: *tlsCA,
			CertPath:   clientCertPath,
			KeyPath:    clientKeyPath,
		}, clientRotator)
		if clientTLSErr != nil {
			logging.L.Fatal("failed to configure client TLS for metadata connection", zap.Error(clientTLSErr))
		}
		dialOpts = append(dialOpts, clientTLSOpt)
	}

	// Connect to the metadata service.
	var metaClient *metadata.GRPCClient
	if *metaAddr != "" {
		var metaErr error
		metaClient, metaErr = metadata.Dial(*metaAddr, dialOpts...)
		if metaErr != nil {
			logging.L.Warn("cannot connect to metadata service; some features disabled",
				zap.String("metaAddr", *metaAddr),
				zap.Error(metaErr),
			)
		}
	}

	// Compute registrationAddr for metadata and failover.
	registrationAddr := *listenAddr
	if *podIP != "" {
		_, port, splitErr := net.SplitHostPort(*listenAddr)
		if splitErr == nil {
			registrationAddr = net.JoinHostPort(*podIP, port)
		}
	}

	// Register NVMe-oF target service. SPDK dataplane is always required;
	// all data-path I/O goes through the Rust dataplane via gRPC.
	var spdkServer *agent.SPDKTargetServer
	if *hostIP == "" {
		logging.L.Info("NVMe-oF target service disabled (--host-ip not set)")
	} else if metaClient == nil {
		logging.L.Warn("NVMe-oF target service disabled: no metadata connection")
	} else {
		spdkServer = agent.NewSPDKTargetServer(*hostIP, *spdkBaseBdev, *testMode, dpClient, metaClient)
		spdkServer.Register(srv)
		logging.L.Info("NVMe-oF target service registered", zap.String("hostIP", *hostIP))

		// Start failover controller for ANA state management.
		fc := failover.New(*nodeID, registrationAddr, *hostIP, metaClient, dpClient, logging.L)
		if err := fc.Start(ctx); err != nil {
			logging.L.Fatal("failover controller start", zap.Error(err))
		}
		defer fc.Stop()
	}

	// Register ChunkService server — bridges S3/Filer gRPC chunk I/O
	// to the Rust SPDK data-plane via gRPC.
	chunkServer := agent.NewChunkServer(dpClient, *spdkBaseBdev)
	chunkServer.Register(srv)
	logging.L.Info("chunk service registered (routes I/O to SPDK dataplane via gRPC)")

	// Start the garbage collector for orphan chunks via SPDK data-plane.
	if metaClient != nil {
		spdkGC := agent.NewSPDKGarbageCollector(dpClient, metaClient, *spdkBaseBdev, *gcInterval)
		spdkGC.Start(ctx)
		defer spdkGC.Stop()
		logging.L.Info("agent garbage collector started (routes through SPDK dataplane via gRPC)",
			zap.Duration("interval", *gcInterval),
			zap.String("bdev", *spdkBaseBdev))
	}

	// Register this node with the metadata service.
	if metaClient != nil {
		defer metaClient.Close()
		go registerNode(ctx, metaClient, *nodeID, registrationAddr, *heartbeatInterval)
	}

	// Start dataplane heartbeat loop to prevent fencing.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fenced, hbErr := dpClient.Heartbeat(*nodeID)
				if hbErr != nil {
					logging.L.Warn("dataplane heartbeat failed", zap.Error(hbErr))
				} else if fenced {
					logging.L.Error("dataplane reports FENCED state")
				}
			}
		}
	}()

	// Start controller-runtime manager for BackendAssignment reconciler.
	ctrllog.SetLogger(ctrlzap.New(ctrlzap.UseDevMode(false)))
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(novastorev1alpha1.AddToScheme(scheme))

	restCfg, cfgErr := config.GetConfig()
	if cfgErr != nil {
		logging.L.Fatal("failed to get kubeconfig for controller-runtime", zap.Error(cfgErr))
	}

	mgr, mgrErr := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: scheme,
		// Disable metrics/health servers — we already run our own.
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "",
	})
	if mgrErr != nil {
		logging.L.Fatal("failed to create controller-runtime manager", zap.Error(mgrErr))
	}

	baReconciler := &agent.BackendAssignmentReconciler{
		Client:   mgr.GetClient(),
		NodeName: *nodeID,
		DPClient: dpClient,
		Logger:   logging.L,
		BaseBdev: *spdkBaseBdev,
	}
	if err := baReconciler.SetupWithManager(mgr); err != nil {
		logging.L.Fatal("failed to setup BackendAssignment reconciler", zap.Error(err))
	}
	logging.L.Info("BackendAssignment reconciler registered", zap.String("nodeName", *nodeID))

	go func() {
		if err := mgr.Start(ctx); err != nil {
			logging.L.Error("controller-runtime manager error", zap.Error(err))
		}
	}()

	// Start Prometheus metrics HTTP server.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:         *metricsAddr,
		Handler:      metricsMux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		logging.L.Info("metrics server listening", zap.String("addr", *metricsAddr))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.L.Error("metrics server error", zap.Error(err))
		}
	}()

	// Start gRPC listener.
	lc := net.ListenConfig{}
	listener, listenErr := lc.Listen(ctx, "tcp", *listenAddr)
	if listenErr != nil {
		logging.L.Fatal("failed to listen", zap.String("addr", *listenAddr), zap.Error(listenErr))
	}

	logging.L.Info("agent gRPC server listening", zap.String("addr", *listenAddr))

	// Graceful shutdown on SIGTERM/SIGINT.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-stop
		logging.L.Info("shutting down agent")
		cancel()
		srv.GracefulStop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	if serveErr := srv.Serve(listener); serveErr != nil {
		logging.L.Fatal("agent gRPC server failed", zap.Error(serveErr))
	}
	logging.L.Info("agent stopped")
}
