// Package main provides the NovaStor storage agent binary.
// The agent runs on each storage node as a management plane: it coordinates
// with the metadata service, manages NVMe-oF targets via the SPDK data plane,
// and exposes Prometheus metrics. All data-path I/O is handled by the Rust
// SPDK data-plane process.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pb "github.com/azrtydxb/novastor/api/proto/dataplane"
	novastorev1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
	"github.com/azrtydxb/novastor/internal/agent"
	"github.com/azrtydxb/novastor/internal/agent/device"
	"github.com/azrtydxb/novastor/internal/agent/failover"
	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/logging"
	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
	"github.com/azrtydxb/novastor/internal/observability"
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

// localCapacity and localUsedBytes track real chunk store capacity
// queried from the Rust dataplane. Updated atomically after InitChunkStore
// succeeds so that buildNodeMeta and syncTopology use real values.
var (
	localCapacity  atomic.Int64
	localUsedBytes atomic.Int64
)

// buildNodeMeta constructs current NodeMeta from SPDK data-plane stats.
// Capacity values come from the localCapacity/localUsedBytes atomics which
// are populated after InitChunkStore queries the Rust dataplane.
// Zone and Rack are read from NODE_ZONE / NODE_RACK environment variables
// (typically injected via the Kubernetes downward API from node labels).
func buildNodeMeta(nodeID, listenAddr, status string) *metadata.NodeMeta {
	totalCap := localCapacity.Load()
	usedCap := localUsedBytes.Load()
	return &metadata.NodeMeta{
		NodeID:            nodeID,
		Address:           listenAddr,
		DiskCount:         1,
		TotalCapacity:     totalCap,
		AvailableCapacity: totalCap - usedCap,
		Zone:              os.Getenv("NODE_ZONE"),
		Rack:              os.Getenv("NODE_RACK"),
		LastHeartbeat:     time.Now().Unix(),
		Status:            status,
	}
}

// refreshCapacity queries the dataplane for current chunk store stats and
// updates the package-level atomics so that subsequent buildNodeMeta calls
// report accurate capacity.
func refreshCapacity(dpClient *dataplane.Client, bdevName string) {
	stats, err := dpClient.ChunkStoreStats(bdevName)
	if err != nil {
		logging.L.Debug("capacity refresh: ChunkStoreStats failed", zap.Error(err))
		return
	}
	totalBytes := int64(stats.GetTotalSlots()) * int64(stats.GetChunkSize())
	usedBytes := int64(stats.GetUsedSlots()) * int64(stats.GetChunkSize())
	localCapacity.Store(totalBytes)
	localUsedBytes.Store(usedBytes)
}

// registerNode sends the initial NodeMeta registration and then periodically
// sends heartbeats until the context is cancelled. On each heartbeat it also
// refreshes capacity from the dataplane so metadata stays current.
func registerNode(ctx context.Context, client *metadata.GRPCClient, dpClient *dataplane.Client, bdevName, nodeID, listenAddr string, heartbeatInterval time.Duration) {
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
			// Refresh capacity from the dataplane before sending the heartbeat
			// so the metadata service always has up-to-date values.
			refreshCapacity(dpClient, bdevName)
			if err := client.PutNodeMeta(ctx, buildNodeMeta(nodeID, listenAddr, "ready")); err != nil {
				logging.L.Warn("heartbeat failed", zap.String("nodeID", nodeID), zap.Error(err))
			} else {
				logging.L.Debug("heartbeat sent", zap.String("nodeID", nodeID))
			}
		}
	}
}

// syncTopology periodically fetches node metadata from the metadata service
// and pushes a CRUSH topology update to the Rust dataplane. This ensures
// the chunk engine's placement algorithm has an accurate view of all nodes.
func syncTopology(ctx context.Context, metaClient *metadata.GRPCClient, dpClient *dataplane.Client) {
	defer func() {
		if r := recover(); r != nil {
			logging.L.Error("topology sync panicked", zap.Any("panic", r))
		}
	}()

	logging.L.Info("topology sync: starting (5s delay)")

	// Small delay to let the metadata registration settle.
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return
	}

	var generation uint64
	push := func() {
		nodes, err := metaClient.ListNodeMetas(ctx)
		if err != nil {
			logging.L.Warn("topology sync: failed to list nodes", zap.Error(err))
			return
		}
		if len(nodes) == 0 {
			logging.L.Warn("topology sync: no nodes found in metadata service")
			return
		}
		logging.L.Info("topology sync: found nodes", zap.Int("count", len(nodes)))

		generation++
		protoNodes := make([]*pb.TopologyNode, 0, len(nodes))
		for _, n := range nodes {
			nodeStatus := "online"
			if n.Status != "ready" {
				nodeStatus = "offline"
			}
			// Extract just the host IP from the agent address (which
			// includes the agent gRPC port, e.g. "192.168.100.13:9100").
			// The dataplane needs only the IP; the port is set separately.
			host := n.Address
			if h, _, splitErr := net.SplitHostPort(n.Address); splitErr == nil {
				host = h
			}
			// Compute CRUSH weight proportional to capacity in GB.
			// Falls back to 1 if capacity is not yet reported.
			weight := uint32(1)
			if n.TotalCapacity > 0 {
				weight = uint32(n.TotalCapacity / (1024 * 1024 * 1024))
				if weight == 0 {
					weight = 1
				}
			}
			protoNodes = append(protoNodes, &pb.TopologyNode{
				NodeId:  n.NodeID,
				Address: host,
				Port:    9500,
				Status:  nodeStatus,
				Backends: []*pb.TopologyBackend{{
					Id:            n.NodeID + "-bdev",
					NodeId:        n.NodeID,
					CapacityBytes: uint64(n.TotalCapacity),
					UsedBytes:     uint64(n.TotalCapacity - n.AvailableCapacity),
					Weight:        weight,
					BackendType:   "bdev",
				}},
			})
		}

		accepted, err := dpClient.UpdateTopology(generation, protoNodes)
		if err != nil {
			logging.L.Warn("topology sync: UpdateTopology RPC failed", zap.Error(err))
			return
		}
		if accepted {
			logging.L.Info("topology sync: pushed to dataplane",
				zap.Uint64("generation", generation),
				zap.Int("nodes", len(protoNodes)),
			)
		}
	}

	// Initial push.
	push()

	// Periodic sync every 30 seconds.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			push()
		}
	}
}

// loadOrCreateNodeUUID reads a persistent node UUID from path, or creates one
// if the file doesn't exist. The UUID is a v4 random UUID.
func loadOrCreateNodeUUID(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		id := strings.TrimSpace(string(data))
		if len(id) >= 36 { // basic UUID length check
			return id, nil
		}
	}
	// Create directory if needed.
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o755); mkErr != nil {
		return "", fmt.Errorf("creating directory for node UUID: %w", mkErr)
	}
	// Generate new UUID.
	id := uuid.New().String()
	if writeErr := os.WriteFile(path, []byte(id+"\n"), 0o644); writeErr != nil {
		return "", fmt.Errorf("writing node UUID: %w", writeErr)
	}
	return id, nil
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

	shutdownTracer := observability.InitTracer("novastor-agent", logging.L)
	defer shutdownTracer()

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

	// Generate or load a persistent node UUID.
	// The UUID is stored at /var/lib/novastor/node-id and survives restarts.
	// The --node-id flag provides the HOSTNAME (used for K8s labels/display),
	// but the storage node UUID is separate and globally unique.
	nodeUUID, err := loadOrCreateNodeUUID("/var/lib/novastor/node-id")
	if err != nil {
		logging.L.Fatal("failed to load/create node UUID", zap.Error(err))
	}
	logging.L.Info("storage node UUID", zap.String("uuid", nodeUUID))

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

	// Build TLS client dial options for outbound gRPC connections
	// (dataplane, metadata). Built early so dpClient can use them.
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
			logging.L.Fatal("failed to configure client TLS", zap.Error(clientTLSErr))
		}
		dialOpts = append(dialOpts, clientTLSOpt)
	}

	// Connect to the Rust data-plane via gRPC with mTLS.
	dpDialOpts := []grpc.DialOption{grpc.WithStatsHandler(otelgrpc.NewClientHandler())}
	if len(dialOpts) > 0 {
		dpDialOpts = append(dpDialOpts, dialOpts...)
	} else {
		dpDialOpts = append(dpDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	logging.L.Info("connecting to Rust data-plane via gRPC", zap.String("addr", *dataplaneAddr))
	dpClient, err := dataplane.Dial(*dataplaneAddr, logging.L, dpDialOpts...)
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

	// Eagerly initialize the chunk store on the dataplane. This ensures
	// the chunk engine is ready even after a dataplane restart (where
	// in-memory SPDK state is lost but the CRDs still say "Ready").
	// InitChunkStore is idempotent; if it already exists, the error
	// contains "already" and we move on.
	go func() {
		// Small delay to let the dataplane finish SPDK init.
		time.Sleep(3 * time.Second)
		// Use K8s hostname as node ID for the dataplane too, so it matches
		// the topology pushed from metadata (which uses hostnames).
		// The persistent UUID is stored but used for future multi-cluster identity.
		if _, err := dpClient.InitChunkStore(*spdkBaseBdev, *nodeID); err != nil {
			if !strings.Contains(err.Error(), "already") {
				logging.L.Warn("startup: chunk store init failed (will be retried by reconciler)",
					zap.String("bdev", *spdkBaseBdev), zap.Error(err))
			} else {
				logging.L.Info("startup: chunk store already initialized",
					zap.String("bdev", *spdkBaseBdev))
			}
		} else {
			logging.L.Info("startup: chunk store initialized",
				zap.String("bdev", *spdkBaseBdev))
		}

		// Query real capacity from the dataplane so that buildNodeMeta
		// and syncTopology use actual values instead of zeros.
		if stats, err := dpClient.ChunkStoreStats(*spdkBaseBdev); err == nil {
			totalBytes := int64(stats.GetTotalSlots()) * int64(stats.GetChunkSize())
			usedBytes := int64(stats.GetUsedSlots()) * int64(stats.GetChunkSize())
			localCapacity.Store(totalBytes)
			localUsedBytes.Store(usedBytes)
			logging.L.Info("startup: chunk store capacity",
				zap.Int64("totalBytes", totalBytes),
				zap.Int64("usedBytes", usedBytes),
			)
		} else {
			logging.L.Warn("startup: failed to query chunk store stats", zap.Error(err))
		}
	}()

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

	serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	srv := grpc.NewServer(serverOpts...)

	// dialOpts (with client TLS) were built earlier, before the dataplane connection.

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
		// Pass hostname as nodeUUID for now — it must match the topology node IDs
		// (which come from metadata, registered with hostname). UUID support is
		// deferred until dual-identity (K8s hostname + storage UUID) is implemented.
		spdkServer = agent.NewSPDKTargetServer(*hostIP, *spdkBaseBdev, *nodeID, *testMode, dpClient, metaClient)
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
		// Use K8s hostname for metadata registration (CSI controller uses
		// these IDs for node affinity / scheduling). The storage UUID is
		// passed to the dataplane separately for CRUSH placement.
		go registerNode(ctx, metaClient, dpClient, *spdkBaseBdev, *nodeID, registrationAddr, *heartbeatInterval)

		// Push cluster topology to the dataplane periodically so the
		// CRUSH placement engine has an up-to-date view of all nodes.
		go syncTopology(ctx, metaClient, dpClient)
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
		NodeUUID: nodeUUID,
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
