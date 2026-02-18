package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	novastorev1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
	"github.com/piwi3910/novastor/internal/controller"
	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metadata"
	"github.com/piwi3910/novastor/internal/operator"
	"github.com/piwi3910/novastor/internal/transport"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(novastorev1alpha1.AddToScheme(scheme))
}

// recoveryRunnable wraps the recovery loop as a controller-runtime Runnable
// so it participates in the manager's lifecycle and graceful shutdown.
type recoveryRunnable struct {
	metaClient       *metadata.GRPCClient
	recovery         *operator.RecoveryManager
	replicator       *operator.GRPCChunkReplicator
	healthChecker    *operator.GRPCHealthChecker
	heartbeatTimeout time.Duration
}

// Start implements the manager.Runnable interface. It runs the recovery
// loop until the context is cancelled.
func (r *recoveryRunnable) Start(ctx context.Context) error {
	logger := logging.L.Named("recovery")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	defer r.replicator.Close()
	defer r.healthChecker.Close()

	logger.Info("recovery loop started",
		zap.Duration("heartbeatTimeout", r.heartbeatTimeout),
	)

	for {
		select {
		case <-ctx.Done():
			logger.Info("recovery loop stopping")
			return nil
		case <-ticker.C:
			r.runCycle(ctx, logger)
		}
	}
}

// runCycle performs one iteration of the recovery loop: refresh heartbeats,
// check node status, recover any down nodes, and process the recovery queue.
func (r *recoveryRunnable) runCycle(ctx context.Context, logger *zap.Logger) {
	// Fetch current node list from metadata and issue heartbeats for
	// nodes whose LastHeartbeat timestamp is recent (within the
	// heartbeat timeout).
	nodes, err := r.metaClient.ListNodeMetas(ctx)
	if err != nil {
		logger.Error("failed to list nodes from metadata", zap.Error(err))
		return
	}

	now := time.Now()
	for _, node := range nodes {
		lastHB := time.Unix(node.LastHeartbeat, 0)
		if now.Sub(lastHB) <= r.heartbeatTimeout {
			r.recovery.Heartbeat(node.NodeID)
		}
	}

	// Transition nodes between Healthy/Suspect/Down based on elapsed time.
	r.recovery.CheckNodes(ctx)

	// For any node that is Down, schedule recovery tasks.
	// We re-check the node list from the RecoveryManager since
	// CheckNodes may have transitioned nodes.
	r.recoverDownNodes(ctx, logger)

	// Process the recovery queue (replicate under-replicated chunks).
	if err := r.recovery.ProcessRecoveryQueue(ctx); err != nil {
		logger.Error("error processing recovery queue", zap.Error(err))
	}
}

// recoverDownNodes iterates tracked nodes and triggers recovery for
// any node that has been marked Down.
func (r *recoveryRunnable) recoverDownNodes(ctx context.Context, logger *zap.Logger) {
	// We need to access the node statuses. We list nodes from metadata
	// again and check which ones the RecoveryManager considers Down by
	// attempting to heartbeat and re-checking. Since RecoveryManager
	// doesn't expose its internal node map directly, we use the metadata
	// node list and probe each node's health.
	nodes, err := r.metaClient.ListNodeMetas(ctx)
	if err != nil {
		logger.Error("failed to list nodes for recovery check", zap.Error(err))
		return
	}

	for _, node := range nodes {
		// Use the health checker to determine if the node is actually down.
		// Only trigger recovery for nodes whose metadata heartbeat is stale
		// AND which fail the active health probe.
		lastHB := time.Unix(node.LastHeartbeat, 0)
		if time.Since(lastHB) <= r.heartbeatTimeout {
			continue
		}

		if r.healthChecker.IsNodeHealthy(ctx, node.NodeID) {
			// Node is actually reachable — update heartbeat.
			r.recovery.Heartbeat(node.NodeID)
			continue
		}

		logger.Warn("recovering down node",
			zap.String("nodeID", node.NodeID),
			zap.Time("lastHeartbeat", time.Unix(node.LastHeartbeat, 0)),
		)
		if err := r.recovery.RecoverNode(ctx, node.NodeID); err != nil {
			logger.Error("failed to recover node",
				zap.String("nodeID", node.NodeID),
				zap.Error(err),
			)
		}
	}
}

func main() {
	var metricsAddr string
	var healthProbeAddr string
	var enableLeaderElection bool
	var showVersion bool
	var metaAddr string
	var recoveryEnabled bool
	var heartbeatTimeoutStr string
	var recoveryConcurrency int
	var tlsCA string
	var tlsCert string
	var tlsKey string
	var tlsRotationInterval time.Duration

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit.")
	flag.StringVar(&metaAddr, "meta-addr", ":7000", "Address of the metadata service gRPC endpoint.")
	flag.BoolVar(&recoveryEnabled, "recovery-enabled", true, "Enable automatic node failure recovery.")
	flag.StringVar(&heartbeatTimeoutStr, "heartbeat-timeout", "60s", "Duration after which a node without heartbeat is considered down.")
	flag.IntVar(&recoveryConcurrency, "recovery-concurrency", 4, "Maximum number of concurrent chunk recovery operations.")
	flag.StringVar(&tlsCA, "tls-ca", "", "Path to CA certificate for mTLS")
	flag.StringVar(&tlsCert, "tls-cert", "", "Path to client certificate for mTLS")
	flag.StringVar(&tlsKey, "tls-key", "", "Path to client key for mTLS")
	flag.DurationVar(&tlsRotationInterval, "tls-rotation-interval", 5*time.Minute, "Interval for TLS certificate rotation checks")

	opts := crzap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if showVersion {
		_, _ = fmt.Fprintf(os.Stdout, "novastor-controller %s (commit: %s, built: %s)\n", version, commit, date)
		os.Exit(0)
	}

	ctrl.SetLogger(crzap.New(crzap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	setupLog.Info("starting novastor-controller", "version", version, "commit", commit, "date", date)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthProbeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "novastor-controller-leader-election",
	})
	if err != nil {
		setupLog.Error(err, "unable to create manager")
		os.Exit(1)
	}

	// Register controllers.
	if err := (&controller.StoragePoolReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "StoragePool")
		os.Exit(1)
	}

	if err := (&controller.BlockVolumeReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BlockVolume")
		os.Exit(1)
	}

	if err := (&controller.SharedFilesystemReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SharedFilesystem")
		os.Exit(1)
	}

	if err := (&controller.ObjectStoreReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ObjectStore")
		os.Exit(1)
	}

	// Set up the recovery subsystem if enabled.
	if recoveryEnabled {
		heartbeatTimeout, parseErr := time.ParseDuration(heartbeatTimeoutStr)
		if parseErr != nil {
			setupLog.Error(parseErr, "invalid heartbeat-timeout value", "value", heartbeatTimeoutStr)
			os.Exit(1)
		}

		// Build gRPC dial options for TLS if certificates are provided.
		ctx := context.Background()
		var dialOpts []grpc.DialOption
		if tlsCA != "" && tlsCert != "" && tlsKey != "" {
			rotator := transport.NewCertRotator(tlsCert, tlsKey, tlsRotationInterval)
			rotator.Start(ctx)
			setupLog.Info("TLS certificate rotation enabled",
				"certPath", tlsCert,
				"keyPath", tlsKey,
				"interval", tlsRotationInterval,
			)
			tlsOpt, tlsErr := transport.NewClientTLSWithRotation(transport.TLSConfig{
				CACertPath: tlsCA,
				CertPath:   tlsCert,
				KeyPath:    tlsKey,
			}, rotator)
			if tlsErr != nil {
				setupLog.Error(tlsErr, "failed to configure TLS")
				os.Exit(1)
			}
			dialOpts = append(dialOpts, tlsOpt)
		}

		metaClient, dialErr := metadata.Dial(metaAddr, dialOpts...)
		if dialErr != nil {
			setupLog.Error(dialErr, "unable to connect to metadata service", "address", metaAddr)
			os.Exit(1)
		}

		placementAdapter := operator.NewMetadataPlacementAdapter(metaClient)
		chunkReplicator := operator.NewGRPCChunkReplicator(metaClient)
		healthChecker := operator.NewGRPCHealthChecker(metaClient, 5*time.Second)

		recoveryMgr := operator.NewRecoveryManager(placementAdapter, chunkReplicator, healthChecker)
		recoveryMgr.SetMaxConcurrent(recoveryConcurrency)
		recoveryMgr.SetDownTimeout(heartbeatTimeout)

		runnable := &recoveryRunnable{
			metaClient:       metaClient,
			recovery:         recoveryMgr,
			replicator:       chunkReplicator,
			healthChecker:    healthChecker,
			heartbeatTimeout: heartbeatTimeout,
		}

		if err := mgr.Add(runnable); err != nil {
			setupLog.Error(err, "unable to add recovery runnable to manager")
			os.Exit(1)
		}

		setupLog.Info("recovery subsystem enabled",
			"metaAddr", metaAddr,
			"heartbeatTimeout", heartbeatTimeout,
			"concurrency", recoveryConcurrency,
		)
	}

	// Health and readiness probes.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up readiness check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
