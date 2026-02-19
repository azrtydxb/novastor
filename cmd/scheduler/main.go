package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/piwi3910/novastor/internal/scheduler"
)

const (
	// schedulerName is the name of the NovaStor scheduler plugin.
	schedulerName = "novastor-data-locality"
)

var (
	logger *zap.Logger
)

func initLogging(logLevel string) error {
	var level zapcore.Level
	switch logLevel {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	var err error
	logger, err = config.Build()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)
	return nil
}

func main() {
	var (
		kubeconfig       = flag.String("kubeconfig", "", "Path to kubeconfig file")
		masterURL        = flag.String("master", "", "URL of the Kubernetes API server")
		localityWeight   = flag.Int64("locality-weight", 10, "Weight for data locality scoring (higher values prefer nodes with local data more strongly)")
		minLocalityScore = flag.Int64("min-locality-score", 1, "Minimum locality score threshold")
		rwxMode          = flag.String("rwx-mode", "balanced", "RWX volume handling mode: 'locality' (prefer local), 'balanced' (distribute), or 'any' (no preference)")
		logLevel         = flag.String("log-level", "info", "Log level: debug, info, warn, error")
		port             = flag.Int("port", 10251, "HTTP port for health checks")
	)
	flag.Parse()

	// Initialize logging.
	if err := initLogging(*logLevel); err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup logging: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	// Create Kubernetes client.
	restConfig, err := clientcmd.BuildConfigFromFlags(*masterURL, *kubeconfig)
	if err != nil {
		logger.Fatal("failed to build kubeconfig", zap.Error(err))
	}

	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logger.Fatal("failed to create kubernetes client", zap.Error(err))
	}

	// Create shared informer factory.
	informerFactory := informers.NewSharedInformerFactory(kubeClient, time.Hour*12)
	podInformer := informerFactory.Core().V1().Pods().Informer()
	pvInformer := informerFactory.Core().V1().PersistentVolumes().Informer()
	pvcInformer := informerFactory.Core().V1().PersistentVolumeClaims().Informer()
	nodeInformer := informerFactory.Core().V1().Nodes().Informer()
	scInformer := informerFactory.Storage().V1().StorageClasses().Informer()

	// Parse RWX mode.
	rwxHandling := scheduler.RWXBalanced
	switch *rwxMode {
	case "locality":
		rwxHandling = scheduler.RWXLocality
	case "balanced":
		rwxHandling = scheduler.RWXBalanced
	case "any":
		rwxHandling = scheduler.RWXAny
	default:
		logger.Fatal("invalid rwx-mode", zap.String("mode", *rwxMode))
	}

	// Create plugin configuration.
	cfg := &scheduler.Config{
		LocalityWeight:   *localityWeight,
		MinLocalityScore: *minLocalityScore,
		RWXMode:          rwxHandling,
		Client:           kubeClient,
		PodLister:        podInformer.GetIndexer(),
		PVLister:         pvInformer.GetIndexer(),
		PVCLister:        pvcInformer.GetIndexer(),
		NodeLister:       nodeInformer.GetIndexer(),
		SCLister:         scInformer.GetIndexer(),
	}

	// Create the plugin.
	plugin := scheduler.NewDataLocalityPlugin(cfg)

	// Start informers and wait for cache sync.
	stopCh := make(chan struct{})
	defer close(stopCh)

	informerFactory.Start(stopCh)

	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced, pvInformer.HasSynced, pvcInformer.HasSynced, nodeInformer.HasSynced, scInformer.HasSynced) {
		logger.Fatal("failed to sync informer caches")
	}

	logger.Info("NovaStor scheduler plugin starting",
		zap.String("scheduler", schedulerName),
		zap.Int64("localityWeight", *localityWeight),
		zap.Int64("minLocalityScore", *minLocalityScore),
		zap.String("rwxMode", *rwxMode),
	)

	// Start the HTTP server for health checks and plugin registration.
	server := NewServer(plugin, kubeClient, *port)
	if err := server.Run(); err != nil {
		logger.Fatal("server failed", zap.Error(err))
	}
}
