package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// --------------- Agent metrics ---------------

	// ChunkCount is the total number of chunks stored on this agent.
	ChunkCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "chunk_count",
		Help:      "Total number of chunks stored on this agent",
	})

	// DiskBytesTotal is the total disk capacity in bytes per device.
	DiskBytesTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "disk_bytes_total",
		Help:      "Total disk capacity in bytes",
	}, []string{"device"})

	// DiskBytesUsed is the used disk space in bytes per device.
	DiskBytesUsed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "disk_bytes_used",
		Help:      "Used disk space in bytes",
	}, []string{"device"})

	// DiskBytesFree is the free disk space in bytes per device.
	DiskBytesFree = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "disk_bytes_free",
		Help:      "Free disk space in bytes",
	}, []string{"device"})

	// ChunkOpsTotal counts chunk operations by type (read, write, delete).
	ChunkOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "chunk_ops_total",
		Help:      "Total chunk operations",
	}, []string{"operation"})

	// ChunkBytesTotal counts bytes transferred by direction (read, write).
	ChunkBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "chunk_bytes_total",
		Help:      "Total chunk bytes transferred",
	}, []string{"direction"})

	// ScrubErrors counts chunks with checksum errors found by the scrubber.
	ScrubErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "agent",
		Name:      "scrub_errors_total",
		Help:      "Total chunks with checksum errors found by scrubber",
	})

	// --------------- Metadata / Raft metrics ---------------

	// RaftState reports the current Raft state (0=follower, 1=candidate, 2=leader).
	RaftState = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "meta",
		Name:      "raft_state",
		Help:      "Raft state (0=follower, 1=candidate, 2=leader)",
	})

	// RaftCommitIndex is the current Raft commit index.
	RaftCommitIndex = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "meta",
		Name:      "raft_commit_index",
		Help:      "Raft commit index",
	})

	// RaftApplyLatency tracks the time to apply a Raft log entry.
	RaftApplyLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "meta",
		Name:      "raft_apply_duration_seconds",
		Help:      "Time to apply raft log entry",
		Buckets:   prometheus.DefBuckets,
	})

	// MetadataOpsTotal counts metadata operations by type.
	MetadataOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "meta",
		Name:      "ops_total",
		Help:      "Total metadata operations",
	}, []string{"operation"})

	// --------------- Controller metrics ---------------

	// PoolNodeCount is the number of nodes in each storage pool.
	PoolNodeCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "controller",
		Name:      "pool_node_count",
		Help:      "Number of nodes in storage pool",
	}, []string{"pool"})

	// PoolCapacityBytes is the total capacity of each storage pool in bytes.
	PoolCapacityBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "controller",
		Name:      "pool_capacity_bytes",
		Help:      "Total pool capacity in bytes",
	}, []string{"pool"})

	// RecoveryPending is the number of chunks pending recovery.
	RecoveryPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "controller",
		Name:      "recovery_chunks_pending",
		Help:      "Chunks pending recovery",
	})

	// RecoveryCompleted counts the total number of chunks recovered.
	RecoveryCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "controller",
		Name:      "recovery_chunks_completed_total",
		Help:      "Chunks recovered",
	})

	// --------------- CSI metrics ---------------

	// VolumeCount is the number of currently provisioned volumes.
	VolumeCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "volume_count",
		Help:      "Number of provisioned volumes",
	})

	// VolumeProvisionDuration tracks the time to provision a volume.
	VolumeProvisionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "volume_provision_duration_seconds",
		Help:      "Time to provision a volume",
		Buckets:   prometheus.DefBuckets,
	})

	// VolumeDeleteDuration tracks the time to delete a volume.
	VolumeDeleteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "volume_delete_duration_seconds",
		Help:      "Time to delete a volume",
		Buckets:   prometheus.DefBuckets,
	})

	// VolumePublishDuration tracks the time to publish a volume to a node.
	VolumePublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "volume_publish_duration_seconds",
		Help:      "Time to publish a volume to a node",
		Buckets:   prometheus.DefBuckets,
	})

	// VolumeUnpublishDuration tracks the time to unpublish a volume from a node.
	VolumeUnpublishDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "volume_unpublish_duration_seconds",
		Help:      "Time to unpublish a volume from a node",
		Buckets:   prometheus.DefBuckets,
	})

	// CapacityQueryDuration tracks the time to query available capacity.
	CapacityQueryDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "csi",
		Name:      "capacity_query_duration_seconds",
		Help:      "Time to query available capacity",
		Buckets:   prometheus.DefBuckets,
	})

	// --------------- S3 gateway metrics ---------------

	// S3RequestsTotal counts S3 requests by operation type.
	S3RequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "s3",
		Name:      "requests_total",
		Help:      "Total S3 requests by operation",
	}, []string{"operation"})

	// S3RequestDuration tracks S3 request duration by operation type.
	S3RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "s3",
		Name:      "request_duration_seconds",
		Help:      "S3 request duration",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation"})

	// S3BytesIn counts total bytes received by the S3 gateway.
	S3BytesIn = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "s3",
		Name:      "bytes_in_total",
		Help:      "Total bytes received by S3 gateway",
	})

	// S3BytesOut counts total bytes sent by the S3 gateway.
	S3BytesOut = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "s3",
		Name:      "bytes_out_total",
		Help:      "Total bytes sent by S3 gateway",
	})

	// --------------- Filer / NFS metrics ---------------

	// NFSOpsTotal counts NFS operations by type.
	NFSOpsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "filer",
		Name:      "nfs_ops_total",
		Help:      "Total NFS operations",
	}, []string{"operation"})

	// NFSOpDuration tracks NFS operation duration by type.
	NFSOpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "filer",
		Name:      "nfs_op_duration_seconds",
		Help:      "NFS operation duration",
		Buckets:   prometheus.DefBuckets,
	}, []string{"operation"})

	// ActiveLocks is the number of active file locks.
	ActiveLocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "filer",
		Name:      "active_locks",
		Help:      "Number of active file locks",
	})

	// --------------- Webhook metrics ---------------

	// WebhookAdmissionReviewsTotal counts admission review requests received.
	WebhookAdmissionReviewsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "webhook",
		Name:      "admission_reviews_total",
		Help:      "Total admission review requests received",
	}, []string{"operation", "resource"})

	// WebhookAdmissionReviewDuration tracks admission review processing duration.
	WebhookAdmissionReviewDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "webhook",
		Name:      "admission_review_duration_seconds",
		Help:      "Admission review processing duration",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"operation", "resource"})

	// WebhookMutationsTotal counts mutation operations performed.
	WebhookMutationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "webhook",
		Name:      "mutations_total",
		Help:      "Total mutation operations performed",
	}, []string{"resource", "mutation_type"})

	// WebhookErrorsTotal counts webhook errors.
	WebhookErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "webhook",
		Name:      "errors_total",
		Help:      "Total webhook errors",
	}, []string{"operation", "error_type"})

	// --------------- Data Mover metrics ---------------

	// DataMoverTasksPending is the number of pending healing tasks.
	DataMoverTasksPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "tasks_pending",
		Help:      "Number of pending healing tasks",
	})

	// DataMoverTasksInProgress is the number of in-progress healing tasks.
	DataMoverTasksInProgress = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "tasks_in_progress",
		Help:      "Number of in-progress healing tasks",
	})

	// DataMoverTasksCompleted counts total completed healing tasks.
	DataMoverTasksCompleted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "tasks_completed_total",
		Help:      "Total completed healing tasks",
	})

	// DataMoverTasksFailed counts total failed healing tasks.
	DataMoverTasksFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "tasks_failed_total",
		Help:      "Total failed healing tasks",
	})

	// DataMoverTaskDuration tracks healing task duration.
	DataMoverTaskDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "task_duration_seconds",
		Help:      "Healing task duration",
		Buckets:   prometheus.ExponentialBucketsRange(1, 600, 10),
	})

	// DataMoverBytesTransferred counts total bytes transferred by healing.
	DataMoverBytesTransferred = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "bytes_transferred_total",
		Help:      "Total bytes transferred by healing operations",
	})

	// DataMoverBandwidthLimit is the current bandwidth limit in bytes/sec.
	DataMoverBandwidthLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "novastor",
		Subsystem: "datamover",
		Name:      "bandwidth_limit_bytes",
		Help:      "Current bandwidth limit in bytes per second",
	})
)

// Register registers all NovaStor metrics with the default Prometheus registry.
func Register() {
	// Agent metrics
	prometheus.MustRegister(ChunkCount)
	prometheus.MustRegister(DiskBytesTotal)
	prometheus.MustRegister(DiskBytesUsed)
	prometheus.MustRegister(DiskBytesFree)
	prometheus.MustRegister(ChunkOpsTotal)
	prometheus.MustRegister(ChunkBytesTotal)
	prometheus.MustRegister(ScrubErrors)

	// Metadata / Raft metrics
	prometheus.MustRegister(RaftState)
	prometheus.MustRegister(RaftCommitIndex)
	prometheus.MustRegister(RaftApplyLatency)
	prometheus.MustRegister(MetadataOpsTotal)

	// Controller metrics
	prometheus.MustRegister(PoolNodeCount)
	prometheus.MustRegister(PoolCapacityBytes)
	prometheus.MustRegister(RecoveryPending)
	prometheus.MustRegister(RecoveryCompleted)

	// CSI metrics
	prometheus.MustRegister(VolumeCount)
	prometheus.MustRegister(VolumeProvisionDuration)
	prometheus.MustRegister(VolumeDeleteDuration)
	prometheus.MustRegister(VolumePublishDuration)
	prometheus.MustRegister(VolumeUnpublishDuration)
	prometheus.MustRegister(CapacityQueryDuration)

	// S3 gateway metrics
	prometheus.MustRegister(S3RequestsTotal)
	prometheus.MustRegister(S3RequestDuration)
	prometheus.MustRegister(S3BytesIn)
	prometheus.MustRegister(S3BytesOut)

	// Filer / NFS metrics
	prometheus.MustRegister(NFSOpsTotal)
	prometheus.MustRegister(NFSOpDuration)
	prometheus.MustRegister(ActiveLocks)

	// Webhook metrics
	prometheus.MustRegister(WebhookAdmissionReviewsTotal)
	prometheus.MustRegister(WebhookAdmissionReviewDuration)
	prometheus.MustRegister(WebhookMutationsTotal)
	prometheus.MustRegister(WebhookErrorsTotal)

	// Data Mover metrics
	prometheus.MustRegister(DataMoverTasksPending)
	prometheus.MustRegister(DataMoverTasksInProgress)
	prometheus.MustRegister(DataMoverTasksCompleted)
	prometheus.MustRegister(DataMoverTasksFailed)
	prometheus.MustRegister(DataMoverTaskDuration)
	prometheus.MustRegister(DataMoverBytesTransferred)
	prometheus.MustRegister(DataMoverBandwidthLimit)
}
