package policy

import (
	"time"

	"github.com/piwi3910/novastor/internal/metrics"
)

// MetricsReporter records compliance and repair metrics.
type MetricsReporter struct{}

// NewMetricsReporter creates a new MetricsReporter.
func NewMetricsReporter() *MetricsReporter {
	return &MetricsReporter{}
}

// ReportPoolCompliance records the compliance status for a pool.
func (m *MetricsReporter) ReportPoolCompliance(poolName, mode string, isCompliant bool) {
	status := 0.0
	if isCompliant {
		status = 1.0
	}
	metrics.PoolComplianceStatus.WithLabelValues(poolName, mode).Set(status)
}

// ReportChunkViolations records the number of chunks with each compliance status.
func (m *MetricsReporter) ReportChunkViolations(poolName string, compliant, underReplicated, unavailable, corrupted int) {
	metrics.ChunkComplianceViolations.WithLabelValues(poolName, "compliant").Set(float64(compliant))
	metrics.ChunkComplianceViolations.WithLabelValues(poolName, "under_replicated").Set(float64(underReplicated))
	metrics.ChunkComplianceViolations.WithLabelValues(poolName, "unavailable").Set(float64(unavailable))
	metrics.ChunkComplianceViolations.WithLabelValues(poolName, "corrupted").Set(float64(corrupted))
}

// ReportScanStart records the start of a compliance scan.
func (m *MetricsReporter) ReportScanStart(_ string) time.Time {
	return time.Now()
}

// ReportScanComplete records the completion of a compliance scan.
func (m *MetricsReporter) ReportScanComplete(scope string, start time.Time) {
	duration := time.Since(start).Seconds()
	metrics.PolicyScanDuration.WithLabelValues(scope).Observe(duration)
}

// ReportRepairQueued records the number of chunks queued for repair.
func (m *MetricsReporter) ReportRepairQueued(count int) {
	metrics.PolicyRepairQueued.Set(float64(count))
}

// ReportRepairCompleted records a completed repair operation.
func (m *MetricsReporter) ReportRepairCompleted() {
	metrics.PolicyRepairCompleted.Inc()
}

// ReportRepairFailed records a failed repair operation.
func (m *MetricsReporter) ReportRepairFailed() {
	metrics.PolicyRepairFailed.Inc()
}

// ReportRepairDuration records the time taken for a repair operation.
func (m *MetricsReporter) ReportRepairDuration(duration time.Duration) {
	metrics.PolicyRepairDuration.Observe(duration.Seconds())
}
