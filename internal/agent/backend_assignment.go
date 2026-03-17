// Package agent provides the NovaStor node agent that watches BackendAssignment
// CRDs and provisions storage backends on the local node via the Rust dataplane.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
	"github.com/azrtydxb/novastor/internal/dataplane"
	"github.com/azrtydxb/novastor/internal/disk"
)

// BackendAssignmentReconciler watches BackendAssignment CRDs for the local node
// and provisions the storage backend via the Rust dataplane gRPC.
type BackendAssignmentReconciler struct {
	client.Client
	NodeName string
	NodeUUID string // Persistent storage node UUID for CRUSH placement
	DPClient *dataplane.Client
	Logger   *zap.Logger
	BaseBdev string // SPDK bdev name to use (must match --spdk-base-bdev)
}

// +kubebuilder:rbac:groups=novastor.io,resources=backendassignments,verbs=get;list;watch
// +kubebuilder:rbac:groups=novastor.io,resources=backendassignments/status,verbs=get;update;patch

// Reconcile handles a BackendAssignment event. It only processes assignments
// targeting this node. The flow is:
//  1. Discover local devices matching the assignment's device filter.
//  2. Pick an available device (not already assigned to another pool).
//  3. Call InitBackend on the dataplane to create the SPDK bdev.
//  4. Call InitChunkStore to set up the chunk engine on the bdev.
//  5. Update the BackendAssignment status to Ready.
func (r *BackendAssignmentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ctx, span := otel.Tracer("novastor-agent").Start(ctx, "BackendAssignment.Reconcile")
	defer span.End()

	logger := log.FromContext(ctx)

	var ba novastorev1alpha1.BackendAssignment
	if err := r.Get(ctx, req.NamespacedName, &ba); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.Logger.Info("reconcile BackendAssignment",
		zap.String("name", ba.Name),
		zap.String("node", ba.Spec.NodeName),
		zap.String("phase", ba.Status.Phase),
	)

	// Only process assignments for this node.
	if ba.Spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil
	}

	// If already provisioned, verify the dataplane state is consistent.
	// After a dataplane restart, the SPDK bdevs and chunk store are lost
	// even though the CRD status still says "Ready". Re-initialize them.
	if ba.Status.Phase == "Ready" {
		return r.ensureDataplaneState(ctx, &ba)
	}

	// Retry failed assignments periodically — the failure may have been
	// transient (e.g. dataplane not ready, bdev name collision after restart).
	if ba.Status.Phase == "Failed" {
		ba.Status.Phase = "Pending"
		ba.Status.Message = "retrying after failure"
		if err := r.Status().Update(ctx, &ba); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("provisioning BackendAssignment",
		"name", ba.Name,
		"backendType", ba.Spec.BackendType,
		"pool", ba.Spec.PoolRef,
	)

	// Mark as Provisioning.
	ba.Status.Phase = "Provisioning"
	ba.Status.Message = "discovering devices"
	if err := r.Status().Update(ctx, &ba); err != nil {
		return ctrl.Result{}, err
	}

	var bdevName string
	var devicePath string
	var pcieAddr string
	var capacity int64

	switch ba.Spec.BackendType {
	case "file":
		bdevName, capacity = r.provisionFileBackend(ctx, &ba)

	case "raw", "lvm":
		var err error
		bdevName, devicePath, pcieAddr, capacity, err = r.provisionDeviceBackend(ctx, &ba)
		if err != nil {
			if isTransientGRPCError(err) {
				// Transient error — requeue without marking as Failed.
				r.Logger.Warn("transient error provisioning backend, will retry",
					zap.String("name", ba.Name), zap.Error(err))
				ba.Status.Message = fmt.Sprintf("retrying: %v", err)
				_ = r.Status().Update(ctx, &ba)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			r.setFailed(ctx, &ba, err.Error())
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

	default:
		r.setFailed(ctx, &ba, fmt.Sprintf("unknown backend type: %s", ba.Spec.BackendType))
		return ctrl.Result{}, nil
	}

	if bdevName == "" {
		r.setFailed(ctx, &ba, "no suitable device found")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Init chunk store on the resulting bdev.
	if _, err := r.DPClient.InitChunkStore(bdevName, r.NodeName); err != nil {
		if !strings.Contains(err.Error(), "already") {
			if isTransientGRPCError(err) {
				r.Logger.Warn("transient error init chunk store, will retry",
					zap.String("name", ba.Name), zap.Error(err))
				ba.Status.Message = fmt.Sprintf("retrying chunk store: %v", err)
				_ = r.Status().Update(ctx, &ba)
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			r.setFailed(ctx, &ba, fmt.Sprintf("init chunk store on %s: %v", bdevName, err))
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
	}

	// Update status to Ready.
	ba.Status.Phase = "Ready"
	ba.Status.Device = devicePath
	ba.Status.PCIeAddr = pcieAddr
	ba.Status.BdevName = bdevName
	ba.Status.Capacity = capacity
	ba.Status.Message = ""
	meta.SetStatusCondition(&ba.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Provisioned",
		Message:            fmt.Sprintf("backend %s provisioned on bdev %s", ba.Spec.BackendType, bdevName),
		ObservedGeneration: ba.Generation,
	})
	if err := r.Status().Update(ctx, &ba); err != nil {
		return ctrl.Result{}, err
	}

	r.Logger.Info("BackendAssignment provisioned",
		zap.String("name", ba.Name),
		zap.String("bdevName", bdevName),
		zap.String("device", devicePath),
		zap.Int64("capacity", capacity),
	)

	return ctrl.Result{}, nil
}

// ensureDataplaneState re-initializes the backend and chunk store on the
// dataplane if they have been lost (e.g. after a dataplane pod restart).
// Both InitBackend and InitChunkStore are idempotent — calling them when the
// state already exists is a no-op.
func (r *BackendAssignmentReconciler) ensureDataplaneState(ctx context.Context, ba *novastorev1alpha1.BackendAssignment) (ctrl.Result, error) {
	bdevName := ba.Status.BdevName
	if bdevName == "" {
		bdevName = r.BaseBdev
	}

	r.Logger.Info("ensureDataplaneState: verifying backend and chunk store",
		zap.String("name", ba.Name),
		zap.String("backendType", ba.Spec.BackendType),
		zap.String("bdevName", bdevName),
		zap.String("device", ba.Status.Device),
	)

	// Re-initialize the backend. InitBackend is idempotent; if the bdev
	// already exists, the dataplane returns success or "already exists".
	switch ba.Spec.BackendType {
	case "file":
		path := "/var/lib/novastor/file"
		if ba.Spec.FileBackend != nil && ba.Spec.FileBackend.Path != "" {
			path = ba.Spec.FileBackend.Path
		}
		var capacityBytes int64 = 100 * 1024 * 1024 * 1024
		if ba.Spec.FileBackend != nil && ba.Spec.FileBackend.MaxCapacityBytes != nil {
			capacityBytes = *ba.Spec.FileBackend.MaxCapacityBytes
		}
		config := map[string]interface{}{
			"path":           path,
			"capacity_bytes": capacityBytes,
			"name":           bdevName,
		}
		configJSON, _ := json.Marshal(config)
		if err := r.DPClient.InitBackend("file", string(configJSON)); err != nil {
			if !strings.Contains(err.Error(), "already") {
				if isTransientGRPCError(err) {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				r.Logger.Warn("ensureDataplaneState: file backend init failed", zap.Error(err))
			}
		}

	case "raw":
		devicePath := ba.Status.Device
		if devicePath == "" {
			return ctrl.Result{}, nil // no device recorded, nothing to re-init
		}
		config := map[string]interface{}{
			"device_path": devicePath,
			"name":        bdevName,
		}
		configJSON, _ := json.Marshal(config)
		if err := r.DPClient.InitBackend("raw", string(configJSON)); err != nil {
			if !strings.Contains(err.Error(), "already") && !strings.Contains(err.Error(), "returned null") {
				if isTransientGRPCError(err) {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				r.Logger.Warn("ensureDataplaneState: raw backend init failed", zap.Error(err))
			}
		}

	case "lvm":
		devicePath := ba.Status.Device
		if devicePath == "" {
			return ctrl.Result{}, nil
		}
		rawConfig := map[string]interface{}{
			"device_path": devicePath,
			"name":        bdevName,
		}
		rawJSON, _ := json.Marshal(rawConfig)
		if err := r.DPClient.InitBackend("raw", string(rawJSON)); err != nil {
			if !strings.Contains(err.Error(), "already") {
				if isTransientGRPCError(err) {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}
		}
		lvsName := fmt.Sprintf("lvs_%s", ba.Spec.PoolRef)
		lvmConfig := map[string]interface{}{
			"bdev_name":    bdevName,
			"lvs_name":     lvsName,
			"cluster_size": 1048576,
		}
		lvmJSON, _ := json.Marshal(lvmConfig)
		if err := r.DPClient.InitBackend("lvm", string(lvmJSON)); err != nil {
			if !strings.Contains(err.Error(), "already") {
				if isTransientGRPCError(err) {
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				r.Logger.Warn("ensureDataplaneState: lvm backend init failed", zap.Error(err))
			}
		}
	}

	// Re-initialize the chunk store.
	if _, err := r.DPClient.InitChunkStore(bdevName, r.NodeName); err != nil {
		if !strings.Contains(err.Error(), "already") {
			if isTransientGRPCError(err) {
				r.Logger.Warn("ensureDataplaneState: chunk store init transient error, will retry",
					zap.String("bdev", bdevName), zap.Error(err))
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			r.Logger.Warn("ensureDataplaneState: chunk store init failed",
				zap.String("bdev", bdevName), zap.Error(err))
		} else {
			r.Logger.Info("ensureDataplaneState: chunk store already initialized",
				zap.String("bdev", bdevName))
		}
	} else {
		r.Logger.Info("ensureDataplaneState: chunk store initialized successfully",
			zap.String("bdev", bdevName))
	}

	return ctrl.Result{}, nil
}

// provisionFileBackend creates an SPDK AIO bdev backed by a file.
func (r *BackendAssignmentReconciler) provisionFileBackend(
	_ context.Context,
	ba *novastorev1alpha1.BackendAssignment,
) (string, int64) {
	path := "/var/lib/novastor/file"
	if ba.Spec.FileBackend != nil && ba.Spec.FileBackend.Path != "" {
		path = ba.Spec.FileBackend.Path
	}

	var capacityBytes int64 = 100 * 1024 * 1024 * 1024 // 100GB default
	if ba.Spec.FileBackend != nil && ba.Spec.FileBackend.MaxCapacityBytes != nil {
		capacityBytes = *ba.Spec.FileBackend.MaxCapacityBytes
	}

	bdevName := r.BaseBdev
	config := map[string]interface{}{
		"path":           path,
		"capacity_bytes": capacityBytes,
		"name":           bdevName,
	}
	configJSON, _ := json.Marshal(config)

	if err := r.DPClient.InitBackend("file", string(configJSON)); err != nil {
		r.Logger.Error("failed to init file backend", zap.Error(err))
		return "", 0
	}

	return bdevName, capacityBytes
}

// provisionDeviceBackend discovers a local device, checks exclusivity, and
// initializes the backend via the dataplane.
func (r *BackendAssignmentReconciler) provisionDeviceBackend(
	ctx context.Context,
	ba *novastorev1alpha1.BackendAssignment,
) (bdevName, devicePath, pcieAddr string, capacity int64, err error) {
	// Discover local devices.
	devices, discErr := disk.DiscoverDevices()
	if discErr != nil {
		return "", "", "", 0, fmt.Errorf("device discovery: %w", discErr)
	}

	// Apply device filter.
	filterOpts := disk.FilterOptions{}
	if ba.Spec.DeviceFilter != nil {
		switch ba.Spec.DeviceFilter.Type {
		case "nvme":
			filterOpts.DeviceType = disk.TypeNVMe
		case "ssd":
			filterOpts.DeviceType = disk.TypeSSD
		case "hdd":
			filterOpts.DeviceType = disk.TypeHDD
		}
		if ba.Spec.DeviceFilter.MinSize != "" {
			minSize, parseErr := resource.ParseQuantity(ba.Spec.DeviceFilter.MinSize)
			if parseErr == nil {
				if minBytes, ok := minSize.AsInt64(); ok {
					filterOpts.MinSizeBytes = uint64(minBytes)
				}
			}
		}
	}
	filtered := disk.FilterDevices(devices, filterOpts)

	if len(filtered) == 0 {
		return "", "", "", 0, fmt.Errorf("no devices match filter (type=%s)", ba.Spec.DeviceFilter.Type)
	}

	// Check device exclusivity: don't use a device already assigned to another pool.
	var existingAssignments novastorev1alpha1.BackendAssignmentList
	if err := r.List(ctx, &existingAssignments, client.MatchingLabels{
		"novastor.io/node": r.NodeName,
	}); err != nil {
		return "", "", "", 0, fmt.Errorf("listing existing assignments: %w", err)
	}

	usedDevices := make(map[string]bool)
	for _, existing := range existingAssignments.Items {
		if existing.Name != ba.Name && existing.Status.Device != "" {
			usedDevices[existing.Status.Device] = true
		}
	}

	// Pick the first available device.
	var chosen *disk.DeviceInfo
	for i := range filtered {
		if !usedDevices[filtered[i].Path] {
			chosen = &filtered[i]
			break
		}
	}
	if chosen == nil {
		return "", "", "", 0, fmt.Errorf("all matching devices are already assigned to other pools")
	}

	devicePath = chosen.Path
	capacity = int64(chosen.SizeBytes)

	// For NVMe devices, extract PCIe address from sysfs.
	if chosen.DeviceType == disk.TypeNVMe {
		pcieAddr = readNVMePCIeAddr(chosen.Path)
	}

	// Initialize the backend via dataplane gRPC.
	// Use the configured base bdev name so it matches --spdk-base-bdev
	// used by the chunk service, target server, and GC.
	backendName := r.BaseBdev

	switch ba.Spec.BackendType {
	case "raw":
		config := map[string]interface{}{
			"device_path": devicePath,
			"name":        backendName,
		}
		configJSON, _ := json.Marshal(config)
		if initErr := r.DPClient.InitBackend("raw", string(configJSON)); initErr != nil {
			// If the bdev already exists (e.g. dataplane not restarted), reuse it.
			// SPDK returns "returned null" when the bdev name is already taken.
			errMsg := initErr.Error()
			if !strings.Contains(errMsg, "already") &&
				!strings.Contains(errMsg, "returned null") {
				return "", "", "", 0, fmt.Errorf("init raw backend: %w", initErr)
			}
			r.Logger.Info("raw backend already exists, reusing",
				zap.String("name", backendName), zap.String("device", devicePath))
		}
		bdevName = backendName

	case "lvm":
		// For LVM, we need to first ensure the NVMe bdev exists, then create an lvol store.
		lvsName := fmt.Sprintf("lvs_%s", ba.Spec.PoolRef)
		config := map[string]interface{}{
			"bdev_name":    backendName,
			"lvs_name":     lvsName,
			"cluster_size": 1048576,
		}
		// First attach the block device as a uring bdev.
		rawConfig := map[string]interface{}{
			"device_path": devicePath,
			"name":        backendName,
		}
		rawJSON, _ := json.Marshal(rawConfig)
		if initErr := r.DPClient.InitBackend("raw", string(rawJSON)); initErr != nil {
			// If it already exists, continue with lvol store creation.
			if !strings.Contains(initErr.Error(), "already") {
				return "", "", "", 0, fmt.Errorf("attach NVMe for lvm: %w", initErr)
			}
		}

		configJSON, _ := json.Marshal(config)
		if initErr := r.DPClient.InitBackend("lvm", string(configJSON)); initErr != nil {
			return "", "", "", 0, fmt.Errorf("init lvm backend: %w", initErr)
		}
		bdevName = lvsName
	}

	return bdevName, devicePath, pcieAddr, capacity, nil
}

// setFailed updates the BackendAssignment status to Failed.
func (r *BackendAssignmentReconciler) setFailed(ctx context.Context, ba *novastorev1alpha1.BackendAssignment, msg string) {
	r.Logger.Error("BackendAssignment failed",
		zap.String("name", ba.Name),
		zap.String("reason", msg),
	)
	ba.Status.Phase = "Failed"
	ba.Status.Message = msg
	meta.SetStatusCondition(&ba.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "ProvisioningFailed",
		Message:            msg,
		ObservedGeneration: ba.Generation,
	})
	_ = r.Status().Update(ctx, ba)
}

// readNVMePCIeAddr reads the PCIe BDF address for an NVMe device from sysfs.
// It reads /sys/block/<dev>/device/address which contains the PCIe BDF address
// (e.g., "0000:01:00.0").
func readNVMePCIeAddr(devPath string) string {
	devName := strings.TrimPrefix(devPath, "/dev/")
	addrPath := fmt.Sprintf("/sys/block/%s/device/address", devName)
	data, err := os.ReadFile(addrPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// isTransientGRPCError returns true if the error is a transient gRPC error
// that should be retried rather than permanently failing.
func isTransientGRPCError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if ok {
		switch s.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.ResourceExhausted:
			return true
		}
	}
	// Also catch common connection errors in error messages.
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "Unavailable") ||
		strings.Contains(msg, "transport is closing")
}

// SetupWithManager registers the BackendAssignment reconciler.
func (r *BackendAssignmentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.BackendAssignment{}).
		Named("backendassignment").
		Complete(r)
}
