package controller

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
)

// QuotaReconciler reconciles StorageQuota objects.
type QuotaReconciler struct {
	client.Client
	MetadataClient MetadataClient
}

// MetadataClient provides the subset of metadata service operations needed by the controller.
type MetadataClient interface {
	// SetQuota sets a quota for a scope.
	SetQuota(ctx context.Context, scope QuotaScopeSpec, storageHard, storageSoft int64, objectCountHard int64) error
	// GetUsage retrieves the current usage for a scope.
	GetUsage(ctx context.Context, scope QuotaScopeSpec) (storageUsed, objectCountUsed int64, err error)
}

// QuotaScopeSpec defines the scope of a quota (matching the CRD type).
type QuotaScopeSpec struct {
	Kind string
	Name string
}

// String returns a unique string identifier for the quota scope.
func (q QuotaScopeSpec) String() string {
	return fmt.Sprintf("%s:%s", q.Kind, q.Name)
}

// +kubebuilder:rbac:groups=novastor.io,resources=storagequotas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=storagequotas/status,verbs=get;update;patch

// Reconcile handles a single reconciliation request for a StorageQuota.
func (r *QuotaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var quota novastorev1alpha1.StorageQuota
	if err := r.Get(ctx, req.NamespacedName, &quota); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling StorageQuota", "name", req.Name, "scopeKind", quota.Spec.Scope.Kind, "scopeName", quota.Spec.Scope.Name)

	// Initialize status if not set
	if quota.Status.Phase == "" {
		quota.Status.Phase = "Active"
		meta.SetStatusCondition(&quota.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "Initialized",
			Message:            "Quota has been initialized",
			ObservedGeneration: quota.Generation,
		})
		if err := r.Status().Update(ctx, &quota); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Sync quota spec to metadata service
	if r.MetadataClient != nil {
		scopeSpec := QuotaScopeSpec{
			Kind: quota.Spec.Scope.Kind,
			Name: quota.Spec.Scope.Name,
		}

		storageHard := int64(0)
		storageSoft := int64(0)
		if quota.Spec.Storage != nil {
			storageHard = quota.Spec.Storage.Hard
			storageSoft = quota.Spec.Storage.Soft
		}

		objectCountHard := int64(0)
		if quota.Spec.ObjectCount != nil {
			objectCountHard = quota.Spec.ObjectCount.Hard
		}

		if err := r.MetadataClient.SetQuota(ctx, scopeSpec, storageHard, storageSoft, objectCountHard); err != nil {
			meta.SetStatusCondition(&quota.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "MetadataSyncFailed",
				Message:            fmt.Sprintf("failed to sync quota to metadata service: %v", err),
				ObservedGeneration: quota.Generation,
			})
			quota.Status.Phase = "Failed"
			if statusErr := r.Status().Update(ctx, &quota); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			// Requeue to retry syncing to metadata
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}

		// Fetch current usage from metadata service
		storageUsed, objectCountUsed, err := r.MetadataClient.GetUsage(ctx, scopeSpec)
		if err != nil {
			meta.SetStatusCondition(&quota.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "UsageFetchFailed",
				Message:            fmt.Sprintf("failed to fetch usage from metadata service: %v", err),
				ObservedGeneration: quota.Generation,
			})
			if statusErr := r.Status().Update(ctx, &quota); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}

		// Update status with current usage
		quota.Status.Storage = &novastorev1alpha1.StorageQuotaStatusStorage{
			Used:       storageUsed,
			UsedString: formatBytes(storageUsed),
		}
		if objectCountHard > 0 {
			quota.Status.ObjectCount = &novastorev1alpha1.ObjectCountQuotaStatus{
				Used: objectCountUsed,
			}
		}

		// Determine phase based on usage vs limits
		newPhase := "Active"
		conditionType := "QuotaExceeded"
		conditionStatus := metav1.ConditionFalse
		reason := "WithinLimits"
		message := "Usage is within quota limits"

		if quota.Spec.Storage != nil {
			if storageUsed > quota.Spec.Storage.Hard {
				newPhase = "Exceeded"
				conditionType = "QuotaExceeded"
				conditionStatus = metav1.ConditionTrue
				reason = "StorageHardExceeded"
				message = fmt.Sprintf("Storage usage %s exceeds hard limit %s", formatBytes(storageUsed), formatBytes(quota.Spec.Storage.Hard))
			} else if quota.Spec.Storage.Soft > 0 && storageUsed > quota.Spec.Storage.Soft {
				newPhase = "Warning"
				conditionType = "QuotaWarning"
				conditionStatus = metav1.ConditionTrue
				reason = "StorageSoftExceeded"
				message = fmt.Sprintf("Storage usage %s exceeds soft limit %s", formatBytes(storageUsed), formatBytes(quota.Spec.Storage.Soft))
			}
		} else if quota.Spec.ObjectCount != nil {
			if objectCountUsed > quota.Spec.ObjectCount.Hard {
				newPhase = "Exceeded"
				conditionType = "QuotaExceeded"
				conditionStatus = metav1.ConditionTrue
				reason = "ObjectCountExceeded"
				message = fmt.Sprintf("Object count %d exceeds hard limit %d", objectCountUsed, quota.Spec.ObjectCount.Hard)
			}
		}

		quota.Status.Phase = newPhase
		meta.SetStatusCondition(&quota.Status.Conditions, metav1.Condition{
			Type:               conditionType,
			Status:             conditionStatus,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: quota.Generation,
		})
		meta.SetStatusCondition(&quota.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "Reconciled",
			Message:            "Quota status has been updated",
			ObservedGeneration: quota.Generation,
		})

		if err := r.Status().Update(ctx, &quota); err != nil {
			return ctrl.Result{}, err
		}

		// If quota is exceeded or warning, requeue sooner for monitoring
		if newPhase == "Exceeded" {
			return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
		}
		if newPhase == "Warning" {
			return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
		}
	}

	return ctrl.Result{}, nil
}

// formatBytes converts a byte count to a human-readable string.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
