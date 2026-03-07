package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
)

// StoragePoolReconciler reconciles StoragePool objects.
type StoragePoolReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=storagepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=storagepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=storagepools/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile handles a single reconciliation request for a StoragePool.
func (r *StoragePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pool novastorev1alpha1.StoragePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling StoragePool", "name", req.Name, "namespace", req.Namespace)

	// Validate spec: dataProtection mode is required.
	if pool.Spec.DataProtection.Mode == "" {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "InvalidSpec",
			Message:            "dataProtection.mode is required",
			ObservedGeneration: pool.Generation,
		})
		pool.Status.Phase = "Failed"
		if err := r.Status().Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Validate replication/erasureCoding config matches mode.
	if pool.Spec.DataProtection.Mode == "replication" && pool.Spec.DataProtection.Replication == nil {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "InvalidSpec",
			Message:            "replication config required when mode is replication",
			ObservedGeneration: pool.Generation,
		})
		pool.Status.Phase = "Failed"
		if err := r.Status().Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if pool.Spec.DataProtection.Mode == "erasureCoding" && pool.Spec.DataProtection.ErasureCoding == nil {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "InvalidSpec",
			Message:            "erasureCoding config required when mode is erasureCoding",
			ObservedGeneration: pool.Generation,
		})
		pool.Status.Phase = "Failed"
		if err := r.Status().Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Count nodes matching the nodeSelector.
	nodeCount, totalCapacity, err := r.countMatchingNodes(ctx, pool.Spec.NodeSelector)
	if err != nil {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "NodeListError",
			Message:            fmt.Sprintf("failed to list nodes: %v", err),
			ObservedGeneration: pool.Generation,
		})
		pool.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &pool); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if nodeCount == 0 {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "NoMatchingNodes",
			Message:            "no nodes match the nodeSelector",
			ObservedGeneration: pool.Generation,
		})
		pool.Status.Phase = "Pending"
		pool.Status.NodeCount = 0
		pool.Status.TotalCapacity = "0"
		if err := r.Status().Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Pool is valid and nodes match.
	pool.Status.Phase = "Ready"
	pool.Status.NodeCount = nodeCount
	pool.Status.TotalCapacity = totalCapacity
	pool.Status.DataProtection = pool.Spec.DataProtection.Mode

	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "PoolReady",
		Message:            fmt.Sprintf("pool is ready with %d node(s)", nodeCount),
		ObservedGeneration: pool.Generation,
	})

	if err := r.Status().Update(ctx, &pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// countMatchingNodes lists nodes matching the given label selector and returns
// the count and aggregate allocatable storage capacity.
func (r *StoragePoolReconciler) countMatchingNodes(ctx context.Context, selector *metav1.LabelSelector) (int, string, error) {
	var nodeList corev1.NodeList

	listOpts := []client.ListOption{}
	if selector != nil {
		sel, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return 0, "0", fmt.Errorf("invalid label selector: %w", err)
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: sel})
	}

	if err := r.List(ctx, &nodeList, listOpts...); err != nil {
		return 0, "0", err
	}

	count := len(nodeList.Items)
	var totalBytes int64
	for i := range nodeList.Items {
		if ephemeral, ok := nodeList.Items[i].Status.Allocatable[corev1.ResourceEphemeralStorage]; ok {
			totalBytes += ephemeral.Value()
		}
	}

	// Format capacity as human-readable.
	capacity := formatCapacity(totalBytes)
	return count, capacity, nil
}

// formatCapacity converts bytes to a human-readable string.
func formatCapacity(bytes int64) string {
	const (
		_  = iota
		ki = 1 << (10 * iota)
		mi
		gi
		ti
	)
	switch {
	case bytes >= int64(ti):
		return fmt.Sprintf("%dTi", bytes/int64(ti))
	case bytes >= int64(gi):
		return fmt.Sprintf("%dGi", bytes/int64(gi))
	case bytes >= int64(mi):
		return fmt.Sprintf("%dMi", bytes/int64(mi))
	case bytes >= int64(ki):
		return fmt.Sprintf("%dKi", bytes/int64(ki))
	default:
		return fmt.Sprintf("%d", bytes)
	}
}

// SetupWithManager registers the StoragePool controller with the manager.
func (r *StoragePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.StoragePool{}).
		Named("storagepool").
		Complete(r)
}
