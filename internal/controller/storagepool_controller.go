package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
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
// +kubebuilder:rbac:groups=novastor.io,resources=backendassignments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=backendassignments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile handles a single reconciliation request for a StoragePool.
func (r *StoragePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pool novastorev1alpha1.StoragePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling StoragePool", "name", req.Name, "namespace", req.Namespace)

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

	// Reconcile BackendAssignments: ensure one per matching node.
	matchingNodes, err := r.getMatchingNodes(ctx, pool.Spec.NodeSelector)
	if err != nil {
		logger.Error(err, "failed to get matching nodes for BackendAssignment reconciliation")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if err := r.reconcileBackendAssignments(ctx, &pool, matchingNodes); err != nil {
		logger.Error(err, "failed to reconcile BackendAssignments")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Pool is valid and nodes match.
	pool.Status.Phase = "Ready"
	pool.Status.NodeCount = nodeCount
	pool.Status.TotalCapacity = totalCapacity

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

// getMatchingNodes returns the list of nodes matching the given label selector.
func (r *StoragePoolReconciler) getMatchingNodes(ctx context.Context, selector *metav1.LabelSelector) ([]corev1.Node, error) {
	var nodeList corev1.NodeList
	listOpts := []client.ListOption{}
	if selector != nil {
		sel, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid label selector: %w", err)
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: sel})
	}
	if err := r.List(ctx, &nodeList, listOpts...); err != nil {
		return nil, err
	}
	return nodeList.Items, nil
}

// reconcileBackendAssignments ensures there is exactly one BackendAssignment per
// matching node for the given StoragePool. Creates missing ones, deletes orphans.
func (r *StoragePoolReconciler) reconcileBackendAssignments(
	ctx context.Context,
	pool *novastorev1alpha1.StoragePool,
	matchingNodes []corev1.Node,
) error {
	logger := log.FromContext(ctx)

	// List existing BackendAssignments for this pool.
	var existingList novastorev1alpha1.BackendAssignmentList
	if err := r.List(ctx, &existingList, client.MatchingLabels{
		"novastor.io/pool": pool.Name,
	}); err != nil {
		return fmt.Errorf("list BackendAssignments: %w", err)
	}

	// Build maps for reconciliation.
	existingByNode := make(map[string]*novastorev1alpha1.BackendAssignment, len(existingList.Items))
	for i := range existingList.Items {
		ba := &existingList.Items[i]
		existingByNode[ba.Spec.NodeName] = ba
	}

	desiredNodes := make(map[string]bool, len(matchingNodes))
	for _, node := range matchingNodes {
		desiredNodes[node.Name] = true
	}

	// Create missing BackendAssignments.
	for _, node := range matchingNodes {
		if _, exists := existingByNode[node.Name]; exists {
			// Check if spec needs updating (pool spec changed).
			existing := existingByNode[node.Name]
			desiredSpec := r.buildAssignmentSpec(pool, node.Name)
			if !equality.Semantic.DeepEqual(existing.Spec, desiredSpec) {
				existing.Spec = desiredSpec
				if err := r.Update(ctx, existing); err != nil {
					return fmt.Errorf("update BackendAssignment %s: %w", existing.Name, err)
				}
				logger.Info("updated BackendAssignment", "name", existing.Name, "node", node.Name)
			}
			continue
		}

		// Create new BackendAssignment.
		ba := &novastorev1alpha1.BackendAssignment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%s", pool.Name, node.Name),
				Namespace: pool.Namespace,
				Labels: map[string]string{
					"novastor.io/pool": pool.Name,
					"novastor.io/node": node.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(pool, novastorev1alpha1.GroupVersion.WithKind("StoragePool")),
				},
			},
			Spec: r.buildAssignmentSpec(pool, node.Name),
		}
		ba.Status.Phase = "Pending"

		if err := r.Create(ctx, ba); err != nil {
			if errors.IsAlreadyExists(err) {
				continue
			}
			return fmt.Errorf("create BackendAssignment for node %s: %w", node.Name, err)
		}
		logger.Info("created BackendAssignment", "name", ba.Name, "node", node.Name, "backend", pool.Spec.BackendType)
	}

	// Delete orphaned BackendAssignments (node no longer matches selector).
	for nodeName, ba := range existingByNode {
		if !desiredNodes[nodeName] {
			if err := r.Delete(ctx, ba); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("delete orphaned BackendAssignment %s: %w", ba.Name, err)
			}
			logger.Info("deleted orphaned BackendAssignment", "name", ba.Name, "node", nodeName)
		}
	}

	return nil
}

// buildAssignmentSpec constructs the desired BackendAssignmentSpec from the pool.
func (r *StoragePoolReconciler) buildAssignmentSpec(
	pool *novastorev1alpha1.StoragePool,
	nodeName string,
) novastorev1alpha1.BackendAssignmentSpec {
	spec := novastorev1alpha1.BackendAssignmentSpec{
		PoolRef:     pool.Name,
		NodeName:    nodeName,
		BackendType: pool.Spec.BackendType,
	}
	if pool.Spec.DeviceFilter != nil {
		df := *pool.Spec.DeviceFilter
		spec.DeviceFilter = &df
	}
	if pool.Spec.FileBackend != nil {
		fb := *pool.Spec.FileBackend
		spec.FileBackend = &fb
		if pool.Spec.FileBackend.MaxCapacityBytes != nil {
			v := *pool.Spec.FileBackend.MaxCapacityBytes
			spec.FileBackend.MaxCapacityBytes = &v
		}
	}
	return spec
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
		Owns(&novastorev1alpha1.BackendAssignment{}).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.nodeToStoragePools)).
		Named("storagepool").
		Complete(r)
}

// nodeToStoragePools maps a Node event to all StoragePools that might select it.
func (r *StoragePoolReconciler) nodeToStoragePools(ctx context.Context, obj client.Object) []ctrl.Request {
	var pools novastorev1alpha1.StoragePoolList
	if err := r.List(ctx, &pools); err != nil {
		return nil
	}
	var requests []ctrl.Request
	for _, pool := range pools.Items {
		requests = append(requests, ctrl.Request{
			NamespacedName: client.ObjectKeyFromObject(&pool),
		})
	}
	return requests
}
