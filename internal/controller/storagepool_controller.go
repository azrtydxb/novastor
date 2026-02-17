package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
)

// StoragePoolReconciler reconciles StoragePool objects.
type StoragePoolReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=storagepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=storagepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=storagepools/finalizers,verbs=update

// Reconcile handles a single reconciliation request for a StoragePool.
func (r *StoragePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pool novastorev1alpha1.StoragePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling StoragePool", "name", req.Name, "namespace", req.Namespace)

	// Set a Ready condition to indicate the controller has processed this resource.
	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "Initializing",
		Message:            "StoragePool controller has observed this resource",
		ObservedGeneration: pool.Generation,
	})

	if pool.Status.Phase == "" {
		pool.Status.Phase = "Pending"
	}

	if err := r.Status().Update(ctx, &pool); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the StoragePool controller with the manager.
func (r *StoragePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.StoragePool{}).
		Named("storagepool").
		Complete(r)
}
