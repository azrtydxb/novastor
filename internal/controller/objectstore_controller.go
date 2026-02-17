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

// ObjectStoreReconciler reconciles ObjectStore objects.
type ObjectStoreReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=objectstores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=objectstores/finalizers,verbs=update

// Reconcile handles a single reconciliation request for an ObjectStore.
func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var store novastorev1alpha1.ObjectStore
	if err := r.Get(ctx, req.NamespacedName, &store); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling ObjectStore", "name", req.Name, "namespace", req.Namespace)

	// Set a Ready condition to indicate the controller has processed this resource.
	meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "Initializing",
		Message:            "ObjectStore controller has observed this resource",
		ObservedGeneration: store.Generation,
	})

	if store.Status.Phase == "" {
		store.Status.Phase = "Pending"
	}

	if err := r.Status().Update(ctx, &store); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the ObjectStore controller with the manager.
func (r *ObjectStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.ObjectStore{}).
		Named("objectstore").
		Complete(r)
}
