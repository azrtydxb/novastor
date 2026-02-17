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

// SharedFilesystemReconciler reconciles SharedFilesystem objects.
type SharedFilesystemReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems/finalizers,verbs=update

// Reconcile handles a single reconciliation request for a SharedFilesystem.
func (r *SharedFilesystemReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var fs novastorev1alpha1.SharedFilesystem
	if err := r.Get(ctx, req.NamespacedName, &fs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling SharedFilesystem", "name", req.Name, "namespace", req.Namespace)

	// Set a Ready condition to indicate the controller has processed this resource.
	meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "Initializing",
		Message:            "SharedFilesystem controller has observed this resource",
		ObservedGeneration: fs.Generation,
	})

	if fs.Status.Phase == "" {
		fs.Status.Phase = "Pending"
	}

	if err := r.Status().Update(ctx, &fs); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the SharedFilesystem controller with the manager.
func (r *SharedFilesystemReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.SharedFilesystem{}).
		Named("sharedfilesystem").
		Complete(r)
}
