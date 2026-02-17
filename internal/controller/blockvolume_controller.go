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

// BlockVolumeReconciler reconciles BlockVolume objects.
type BlockVolumeReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes/finalizers,verbs=update

// Reconcile handles a single reconciliation request for a BlockVolume.
func (r *BlockVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var volume novastorev1alpha1.BlockVolume
	if err := r.Get(ctx, req.NamespacedName, &volume); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling BlockVolume", "name", req.Name, "namespace", req.Namespace)

	// Set a Ready condition to indicate the controller has processed this resource.
	meta.SetStatusCondition(&volume.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "Initializing",
		Message:            "BlockVolume controller has observed this resource",
		ObservedGeneration: volume.Generation,
	})

	if volume.Status.Phase == "" {
		volume.Status.Phase = "Pending"
	}

	if err := r.Status().Update(ctx, &volume); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the BlockVolume controller with the manager.
func (r *BlockVolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.BlockVolume{}).
		Named("blockvolume").
		Complete(r)
}
