package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
)

const (
	csiDriverName = "novastor.csi.novastor.io"
)

// BlockVolumeReconciler reconciles BlockVolume objects.
type BlockVolumeReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=blockvolumes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles a single reconciliation request for a BlockVolume.
func (r *BlockVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var volume novastorev1alpha1.BlockVolume
	if err := r.Get(ctx, req.NamespacedName, &volume); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling BlockVolume", "name", req.Name, "namespace", req.Namespace)

	// Look up the referenced StoragePool (cluster-scoped, so no namespace).
	var pool novastorev1alpha1.StoragePool
	poolKey := types.NamespacedName{Name: volume.Spec.Pool}
	if err := r.Get(ctx, poolKey, &pool); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&volume.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "PoolNotFound",
				Message:            fmt.Sprintf("StoragePool %q not found", volume.Spec.Pool),
				ObservedGeneration: volume.Generation,
			})
			volume.Status.Phase = "Pending"
			if statusErr := r.Status().Update(ctx, &volume); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Check pool readiness.
	if pool.Status.Phase != "Ready" {
		meta.SetStatusCondition(&volume.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "PoolNotReady",
			Message:            fmt.Sprintf("StoragePool %q is not ready (phase: %s)", volume.Spec.Pool, pool.Status.Phase),
			ObservedGeneration: volume.Generation,
		})
		volume.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &volume); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Set status fields from spec.
	volume.Status.Pool = volume.Spec.Pool
	volume.Status.AccessMode = volume.Spec.AccessMode

	// Create or update the PersistentVolume.
	pvName := fmt.Sprintf("novastor-%s-%s", volume.Namespace, volume.Name)
	pv := &corev1.PersistentVolume{}
	pv.Name = pvName

	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, pv, func() error {
		// Parse size.
		qty, parseErr := resource.ParseQuantity(volume.Spec.Size)
		if parseErr != nil {
			return fmt.Errorf("invalid volume size %q: %w", volume.Spec.Size, parseErr)
		}

		// Map access mode.
		var accessMode corev1.PersistentVolumeAccessMode
		switch volume.Spec.AccessMode {
		case "ReadWriteOnce":
			accessMode = corev1.ReadWriteOnce
		case "ReadOnlyMany":
			accessMode = corev1.ReadOnlyMany
		default:
			accessMode = corev1.ReadWriteOnce
		}

		pv.Spec = corev1.PersistentVolumeSpec{
			Capacity: corev1.ResourceList{
				corev1.ResourceStorage: qty,
			},
			AccessModes: []corev1.PersistentVolumeAccessMode{accessMode},
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       csiDriverName,
					VolumeHandle: fmt.Sprintf("%s/%s", volume.Namespace, volume.Name),
					VolumeAttributes: map[string]string{
						"pool":       volume.Spec.Pool,
						"accessMode": volume.Spec.AccessMode,
					},
				},
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
		}

		// Set labels to track ownership (PVs are cluster-scoped so we cannot
		// set a namespaced owner reference).
		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}
		pv.Labels["novastor.io/blockvolume"] = volume.Name
		pv.Labels["novastor.io/namespace"] = volume.Namespace

		return nil
	})
	if err != nil {
		meta.SetStatusCondition(&volume.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "PVCreateError",
			Message:            fmt.Sprintf("failed to create PersistentVolume: %v", err),
			ObservedGeneration: volume.Generation,
		})
		volume.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &volume); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	logger.Info("PersistentVolume reconciled", "name", pvName, "operation", result)

	// Volume is bound.
	volume.Status.Phase = "Bound"
	meta.SetStatusCondition(&volume.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "VolumeBound",
		Message:            fmt.Sprintf("PersistentVolume %s created and bound", pvName),
		ObservedGeneration: volume.Generation,
	})

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
