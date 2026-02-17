package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	novastorev1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
)

const (
	nfsFilerImage = "novastor/nfs-filer:latest"
	nfsPort       = int32(2049)
)

// SharedFilesystemReconciler reconciles SharedFilesystem objects.
type SharedFilesystemReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=sharedfilesystems/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles a single reconciliation request for a SharedFilesystem.
func (r *SharedFilesystemReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var fs novastorev1alpha1.SharedFilesystem
	if err := r.Get(ctx, req.NamespacedName, &fs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling SharedFilesystem", "name", req.Name, "namespace", req.Namespace)

	// Look up the referenced StoragePool.
	var pool novastorev1alpha1.StoragePool
	poolKey := types.NamespacedName{Name: fs.Spec.Pool}
	if err := r.Get(ctx, poolKey, &pool); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "PoolNotFound",
				Message:            fmt.Sprintf("StoragePool %q not found", fs.Spec.Pool),
				ObservedGeneration: fs.Generation,
			})
			fs.Status.Phase = "Pending"
			if statusErr := r.Status().Update(ctx, &fs); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Check pool readiness.
	if pool.Status.Phase != "Ready" {
		meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "PoolNotReady",
			Message:            fmt.Sprintf("StoragePool %q is not ready (phase: %s)", fs.Spec.Pool, pool.Status.Phase),
			ObservedGeneration: fs.Generation,
		})
		fs.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &fs); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update the NFS filer Deployment.
	deployName := fmt.Sprintf("novastor-nfs-%s", fs.Name)
	if err := r.reconcileNFSDeployment(ctx, &fs, deployName); err != nil {
		meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "DeploymentError",
			Message:            fmt.Sprintf("failed to reconcile NFS deployment: %v", err),
			ObservedGeneration: fs.Generation,
		})
		fs.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &fs); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update the NFS Service.
	svcName := fmt.Sprintf("novastor-nfs-%s", fs.Name)
	if err := r.reconcileNFSService(ctx, &fs, svcName, deployName); err != nil {
		meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ServiceError",
			Message:            fmt.Sprintf("failed to reconcile NFS service: %v", err),
			ObservedGeneration: fs.Generation,
		})
		fs.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &fs); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Set endpoint and mark ready.
	fs.Status.Endpoint = fmt.Sprintf("%s.%s.svc:%d", svcName, fs.Namespace, nfsPort)
	fs.Status.Phase = "Ready"

	meta.SetStatusCondition(&fs.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "FilesystemReady",
		Message:            fmt.Sprintf("NFS filer available at %s", fs.Status.Endpoint),
		ObservedGeneration: fs.Generation,
	})

	if err := r.Status().Update(ctx, &fs); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SharedFilesystemReconciler) reconcileNFSDeployment(ctx context.Context, fs *novastorev1alpha1.SharedFilesystem, name string) error {
	var replicas int32 = 1
	deploy := &appsv1.Deployment{}
	deploy.Name = name
	deploy.Namespace = fs.Namespace

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		appLabels := map[string]string{
			"app.kubernetes.io/name":       "novastor-nfs-filer",
			"app.kubernetes.io/instance":   fs.Name,
			"app.kubernetes.io/managed-by": "novastor-controller",
		}

		deploy.Spec = appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: appLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: appLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "nfs-filer",
							Image: nfsFilerImage,
							Ports: []corev1.ContainerPort{
								{
									Name:          "nfs",
									ContainerPort: nfsPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Env: []corev1.EnvVar{
								{Name: "POOL_NAME", Value: fs.Spec.Pool},
								{Name: "CAPACITY", Value: fs.Spec.Capacity},
								{Name: "ACCESS_MODE", Value: fs.Spec.AccessMode},
							},
						},
					},
				},
			},
		}

		return controllerutil.SetControllerReference(fs, deploy, r.Scheme())
	})
	return err
}

func (r *SharedFilesystemReconciler) reconcileNFSService(ctx context.Context, fs *novastorev1alpha1.SharedFilesystem, name, deployName string) error {
	svc := &corev1.Service{}
	svc.Name = name
	svc.Namespace = fs.Namespace

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		appLabels := map[string]string{
			"app.kubernetes.io/name":       "novastor-nfs-filer",
			"app.kubernetes.io/instance":   fs.Name,
			"app.kubernetes.io/managed-by": "novastor-controller",
		}

		svc.Spec = corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: appLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       "nfs",
					Port:       nfsPort,
					TargetPort: intstr.FromInt32(nfsPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}

		return controllerutil.SetControllerReference(fs, svc, r.Scheme())
	})
	return err
}

// SetupWithManager registers the SharedFilesystem controller with the manager.
func (r *SharedFilesystemReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.SharedFilesystem{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Named("sharedfilesystem").
		Complete(r)
}
