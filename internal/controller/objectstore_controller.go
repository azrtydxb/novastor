package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	defaultS3GatewayImage = "novastor/novastor-s3gw:v0.1.0"
	defaultS3Replicas     = int32(2)
	accessKeyLength       = 20
	secretKeyLength       = 40
)

// ObjectStoreReconciler reconciles ObjectStore objects.
type ObjectStoreReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=novastor.io,resources=objectstores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=novastor.io,resources=objectstores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=novastor.io,resources=objectstores/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles a single reconciliation request for an ObjectStore.
func (r *ObjectStoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var store novastorev1alpha1.ObjectStore
	if err := r.Get(ctx, req.NamespacedName, &store); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("reconciling ObjectStore", "name", req.Name, "namespace", req.Namespace)

	// Look up the referenced StoragePool.
	var pool novastorev1alpha1.StoragePool
	poolKey := types.NamespacedName{Name: store.Spec.Pool}
	if err := r.Get(ctx, poolKey, &pool); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "PoolNotFound",
				Message:            fmt.Sprintf("StoragePool %q not found", store.Spec.Pool),
				ObservedGeneration: store.Generation,
			})
			store.Status.Phase = "Pending"
			if statusErr := r.Status().Update(ctx, &store); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	// Check pool readiness.
	if pool.Status.Phase != "Ready" {
		meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "PoolNotReady",
			Message:            fmt.Sprintf("StoragePool %q is not ready (phase: %s)", store.Spec.Pool, pool.Status.Phase),
			ObservedGeneration: store.Generation,
		})
		store.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &store); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update the credentials Secret.
	secretName := fmt.Sprintf("novastor-s3-%s-keys", store.Name)
	if err := r.reconcileS3Secret(ctx, &store, secretName); err != nil {
		meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "SecretError",
			Message:            fmt.Sprintf("failed to reconcile S3 secret: %v", err),
			ObservedGeneration: store.Generation,
		})
		store.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &store); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update the S3 gateway Deployment.
	deployName := fmt.Sprintf("novastor-s3-%s", store.Name)
	if err := r.reconcileS3Deployment(ctx, &store, deployName, secretName); err != nil {
		meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "DeploymentError",
			Message:            fmt.Sprintf("failed to reconcile S3 deployment: %v", err),
			ObservedGeneration: store.Generation,
		})
		store.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &store); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update the S3 gateway Service.
	svcName := fmt.Sprintf("novastor-s3-%s", store.Name)
	port := store.Spec.Endpoint.Service.Port
	if err := r.reconcileS3Service(ctx, &store, svcName, deployName, port); err != nil {
		meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "ServiceError",
			Message:            fmt.Sprintf("failed to reconcile S3 service: %v", err),
			ObservedGeneration: store.Generation,
		})
		store.Status.Phase = "Pending"
		if statusErr := r.Status().Update(ctx, &store); statusErr != nil {
			return ctrl.Result{}, statusErr
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Set endpoint and mark ready.
	store.Status.Endpoint = fmt.Sprintf("%s.%s.svc:%d", svcName, store.Namespace, port)
	store.Status.Phase = "Ready"

	meta.SetStatusCondition(&store.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "ObjectStoreReady",
		Message:            fmt.Sprintf("S3 gateway available at %s", store.Status.Endpoint),
		ObservedGeneration: store.Generation,
	})

	if err := r.Status().Update(ctx, &store); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ObjectStoreReconciler) reconcileS3Secret(ctx context.Context, store *novastorev1alpha1.ObjectStore, name string) error {
	secret := &corev1.Secret{}
	secret.Name = name
	secret.Namespace = store.Namespace

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		// Only generate keys if the secret does not already have them
		// (preserve existing keys across reconciliations).
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		if _, ok := secret.Data["access-key"]; !ok {
			accessKey, genErr := generateRandomHex(accessKeyLength)
			if genErr != nil {
				return fmt.Errorf("generating access key: %w", genErr)
			}
			secret.Data["access-key"] = []byte(accessKey)
		}
		if _, ok := secret.Data["secret-key"]; !ok {
			secretKey, genErr := generateRandomHex(secretKeyLength)
			if genErr != nil {
				return fmt.Errorf("generating secret key: %w", genErr)
			}
			secret.Data["secret-key"] = []byte(secretKey)
		}

		secret.Type = corev1.SecretTypeOpaque

		if secret.Labels == nil {
			secret.Labels = map[string]string{}
		}
		secret.Labels["app.kubernetes.io/managed-by"] = "novastor-controller"
		secret.Labels["app.kubernetes.io/instance"] = store.Name

		return controllerutil.SetControllerReference(store, secret, r.Scheme())
	})
	return err
}

func (r *ObjectStoreReconciler) reconcileS3Deployment(ctx context.Context, store *novastorev1alpha1.ObjectStore, name, secretName string) error {
	replicas := defaultS3Replicas
	deploy := &appsv1.Deployment{}
	deploy.Name = name
	deploy.Namespace = store.Namespace

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		appLabels := map[string]string{
			"app.kubernetes.io/name":       "novastor-s3-gateway",
			"app.kubernetes.io/instance":   store.Name,
			"app.kubernetes.io/managed-by": "novastor-controller",
		}

		port := store.Spec.Endpoint.Service.Port

		image := store.Spec.Image
		if image == "" {
			image = defaultS3GatewayImage
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
							Name:  "s3-gateway",
							Image: image,
							Ports: []corev1.ContainerPort{
								{
									Name:          "s3",
									ContainerPort: port,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							Env: []corev1.EnvVar{
								{Name: "POOL_NAME", Value: store.Spec.Pool},
								{Name: "S3_PORT", Value: fmt.Sprintf("%d", port)},
								{
									Name: "ACCESS_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
											Key:                  "access-key",
										},
									},
								},
								{
									Name: "SECRET_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
											Key:                  "secret-key",
										},
									},
								},
							},
						},
					},
				},
			},
		}

		return controllerutil.SetControllerReference(store, deploy, r.Scheme())
	})
	return err
}

func (r *ObjectStoreReconciler) reconcileS3Service(ctx context.Context, store *novastorev1alpha1.ObjectStore, name, deployName string, port int32) error {
	svc := &corev1.Service{}
	svc.Name = name
	svc.Namespace = store.Namespace

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		appLabels := map[string]string{
			"app.kubernetes.io/name":       "novastor-s3-gateway",
			"app.kubernetes.io/instance":   store.Name,
			"app.kubernetes.io/managed-by": "novastor-controller",
		}

		svc.Spec = corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: appLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       "s3",
					Port:       port,
					TargetPort: intstr.FromInt32(port),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}

		return controllerutil.SetControllerReference(store, svc, r.Scheme())
	})
	return err
}

// generateRandomHex generates a random hex string of the given byte length.
func generateRandomHex(byteLen int) (string, error) {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// SetupWithManager registers the ObjectStore controller with the manager.
func (r *ObjectStoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&novastorev1alpha1.ObjectStore{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.Secret{}).
		Named("objectstore").
		Complete(r)
}
