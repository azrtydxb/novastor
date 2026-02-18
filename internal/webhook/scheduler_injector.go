package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/piwi3910/novastor/internal/logging"
	"github.com/piwi3910/novastor/internal/metrics"
)

const (
	// SchedulerName is the name of the NovaStor custom scheduler.
	SchedulerName = "novastor-scheduler"

	// SkipAnnotation is the annotation that can be set to "true" to skip
	// scheduler injection for a specific pod.
	SkipAnnotation = "novastor.io/skip-scheduler-injection"

	// NovastorProvisioner is the CSI provisioner name for NovaStor storage classes.
	NovastorProvisioner = "novastor.csi.novastor.io"
)

// SchedulerInjector is a mutating webhook that injects the NovaStor scheduler
// name into pods that use NovaStor PVCs.
type SchedulerInjector struct {
	client kubernetes.Interface
}

// NewSchedulerInjector creates a new SchedulerInjector.
func NewSchedulerInjector(client kubernetes.Interface) *SchedulerInjector {
	return &SchedulerInjector{
		client: client,
	}
}

// Inject injects the scheduler name into the pod if it uses NovaStor PVCs.
// It returns a JSON patch operation if injection is needed, or nil if not.
func (si *SchedulerInjector) Inject(ctx context.Context, ar *admissionv1.AdmissionReview) ([]byte, error) {
	req := ar.Request

	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return nil, fmt.Errorf("unmarshaling pod: %w", err)
	}

	// Check if the pod has the skip annotation.
	if skip, ok := pod.Annotations[SkipAnnotation]; ok && strings.ToLower(skip) == "true" {
		logging.L.Debug("skipping scheduler injection due to annotation",
			zap.String("namespace", req.Namespace),
			zap.String("pod", pod.Name),
		)
		return nil, nil
	}

	// If the pod already has a scheduler name set, don't override it.
	if pod.Spec.SchedulerName != "" && pod.Spec.SchedulerName != SchedulerName {
		logging.L.Debug("pod already has scheduler name set, skipping injection",
			zap.String("namespace", req.Namespace),
			zap.String("pod", pod.Name),
			zap.String("schedulerName", pod.Spec.SchedulerName),
		)
		return nil, nil
	}

	// Check if the pod uses any NovaStor PVCs.
	usesNovaStor, err := si.usesNovaStorPVCs(ctx, &pod, req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("checking PVCs: %w", err)
	}

	if !usesNovaStor {
		logging.L.Debug("pod does not use NovaStor PVCs, skipping injection",
			zap.String("namespace", req.Namespace),
			zap.String("pod", pod.Name),
		)
		return nil, nil
	}

	// Create the JSON patch to inject the scheduler name.
	patch := []map[string]interface{}{
		{
			"op":    "replace",
			"path":  "/spec/schedulerName",
			"value": SchedulerName,
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshaling patch: %w", err)
	}

	logging.L.Info("injecting NovaStor scheduler into pod",
		zap.String("namespace", req.Namespace),
		zap.String("pod", pod.Name),
		zap.String("schedulerName", SchedulerName),
	)

	return patchBytes, nil
}

// usesNovaStorPVCs checks if the pod uses any PVCs that belong to NovaStor storage classes.
func (si *SchedulerInjector) usesNovaStorPVCs(ctx context.Context, pod *corev1.Pod, namespace string) (bool, error) {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := volume.PersistentVolumeClaim.ClaimName

		// Get the PVC to find its storage class.
		pvc, err := si.client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err != nil {
			// If the PVC doesn't exist yet, we can't determine the storage class.
			// In this case, we allow the pod to proceed without injection.
			logging.L.Debug("unable to get PVC, assuming not NovaStor",
				zap.String("pvc", pvcName),
				zap.Error(err),
			)
			continue
		}

		// If the PVC has no storage class, skip it.
		if pvc.Spec.StorageClassName == nil {
			continue
		}

		// Get the storage class to check if it's a NovaStor storage class.
		storageClassName := *pvc.Spec.StorageClassName
		sc, err := si.client.StorageV1().StorageClasses().Get(ctx, storageClassName, metav1.GetOptions{})
		if err != nil {
			logging.L.Debug("unable to get storage class, assuming not NovaStor",
				zap.String("storageClass", storageClassName),
				zap.Error(err),
			)
			continue
		}

		// Check if the storage class uses the NovaStor provisioner.
		if sc.Provisioner == NovastorProvisioner {
			return true, nil
		}
	}

	return false, nil
}

// ServeHTTP implements the http.Handler interface for the webhook.
func (si *SchedulerInjector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var admissionReview admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&admissionReview); err != nil {
		http.Error(w, fmt.Sprintf("failed to decode request: %v", err), http.StatusBadRequest)
		metrics.WebhookErrorsTotal.WithLabelValues("decode", "bad_request").Inc()
		return
	}

	// Validate that we have a request.
	if admissionReview.Request == nil {
		http.Error(w, "nil request", http.StatusBadRequest)
		metrics.WebhookErrorsTotal.WithLabelValues("validate", "nil_request").Inc()
		return
	}

	// Extract labels for metrics.
	operation := string(admissionReview.Request.Operation)
	resource := admissionReview.Request.Kind.Kind

	// Only handle CREATE operations on pods.
	if admissionReview.Request.Kind.Kind != "Pod" || admissionReview.Request.Operation != admissionv1.Create {
		http.Error(w, "expected CREATE operation on Pod", http.StatusBadRequest)
		metrics.WebhookErrorsTotal.WithLabelValues(operation, "invalid_resource").Inc()
		return
	}

	// Record admission review received.
	metrics.WebhookAdmissionReviewsTotal.WithLabelValues(operation, resource).Inc()

	// Perform the injection.
	ctx := r.Context()
	patch, err := si.Inject(ctx, &admissionReview)

	// Record duration.
	duration := time.Since(start).Seconds()
	metrics.WebhookAdmissionReviewDuration.WithLabelValues(operation, resource).Observe(duration)

	if err != nil {
		logging.L.Error("failed to inject scheduler",
			zap.Error(err),
			zap.String("namespace", admissionReview.Request.Namespace),
			zap.String("pod", admissionReview.Request.Name),
		)
		metrics.WebhookErrorsTotal.WithLabelValues(operation, "injection_failed").Inc()
		// Return a failure response.
		response := &admissionv1.AdmissionResponse{
			UID:     admissionReview.Request.UID,
			Allowed: false,
			Result: &metav1.Status{
				Message: err.Error(),
			},
		}
		admissionReview.Response = response
	} else {
		// Return a success response with the patch.
		response := &admissionv1.AdmissionResponse{
			UID:     admissionReview.Request.UID,
			Allowed: true,
			Patch:   patch,
			PatchType: func() *admissionv1.PatchType {
				pt := admissionv1.PatchTypeJSONPatch
				return &pt
			}(),
		}
		admissionReview.Response = response

		// Record mutation if patch was applied.
		if len(patch) > 0 {
			metrics.WebhookMutationsTotal.WithLabelValues(resource, "scheduler_injection").Inc()
		}
	}

	responseJSON, err := json.Marshal(admissionReview)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal response: %v", err), http.StatusInternalServerError)
		metrics.WebhookErrorsTotal.WithLabelValues(operation, "marshal_response").Inc()
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(responseJSON); err != nil {
		logging.L.Error("failed to write response", zap.Error(err))
		metrics.WebhookErrorsTotal.WithLabelValues(operation, "write_response").Inc()
	}
}

// IsNovaStorStorageClass checks if the given storage class is a NovaStor storage class.
func IsNovaStorStorageClass(sc *storagev1.StorageClass) bool {
	return sc.Provisioner == NovastorProvisioner
}
