package webhook

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

// newFakeClient creates a fake kubernetes client with the given objects.
// It properly namespaces PVCs in the "default" namespace.
func newFakeClient(namespace string, pvcs []runtime.Object, storageClasses []runtime.Object) kubernetes.Interface {
	// Set namespace for all PVCs
	for _, pvcObj := range pvcs {
		if pvc, ok := pvcObj.(*corev1.PersistentVolumeClaim); ok {
			pvc.Namespace = namespace
		}
	}
	objects := append(pvcs, storageClasses...)
	return fake.NewSimpleClientset(objects...)
}

func TestUsesNovaStorPVCs(t *testing.T) {
	tests := []struct {
		name           string
		pod            *corev1.Pod
		namespace      string
		pvcs           []runtime.Object
		storageClasses []runtime.Object
		want           bool
		wantErr        bool
	}{
		{
			name: "pod with NovaStor PVC",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-replicated"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-replicated",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			want: true,
		},
		{
			name: "pod with non-NovaStor PVC",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "standard-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("standard"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard",
					},
					Provisioner: "kubernetes.io/aws-ebs",
				},
			},
			want: false,
		},
		{
			name: "pod with no PVCs",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "config",
									},
								},
							},
						},
					},
				},
			},
			namespace: "default",
			want:      false,
		},
		{
			name: "pod with multiple PVCs, one NovaStor",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "standard-pvc",
								},
							},
						},
						{
							Name: "cache",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("standard"),
					},
				},
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-erasure"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard",
					},
					Provisioner: "kubernetes.io/aws-ebs",
				},
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-erasure",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			want: true,
		},
		{
			name: "pod with PVC but no storage class",
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "no-sc-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "no-sc-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: nil,
					},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeClient(tt.namespace, tt.pvcs, tt.storageClasses)
			injector := NewSchedulerInjector(client)

			got, err := injector.usesNovaStorPVCs(context.Background(), tt.pod, tt.namespace)
			if (err != nil) != tt.wantErr {
				t.Errorf("usesNovaStorPVCs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("usesNovaStorPVCs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInject(t *testing.T) {
	tests := []struct {
		name              string
		pod               *corev1.Pod
		namespace         string
		pvcs              []runtime.Object
		storageClasses    []runtime.Object
		wantPatch         bool
		skipAnnotation    bool
		existingScheduler string
	}{
		{
			name: "inject scheduler for NovaStor PVC",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-replicated"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-replicated",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			wantPatch: true,
		},
		{
			name: "no injection for non-NovaStor PVC",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "standard-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("standard"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "standard",
					},
					Provisioner: "kubernetes.io/aws-ebs",
				},
			},
			wantPatch: false,
		},
		{
			name: "skip injection when annotation is set",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					Annotations: map[string]string{
						SkipAnnotation: "true",
					},
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-replicated"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-replicated",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			wantPatch:      false,
			skipAnnotation: true,
		},
		{
			name: "don't override existing scheduler",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: corev1.PodSpec{
					SchedulerName: "custom-scheduler",
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-replicated"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-replicated",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			wantPatch:         false,
			existingScheduler: "custom-scheduler",
		},
		{
			name: "inject when existing scheduler is NovaStor scheduler",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
				},
				Spec: corev1.PodSpec{
					SchedulerName: SchedulerName,
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "novastor-pvc",
								},
							},
						},
					},
				},
			},
			namespace: "default",
			pvcs: []runtime.Object{
				&corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-pvc",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: strPtr("novastor-block-replicated"),
					},
				},
			},
			storageClasses: []runtime.Object{
				&storagev1.StorageClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: "novastor-block-replicated",
					},
					Provisioner: NovastorProvisioner,
				},
			},
			wantPatch:         true,
			existingScheduler: SchedulerName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeClient(tt.namespace, tt.pvcs, tt.storageClasses)
			injector := NewSchedulerInjector(client)

			podJSON, err := json.Marshal(tt.pod)
			if err != nil {
				t.Fatalf("failed to marshal pod: %v", err)
			}

			ar := &admissionv1.AdmissionReview{
				Request: &admissionv1.AdmissionRequest{
					UID:       "test-uid",
					Kind:      metav1.GroupVersionKind{Kind: "Pod"},
					Operation: admissionv1.Create,
					Namespace: tt.namespace,
					Object: runtime.RawExtension{
						Raw: podJSON,
					},
				},
			}

			patch, err := injector.Inject(context.Background(), ar)
			if err != nil {
				t.Fatalf("Inject() error = %v", err)
			}

			hasPatch := patch != nil
			if hasPatch != tt.wantPatch {
				t.Errorf("Inject() hasPatch = %v, want %v", hasPatch, tt.wantPatch)
			}

			if tt.wantPatch && patch != nil {
				var patchOps []map[string]interface{}
				if err := json.Unmarshal(patch, &patchOps); err != nil {
					t.Fatalf("failed to unmarshal patch: %v", err)
				}

				if len(patchOps) != 1 {
					t.Errorf("expected 1 patch operation, got %d", len(patchOps))
				}

				if len(patchOps) > 0 {
					op, ok := patchOps[0]["op"].(string)
					if !ok || op != "replace" {
						t.Errorf("expected op 'replace', got %v", patchOps[0]["op"])
					}

					value, ok := patchOps[0]["value"].(string)
					if !ok || value != SchedulerName {
						t.Errorf("expected value '%s', got %v", SchedulerName, patchOps[0]["value"])
					}
				}
			}
		})
	}
}

func TestIsNovaStorStorageClass(t *testing.T) {
	tests := []struct {
		name string
		sc   *storagev1.StorageClass
		want bool
	}{
		{
			name: "NovaStor storage class",
			sc: &storagev1.StorageClass{
				Provisioner: NovastorProvisioner,
			},
			want: true,
		},
		{
			name: "non-NovaStor storage class",
			sc: &storagev1.StorageClass{
				Provisioner: "kubernetes.io/aws-ebs",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNovaStorStorageClass(tt.sc); got != tt.want {
				t.Errorf("IsNovaStorStorageClass() = %v, want %v", got, tt.want)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}
