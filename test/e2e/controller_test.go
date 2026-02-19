//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	novastorv1alpha1 "github.com/piwi3910/novastor/api/v1alpha1"
)

func init() {
	// Add NovaStor types to the scheme
	if err := novastorv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		panic(fmt.Sprintf("failed to add NovaStor types to scheme: %v", err))
	}
}

// NewClient creates a new Kubernetes client for testing.
func NewClient() (client.Client, error) {
	var config *rest.Config
	var err error

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s config: %w", err)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	return k8sClient, nil
}

// TestControllerDeployment verifies that the NovaStor controller is deployed
// and functioning correctly by creating test resources and verifying their
// reconciliation.
//
// This test requires a Kubernetes cluster and will be skipped if:
// - KUBECONFIG is not set
// - Not running in-cluster
//
// Note: In CI, a kind cluster is provisioned by the workflow.
func TestControllerDeployment(t *testing.T) {
	k8sClient, err := NewClient()
	if err != nil {
		t.Fatalf("failed to create k8s client: %v", err)
	}

	ctx := context.Background()

	// Create test namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "novastor-e2e-test",
			Labels: map[string]string{
				"novastor.io/test": "true",
			},
		},
	}
	if err := k8sClient.Create(ctx, ns); err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("failed to create test namespace: %v", err)
	}
	defer cleanupNamespace(t, ctx, k8sClient, ns.Name)

	t.Run("StoragePool", func(t *testing.T) {
		testStoragePool(t, ctx, k8sClient)
	})

	t.Run("BlockVolume", func(t *testing.T) {
		testBlockVolume(t, ctx, k8sClient)
	})

	t.Run("SharedFilesystem", func(t *testing.T) {
		testSharedFilesystem(t, ctx, k8sClient)
	})

	t.Run("ObjectStore", func(t *testing.T) {
		testObjectStore(t, ctx, k8sClient)
	})
}

func testStoragePool(t *testing.T, ctx context.Context, k8sClient client.Client) {
	pool := &novastorv1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "e2e-test-pool",
		},
		Spec: novastorv1alpha1.StoragePoolSpec{
			NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"novastor.io/storage-tier": "nvme",
				},
			},
			DataProtection: novastorv1alpha1.DataProtectionSpec{
				Mode: "replication",
				Replication: &novastorv1alpha1.ReplicationSpec{
					Factor:      3,
					WriteQuorum: 2,
				},
			},
		},
	}

	if err := k8sClient.Create(ctx, pool); err != nil {
		t.Fatalf("failed to create StoragePool: %v", err)
	}
	defer k8sClient.Delete(ctx, pool)

	// Wait for pool to become ready
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var fetchedPool novastorv1alpha1.StoragePool
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: pool.Name}, &fetchedPool); err != nil {
			return false, err
		}
		if fetchedPool.Status.Phase == "Ready" {
			t.Logf("StoragePool is ready: nodes=%d, capacity=%s",
				fetchedPool.Status.NodeCount, fetchedPool.Status.TotalCapacity)
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("StoragePool did not become ready: %v", err)
	}
}

func testBlockVolume(t *testing.T, ctx context.Context, k8sClient client.Client) {
	volume := &novastorv1alpha1.BlockVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-test-blockvolume",
			Namespace: "novastor-e2e-test",
		},
		Spec: novastorv1alpha1.BlockVolumeSpec{
			Pool:       "e2e-test-pool",
			Size:       "10Gi",
			AccessMode: "ReadWriteOnce",
		},
	}

	if err := k8sClient.Create(ctx, volume); err != nil {
		t.Fatalf("failed to create BlockVolume: %v", err)
	}
	defer k8sClient.Delete(ctx, volume)

	// Wait for volume to become bound
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var fetchedVolume novastorv1alpha1.BlockVolume
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: volume.Namespace, Name: volume.Name}, &fetchedVolume); err != nil {
			return false, err
		}
		if fetchedVolume.Status.Phase == "Bound" {
			t.Logf("BlockVolume is bound: pool=%s", fetchedVolume.Status.Pool)

			// Verify PV exists
			pvName := fmt.Sprintf("novastor-%s-%s", volume.Namespace, volume.Name)
			var pv corev1.PersistentVolume
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pvName}, &pv); err != nil {
				t.Errorf("PersistentVolume %s not found: %v", pvName, err)
			} else {
				t.Logf("PersistentVolume %s found", pvName)
			}
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("BlockVolume did not become bound: %v", err)
	}
}

func testSharedFilesystem(t *testing.T, ctx context.Context, k8sClient client.Client) {
	fs := &novastorv1alpha1.SharedFilesystem{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-test-filesystem",
			Namespace: "novastor-e2e-test",
		},
		Spec: novastorv1alpha1.SharedFilesystemSpec{
			Pool:       "e2e-test-pool",
			Capacity:   "50Gi",
			AccessMode: "ReadWriteMany",
			Export: &novastorv1alpha1.ExportSpec{
				Protocol: "nfs",
			},
		},
	}

	if err := k8sClient.Create(ctx, fs); err != nil {
		t.Fatalf("failed to create SharedFilesystem: %v", err)
	}
	defer k8sClient.Delete(ctx, fs)

	// Wait for filesystem to become ready
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var fetchedFS novastorv1alpha1.SharedFilesystem
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: fs.Namespace, Name: fs.Name}, &fetchedFS); err != nil {
			return false, err
		}
		if fetchedFS.Status.Phase == "Ready" {
			t.Logf("SharedFilesystem is ready: endpoint=%s", fetchedFS.Status.Endpoint)

			// Verify Deployment and Service exist
			deployName := "novastor-nfs-e2e-test-filesystem"
			var deploy appsv1.Deployment
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: fs.Namespace, Name: deployName}, &deploy); err != nil {
				t.Errorf("NFS Deployment %s not found: %v", deployName, err)
			} else {
				t.Logf("NFS Deployment %s found", deployName)
			}

			svcName := "novastor-nfs-e2e-test-filesystem"
			var svc corev1.Service
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: fs.Namespace, Name: svcName}, &svc); err != nil {
				t.Errorf("NFS Service %s not found: %v", svcName, err)
			} else {
				t.Logf("NFS Service %s found", svcName)
			}
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("SharedFilesystem did not become ready: %v", err)
	}
}

func testObjectStore(t *testing.T, ctx context.Context, k8sClient client.Client) {
	store := &novastorv1alpha1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-test-objectstore",
			Namespace: "novastor-e2e-test",
		},
		Spec: novastorv1alpha1.ObjectStoreSpec{
			Pool: "e2e-test-pool",
			Endpoint: novastorv1alpha1.ObjectEndpointSpec{
				Service: novastorv1alpha1.ObjectServiceSpec{
					Port: 9000,
				},
			},
			BucketPolicy: &novastorv1alpha1.BucketPolicySpec{
				MaxBuckets: 10,
				Versioning: "disabled",
			},
		},
	}

	if err := k8sClient.Create(ctx, store); err != nil {
		t.Fatalf("failed to create ObjectStore: %v", err)
	}
	defer k8sClient.Delete(ctx, store)

	// Wait for object store to become ready
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		var fetchedStore novastorv1alpha1.ObjectStore
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: store.Namespace, Name: store.Name}, &fetchedStore); err != nil {
			return false, err
		}
		if fetchedStore.Status.Phase == "Ready" {
			t.Logf("ObjectStore is ready: endpoint=%s", fetchedStore.Status.Endpoint)

			// Verify Secret, Deployment and Service exist
			secretName := "novastor-s3-e2e-test-objectstore-keys"
			var secret corev1.Secret
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: store.Namespace, Name: secretName}, &secret); err != nil {
				t.Errorf("S3 Secret %s not found: %v", secretName, err)
			} else {
				t.Logf("S3 Secret %s found", secretName)
			}

			deployName := "novastor-s3-e2e-test-objectstore"
			var deploy appsv1.Deployment
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: store.Namespace, Name: deployName}, &deploy); err != nil {
				t.Errorf("S3 Deployment %s not found: %v", deployName, err)
			} else {
				t.Logf("S3 Deployment %s found", deployName)
			}

			svcName := "novastor-s3-e2e-test-objectstore"
			var svc corev1.Service
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: store.Namespace, Name: svcName}, &svc); err != nil {
				t.Errorf("S3 Service %s not found: %v", svcName, err)
			} else {
				t.Logf("S3 Service %s found", svcName)
			}
			return true, nil
		}
		return false, nil
	}); err != nil {
		t.Fatalf("ObjectStore did not become ready: %v", err)
	}
}

func cleanupNamespace(t *testing.T, ctx context.Context, k8sClient client.Client, name string) {
	t.Logf("Cleaning up namespace %s", name)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Delete(ctx, ns); err != nil && !errors.IsNotFound(err) {
		t.Logf("Warning: failed to delete namespace: %v", err)
	}
}
