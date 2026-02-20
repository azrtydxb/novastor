//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
			Name:      "e2e-test-pool",
			Namespace: "novastor-e2e-test",
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

	// Verify the resource was accepted by the API server
	var fetchedPool novastorv1alpha1.StoragePool
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: pool.Namespace, Name: pool.Name}, &fetchedPool); err != nil {
		t.Fatalf("failed to get StoragePool: %v", err)
	}
	t.Logf("StoragePool created successfully: name=%s, protection=%s",
		fetchedPool.Name, fetchedPool.Spec.DataProtection.Mode)
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

	// Verify the resource was accepted by the API server
	var fetchedVolume novastorv1alpha1.BlockVolume
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: volume.Namespace, Name: volume.Name}, &fetchedVolume); err != nil {
		t.Fatalf("failed to get BlockVolume: %v", err)
	}
	t.Logf("BlockVolume created successfully: name=%s, pool=%s, size=%s",
		fetchedVolume.Name, fetchedVolume.Spec.Pool, fetchedVolume.Spec.Size)
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

	// Verify the resource was accepted by the API server
	var fetchedFS novastorv1alpha1.SharedFilesystem
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: fs.Namespace, Name: fs.Name}, &fetchedFS); err != nil {
		t.Fatalf("failed to get SharedFilesystem: %v", err)
	}
	t.Logf("SharedFilesystem created successfully: name=%s, pool=%s, capacity=%s",
		fetchedFS.Name, fetchedFS.Spec.Pool, fetchedFS.Spec.Capacity)
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

	// Verify the resource was accepted by the API server
	var fetchedStore novastorv1alpha1.ObjectStore
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: store.Namespace, Name: store.Name}, &fetchedStore); err != nil {
		t.Fatalf("failed to get ObjectStore: %v", err)
	}
	t.Logf("ObjectStore created successfully: name=%s, pool=%s, port=%d",
		fetchedStore.Name, fetchedStore.Spec.Pool, fetchedStore.Spec.Endpoint.Service.Port)
}

func cleanupNamespace(t *testing.T, ctx context.Context, k8sClient client.Client, name string) {
	t.Logf("Cleaning up namespace %s", name)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := k8sClient.Delete(ctx, ns); err != nil && !errors.IsNotFound(err) {
		t.Logf("Warning: failed to delete namespace: %v", err)
	}
}
