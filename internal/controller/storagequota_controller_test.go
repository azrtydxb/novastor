package controller

import (
	"context"
	"testing"

	"github.com/piwi3910/novastor/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestQuotaReconcile_Initialization(t *testing.T) {
	// Create a fake client
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	// Create a test quota
	quota := &v1alpha1.StorageQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-quota",
		},
		Spec: v1alpha1.StorageQuotaSpec{
			Scope: v1alpha1.QuotaScope{
				Kind: "Namespace",
				Name: "default",
			},
			Storage: &v1alpha1.StorageQuotaSpecStorage{
				Hard: 100 * 1024 * 1024 * 1024, // 100GB
				Soft: 80 * 1024 * 1024 * 1024, // 80GB
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(quota).WithStatusSubresource(quota).Build()

	// Create reconciler
	r := &QuotaReconciler{
		Client: fakeClient,
	}

	// Reconcile
	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-quota",
			Namespace: "", // Cluster-scoped
		},
	}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Should requeue immediately to sync to metadata service
	if result.Requeue || result.RequeueAfter > 0 {
		// Expected for initial sync
	}

	// Verify status was initialized
	var updated v1alpha1.StorageQuota
	if err := fakeClient.Get(context.Background(), req.NamespacedName, &updated); err != nil {
		t.Fatalf("failed to get updated quota: %v", err)
	}

	if updated.Status.Phase == "" {
		t.Error("status.phase should be initialized")
	}
}

// mockQuotaMetadataClient is a test implementation of MetadataClient.
type mockQuotaMetadataClient struct{}

func (m *mockQuotaMetadataClient) SetQuota(ctx context.Context, scope QuotaScopeSpec, storageHard, storageSoft int64, objectCountHard int64) error {
	// No-op for tests
	return nil
}

func (m *mockQuotaMetadataClient) GetUsage(ctx context.Context, scope QuotaScopeSpec) (storageUsed, objectCountUsed int64, err error) {
	// Return mock usage - 5GB used
	return 5 * 1024 * 1024 * 1024, 100, nil
}
