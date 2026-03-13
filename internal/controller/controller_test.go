package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	novastorev1alpha1 "github.com/azrtydxb/novastor/api/v1alpha1"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = novastorev1alpha1.AddToScheme(s)
	return s
}

func TestStoragePoolReconciler_ValidPool(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pool",
		},
		Spec: novastorev1alpha1.StoragePoolSpec{
			NodeSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"storage": "fast"},
			},
			DataProtection: novastorev1alpha1.DataProtectionSpec{
				Mode: "replication",
				Replication: &novastorev1alpha1.ReplicationSpec{
					Factor: 3,
				},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-1",
			Labels: map[string]string{"storage": "fast"},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, node).
		WithStatusSubresource(pool).
		Build()

	reconciler := &StoragePoolReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-pool"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify status was updated.
	var updated novastorev1alpha1.StoragePool
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-pool"}, &updated); err != nil {
		t.Fatalf("failed to get updated pool: %v", err)
	}

	if updated.Status.Phase != "Ready" {
		t.Errorf("expected phase Ready, got %q", updated.Status.Phase)
	}
	if updated.Status.NodeCount != 1 {
		t.Errorf("expected nodeCount 1, got %d", updated.Status.NodeCount)
	}
	if updated.Status.DataProtection != "replication" {
		t.Errorf("expected dataProtection replication, got %q", updated.Status.DataProtection)
	}
	if updated.Status.TotalCapacity == "" || updated.Status.TotalCapacity == "0" {
		t.Errorf("expected non-zero totalCapacity, got %q", updated.Status.TotalCapacity)
	}

	// Verify Ready condition is True.
	found := false
	for _, c := range updated.Status.Conditions {
		if c.Type == "Ready" {
			found = true
			if c.Status != metav1.ConditionTrue {
				t.Errorf("expected Ready condition to be True, got %s", c.Status)
			}
		}
	}
	if !found {
		t.Error("Ready condition not found")
	}
}

func TestStoragePoolReconciler_InvalidSpec(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bad-pool",
		},
		Spec: novastorev1alpha1.StoragePoolSpec{
			DataProtection: novastorev1alpha1.DataProtectionSpec{
				Mode: "", // Invalid: empty mode.
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool).
		WithStatusSubresource(pool).
		Build()

	reconciler := &StoragePoolReconciler{Client: fakeClient}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "bad-pool"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated novastorev1alpha1.StoragePool
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "bad-pool"}, &updated); err != nil {
		t.Fatalf("failed to get updated pool: %v", err)
	}

	if updated.Status.Phase != "Failed" {
		t.Errorf("expected phase Failed, got %q", updated.Status.Phase)
	}
}

func TestBlockVolumeReconciler_CreatesPV(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "block-pool",
		},
		Status: novastorev1alpha1.StoragePoolStatus{
			Phase: "Ready",
		},
	}

	volume := &novastorev1alpha1.BlockVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vol",
			Namespace: "default",
		},
		Spec: novastorev1alpha1.BlockVolumeSpec{
			Pool:       "block-pool",
			Size:       "10Gi",
			AccessMode: "ReadWriteOnce",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, volume).
		WithStatusSubresource(pool, volume).
		Build()

	reconciler := &BlockVolumeReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-vol", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify status.
	var updated novastorev1alpha1.BlockVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-vol", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get updated volume: %v", err)
	}

	if updated.Status.Phase != "Bound" {
		t.Errorf("expected phase Bound, got %q", updated.Status.Phase)
	}
	if updated.Status.Pool != "block-pool" {
		t.Errorf("expected pool block-pool, got %q", updated.Status.Pool)
	}
	if updated.Status.AccessMode != "ReadWriteOnce" {
		t.Errorf("expected accessMode ReadWriteOnce, got %q", updated.Status.AccessMode)
	}

	// Verify PV was created.
	pvName := "novastor-default-test-vol"
	var pv corev1.PersistentVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: pvName}, &pv); err != nil {
		t.Fatalf("PV %q not found: %v", pvName, err)
	}

	if pv.Spec.CSI == nil {
		t.Fatal("expected CSI volume source, got nil")
	}
	if pv.Spec.CSI.Driver != csiDriverName {
		t.Errorf("expected CSI driver %q, got %q", csiDriverName, pv.Spec.CSI.Driver)
	}
	if pv.Spec.CSI.VolumeHandle != "default/test-vol" {
		t.Errorf("expected volume handle default/test-vol, got %q", pv.Spec.CSI.VolumeHandle)
	}

	expectedCapacity := resource.MustParse("10Gi")
	if qty, ok := pv.Spec.Capacity[corev1.ResourceStorage]; !ok || !qty.Equal(expectedCapacity) {
		t.Errorf("expected capacity 10Gi, got %v", pv.Spec.Capacity)
	}
}

func TestBlockVolumeReconciler_PoolNotFound(t *testing.T) {
	scheme := testScheme()

	volume := &novastorev1alpha1.BlockVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan-vol",
			Namespace: "default",
		},
		Spec: novastorev1alpha1.BlockVolumeSpec{
			Pool:       "nonexistent",
			Size:       "5Gi",
			AccessMode: "ReadWriteOnce",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(volume).
		WithStatusSubresource(volume).
		Build()

	reconciler := &BlockVolumeReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "orphan-vol", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue when pool not found")
	}

	var updated novastorev1alpha1.BlockVolume
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "orphan-vol", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get volume: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", updated.Status.Phase)
	}
}

func TestSharedFilesystemReconciler_CreatesDeploymentAndService(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fs-pool",
		},
		Status: novastorev1alpha1.StoragePoolStatus{
			Phase: "Ready",
		},
	}

	fs := &novastorev1alpha1.SharedFilesystem{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-fs",
			Namespace: "default",
			UID:       "test-uid-1234",
		},
		Spec: novastorev1alpha1.SharedFilesystemSpec{
			Pool:       "fs-pool",
			Capacity:   "50Gi",
			AccessMode: "ReadWriteMany",
			Export:     &novastorev1alpha1.ExportSpec{Protocol: "nfs"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, fs).
		WithStatusSubresource(pool, fs).
		Build()

	reconciler := &SharedFilesystemReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-fs", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify status.
	var updated novastorev1alpha1.SharedFilesystem
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-fs", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get updated fs: %v", err)
	}

	if updated.Status.Phase != "Ready" {
		t.Errorf("expected phase Ready, got %q", updated.Status.Phase)
	}
	expectedEndpoint := "novastor-nfs-test-fs.default.svc:2049"
	if updated.Status.Endpoint != expectedEndpoint {
		t.Errorf("expected endpoint %q, got %q", expectedEndpoint, updated.Status.Endpoint)
	}

	// Verify Deployment was created.
	var deploy appsv1.Deployment
	deployKey := types.NamespacedName{Name: "novastor-nfs-test-fs", Namespace: "default"}
	if err := fakeClient.Get(context.Background(), deployKey, &deploy); err != nil {
		t.Fatalf("Deployment not found: %v", err)
	}
	if *deploy.Spec.Replicas != 1 {
		t.Errorf("expected 1 replica, got %d", *deploy.Spec.Replicas)
	}
	if len(deploy.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(deploy.Spec.Template.Spec.Containers))
	}
	expectedImage := "ghcr.io/azrtydxb/novastor/novastor-filer:latest"
	if deploy.Spec.Template.Spec.Containers[0].Image != expectedImage {
		t.Errorf("expected image %q, got %q", expectedImage, deploy.Spec.Template.Spec.Containers[0].Image)
	}

	// Verify Service was created.
	var svc corev1.Service
	svcKey := types.NamespacedName{Name: "novastor-nfs-test-fs", Namespace: "default"}
	if err := fakeClient.Get(context.Background(), svcKey, &svc); err != nil {
		t.Fatalf("Service not found: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("expected ClusterIP service, got %s", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != nfsPort {
		t.Errorf("expected port %d, got %v", nfsPort, svc.Spec.Ports)
	}
}

func TestObjectStoreReconciler_CreatesDeploymentAndService(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "obj-pool",
		},
		Status: novastorev1alpha1.StoragePoolStatus{
			Phase: "Ready",
		},
	}

	store := &novastorev1alpha1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-store",
			Namespace: "default",
			UID:       "test-uid-5678",
		},
		Spec: novastorev1alpha1.ObjectStoreSpec{
			Pool: "obj-pool",
			Endpoint: novastorev1alpha1.ObjectEndpointSpec{
				Service: novastorev1alpha1.ObjectServiceSpec{Port: 9000},
			},
			BucketPolicy: &novastorev1alpha1.BucketPolicySpec{
				MaxBuckets: 100,
				Versioning: "enabled",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, store).
		WithStatusSubresource(pool, store).
		Build()

	reconciler := &ObjectStoreReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-store", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected no requeue, got RequeueAfter=%v", result.RequeueAfter)
	}

	// Verify status.
	var updated novastorev1alpha1.ObjectStore
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-store", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get updated store: %v", err)
	}

	if updated.Status.Phase != "Ready" {
		t.Errorf("expected phase Ready, got %q", updated.Status.Phase)
	}
	expectedEndpoint := "novastor-s3-test-store.default.svc:9000"
	if updated.Status.Endpoint != expectedEndpoint {
		t.Errorf("expected endpoint %q, got %q", expectedEndpoint, updated.Status.Endpoint)
	}

	// Verify Deployment.
	var deploy appsv1.Deployment
	deployKey := types.NamespacedName{Name: "novastor-s3-test-store", Namespace: "default"}
	if err := fakeClient.Get(context.Background(), deployKey, &deploy); err != nil {
		t.Fatalf("Deployment not found: %v", err)
	}
	if *deploy.Spec.Replicas != defaultS3Replicas {
		t.Errorf("expected %d replicas, got %d", defaultS3Replicas, *deploy.Spec.Replicas)
	}

	// Verify Service.
	var svc corev1.Service
	svcKey := types.NamespacedName{Name: "novastor-s3-test-store", Namespace: "default"}
	if err := fakeClient.Get(context.Background(), svcKey, &svc); err != nil {
		t.Fatalf("Service not found: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("expected ClusterIP service, got %s", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 9000 {
		t.Errorf("expected port 9000, got %v", svc.Spec.Ports)
	}

	// Verify Secret with access/secret keys.
	var secret corev1.Secret
	secretKey := types.NamespacedName{Name: "novastor-s3-test-store-keys", Namespace: "default"}
	if err := fakeClient.Get(context.Background(), secretKey, &secret); err != nil {
		t.Fatalf("Secret not found: %v", err)
	}
	if _, ok := secret.Data["access-key"]; !ok {
		t.Error("access-key not found in secret")
	}
	if _, ok := secret.Data["secret-key"]; !ok {
		t.Error("secret-key not found in secret")
	}
	if len(secret.Data["access-key"]) == 0 {
		t.Error("access-key is empty")
	}
	if len(secret.Data["secret-key"]) == 0 {
		t.Error("secret-key is empty")
	}
}

func TestObjectStoreReconciler_PoolNotReady(t *testing.T) {
	scheme := testScheme()

	pool := &novastorev1alpha1.StoragePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pending-pool",
		},
		Status: novastorev1alpha1.StoragePoolStatus{
			Phase: "Pending",
		},
	}

	store := &novastorev1alpha1.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "waiting-store",
			Namespace: "default",
		},
		Spec: novastorev1alpha1.ObjectStoreSpec{
			Pool: "pending-pool",
			Endpoint: novastorev1alpha1.ObjectEndpointSpec{
				Service: novastorev1alpha1.ObjectServiceSpec{Port: 9000},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pool, store).
		WithStatusSubresource(pool, store).
		Build()

	reconciler := &ObjectStoreReconciler{Client: fakeClient}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "waiting-store", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("expected requeue when pool not ready")
	}

	var updated novastorev1alpha1.ObjectStore
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "waiting-store", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get store: %v", err)
	}
	if updated.Status.Phase != "Pending" {
		t.Errorf("expected phase Pending, got %q", updated.Status.Phase)
	}
}

// Suppress unused import warnings for packages used only in type assertions.
var _ client.Client
