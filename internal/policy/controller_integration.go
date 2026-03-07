package policy

import (
	"context"
	"fmt"

	"github.com/azrtydxb/novastor/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// K8sPoolLookup implements PoolLookup using the Kubernetes API.
type K8sPoolLookup struct {
	client client.Client
}

// NewK8sPoolLookup creates a new K8sPoolLookup.
func NewK8sPoolLookup(client client.Client) *K8sPoolLookup {
	return &K8sPoolLookup{client: client}
}

// ListPools retrieves all StoragePool resources from Kubernetes.
func (l *K8sPoolLookup) ListPools(ctx context.Context) ([]*v1alpha1.StoragePool, error) {
	var poolList v1alpha1.StoragePoolList
	if err := l.client.List(ctx, &poolList); err != nil {
		return nil, fmt.Errorf("listing storage pools: %w", err)
	}

	pools := make([]*v1alpha1.StoragePool, len(poolList.Items))
	for i := range poolList.Items {
		pools[i] = &poolList.Items[i]
	}
	return pools, nil
}

// GetPool retrieves a single StoragePool by name from Kubernetes.
func (l *K8sPoolLookup) GetPool(ctx context.Context, name string) (*v1alpha1.StoragePool, error) {
	var pool v1alpha1.StoragePool
	if err := l.client.Get(ctx, client.ObjectKey{Name: name}, &pool); err != nil {
		return nil, fmt.Errorf("getting storage pool %s: %w", name, err)
	}
	return &pool, nil
}

// K8sNodeAvailabilityChecker implements NodeAvailabilityChecker using the Kubernetes API.
// It checks if a node is Ready according to Kubernetes.
type K8sNodeAvailabilityChecker struct {
	client client.Client
}

// NewK8sNodeAvailabilityChecker creates a new K8sNodeAvailabilityChecker.
func NewK8sNodeAvailabilityChecker(client client.Client) *K8sNodeAvailabilityChecker {
	return &K8sNodeAvailabilityChecker{client: client}
}

// IsNodeAvailable checks if a Kubernetes node is Ready.
func (c *K8sNodeAvailabilityChecker) IsNodeAvailable(ctx context.Context, nodeID string) bool {
	// Check if there's a Node resource matching the nodeID
	var node corev1.Node
	if err := c.client.Get(ctx, client.ObjectKey{Name: nodeID}, &node); err != nil {
		return false
	}

	// Check the Ready condition
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}

	return false
}
