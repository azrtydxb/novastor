package scheduler

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// NovastorProvisioner is the CSI provisioner name for NovaStor storage classes.
	NovastorProvisioner = "novastor.csi.novastor.io"

	// NovaStorTopologyKey is the topology key used for CSI topology.
	NovaStorTopologyKey = "novastor.io/node"

	// NovaStorNodeLabel is the node label indicating a NovaStor storage node.
	NovaStorNodeLabel = "novastor.io/storage-node"

	// PluginName is the name of the scheduler plugin.
	PluginName = "DataLocality"

	// ScoreMax is the maximum score a node can receive from this plugin.
	ScoreMax int64 = 100

	// ScoreMin is the minimum score a node can receive from this plugin.
	ScoreMin int64 = 0
)

// NodeScorer is the interface for scoring nodes based on data locality.
type NodeScorer interface {
	// ScoreNode scores a node for a pod based on data locality.
	ScoreNode(ctx context.Context, pod *corev1.Pod, node *corev1.Node) (int64, error)
}

// DataLocalityPlugin is a scheduler plugin that prefers nodes holding local
// volume replicas for data locality.
type DataLocalityPlugin struct {
	cfg *Config
}

// NewDataLocalityPlugin creates a new DataLocalityPlugin.
func NewDataLocalityPlugin(cfg *Config) *DataLocalityPlugin {
	if cfg == nil {
		cfg = &Config{}
	}
	return &DataLocalityPlugin{
		cfg: cfg,
	}
}

// Name returns the name of the plugin.
func (p *DataLocalityPlugin) Name() string {
	return PluginName
}

// Score scores nodes based on data locality.
// Nodes that already hold volume data replicas receive higher scores.
func (p *DataLocalityPlugin) Score(ctx context.Context, pod *corev1.Pod, node string) (int64, error) {
	nodeObj, err := p.getNode(ctx, node)
	if err != nil {
		return ScoreMin, fmt.Errorf("getting node %s: %w", node, err)
	}

	// Get NovaStor PVCs for this pod.
	pvcs, err := p.getNovaStorPVCs(ctx, pod)
	if err != nil {
		return ScoreMin, err
	}

	// If no NovaStor PVCs, return neutral score.
	if len(pvcs) == 0 {
		return ScoreMin / 2, nil
	}

	// Score this node based on locality for each PVC.
	totalScore := ScoreMin
	localDataCount := 0

	for _, pvcInfo := range pvcs {
		score, hasLocalData := p.scoreNodeForVolume(ctx, nodeObj, &pvcInfo)
		totalScore += score
		if hasLocalData {
			localDataCount++
		}
	}

	// Apply bonus if node has any local data.
	if localDataCount > 0 && p.cfg.MinLocalityScore > 0 {
		bonus := p.cfg.MinLocalityScore * int64(localDataCount)
		totalScore += bonus
		if totalScore > ScoreMax {
			totalScore = ScoreMax
		}
	}

	// Log scoring decision for debugging.
	zap.L().Debug("scored node for pod",
		zap.String("node", node),
		zap.String("pod", pod.Name),
		zap.Int64("score", totalScore),
		zap.Int("localDataCount", localDataCount),
	)

	return totalScore, nil
}

// scoreNodeForVolume scores a node for a specific volume based on data locality.
func (p *DataLocalityPlugin) scoreNodeForVolume(ctx context.Context, node *corev1.Node, pvcInfo *PVCInfo) (score int64, hasLocalData bool) {
	// For RWX volumes, apply RWX mode handling.
	if pvcInfo.IsRWX {
		switch p.cfg.RWXMode {
		case RWXAny:
			// No preference for RWX volumes.
			return ScoreMin / 2, false
		case RWXBalanced:
			// Give moderate score if node is a storage node.
			if p.isStorageNode(node) {
				return ScoreMax / 2, true
			}
			return ScoreMin / 4, false
		case RWXLocality:
			// Fall through to normal locality scoring.
		}
	}

	// Check if this node is in the volume's topology segment.
	// For NovaStor, the topology segment is stored in the PV's topology labels.
	hasTopology, err := p.hasDataOnNode(ctx, node, pvcInfo)
	if err != nil {
		zap.L().Warn("failed to check node topology",
			zap.String("node", node.Name),
			zap.String("pvc", pvcInfo.PVCName),
			zap.Error(err),
		)
		return ScoreMin, false
	}

	if hasTopology {
		// Node has local data - apply locality weight.
		return p.cfg.LocalityWeight, true
	}

	// Node doesn't have local data.
	return ScoreMin, false
}

// hasDataOnNode checks if the node holds data for the given volume.
// It does this by checking:
// 1. The PV's topology segments for NovaStor topology key
// 2. Whether the node's label matches the topology segment
func (p *DataLocalityPlugin) hasDataOnNode(ctx context.Context, node *corev1.Node, pvcInfo *PVCInfo) (bool, error) {
	// Get the bound PV for this PVC.
	pv, err := p.getBoundPV(ctx, pvcInfo)
	if err != nil {
		return false, fmt.Errorf("getting bound PV: %w", err)
	}
	if pv == nil {
		return false, nil // PVC not yet bound
	}

	// Check NovaStor-specific topology.
	// NovaStor sets topology segments in the PV during provisioning.
	if pv.Spec.NodeAffinity != nil && pv.Spec.NodeAffinity.Required != nil {
		for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
			for _, expr := range term.MatchExpressions {
				if expr.Key == NovaStorTopologyKey {
					// Check if node matches any of the topology segments.
					nodeValue, ok := node.Labels[NovaStorTopologyKey]
					if !ok {
						continue
					}
					for _, value := range expr.Values {
						if value == nodeValue {
							return true, nil
						}
					}
				}
			}
		}
	}

	// Fallback: check if node is a NovaStor storage node and has access.
	// This is used when legacy topology info isn't available.
	if p.isStorageNode(node) {
		// For RWO volumes, only the target node should be considered.
		if !pvcInfo.IsRWX && pv.Annotations != nil {
			if targetNode, ok := pv.Annotations["novastor.io/target-node"]; ok {
				return targetNode == node.Name, nil
			}
		}
		// For RWX or unknown, any storage node is considered.
		return true, nil
	}

	return false, nil
}

// isStorageNode checks if the node is a NovaStor storage node.
func (p *DataLocalityPlugin) isStorageNode(node *corev1.Node) bool {
	if node.Labels == nil {
		return false
	}
	_, ok := node.Labels[NovaStorNodeLabel]
	return ok
}

// getNovaStorPVCs retrieves all NovaStor PVCs used by the pod.
func (p *DataLocalityPlugin) getNovaStorPVCs(ctx context.Context, pod *corev1.Pod) ([]PVCInfo, error) {
	var pvcs []PVCInfo

	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		pvcName := vol.PersistentVolumeClaim.ClaimName
		pvc, err := p.getPVC(ctx, pod.Namespace, pvcName)
		if err != nil {
			if errors.IsNotFound(err) {
				continue // PVC might not exist yet
			}
			return nil, fmt.Errorf("getting PVC %s/%s: %w", pod.Namespace, pvcName, err)
		}

		// Check if PVC uses a NovaStor storage class.
		isNovaStor, isRWX, err := p.isNovaStorPVC(ctx, pvc)
		if err != nil {
			return nil, fmt.Errorf("checking PVC %s/%s: %w", pod.Namespace, pvcName, err)
		}

		if isNovaStor {
			pvcs = append(pvcs, PVCInfo{
				Namespace: pod.Namespace,
				PVCName:   pvcName,
				IsRWX:     isRWX,
			})
		}
	}

	return pvcs, nil
}

// isNovaStorPVC checks if the PVC is a NovaStor PVC and whether it's RWX.
func (p *DataLocalityPlugin) isNovaStorPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, bool, error) {
	if pvc.Spec.StorageClassName == nil {
		return false, false, nil
	}

	scName := *pvc.Spec.StorageClassName
	sc, err := p.getStorageClass(ctx, scName)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, false, nil
		}
		return false, false, err
	}

	if sc.Provisioner != NovastorProvisioner {
		return false, false, nil
	}

	// Check if RWX.
	isRWX := false
	for _, am := range sc.AllowedTopologies {
		for _, exp := range am.MatchLabelExpressions {
			if exp.Key == "novastor.io/access-mode" {
				for _, val := range exp.Values {
					if val == "RWX" {
						isRWX = true
						break
					}
				}
			}
		}
	}

	// Also check PVC access modes.
	if !isRWX {
		for _, am := range pvc.Spec.AccessModes {
			if am == corev1.ReadWriteMany {
				isRWX = true
				break
			}
		}
	}

	return true, isRWX, nil
}

// getNode retrieves a node by name.
func (p *DataLocalityPlugin) getNode(ctx context.Context, name string) (*corev1.Node, error) {
	if p.cfg.NodeLister != nil {
		obj, exists, err := p.cfg.NodeLister.GetByKey(name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.NewNotFound(corev1.Resource("nodes"), name)
		}
		return obj.(*corev1.Node), nil
	}
	return p.cfg.Client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

// getPVC retrieves a PVC by namespace and name.
func (p *DataLocalityPlugin) getPVC(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	if p.cfg.PVCLister != nil {
		key := fmt.Sprintf("%s/%s", namespace, name)
		obj, exists, err := p.cfg.PVCLister.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.NewNotFound(corev1.Resource("persistentvolumeclaims"), name)
		}
		return obj.(*corev1.PersistentVolumeClaim), nil
	}
	return p.cfg.Client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
}

// getBoundPV retrieves the PV bound to the given PVC.
func (p *DataLocalityPlugin) getBoundPV(ctx context.Context, pvcInfo *PVCInfo) (*corev1.PersistentVolume, error) {
	// First get the PVC to find bound PV name.
	pvc, err := p.getPVC(ctx, pvcInfo.Namespace, pvcInfo.PVCName)
	if err != nil {
		return nil, err
	}

	if pvc.Spec.VolumeName == "" {
		return nil, nil // Not yet bound
	}

	if p.cfg.PVLister != nil {
		obj, exists, err := p.cfg.PVLister.GetByKey(pvc.Spec.VolumeName)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.NewNotFound(corev1.Resource("persistentvolumes"), pvc.Spec.VolumeName)
		}
		return obj.(*corev1.PersistentVolume), nil
	}
	return p.cfg.Client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
}

// getStorageClass retrieves a storage class by name.
func (p *DataLocalityPlugin) getStorageClass(ctx context.Context, name string) (*storagev1.StorageClass, error) {
	if p.cfg.SCLister != nil {
		obj, exists, err := p.cfg.SCLister.GetByKey(name)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.NewNotFound(storagev1.Resource("storageclasses"), name)
		}
		return obj.(*storagev1.StorageClass), nil
	}
	return p.cfg.Client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
}

// PVCInfo holds information about a PVC used by a pod.
type PVCInfo struct {
	Namespace string
	PVCName   string
	IsRWX     bool
}
