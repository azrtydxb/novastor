package scheduler

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
)

func TestNewDataLocalityPlugin(t *testing.T) {
	t.Run("creates plugin with valid config", func(t *testing.T) {
		cfg := &Config{
			LocalityWeight:   10,
			MinLocalityScore: 1,
			RWXMode:          RWXBalanced,
		}

		plugin := NewDataLocalityPlugin(cfg)

		if plugin == nil {
			t.Fatal("expected non-nil plugin")
		}
		if plugin.cfg != cfg {
			t.Errorf("expected config %v, got %v", cfg, plugin.cfg)
		}
	})

	t.Run("creates plugin with nil config", func(t *testing.T) {
		plugin := NewDataLocalityPlugin(nil)

		if plugin == nil {
			t.Fatal("expected non-nil plugin")
		}
		if plugin.cfg == nil {
			t.Error("expected non-nil config after default initialization")
		}
	})
}

func TestDataLocalityPlugin_Name(t *testing.T) {
	plugin := NewDataLocalityPlugin(&Config{})

	if name := plugin.Name(); name != PluginName {
		t.Errorf("expected name %s, got %s", PluginName, name)
	}
}

func TestDataLocalityPlugin_Score(t *testing.T) {
	t.Run("returns neutral score for pods without NovaStor PVCs", func(t *testing.T) {
		client := fakeclientset.NewClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
				},
			},
		)
		cfg := DefaultConfig(client)
		plugin := NewDataLocalityPlugin(cfg)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{},
		}

		score, err := plugin.Score(context.Background(), pod, "node1")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if score != ScoreMin/2 {
			t.Errorf("expected score %d, got %d", ScoreMin/2, score)
		}
	})

	t.Run("returns higher score for nodes with local data", func(t *testing.T) {
		client := fakeclientset.NewClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "storage-node-1",
					Labels: map[string]string{
						NovaStorNodeLabel:   "true",
						NovaStorTopologyKey: "node1",
					},
				},
			},
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "compute-node-1",
				},
			},
			&corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pv-1",
				},
				Spec: corev1.PersistentVolumeSpec{
					NodeAffinity: &corev1.VolumeNodeAffinity{
						Required: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      NovaStorTopologyKey,
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"node1"},
										},
									},
								},
							},
						},
					},
					ClaimRef: &corev1.ObjectReference{
						Name:      "pvc-1",
						Namespace: "default",
					},
				},
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-1",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: strPtr("novastor-sc"),
					VolumeName:       "pv-1",
				},
			},
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "novastor-sc",
				},
				Provisioner: NovastorProvisioner,
			},
		)

		cfg := DefaultConfig(client)
		cfg.LocalityWeight = 10
		cfg.MinLocalityScore = 5
		plugin := NewDataLocalityPlugin(cfg)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "pvc-1",
							},
						},
					},
				},
			},
		}

		// Score storage node - should have higher score.
		storageScore, err := plugin.Score(context.Background(), pod, "storage-node-1")
		if err != nil {
			t.Errorf("expected no error for storage node, got %v", err)
		}
		// Expected: ScoreMin (0) + LocalityWeight (10) + MinLocalityScore (5) = 15
		expectedScore := int64(15)
		if storageScore != expectedScore {
			t.Errorf("expected storage node score %d, got %d", expectedScore, storageScore)
		}

		// Score compute node - should have lower score.
		computeScore, err := plugin.Score(context.Background(), pod, "compute-node-1")
		if err != nil {
			t.Errorf("expected no error for compute node, got %v", err)
		}
		// Expected: ScoreMin (0) = 0 (no local data)
		if computeScore != ScoreMin {
			t.Errorf("expected compute node score %d, got %d", ScoreMin, computeScore)
		}
		if computeScore >= storageScore {
			t.Errorf("expected compute node score %d < storage node score %d", computeScore, storageScore)
		}
	})

	t.Run("applies RWX mode correctly", func(t *testing.T) {
		client := fakeclientset.NewClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "storage-node",
					Labels: map[string]string{
						NovaStorNodeLabel: "true",
					},
				},
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rwx-pvc",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: strPtr("novastor-rwx-sc"),
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				},
			},
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "novastor-rwx-sc",
				},
				Provisioner: NovastorProvisioner,
			},
		)

		t.Run("RWXAny gives neutral score", func(t *testing.T) {
			cfg := DefaultConfig(client)
			cfg.RWXMode = RWXAny
			plugin := NewDataLocalityPlugin(cfg)

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "rwx-pvc",
								},
							},
						},
					},
				},
			}

			score, err := plugin.Score(context.Background(), pod, "storage-node")
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			// RWXAny should return ScoreMin/2 (neutral)
			if score != ScoreMin/2 {
				t.Errorf("expected score %d for RWXAny, got %d", ScoreMin/2, score)
			}
		})

		t.Run("RWXBalanced gives moderate score for storage nodes", func(t *testing.T) {
			cfg := DefaultConfig(client)
			cfg.RWXMode = RWXBalanced
			plugin := NewDataLocalityPlugin(cfg)

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "rwx-pvc",
								},
							},
						},
					},
				},
			}

			score, err := plugin.Score(context.Background(), pod, "storage-node")
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			// RWXBalanced: ScoreMax/2 (50) + ScoreMin (0) = 50
			expectedScore := ScoreMax/2 + cfg.MinLocalityScore
			if score != expectedScore {
				t.Errorf("expected score %d for RWXBalanced, got %d", expectedScore, score)
			}
		})
	})

	t.Run("handles unbound PVCs gracefully", func(t *testing.T) {
		client := fakeclientset.NewClientset(
			&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-1",
					Labels: map[string]string{
						NovaStorNodeLabel: "true",
					},
				},
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unbound-pvc",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: strPtr("novastor-sc"),
				},
			},
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "novastor-sc",
				},
				Provisioner: NovastorProvisioner,
			},
		)

		cfg := DefaultConfig(client)
		plugin := NewDataLocalityPlugin(cfg)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: "unbound-pvc",
							},
						},
					},
				},
			},
		}

		// Should not error and should give storage node some score
		score, err := plugin.Score(context.Background(), pod, "node-1")
		if err != nil {
			t.Errorf("expected no error for unbound PVC, got %v", err)
		}
		// Storage nodes get some score even for unbound PVCs due to isStorageNode check
		if score < ScoreMin {
			t.Errorf("expected score >= %d, got %d", ScoreMin, score)
		}
	})
}

func TestDataLocalityPlugin_isStorageNode(t *testing.T) {
	tests := []struct {
		name     string
		node     *corev1.Node
		expected bool
	}{
		{
			name: "storage node with label",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "storage-1",
					Labels: map[string]string{
						NovaStorNodeLabel: "true",
					},
				},
			},
			expected: true,
		},
		{
			name: "compute node without label",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "compute-1",
					Labels: map[string]string{},
				},
			},
			expected: false,
		},
		{
			name: "node with label set to false",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "mixed-1",
					Labels: map[string]string{
						NovaStorNodeLabel: "false",
					},
				},
			},
			expected: true, // presence of label matters, not value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := createTestPlugin()
			result := plugin.isStorageNode(tt.node)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	client := fakeclientset.NewClientset()
	cfg := DefaultConfig(client)

	if cfg.LocalityWeight != 10 {
		t.Errorf("expected LocalityWeight 10, got %d", cfg.LocalityWeight)
	}
	if cfg.MinLocalityScore != 1 {
		t.Errorf("expected MinLocalityScore 1, got %d", cfg.MinLocalityScore)
	}
	if cfg.RWXMode != RWXBalanced {
		t.Errorf("expected RWXMode %d, got %d", RWXBalanced, cfg.RWXMode)
	}
	if cfg.Client != client {
		t.Error("expected client to be set")
	}
}

// Helper functions

func createTestPlugin() *DataLocalityPlugin {
	client := fakeclientset.NewClientset()
	cfg := DefaultConfig(client)
	return NewDataLocalityPlugin(cfg)
}

func strPtr(s string) *string {
	return &s
}
