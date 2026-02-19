// Package scheduler provides data-locality aware scheduling for NovaStor.
// This package implements the configuration for the Kubernetes scheduler
// plugin that prefers nodes with locally available data.
package scheduler

import (
	"k8s.io/client-go/kubernetes"
	cache "k8s.io/client-go/tools/cache"
)

// RWXMode defines how RWX (ReadWriteMany) volumes are handled for locality scoring.
type RWXMode int

const (
	// RWXLocality prefers nodes with local data even for RWX volumes.
	RWXLocality RWXMode = iota
	// RWXBalanced distributes RWX pods across nodes that have replicas.
	RWXBalanced
	// RWXAny does not apply locality scoring for RWX volumes.
	RWXAny
)

// Config holds the configuration for the data locality scheduler plugin.
type Config struct {
	// LocalityWeight is the weight added to a node's score if it holds local data.
	// Higher values mean data locality is preferred more strongly.
	LocalityWeight int64

	// MinLocalityScore is the minimum score a node gets if it has any local data.
	// This ensures that nodes with data are always preferred over nodes without.
	MinLocalityScore int64

	// RWXMode defines how RWX volumes are handled for locality scoring.
	RWXMode RWXMode

	// Client is the Kubernetes client used for API calls.
	Client kubernetes.Interface

	// PodLister is the shared informer indexer for Pods.
	PodLister cache.Indexer

	// PVLister is the shared informer indexer for PersistentVolumes.
	PVLister cache.Indexer

	// PVCLister is the shared informer indexer for PersistentVolumeClaims.
	PVCLister cache.Indexer

	// NodeLister is the shared informer indexer for Nodes.
	NodeLister cache.Indexer

	// SCLister is the shared informer indexer for StorageClasses.
	SCLister cache.Indexer
}

// DefaultConfig returns a config with sensible defaults.
func DefaultConfig(client kubernetes.Interface) *Config {
	return &Config{
		LocalityWeight:   10,
		MinLocalityScore: 1,
		RWXMode:          RWXBalanced,
		Client:           client,
	}
}
