package chunk

import (
	"fmt"
	"strings"
	"sync"
)

// BackendFactory creates a Store instance from a configuration.
type BackendFactory func(config map[string]string) (Store, error)

// backendRegistry holds registered backend factories.
var (
	backendRegistry = make(map[string]BackendFactory)
	registryMutex   sync.RWMutex
)

// RegisterBackend registers a backend factory for a given backend type.
// Panics if a factory with the same name is already registered.
// This function is typically called in init() functions of backend packages.
func RegisterBackend(name string, factory BackendFactory) {
	registryMutex.Lock()
	defer registryMutex.Unlock()
	if _, exists := backendRegistry[name]; exists {
		panic(fmt.Sprintf("backend %q already registered", name))
	}
	backendRegistry[name] = factory
}

// CreateBackend creates a new Store instance of the specified backend type.
// The config parameter provides backend-specific configuration options.
// Returns an error if the backend type is not registered.
func CreateBackend(backendType string, config map[string]string) (Store, error) {
	registryMutex.RLock()
	factory, exists := backendRegistry[backendType]
	registryMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("backend type %q not registered (available: %v)", backendType, GetRegisteredBackends())
	}

	return factory(config)
}

// GetRegisteredBackends returns a list of all registered backend names.
func GetRegisteredBackends() []string {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	names := make([]string, 0, len(backendRegistry))
	for name := range backendRegistry {
		names = append(names, name)
	}
	return names
}

// init registers built-in backend factories.
func init() {
	// Register the local filesystem backend.
	RegisterBackend("local", func(config map[string]string) (Store, error) {
		dir := config["dir"]
		if dir == "" {
			dir = config["path"]
		}
		if dir == "" {
			return nil, fmt.Errorf("local backend requires 'dir' or 'path' config")
		}
		return NewLocalStore(dir)
	})

	// Register the in-memory backend.
	RegisterBackend("memory", func(config map[string]string) (Store, error) {
		return NewMemoryStore(), nil
	})

	// Register the block device backend (BlueStore-style).
	RegisterBackend("block", func(config map[string]string) (Store, error) {
		devicesStr := config["devices"]
		if devicesStr == "" {
			return nil, fmt.Errorf("block backend requires 'devices' config (comma-separated device paths)")
		}

		metadataDir := config["metadata_dir"]
		if metadataDir == "" {
			metadataDir = config["metadataDir"]
		}

		autoSync := config["auto_sync"] == "true" || config["autoSync"] == "true"

		var devices []string
		for _, d := range strings.Split(devicesStr, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				devices = append(devices, d)
			}
		}

		if len(devices) == 0 {
			return nil, fmt.Errorf("no valid devices specified")
		}

		blockConfig := BlockStoreConfig{
			Devices:     devices,
			MetadataDir: metadataDir,
			AutoSync:    autoSync,
			Logger:      zap.NewNop(),
		}

		return NewBlockStore(blockConfig)
	})
}
