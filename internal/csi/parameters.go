package csi

import (
	"fmt"
	"strconv"

	"github.com/azrtydxb/novastor/internal/metadata"
)

const (
	// Parameter keys for StorageClass parameters.
	paramProtectionMode = "protection"
	paramDataShards     = "dataShards"
	paramParityShards   = "parityShards"
	paramReplicas       = "replicas"
	paramWriteQuorum    = "writeQuorum"

	// Default values for erasure coding.
	defaultDataShards   = 4
	defaultParityShards = 2

	// Default values for replication.
	defaultReplicationFactor = 3
	defaultWriteQuorum       = 2 // majority of 3
)

// ProtectionParams holds parsed protection parameters from a StorageClass.
type ProtectionParams struct {
	Config *metadata.DataProtectionConfig
}

// ParseProtectionParams extracts and validates protection parameters from
// the CSI CreateVolume request's parameters map (from StorageClass).
func ParseProtectionParams(params map[string]string) (*ProtectionParams, error) {
	if params == nil {
		// No parameters specified - use default replication.
		return &ProtectionParams{
			Config: &metadata.DataProtectionConfig{
				Mode: metadata.ProtectionModeReplication,
				Replication: &metadata.ReplicationConfig{
					Factor:      defaultReplicationFactor,
					WriteQuorum: defaultWriteQuorum,
				},
			},
		}, nil
	}

	mode := params[paramProtectionMode]
	switch mode {
	case "", "replication":
		return parseReplicationParams(params)
	case "erasure-coding", "erasureCoding":
		return parseErasureCodingParams(params)
	default:
		return nil, fmt.Errorf("invalid protection mode %q: must be 'replication' or 'erasure-coding'", mode)
	}
}

// parseReplicationParams parses replication-specific parameters.
func parseReplicationParams(params map[string]string) (*ProtectionParams, error) {
	factor := defaultReplicationFactor
	if f := params[paramReplicas]; f != "" {
		val, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %q: %w", paramReplicas, f, err)
		}
		if val < 1 || val > 5 {
			return nil, fmt.Errorf("%s must be between 1 and 5, got %d", paramReplicas, val)
		}
		factor = val
	}

	// Calculate default write quorum as majority (factor/2 + 1).
	writeQuorum := (factor / 2) + 1
	if wq := params[paramWriteQuorum]; wq != "" {
		val, err := strconv.Atoi(wq)
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %q: %w", paramWriteQuorum, wq, err)
		}
		if val < 1 || val > factor {
			return nil, fmt.Errorf("%s must be between 1 and replicas (%d), got %d", paramWriteQuorum, factor, val)
		}
		writeQuorum = val
	}

	return &ProtectionParams{
		Config: &metadata.DataProtectionConfig{
			Mode: metadata.ProtectionModeReplication,
			Replication: &metadata.ReplicationConfig{
				Factor:      factor,
				WriteQuorum: writeQuorum,
			},
		},
	}, nil
}

// parseErasureCodingParams parses erasure coding specific parameters.
func parseErasureCodingParams(params map[string]string) (*ProtectionParams, error) {
	dataShards := defaultDataShards
	if ds := params[paramDataShards]; ds != "" {
		val, err := strconv.Atoi(ds)
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %q: %w", paramDataShards, ds, err)
		}
		if val < 2 {
			return nil, fmt.Errorf("%s must be at least 2, got %d", paramDataShards, val)
		}
		dataShards = val
	}

	parityShards := defaultParityShards
	if ps := params[paramParityShards]; ps != "" {
		val, err := strconv.Atoi(ps)
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %q: %w", paramParityShards, ps, err)
		}
		if val < 1 {
			return nil, fmt.Errorf("%s must be at least 1, got %d", paramParityShards, val)
		}
		parityShards = val
	}

	return &ProtectionParams{
		Config: &metadata.DataProtectionConfig{
			Mode: metadata.ProtectionModeErasureCoding,
			ErasureCoding: &metadata.ErasureCodingConfig{
				DataShards:   dataShards,
				ParityShards: parityShards,
			},
		},
	}, nil
}

// ShardCountForChunk returns the number of shards a chunk will be encoded into
// based on the protection configuration. For replication, this equals the
// replication factor. For erasure coding, this equals dataShards + parityShards.
func (p *ProtectionParams) ShardCountForChunk() int {
	switch p.Config.Mode {
	case metadata.ProtectionModeErasureCoding:
		if p.Config.ErasureCoding != nil {
			return p.Config.ErasureCoding.DataShards + p.Config.ErasureCoding.ParityShards
		}
		return defaultDataShards + defaultParityShards
	case metadata.ProtectionModeReplication:
		if p.Config.Replication != nil {
			return p.Config.Replication.Factor
		}
		return defaultReplicationFactor
	default:
		return defaultReplicationFactor
	}
}

// IsErasureCoding returns true if the protection mode is erasure coding.
func (p *ProtectionParams) IsErasureCoding() bool {
	return p.Config.Mode == metadata.ProtectionModeErasureCoding
}

// IsReplication returns true if the protection mode is replication.
func (p *ProtectionParams) IsReplication() bool {
	return p.Config.Mode == metadata.ProtectionModeReplication
}

// toProtectionConfig converts ProtectionParams to the controller's internal protectionConfig.
func (p *ProtectionParams) toProtectionConfig() protectionConfig {
	cfg := protectionConfig{
		replicas:     defaultReplicationFactor,
		dataShards:   0,
		parityShards: 0,
	}

	if p.Config == nil {
		return cfg
	}

	switch p.Config.Mode {
	case metadata.ProtectionModeErasureCoding:
		if p.Config.ErasureCoding != nil {
			cfg.dataShards = p.Config.ErasureCoding.DataShards
			cfg.parityShards = p.Config.ErasureCoding.ParityShards
		}
	case metadata.ProtectionModeReplication:
		if p.Config.Replication != nil {
			cfg.replicas = p.Config.Replication.Factor
		}
	}

	return cfg
}
