package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	bucketVolumeOwners = "volume-owners"
	deadThreshold      = 5 * time.Second
)

// VolumeOwnership tracks which agent owns the write path for a volume.
type VolumeOwnership struct {
	VolumeID   string `json:"volume_id"`
	OwnerAddr  string `json:"owner_addr"`
	OwnerSince int64  `json:"owner_since"`
	Generation uint64 `json:"generation"`
}

// SetVolumeOwner stores or updates volume ownership.
func (s *RaftStore) SetVolumeOwner(ownership *VolumeOwnership) error {
	data, err := json.Marshal(ownership)
	if err != nil {
		return fmt.Errorf("marshal ownership: %w", err)
	}
	return s.apply(&fsmOp{
		Op:     opPut,
		Bucket: bucketVolumeOwners,
		Key:    ownership.VolumeID,
		Value:  data,
	})
}

// GetVolumeOwner retrieves volume ownership by volume ID.
func (s *RaftStore) GetVolumeOwner(volumeID string) (*VolumeOwnership, error) {
	data, err := s.fsm.Get(bucketVolumeOwners, volumeID)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) || errors.Is(err, ErrBucketNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get volume owner: %w", err)
	}
	if data == nil {
		return nil, nil
	}
	var ownership VolumeOwnership
	if err := json.Unmarshal(data, &ownership); err != nil {
		return nil, fmt.Errorf("unmarshal ownership: %w", err)
	}
	return &ownership, nil
}

// RequestOwnership attempts to claim volume ownership. It succeeds only if the
// current owner is dead (stale heartbeat or offline status).
func (s *RaftStore) RequestOwnership(volumeID, requesterAddr string) (granted bool, generation uint64, err error) {
	current, err := s.GetVolumeOwner(volumeID)
	if err != nil {
		return false, 0, fmt.Errorf("get current owner: %w", err)
	}

	// If there's a current owner, check if it's dead
	if current != nil && current.OwnerAddr != "" {
		ownerNode, err := s.findNodeByAddr(current.OwnerAddr)
		if err == nil && ownerNode != nil {
			lastHB := time.Unix(ownerNode.LastHeartbeat, 0)
			if time.Since(lastHB) < deadThreshold && ownerNode.Status != "offline" {
				return false, current.Generation, nil
			}
		}
	}

	// Grant ownership
	newGen := uint64(1)
	if current != nil {
		newGen = current.Generation + 1
	}

	ownership := &VolumeOwnership{
		VolumeID:   volumeID,
		OwnerAddr:  requesterAddr,
		OwnerSince: time.Now().Unix(),
		Generation: newGen,
	}
	if err := s.SetVolumeOwner(ownership); err != nil {
		return false, 0, fmt.Errorf("set new owner: %w", err)
	}
	return true, newGen, nil
}

// findNodeByAddr looks up a node by its address field.
func (s *RaftStore) findNodeByAddr(addr string) (*NodeMeta, error) {
	nodes, err := s.ListNodeMetas(context.Background())
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		if n.Address == addr {
			return n, nil
		}
	}
	return nil, nil
}
