package metadata

import (
	"context"
	"encoding/json"
	"fmt"
)

// SnapshotMeta holds the metadata for a volume snapshot persisted in the
// metadata store. Snapshots are metadata-only since chunks are immutable
// (content-addressed), so a snapshot is simply a reference to the source
// volume's chunk list at the moment the snapshot was taken.
type SnapshotMeta struct {
	SnapshotID     string   `json:"snapshotID"`
	SourceVolumeID string   `json:"sourceVolumeID"`
	SizeBytes      uint64   `json:"sizeBytes"`
	ChunkIDs       []string `json:"chunkIDs"`
	CreationTime   int64    `json:"creationTime"` // Unix nanoseconds
	ReadyToUse     bool     `json:"readyToUse"`
}

// PutSnapshot stores snapshot metadata via Raft consensus.
func (s *RaftStore) PutSnapshot(_ context.Context, meta *SnapshotMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling snapshot meta: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketSnapshots, Key: meta.SnapshotID, Value: data})
}

// GetSnapshot retrieves snapshot metadata by snapshot ID.
func (s *RaftStore) GetSnapshot(_ context.Context, snapshotID string) (*SnapshotMeta, error) {
	data, err := s.fsm.Get(bucketSnapshots, snapshotID)
	if err != nil {
		return nil, err
	}
	var meta SnapshotMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling snapshot meta: %w", err)
	}
	return &meta, nil
}

// DeleteSnapshot removes snapshot metadata via Raft consensus.
func (s *RaftStore) DeleteSnapshot(_ context.Context, snapshotID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketSnapshots, Key: snapshotID})
}

// ListSnapshots returns all snapshot metadata entries.
func (s *RaftStore) ListSnapshots(_ context.Context) ([]*SnapshotMeta, error) {
	all, err := s.fsm.GetAll(bucketSnapshots)
	if err != nil {
		return nil, err
	}
	result := make([]*SnapshotMeta, 0, len(all))
	for k, v := range all {
		var meta SnapshotMeta
		if err := json.Unmarshal(v, &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling snapshot meta for key %s: %w", k, err)
		}
		result = append(result, &meta)
	}
	return result, nil
}
