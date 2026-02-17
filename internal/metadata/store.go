package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type VolumeMeta struct {
	VolumeID  string   `json:"volumeID"`
	Pool      string   `json:"pool"`
	SizeBytes uint64   `json:"sizeBytes"`
	ChunkIDs  []string `json:"chunkIDs"`
}

type PlacementMap struct {
	ChunkID string   `json:"chunkID"`
	Nodes   []string `json:"nodes"`
}

type RaftStore struct {
	raft *raft.Raft
	fsm  *FSM
}

func NewRaftStore(nodeID, dataDir, bindAddr string, bootstrap bool) (*RaftStore, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.SnapshotInterval = 30 * time.Second
	config.SnapshotThreshold = 1024

	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving bind address: %w", err)
	}

	transport, err := raft.NewTCPTransport(addr.String(), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating transport: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}

	logStorePath := filepath.Join(dataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}

	fsm := NewFSM()

	r, err := raft.NewRaft(config, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	if bootstrap {
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(cfg)
	}

	return &RaftStore{raft: r, fsm: fsm}, nil
}

func (s *RaftStore) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *RaftStore) Close() error {
	return s.raft.Shutdown().Error()
}

func (s *RaftStore) apply(op *fsmOp) error {
	data, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshaling operation: %w", err)
	}
	f := s.raft.Apply(data, 5*time.Second)
	if err := f.Error(); err != nil {
		return fmt.Errorf("applying raft log: %w", err)
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok {
			return e
		}
	}
	return nil
}

func (s *RaftStore) PutVolumeMeta(_ context.Context, meta *VolumeMeta) error {
	data, _ := json.Marshal(meta)
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketVolumes, Key: meta.VolumeID, Value: data})
}

func (s *RaftStore) GetVolumeMeta(_ context.Context, volumeID string) (*VolumeMeta, error) {
	data, err := s.fsm.Get(bucketVolumes, volumeID)
	if err != nil {
		return nil, err
	}
	var meta VolumeMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshaling volume meta: %w", err)
	}
	return &meta, nil
}

func (s *RaftStore) DeleteVolumeMeta(_ context.Context, volumeID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketVolumes, Key: volumeID})
}

func (s *RaftStore) PutPlacementMap(_ context.Context, pm *PlacementMap) error {
	data, _ := json.Marshal(pm)
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketPlacements, Key: pm.ChunkID, Value: data})
}

func (s *RaftStore) GetPlacementMap(_ context.Context, chunkID string) (*PlacementMap, error) {
	data, err := s.fsm.Get(bucketPlacements, chunkID)
	if err != nil {
		return nil, err
	}
	var pm PlacementMap
	if err := json.Unmarshal(data, &pm); err != nil {
		return nil, fmt.Errorf("unmarshaling placement map: %w", err)
	}
	return &pm, nil
}
