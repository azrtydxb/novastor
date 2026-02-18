package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type VolumeMeta struct {
	VolumeID  string   `json:"volumeID"`
	Pool      string   `json:"pool"`
	SizeBytes uint64   `json:"sizeBytes"`
	ChunkIDs  []string `json:"chunkIDs"`

	// NVMe-oF target fields populated by the CSI controller after target creation.
	TargetNodeID  string `json:"targetNodeID,omitempty"`
	TargetAddress string `json:"targetAddress,omitempty"`
	TargetPort    string `json:"targetPort,omitempty"`
	SubsystemNQN  string `json:"subsystemNQN,omitempty"`
}

type PlacementMap struct {
	ChunkID string   `json:"chunkID"`
	Nodes   []string `json:"nodes"`
}

type RaftStore struct {
	raft *raft.Raft
	fsm  MetadataFSM
}

// RaftConfig holds all configuration needed to create a RaftStore.
type RaftConfig struct {
	// NodeID is the unique identifier for this Raft node.
	NodeID string
	// DataDir is the directory where Raft log and snapshot data is persisted.
	DataDir string
	// RaftAddr is the TCP address this node listens on for Raft consensus traffic (e.g. ":7000").
	RaftAddr string
	// JoinAddrs is a comma-separated list of existing Raft peer addresses to join.
	// When empty, the node bootstraps as a single-node cluster.
	JoinAddrs string
	// BootstrapExpect is the number of nodes expected to form the initial cluster.
	// When > 0 and no existing Raft state exists, the first node to start will
	// bootstrap and others will join. When 0, uses legacy behavior.
	BootstrapExpect int
	// Backend selects the FSM storage backend. Valid values are "memory" and
	// "badger". When empty, defaults to "badger" for persistent storage.
	Backend string
}

// NewRaftStore creates a Raft-backed metadata store.
//
// If cfg.JoinAddrs is empty the node bootstraps a single-node cluster (the
// original behaviour preserved for backwards compatibility).  When cfg.JoinAddrs
// contains one or more comma-separated peer addresses the node skips bootstrap
// and instead dials each peer in turn until one accepts the AddVoter RPC,
// joining the existing cluster.
func NewRaftStore(cfg RaftConfig) (*RaftStore, error) {
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotInterval = 30 * time.Second
	raftCfg.SnapshotThreshold = 1024

	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolving bind address: %w", err)
	}

	// Raft requires an advertisable address; 0.0.0.0 is not valid.
	// When bound to all interfaces, find a non-loopback IP from network interfaces.
	advertise := addr
	if addr.IP.IsUnspecified() {
		if ifaces, ifErr := net.InterfaceAddrs(); ifErr == nil {
			for _, a := range ifaces {
				if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					advertise = &net.TCPAddr{IP: ipNet.IP, Port: addr.Port}
					break
				}
			}
		}
		if advertise.IP.IsUnspecified() {
			return nil, fmt.Errorf("resolving advertise address: no non-loopback IPv4 address found")
		}
	}

	transport, err := raft.NewTCPTransport(addr.String(), advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating transport: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("creating snapshot store: %w", err)
	}

	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.NewBoltStore(logStorePath)
	if err != nil {
		return nil, fmt.Errorf("creating log store: %w", err)
	}

	var fsm MetadataFSM
	switch cfg.Backend {
	case "memory":
		fsm = NewFSM()
	case "badger", "":
		badgerDir := filepath.Join(cfg.DataDir, "badger")
		if err := os.MkdirAll(badgerDir, 0o750); err != nil {
			return nil, fmt.Errorf("creating badger data dir: %w", err)
		}
		badgerFSM, err := NewBadgerFSM(badgerDir)
		if err != nil {
			return nil, fmt.Errorf("creating badger fsm: %w", err)
		}
		fsm = badgerFSM
	default:
		return nil, fmt.Errorf("unknown backend %q: valid values are \"memory\" and \"badger\"", cfg.Backend)
	}

	r, err := raft.NewRaft(raftCfg, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("creating raft: %w", err)
	}

	store := &RaftStore{raft: r, fsm: fsm}

	if cfg.JoinAddrs == "" {
		// Bootstrap as a single-node cluster.
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(bootstrapCfg)
		return store, nil
	}

	// Join an existing cluster by calling AddVoter on each peer until one succeeds.
	// Filter out our own address from the peer list to avoid self-join deadlock.
	peers := splitAndTrim(cfg.JoinAddrs)
	var filteredPeers []string
	ownAddr := advertise.String()
	for _, p := range peers {
		// Resolve the peer address to see if it's us.
		resolved, resolveErr := net.ResolveTCPAddr("tcp", p)
		if resolveErr != nil || resolved.String() != ownAddr {
			filteredPeers = append(filteredPeers, p)
		}
	}

	if len(filteredPeers) == 0 {
		// All join addresses pointed at ourselves — bootstrap.
		bootstrapCfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		}
		r.BootstrapCluster(bootstrapCfg)
		return store, nil
	}

	maxJoinAttempts := 30
	if cfg.BootstrapExpect > 0 {
		maxJoinAttempts = 5 // Reduced: will fall back to bootstrap.
	}
	if err := store.joinCluster(cfg.NodeID, ownAddr, filteredPeers, maxJoinAttempts); err != nil {
		if cfg.BootstrapExpect > 0 {
			bootstrapCfg := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(cfg.NodeID),
						Address: transport.LocalAddr(),
					},
				},
			}
			if future := r.BootstrapCluster(bootstrapCfg); future.Error() != nil {
				return nil, fmt.Errorf("bootstrap after failed join: %w", future.Error())
			}
			return store, nil
		}
		return nil, fmt.Errorf("joining raft cluster: %w", err)
	}

	return store, nil
}

// joinCluster attempts to add this node as a voter to an existing Raft cluster
// by contacting each of the provided peer addresses in turn.  It retries with
// a small back-off to tolerate a brief window where no leader is available
// (e.g. immediately after all peers start simultaneously).
func (s *RaftStore) joinCluster(nodeID, raftAddr string, peers []string, maxAttempts int) error {
	const retryDelay = 2 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Check if we might already be a member of the cluster.
		configFuture := s.raft.GetConfiguration()
		if err := configFuture.Error(); err == nil {
			for _, srv := range configFuture.Configuration().Servers {
				if string(srv.ID) == nodeID {
					// Already a member – nothing more to do.
					return nil
				}
			}
		}

		// Attempt to add this node as a voter.  AddVoter must be called on the
		// leader; if this node is not yet the leader the future will return
		// ErrNotLeader and we retry after a delay.
		addFuture := s.raft.AddVoter(
			raft.ServerID(nodeID),
			raft.ServerAddress(raftAddr),
			0, // prevIndex: 0 means append unconditionally
			5*time.Second,
		)
		if err := addFuture.Error(); err == nil {
			return nil
		}

		time.Sleep(retryDelay)
	}

	return fmt.Errorf("failed to join cluster after %d attempts via peers %v", maxAttempts, peers)
}

func (s *RaftStore) IsLeader() bool {
	return s.raft.State() == raft.Leader
}

func (s *RaftStore) Close() error {
	raftErr := s.raft.Shutdown().Error()
	fsmErr := s.fsm.Close()
	if raftErr != nil {
		return raftErr
	}
	return fsmErr
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

func (s *RaftStore) ListVolumesMeta(_ context.Context) ([]*VolumeMeta, error) {
	all, err := s.fsm.GetAll(bucketVolumes)
	if err != nil {
		return nil, fmt.Errorf("listing volumes: %w", err)
	}
	result := make([]*VolumeMeta, 0, len(all))
	for _, data := range all {
		var meta VolumeMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling volume meta: %w", err)
		}
		result = append(result, &meta)
	}
	return result, nil
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

// ListPlacementMaps returns all placement map entries.
func (s *RaftStore) ListPlacementMaps(_ context.Context) ([]*PlacementMap, error) {
	all, err := s.fsm.GetAll(bucketPlacements)
	if err != nil {
		return nil, fmt.Errorf("listing placement maps: %w", err)
	}
	result := make([]*PlacementMap, 0, len(all))
	for _, data := range all {
		var pm PlacementMap
		if err := json.Unmarshal(data, &pm); err != nil {
			return nil, fmt.Errorf("unmarshaling placement map: %w", err)
		}
		result = append(result, &pm)
	}
	return result, nil
}

// splitAndTrim splits a comma-separated list of addresses and trims whitespace.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
