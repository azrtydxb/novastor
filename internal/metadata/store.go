package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/piwi3910/novastor/internal/metrics"
)

// Type aliases for backward compatibility and code clarity.
// These types are fully defined in protection.go.
type VolumeProtectionMode = ProtectionMode

// DataProtectionConfig is an alias for ProtectionProfile for code clarity.
// In storage context, we talk about "data protection configuration"
// while in metadata context we use "protection profile".
type DataProtectionConfig = ProtectionProfile

// ReplicationConfig is an alias for ReplicationProfile.
type ReplicationConfig = ReplicationProfile

// ErasureCodingConfig is an alias for ErasureCodingProfile.
type ErasureCodingConfig = ErasureCodingProfile

// VolumeMeta stores metadata about a provisioned volume.
type VolumeMeta struct {
	VolumeID  string   `json:"volumeID"`
	Pool      string   `json:"pool"`
	SizeBytes uint64   `json:"sizeBytes"`
	ChunkIDs  []string `json:"chunkIDs"`

	// DataProtection specifies how the volume's data is protected.
	DataProtection *DataProtectionConfig `json:"dataProtection,omitempty"`

	// NVMe-oF target fields populated by the CSI controller after target creation.
	TargetNodeID  string `json:"targetNodeID,omitempty"`
	TargetAddress string `json:"targetAddress,omitempty"`
	TargetPort    string `json:"targetPort,omitempty"`
	SubsystemNQN  string `json:"subsystemNQN,omitempty"`

	// ProtectionProfile specifies the data protection settings for this volume.
	ProtectionProfile *ProtectionProfile `json:"protectionProfile,omitempty"`

	// ComplianceInfo tracks the current compliance state of this volume.
	ComplianceInfo *ComplianceInfo `json:"complianceInfo,omitempty"`
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
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling volume meta: %w", err)
	}
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
	data, err := json.Marshal(pm)
	if err != nil {
		return fmt.Errorf("marshaling placement map: %w", err)
	}
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

// DeletePlacementMap removes a placement map entry.
func (s *RaftStore) DeletePlacementMap(_ context.Context, chunkID string) error {
	return s.apply(&fsmOp{Op: opDelete, Bucket: bucketPlacements, Key: chunkID})
}

// ---- Lock lease operations ----

// AcquireLock attempts to acquire a file lock lease. If successful, returns the lease ID.
// If a conflicting lock exists, returns an error with the conflicting owner.
func (s *RaftStore) AcquireLock(ctx context.Context, args *AcquireLockArgs) (*AcquireLockResult, error) {
	// First check for conflicts by reading existing locks for this inode.
	locks, err := s.getLocksForInode(ctx, args.Ino)
	if err != nil {
		return nil, fmt.Errorf("checking existing locks: %w", err)
	}

	// Filter out expired locks and check for conflicts.
	candidateLock := &LockLease{
		Owner:     args.Owner,
		VolumeID:  args.VolumeID,
		Ino:       args.Ino,
		Start:     args.Start,
		End:       args.End,
		Type:      args.Type,
		ExpiresAt: time.Now().Add(args.TTL).UnixNano(),
		FilerID:   args.FilerID,
	}

	for _, existing := range locks {
		if existing.IsExpired() {
			// Clean up expired lock asynchronously.
			go s.ReleaseLock(context.Background(), &ReleaseLockArgs{
				LeaseID: existing.LeaseID,
				Owner:   existing.Owner,
			})
			continue
		}
		if conflicts(candidateLock, existing) {
			return &AcquireLockResult{
					ConflictingOwner: existing.Owner,
				}, fmt.Errorf("lock conflict: owner %q holds conflicting %s lock on inode %d [%d,%d)",
					existing.Owner, existing.Type, existing.Ino, existing.Start, existing.End)
		}
	}

	// No conflicts, create the lease.
	leaseID := GenerateLeaseID()
	candidateLock.LeaseID = leaseID

	// Store the lease.
	leaseData, err := json.Marshal(candidateLock)
	if err != nil {
		return nil, fmt.Errorf("marshaling lease: %w", err)
	}
	if err := s.apply(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockKey(leaseID), Value: leaseData}); err != nil {
		return nil, fmt.Errorf("storing lease: %w", err)
	}

	// Update the index.
	if err := s.addLockToIndex(ctx, args.Ino, leaseID); err != nil {
		// Best effort cleanup on index update failure.
		_ = s.apply(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockKey(leaseID)})
		return nil, fmt.Errorf("updating lock index: %w", err)
	}

	return &AcquireLockResult{
		LeaseID:   leaseID,
		ExpiresAt: candidateLock.ExpiresAt,
	}, nil
}

// RenewLock extends the expiration time of an existing lock lease.
func (s *RaftStore) RenewLock(_ context.Context, args *RenewLockArgs) (*LockLease, error) {
	leaseData, err := s.fsm.Get(bucketLocks, lockKey(args.LeaseID))
	if err != nil {
		return nil, fmt.Errorf("lease not found: %w", err)
	}

	var lease LockLease
	if err := json.Unmarshal(leaseData, &lease); err != nil {
		return nil, fmt.Errorf("unmarshaling lease: %w", err)
	}

	if lease.IsExpired() {
		return nil, fmt.Errorf("cannot renew expired lease %s", args.LeaseID)
	}

	// Update expiration.
	lease.ExpiresAt = time.Now().Add(args.TTL).UnixNano()

	newLeaseData, err := json.Marshal(lease)
	if err != nil {
		return nil, fmt.Errorf("marshaling lease: %w", err)
	}
	if err := s.apply(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockKey(args.LeaseID), Value: newLeaseData}); err != nil {
		return nil, fmt.Errorf("storing renewed lease: %w", err)
	}

	return &lease, nil
}

// ReleaseLock releases a lock lease, removing it from the metadata store.
func (s *RaftStore) ReleaseLock(ctx context.Context, args *ReleaseLockArgs) error {
	// Verify the lease exists and belongs to the owner (if specified).
	leaseData, err := s.fsm.Get(bucketLocks, lockKey(args.LeaseID))
	if err != nil {
		return fmt.Errorf("lease not found: %w", err)
	}

	var lease LockLease
	if err := json.Unmarshal(leaseData, &lease); err != nil {
		return fmt.Errorf("unmarshaling lease: %w", err)
	}

	if args.Owner != "" && lease.Owner != args.Owner {
		return fmt.Errorf("lease owner mismatch: expected %q, got %q", lease.Owner, args.Owner)
	}

	// Remove the lease.
	if err := s.apply(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockKey(args.LeaseID)}); err != nil {
		return fmt.Errorf("deleting lease: %w", err)
	}

	// Remove from index.
	if err := s.removeLockFromIndex(ctx, lease.Ino, args.LeaseID); err != nil {
		// Log but don't fail - stale index entries are cleaned up during reads.
		return fmt.Errorf("removing from index: %w", err)
	}

	return nil
}

// TestLock checks if a lock could be acquired without actually acquiring it.
// Returns nil if no conflict exists, or the conflicting lock if one does.
func (s *RaftStore) TestLock(ctx context.Context, args *TestLockArgs) (*LockLease, error) {
	locks, err := s.getLocksForInode(ctx, args.Ino)
	if err != nil {
		return nil, fmt.Errorf("checking existing locks: %w", err)
	}

	candidateLock := &LockLease{
		VolumeID: args.VolumeID,
		Ino:      args.Ino,
		Start:    args.Start,
		End:      args.End,
		Type:     args.Type,
	}

	for _, existing := range locks {
		if existing.IsExpired() {
			continue
		}
		if conflicts(candidateLock, existing) {
			return existing, nil
		}
	}

	return nil, nil
}

// GetLock retrieves a lock lease by ID.
func (s *RaftStore) GetLock(_ context.Context, leaseID string) (*LockLease, error) {
	leaseData, err := s.fsm.Get(bucketLocks, lockKey(leaseID))
	if err != nil {
		return nil, fmt.Errorf("lease not found: %w", err)
	}

	var lease LockLease
	if err := json.Unmarshal(leaseData, &lease); err != nil {
		return nil, fmt.Errorf("unmarshaling lease: %w", err)
	}

	return &lease, nil
}

// ListLocks returns all active (non-expired) locks, optionally filtered by volume ID.
func (s *RaftStore) ListLocks(_ context.Context, volumeID string) ([]*LockLease, error) {
	all, err := s.fsm.GetAll(bucketLocks)
	if err != nil {
		return nil, fmt.Errorf("listing locks: %w", err)
	}

	var result []*LockLease
	now := time.Now().UnixNano()

	for _, data := range all {
		var lease LockLease
		if err := json.Unmarshal(data, &lease); err != nil {
			continue // Skip corrupted entries.
		}
		if lease.ExpiresAt < now {
			continue // Skip expired leases.
		}
		if volumeID != "" && lease.VolumeID != volumeID {
			continue // Filter by volume.
		}
		result = append(result, &lease)
	}

	return result, nil
}

// CleanupExpiredLocks removes expired lock leases. Should be called periodically.
func (s *RaftStore) CleanupExpiredLocks(ctx context.Context) (int, error) {
	all, err := s.fsm.GetAll(bucketLocks)
	if err != nil {
		return 0, fmt.Errorf("listing locks: %w", err)
	}

	now := time.Now().UnixNano()
	cleaned := 0

	for key, data := range all {
		var lease LockLease
		if err := json.Unmarshal(data, &lease); err != nil {
			continue
		}
		if lease.ExpiresAt < now {
			// Remove expired lease.
			if err := s.apply(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: key}); err == nil {
				_ = s.removeLockFromIndex(ctx, lease.Ino, lease.LeaseID)
				cleaned++
			}
		}
	}

	return cleaned, nil
}

// getLocksForInode retrieves all locks for a given inode.
// Returns nil, nil if no locks exist for this inode (not found is OK).
func (s *RaftStore) getLocksForInode(_ context.Context, ino uint64) ([]*LockLease, error) {
	// Get the index for this inode.
	indexData, err := s.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		// No locks for this inode yet - not found is acceptable.
		if errors.Is(err, ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("getting lock index: %w", err)
	}

	var index lockIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return nil, fmt.Errorf("unmarshaling lock index: %w", err)
	}

	var locks []*LockLease
	for _, leaseID := range index.LeaseIDs {
		leaseData, err := s.fsm.Get(bucketLocks, lockKey(leaseID))
		if err != nil {
			// Lease may have been deleted, skip.
			continue
		}
		var lease LockLease
		if err := json.Unmarshal(leaseData, &lease); err != nil {
			continue
		}
		locks = append(locks, &lease)
	}

	return locks, nil
}

// addLockToIndex adds a lease ID to the inode's lock index.
func (s *RaftStore) addLockToIndex(_ context.Context, ino uint64, leaseID string) error {
	indexData, err := s.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		// No index yet, create a new one.
		index := lockIndex{
			Ino:      ino,
			LeaseIDs: []string{leaseID},
		}
		data, err := json.Marshal(index)
		if err != nil {
			return fmt.Errorf("marshaling new index: %w", err)
		}
		return s.apply(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
	}

	var index lockIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return fmt.Errorf("unmarshaling index: %w", err)
	}

	// Check for duplicates.
	for _, existingID := range index.LeaseIDs {
		if existingID == leaseID {
			return nil // Already in index.
		}
	}

	index.LeaseIDs = append(index.LeaseIDs, leaseID)
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshaling updated index: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
}

// removeLockFromIndex removes a lease ID from the inode's lock index.
func (s *RaftStore) removeLockFromIndex(_ context.Context, ino uint64, leaseID string) error {
	indexData, err := s.fsm.Get(bucketLocks, lockIndexKey(ino))
	if err != nil {
		// Index doesn't exist, nothing to do - not found is acceptable.
		if errors.Is(err, ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("getting lock index: %w", err)
	}

	var index lockIndex
	if err := json.Unmarshal(indexData, &index); err != nil {
		return fmt.Errorf("unmarshaling index: %w", err)
	}

	// Filter out the lease ID.
	newLeaseIDs := make([]string, 0, len(index.LeaseIDs))
	found := false
	for _, existingID := range index.LeaseIDs {
		if existingID != leaseID {
			newLeaseIDs = append(newLeaseIDs, existingID)
		} else {
			found = true
		}
	}

	if !found {
		return nil // Lease ID not in index.
	}

	if len(newLeaseIDs) == 0 {
		// Remove the index entry entirely.
		return s.apply(&fsmOp{Op: opDelete, Bucket: bucketLocks, Key: lockIndexKey(ino)})
	}

	index.LeaseIDs = newLeaseIDs
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshaling updated index: %w", err)
	}
	return s.apply(&fsmOp{Op: opPut, Bucket: bucketLocks, Key: lockIndexKey(ino), Value: data})
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

// StartMetricsMonitor starts a background goroutine to periodically update
// Raft state metrics. Call once after creating the RaftStore.
func (s *RaftStore) StartMetricsMonitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := s.raft.State()
				switch state {
				case raft.Follower:
					metrics.RaftState.Set(0)
				case raft.Candidate:
					metrics.RaftState.Set(1)
				case raft.Leader:
					metrics.RaftState.Set(2)
				default:
					metrics.RaftState.Set(-1)
				}

				lastIndex := s.raft.LastIndex()
				metrics.RaftCommitIndex.Set(float64(lastIndex))
			}
		}
	}()
}
