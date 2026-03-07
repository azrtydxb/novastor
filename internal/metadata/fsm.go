package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	pb "github.com/azrtydxb/novastor/api/proto/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
)

const (
	opPut         = "put"
	opDelete      = "delete"
	opAddQuota    = "addQuota"
	opSubQuota    = "subQuota"
	opAllocateIno = "allocateIno"

	bucketVolumes          = "volumes"
	bucketCounters         = "counters"
	bucketPlacements       = "placements"
	bucketObjects          = "objects"
	bucketBuckets          = "buckets" // S3 buckets, not FSM buckets
	bucketMultipart        = "multipart"
	bucketSnapshots        = "snapshots"
	bucketShardPlacements  = "shardPlacements"
	bucketVolumeCompliance = "volumeCompliance"
	bucketHealTasks        = "healTasks"
	bucketChunkHealLocks   = "chunkHealLocks"
	bucketLocks            = "locks" // File lock leases
	bucketQuotas           = "quotas"
	bucketUsage            = "usage"
)

// Sentinel errors for FSM operations.
var (
	// ErrBucketNotFound is returned when attempting to access a non-existent bucket.
	ErrBucketNotFound = errors.New("bucket not found")
	// ErrKeyNotFound is returned when a key does not exist in a bucket.
	ErrKeyNotFound = errors.New("key not found")
)

// MetadataFSM defines the interface that both the in-memory FSM and the
// BadgerDB-backed FSM implement. It extends raft.FSM with typed read
// accessors and a Close method for resource cleanup.
type MetadataFSM interface {
	raft.FSM
	Get(bucket, key string) ([]byte, error)
	GetAll(bucket string) (map[string][]byte, error)
	Close() error
}

type fsmOp struct {
	Op     string `json:"op"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
}

// FSM is the in-memory implementation of MetadataFSM. It stores all metadata
// in nested maps protected by a read-write mutex. Suitable for testing and
// small deployments where persistence is handled entirely by Raft snapshots.
type FSM struct {
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
}

// Compile-time check that FSM implements MetadataFSM.
var _ MetadataFSM = (*FSM)(nil)

// NewFSM creates a new in-memory FSM with all required buckets initialized.
func NewFSM() *FSM {
	return &FSM{
		buckets: map[string]map[string][]byte{
			bucketVolumes:          {},
			bucketPlacements:       {},
			bucketObjects:          {},
			bucketBuckets:          {},
			bucketMultipart:        {},
			bucketSnapshots:        {},
			bucketShardPlacements:  {},
			bucketVolumeCompliance: {},
			bucketHealTasks:        {},
			bucketChunkHealLocks:   {},
			bucketLocks:            {},
			bucketCounters:         {},
			bucketQuotas:           {},
			bucketUsage:            {},
			"nodes":                {},
			"inodes":               {},
			"dirents":              {},
		},
	}
}

// Apply applies a Raft log entry to the FSM, modifying the in-memory state.
func (f *FSM) Apply(log *raft.Log) interface{} {
	start := time.Now()
	defer func() {
		metrics.RaftApplyLatency.Observe(time.Since(start).Seconds())
	}()

	var op fsmOp
	var pbOp pb.FsmOp
	if err := proto.Unmarshal(log.Data, &pbOp); err != nil {
		// Fall back to JSON for backward compatibility with pre-protobuf log entries.
		if jsonErr := json.Unmarshal(log.Data, &op); jsonErr != nil {
			return fmt.Errorf("unmarshaling fsm op: %w", err)
		}
	} else {
		op = fsmOp{Op: pbOp.Op, Bucket: pbOp.Bucket, Key: pbOp.Key, Value: pbOp.Value}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	bucket, ok := f.buckets[op.Bucket]
	if !ok {
		f.buckets[op.Bucket] = make(map[string][]byte)
		bucket = f.buckets[op.Bucket]
	}
	switch op.Op {
	case opPut:
		bucket[op.Key] = op.Value
	case opDelete:
		delete(bucket, op.Key)
	case opAddQuota:
		// AddQu atomically adds a delta to a usage counter.
		// The value is the delta to add (may be negative for subtraction).
		var delta int64
		if err := json.Unmarshal(op.Value, &delta); err != nil {
			return fmt.Errorf("unmarshaling quota delta: %w", err)
		}
		current := int64(0)
		if existing, ok := bucket[op.Key]; ok {
			if err := json.Unmarshal(existing, &current); err != nil {
				return fmt.Errorf("unmarshaling current usage: %w", err)
			}
		}
		newVal := current + delta
		// Prevent underflow
		if newVal < 0 {
			newVal = 0
		}
		updated, _ := json.Marshal(newVal)
		bucket[op.Key] = updated
	case opSubQuota:
		// SubQu atomically subtracts a delta from a usage counter.
		// This is an alias for AddQuota with a negative delta for clarity.
		var delta int64
		if err := json.Unmarshal(op.Value, &delta); err != nil {
			return fmt.Errorf("unmarshaling quota delta: %w", err)
		}
		current := int64(0)
		if existing, ok := bucket[op.Key]; ok {
			if err := json.Unmarshal(existing, &current); err != nil {
				return fmt.Errorf("unmarshaling current usage: %w", err)
			}
		}
		newVal := current - delta
		// Prevent underflow
		if newVal < 0 {
			newVal = 0
		}
		updated, _ := json.Marshal(newVal)
		bucket[op.Key] = updated
	case opAllocateIno:
		// Atomically increment the inode counter and return the new value.
		current := uint64(2) // Default start value (1 is reserved for root).
		if existing, ok := bucket[op.Key]; ok {
			if err := json.Unmarshal(existing, &current); err != nil {
				return fmt.Errorf("unmarshaling current inode counter: %w", err)
			}
		}
		next := current + 1
		updated, _ := json.Marshal(next)
		bucket[op.Key] = updated
		return next
	default:
		return fmt.Errorf("unknown op: %s", op.Op)
	}
	return nil
}

// Get retrieves a value by key from the specified bucket.
func (f *FSM) Get(bucket, key string) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, ok := f.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("%w: bucket %s", ErrBucketNotFound, bucket)
	}
	data, ok := b[key]
	if !ok {
		return nil, fmt.Errorf("%w: key %s not found in bucket %s", ErrKeyNotFound, key, bucket)
	}
	return data, nil
}

// GetAll retrieves all key-value pairs from the specified bucket.
func (f *FSM) GetAll(bucket string) (map[string][]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, ok := f.buckets[bucket]
	if !ok {
		return nil, nil
	}
	cp := make(map[string][]byte, len(b))
	for k, v := range b {
		val := make([]byte, len(v))
		copy(val, v)
		cp[k] = val
	}
	return cp, nil
}

type fsmSnapshot struct {
	data map[string]map[string][]byte
}

// Snapshot creates a point-in-time snapshot of the FSM state.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := make(map[string]map[string][]byte)
	for bk, bv := range f.buckets {
		bucket := make(map[string][]byte)
		for k, v := range bv {
			val := make([]byte, len(v))
			copy(val, v)
			bucket[k] = val
		}
		cp[bk] = bucket
	}
	return &fsmSnapshot{data: cp}, nil
}

// Restore restores the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}
	var data map[string]map[string][]byte
	var snap pb.FsmSnapshot
	if err := proto.Unmarshal(raw, &snap); err != nil {
		// Fall back to JSON for backward compatibility with pre-protobuf snapshots.
		if jsonErr := json.Unmarshal(raw, &data); jsonErr != nil {
			return fmt.Errorf("decoding snapshot: %w (json fallback: %v)", err, jsonErr)
		}
	} else {
		data = make(map[string]map[string][]byte, len(snap.Buckets))
		for bk, bv := range snap.Buckets {
			bucket := make(map[string][]byte, len(bv.Entries))
			for k, v := range bv.Entries {
				// Copy byte slices to avoid retaining references to the
				// protobuf unmarshal buffer.
				val := make([]byte, len(v))
				copy(val, v)
				bucket[k] = val
			}
			data[bk] = bucket
		}
	}
	f.mu.Lock()
	f.buckets = data
	f.mu.Unlock()
	return nil
}

// Persist writes the snapshot data to the sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// Convert map[string]map[string][]byte → pb.FsmSnapshot.
	pbBuckets := make(map[string]*pb.FsmBucket, len(s.data))
	for bk, bv := range s.data {
		pbBuckets[bk] = &pb.FsmBucket{Entries: bv}
	}
	data, err := proto.Marshal(&pb.FsmSnapshot{Buckets: pbBuckets})
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("marshaling snapshot: %w", err)
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("writing snapshot: %w", err)
	}
	return sink.Close()
}

// Release releases resources associated with the snapshot.
func (s *fsmSnapshot) Release() {}

// Close is a no-op for the in-memory FSM since there are no resources to release.
func (f *FSM) Close() error {
	return nil
}
