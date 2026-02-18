package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

const (
	opPut    = "put"
	opDelete = "delete"

	bucketVolumes    = "volumes"
	bucketPlacements = "placements"
	bucketObjects    = "objects"
	bucketBuckets    = "buckets" // S3 buckets, not FSM buckets
	bucketMultipart  = "multipart"
	bucketSnapshots  = "snapshots"
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

func NewFSM() *FSM {
	return &FSM{
		buckets: map[string]map[string][]byte{
			bucketVolumes:    {},
			bucketPlacements: {},
			bucketObjects:    {},
			bucketBuckets:    {},
			bucketMultipart:  {},
			bucketSnapshots:  {},
			"nodes":          {},
			"inodes":         {},
			"dirents":        {},
		},
	}
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var op fsmOp
	if err := json.Unmarshal(log.Data, &op); err != nil {
		return fmt.Errorf("unmarshaling fsm op: %w", err)
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
	default:
		return fmt.Errorf("unknown op: %s", op.Op)
	}
	return nil
}

func (f *FSM) Get(bucket, key string) ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, ok := f.buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("bucket %s not found", bucket)
	}
	data, ok := b[key]
	if !ok {
		return nil, fmt.Errorf("key %s not found in bucket %s", key, bucket)
	}
	return data, nil
}

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

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var data map[string]map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return fmt.Errorf("decoding snapshot: %w", err)
	}
	f.mu.Lock()
	f.buckets = data
	f.mu.Unlock()
	return nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s.data)
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

func (s *fsmSnapshot) Release() {}

// Close is a no-op for the in-memory FSM since there are no resources to release.
func (f *FSM) Close() error {
	return nil
}
