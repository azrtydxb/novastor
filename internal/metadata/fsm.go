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
	bucketBuckets    = "buckets"    // S3 buckets, not FSM buckets
	bucketMultipart  = "multipart"
)

type fsmOp struct {
	Op     string `json:"op"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
}

type FSM struct {
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
}

func NewFSM() *FSM {
	return &FSM{
		buckets: map[string]map[string][]byte{
			bucketVolumes:    {},
			bucketPlacements: {},
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
