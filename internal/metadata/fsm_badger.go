package metadata

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"

	"github.com/piwi3910/novastor/internal/metrics"
)

// BadgerFSM is a persistent implementation of MetadataFSM backed by BadgerDB.
// Keys are stored as composite "bucket:key" strings to partition data into
// logical buckets while keeping a single key-value namespace.
type BadgerFSM struct {
	db *badger.DB
}

// Compile-time check that BadgerFSM implements MetadataFSM.
var _ MetadataFSM = (*BadgerFSM)(nil)

// NewBadgerFSM opens (or creates) a BadgerDB database at the given directory
// path and returns a BadgerFSM ready for use as a Raft FSM.
func NewBadgerFSM(dir string) (*BadgerFSM, error) {
	opts := badger.DefaultOptions(dir).
		WithLogger(nil) // Suppress BadgerDB's internal logging.
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening badger db at %s: %w", dir, err)
	}
	return &BadgerFSM{db: db}, nil
}

// compositeKey builds the "bucket:key" composite key used for BadgerDB storage.
func compositeKey(bucket, key string) []byte {
	return []byte(bucket + ":" + key)
}

// bucketPrefix returns the prefix used to scan all keys within a bucket.
func bucketPrefix(bucket string) []byte {
	return []byte(bucket + ":")
}

// Apply handles a single Raft log entry by executing the encoded put or delete
// operation against BadgerDB. It returns nil on success or an error.
func (f *BadgerFSM) Apply(log *raft.Log) interface{} {
	start := time.Now()
	defer func() {
		metrics.RaftApplyLatency.Observe(time.Since(start).Seconds())
	}()

	var op fsmOp
	if err := json.Unmarshal(log.Data, &op); err != nil {
		return fmt.Errorf("unmarshaling fsm op: %w", err)
	}

	switch op.Op {
	case opPut:
		err := f.db.Update(func(txn *badger.Txn) error {
			return txn.Set(compositeKey(op.Bucket, op.Key), op.Value)
		})
		if err != nil {
			return fmt.Errorf("badger put: %w", err)
		}
	case opDelete:
		err := f.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(compositeKey(op.Bucket, op.Key))
		})
		if err != nil {
			return fmt.Errorf("badger delete: %w", err)
		}
	case opAddQuota:
		// AddQu atomically adds a delta to a usage counter.
		err := f.db.Update(func(txn *badger.Txn) error {
			var delta int64
			if err := json.Unmarshal(op.Value, &delta); err != nil {
				return fmt.Errorf("unmarshaling quota delta: %w", err)
			}
			current := int64(0)
			item, err := txn.Get(compositeKey(op.Bucket, op.Key))
			if err == nil {
				val, err := item.ValueCopy(nil)
				if err == nil {
					if err := json.Unmarshal(val, &current); err != nil {
						return fmt.Errorf("unmarshaling current usage: %w", err)
					}
				}
			}
			newVal := current + delta
			if newVal < 0 {
				newVal = 0
			}
			updated, _ := json.Marshal(newVal)
			return txn.Set(compositeKey(op.Bucket, op.Key), updated)
		})
		if err != nil {
			return fmt.Errorf("badger add quota: %w", err)
		}
	case opSubQuota:
		// SubQu atomically subtracts a delta from a usage counter.
		err := f.db.Update(func(txn *badger.Txn) error {
			var delta int64
			if err := json.Unmarshal(op.Value, &delta); err != nil {
				return fmt.Errorf("unmarshaling quota delta: %w", err)
			}
			current := int64(0)
			item, err := txn.Get(compositeKey(op.Bucket, op.Key))
			if err == nil {
				val, err := item.ValueCopy(nil)
				if err == nil {
					if err := json.Unmarshal(val, &current); err != nil {
						return fmt.Errorf("unmarshaling current usage: %w", err)
					}
				}
			}
			newVal := current - delta
			if newVal < 0 {
				newVal = 0
			}
			updated, _ := json.Marshal(newVal)
			return txn.Set(compositeKey(op.Bucket, op.Key), updated)
		})
		if err != nil {
			return fmt.Errorf("badger sub quota: %w", err)
		}
	default:
		return fmt.Errorf("unknown op: %s", op.Op)
	}
	return nil
}

// Get retrieves a single value from the specified bucket and key.
func (f *BadgerFSM) Get(bucket, key string) ([]byte, error) {
	var val []byte
	err := f.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(compositeKey(bucket, key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, fmt.Errorf("key %s not found in bucket %s", key, bucket)
		}
		return nil, fmt.Errorf("badger get: %w", err)
	}
	return val, nil
}

// GetAll returns all key-value pairs within the specified bucket. The returned
// map uses the original key (without the bucket prefix). If the bucket has no
// entries, nil is returned.
func (f *BadgerFSM) GetAll(bucket string) (map[string][]byte, error) {
	prefix := bucketPrefix(bucket)
	prefixLen := len(prefix)
	var result map[string][]byte

	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			// Strip the "bucket:" prefix to get the original key.
			originalKey := string(k[prefixLen:])
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("reading value for key %s: %w", originalKey, err)
			}
			if result == nil {
				result = make(map[string][]byte)
			}
			result[originalKey] = val
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badger prefix scan: %w", err)
	}
	return result, nil
}

// badgerSnapshotEntry represents a single key-value pair in the snapshot stream.
type badgerSnapshotEntry struct {
	Key   []byte `json:"k"`
	Value []byte `json:"v"`
}

// Snapshot creates a point-in-time snapshot of the BadgerDB state. The snapshot
// streams entries rather than loading everything into memory, using a length-
// prefixed binary format for each JSON-encoded entry.
func (f *BadgerFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Collect all key-value pairs. BadgerDB transactions provide a consistent
	// view, so we iterate within a single read transaction.
	var entries []badgerSnapshotEntry
	err := f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			val, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("reading value during snapshot: %w", err)
			}
			entries = append(entries, badgerSnapshotEntry{Key: key, Value: val})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("creating badger snapshot: %w", err)
	}
	return &badgerFSMSnapshot{entries: entries}, nil
}

// Restore replaces the entire BadgerDB state with data from the snapshot reader.
// All existing data is dropped before loading the snapshot.
func (f *BadgerFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Drop all existing data.
	if err := f.db.DropAll(); err != nil {
		return fmt.Errorf("dropping badger data before restore: %w", err)
	}

	// Read length-prefixed entries from the stream.
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(rc, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return fmt.Errorf("reading entry length: %w", err)
		}
		entryLen := binary.BigEndian.Uint32(lenBuf[:])
		entryBuf := make([]byte, entryLen)
		if _, err := io.ReadFull(rc, entryBuf); err != nil {
			return fmt.Errorf("reading entry data: %w", err)
		}
		var entry badgerSnapshotEntry
		if err := json.Unmarshal(entryBuf, &entry); err != nil {
			return fmt.Errorf("unmarshaling snapshot entry: %w", err)
		}
		if err := f.db.Update(func(txn *badger.Txn) error {
			return txn.Set(entry.Key, entry.Value)
		}); err != nil {
			return fmt.Errorf("restoring entry: %w", err)
		}
	}
	return nil
}

// Close closes the underlying BadgerDB database, releasing all resources.
func (f *BadgerFSM) Close() error {
	return f.db.Close()
}

// badgerFSMSnapshot holds snapshot data for streaming to a raft.SnapshotSink.
type badgerFSMSnapshot struct {
	entries []badgerSnapshotEntry
}

// Persist writes all snapshot entries to the sink using a length-prefixed
// binary format: [4-byte big-endian length][JSON-encoded entry] per record.
func (s *badgerFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	for _, entry := range s.entries {
		data, err := json.Marshal(entry)
		if err != nil {
			sink.Cancel()
			return fmt.Errorf("marshaling snapshot entry: %w", err)
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		if _, err := sink.Write(lenBuf[:]); err != nil {
			sink.Cancel()
			return fmt.Errorf("writing entry length: %w", err)
		}
		if _, err := sink.Write(data); err != nil {
			sink.Cancel()
			return fmt.Errorf("writing entry data: %w", err)
		}
	}
	return sink.Close()
}

// Release is a no-op; snapshot data is garbage-collected normally.
func (s *badgerFSMSnapshot) Release() {}
