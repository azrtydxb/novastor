package chunk

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type ReplicationManager struct {
	stores map[string]Store
}

func NewReplicationManager(stores map[string]Store) *ReplicationManager {
	return &ReplicationManager{stores: stores}
}

func (rm *ReplicationManager) Replicate(ctx context.Context, c *Chunk, nodes []string, writeQuorum int) error {
	if len(nodes) < writeQuorum {
		return fmt.Errorf("not enough nodes (%d) to meet write quorum (%d)", len(nodes), writeQuorum)
	}
	var (
		wg        sync.WaitGroup
		successes atomic.Int32
		lastErr   atomic.Value
	)
	for _, node := range nodes {
		store, ok := rm.stores[node]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(s Store, n string) {
			defer wg.Done()
			if err := s.Put(ctx, c); err != nil {
				lastErr.Store(fmt.Errorf("replication to %s failed: %w", n, err))
				return
			}
			successes.Add(1)
		}(store, node)
	}
	wg.Wait()
	if int(successes.Load()) < writeQuorum {
		errVal := lastErr.Load()
		if errVal != nil {
			return fmt.Errorf("write quorum not met (%d/%d): %w", successes.Load(), writeQuorum, errVal.(error))
		}
		return fmt.Errorf("write quorum not met (%d/%d)", successes.Load(), writeQuorum)
	}
	return nil
}

func (rm *ReplicationManager) ReadFromAny(ctx context.Context, id ChunkID, nodes []string) (*Chunk, error) {
	var lastErr error
	for _, node := range nodes {
		store, ok := rm.stores[node]
		if !ok {
			continue
		}
		c, err := store.Get(ctx, id)
		if err != nil {
			lastErr = fmt.Errorf("node %s: %w", node, err)
			continue
		}
		return c, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("all replicas failed for chunk %s: %w", id, lastErr)
	}
	return nil, fmt.Errorf("no nodes available for chunk %s", id)
}
