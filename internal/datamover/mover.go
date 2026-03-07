package datamover

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/azrtydxb/novastor/internal/metadata"
	"github.com/azrtydxb/novastor/internal/metrics"
)

// dataMover implements the DataMover interface.
type dataMover struct {
	config     *Config
	replicator ShardReplicator
	locker     ChunkLocker
	metaStore  MetadataStore
	ecFactory  ErasureCoderFactory

	taskQueue chan *metadata.HealTask
	workers   []*worker
	workerWG  sync.WaitGroup
	stopOnce  sync.Once
	stopCh    chan struct{}
	mu        sync.RWMutex
	running   bool
}

// NewDataMover creates a new DataMover with the given configuration.
func NewDataMover(cfg *Config, replicator ShardReplicator, metaStore MetadataStore, ecFactory ErasureCoderFactory) DataMover {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	locker := NewMetadataChunkLocker(metaStore)

	return &dataMover{
		config:     cfg,
		replicator: replicator,
		locker:     locker,
		metaStore:  metaStore,
		ecFactory:  ecFactory,
		taskQueue:  make(chan *metadata.HealTask, 1000),
		stopCh:     make(chan struct{}),
	}
}

// Submit adds a task to the execution queue (idempotent: same taskID → no-op).
func (dm *dataMover) Submit(ctx context.Context, task *metadata.HealTask) error {
	// Check if task already exists
	existing, err := dm.metaStore.GetHealTask(ctx, task.ID)
	if err == nil {
		// Task exists - check status
		switch existing.Status {
		case "completed":
			// Already done, no-op
			return nil
		case "in-progress", "pending":
			// Already queued or running, no-op
			return nil
		}
	}

	// Set timestamps if not set
	now := time.Now().Unix()
	if task.CreatedAt == 0 {
		task.CreatedAt = now
	}
	task.UpdatedAt = now
	task.Status = "pending"

	// Write to metadata store
	if err := dm.metaStore.PutHealTask(ctx, task); err != nil {
		return fmt.Errorf("storing task: %w", err)
	}

	// Try to queue to in-memory channel
	select {
	case dm.taskQueue <- task:
		metrics.DataMoverTasksPending.Inc()
		return nil
	default:
		// Queue full - task will be picked up by recovery
		log.Printf("DataMover: task queue full, task %s will be picked up by recovery", task.ID)
		return nil
	}
}

// Status returns current progress for a task.
func (dm *dataMover) Status(ctx context.Context, taskID string) (*metadata.HealTask, error) {
	return dm.metaStore.GetHealTask(ctx, taskID)
}

// Cancel attempts to stop a task (best-effort).
func (dm *dataMover) Cancel(ctx context.Context, taskID string) error {
	task, err := dm.metaStore.GetHealTask(ctx, taskID)
	if err != nil {
		return fmt.Errorf("getting task: %w", err)
	}

	// Only can cancel pending tasks
	if task.Status != "pending" {
		return fmt.Errorf("cannot cancel task in status %s", task.Status)
	}

	task.Status = "cancelled"
	task.UpdatedAt = time.Now().Unix()
	return dm.metaStore.PutHealTask(ctx, task)
}

// Start begins the worker pool; blocks until ctx is canceled.
func (dm *dataMover) Start(ctx context.Context) error {
	dm.mu.Lock()
	if dm.running {
		dm.mu.Unlock()
		return fmt.Errorf("data mover already running")
	}
	dm.running = true
	dm.mu.Unlock()

	// Recover existing tasks on startup
	if err := dm.recoverTasks(ctx); err != nil {
		log.Printf("DataMover: error recovering tasks: %v", err)
	}

	// Start workers
	for i := 0; i < dm.config.WorkerCount; i++ {
		w := newWorker(i, dm.replicator, dm.locker, dm.metaStore, dm.ecFactory)
		dm.workers = append(dm.workers, w)
		dm.workerWG.Add(1)
		go dm.runWorker(ctx, w)
	}

	// Start queue scanner for recovery
	go dm.scanQueue(ctx)

	log.Printf("DataMover: started with %d workers", dm.config.WorkerCount)

	// Wait for stop signal
	<-dm.stopCh
	dm.workerWG.Wait()

	dm.mu.Lock()
	dm.running = false
	dm.mu.Unlock()

	return nil
}

// Stop gracefully shuts down the data mover.
func (dm *dataMover) Stop() {
	dm.stopOnce.Do(func() {
		close(dm.stopCh)
	})
}

// recoverTasks reloads pending/in-progress tasks from metadata on startup.
func (dm *dataMover) recoverTasks(ctx context.Context) error {
	tasks, err := dm.metaStore.ListPendingHealTasks(ctx)
	if err != nil {
		return fmt.Errorf("listing pending tasks: %w", err)
	}

	// Sort by priority (lower = higher priority)
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Priority < tasks[j].Priority
	})

	log.Printf("DataMover: recovered %d tasks", len(tasks))

	// Re-queue all pending tasks
	for _, task := range tasks {
		if task.Status == "pending" {
			select {
			case dm.taskQueue <- task:
			default:
				log.Printf("DataMover: queue full during recovery, task %s will be scanned later", task.ID)
			}
		}
	}

	return nil
}

// runWorker runs a single worker that processes tasks from the queue.
func (dm *dataMover) runWorker(ctx context.Context, w *worker) {
	defer dm.workerWG.Done()
	defer w.stop()

	log.Printf("DataMover: worker %d starting", w.id)

	for {
		select {
		case <-ctx.Done():
			log.Printf("DataMover: worker %d shutting down", w.id)
			return
		case task := <-dm.taskQueue:
			if task == nil {
				return
			}

			metrics.DataMoverTasksPending.Dec()
			metrics.DataMoverTasksInProgress.Inc()

			startTime := time.Now()
			err := w.processTask(ctx, task, dm.config)
			duration := time.Since(startTime)

			metrics.DataMoverTasksInProgress.Dec()

			if err != nil {
				if task.Status == "failed" {
					metrics.DataMoverTasksFailed.Inc()
					log.Printf("DataMover: worker %d task %s failed after %v: %v", w.id, task.ID, duration, err)
				} else {
					// Will retry
					log.Printf("DataMover: worker %d task %s retrying: %v", w.id, task.ID, err)
					// Re-queue for retry
					select {
					case dm.taskQueue <- task:
						metrics.DataMoverTasksPending.Inc()
					default:
						// Queue full - will be picked up by scanner
					}
				}
			} else {
				metrics.DataMoverTasksCompleted.Inc()
				metrics.DataMoverTaskDuration.Observe(duration.Seconds())
				log.Printf("DataMover: worker %d task %s completed in %v", w.id, task.ID, duration)
			}
		}
	}
}

// scanQueue periodically scans metadata for tasks that aren't in the in-memory queue.
// This handles tasks that were submitted but dropped due to queue full, and also
// re-queues expired in-progress tasks.
func (dm *dataMover) scanQueue(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tasks, err := dm.metaStore.ListPendingHealTasks(ctx)
			if err != nil {
				log.Printf("DataMover: error scanning for pending tasks: %v", err)
				continue
			}

			queuedCount := 0
			for _, task := range tasks {
				// Only queue pending tasks, not in-progress
				if task.Status == "pending" {
					select {
					case dm.taskQueue <- task:
						queuedCount++
					default:
						// Queue full, will retry next scan
					}
				}
			}

			if queuedCount > 0 {
				log.Printf("DataMover: scanned and queued %d pending tasks", queuedCount)
			}
		}
	}
}
