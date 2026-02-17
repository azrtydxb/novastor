package chunk

import (
	"context"
	"log"
	"sync"
	"time"
)

// ScrubReporter receives reports of corrupt chunks.
type ScrubReporter interface {
	ReportCorruptChunk(ctx context.Context, chunkID ChunkID) error
}

// Scrubber periodically verifies chunk integrity.
type Scrubber struct {
	store     Store
	reporter  ScrubReporter
	interval  time.Duration
	rateLimit int64 // bytes per second, 0 = unlimited

	mu        sync.Mutex
	lastScrub time.Time
	chunksOK  int64
	chunksBad int64
	running   bool
	cancel    context.CancelFunc
}

// NewScrubber creates a Scrubber with the given store, reporter, and interval.
func NewScrubber(store Store, reporter ScrubReporter, interval time.Duration) *Scrubber {
	return &Scrubber{
		store:    store,
		reporter: reporter,
		interval: interval,
	}
}

// Start begins the background scrub loop. It returns immediately.
func (s *Scrubber) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	go s.loop(ctx)
}

// Stop halts the background scrub loop.
func (s *Scrubber) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.running = false
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
}

func (s *Scrubber) loop(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run an initial scrub immediately.
	ok, bad, err := s.ScrubOnce(ctx)
	if err != nil {
		log.Printf("scrub error: %v", err)
	} else {
		log.Printf("scrub completed: %d ok, %d bad", ok, bad)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, bad, err := s.ScrubOnce(ctx)
			if err != nil {
				log.Printf("scrub error: %v", err)
			} else {
				log.Printf("scrub completed: %d ok, %d bad", ok, bad)
			}
		}
	}
}

// ScrubOnce scans all chunks and verifies each checksum.
// It returns the count of healthy and corrupt chunks.
func (s *Scrubber) ScrubOnce(ctx context.Context) (ok, bad int64, err error) {
	ids, err := s.store.List(ctx)
	if err != nil {
		return 0, 0, err
	}

	for _, id := range ids {
		select {
		case <-ctx.Done():
			return ok, bad, ctx.Err()
		default:
		}

		c, getErr := s.store.Get(ctx, id)
		if getErr != nil {
			// Get failed — treat as corrupt since the store's own integrity
			// check (inside Get) may have caught the corruption.
			log.Printf("scrub: error reading chunk %s: %v", id, getErr)
			bad++
			if reportErr := s.reporter.ReportCorruptChunk(ctx, id); reportErr != nil {
				log.Printf("scrub: error reporting corrupt chunk %s: %v", id, reportErr)
			}
			continue
		}

		if verifyErr := c.VerifyChecksum(); verifyErr != nil {
			log.Printf("scrub: corrupt chunk %s: %v", id, verifyErr)
			bad++
			if reportErr := s.reporter.ReportCorruptChunk(ctx, id); reportErr != nil {
				log.Printf("scrub: error reporting corrupt chunk %s: %v", id, reportErr)
			}
		} else {
			ok++
		}
	}

	s.mu.Lock()
	s.lastScrub = time.Now()
	s.chunksOK = ok
	s.chunksBad = bad
	s.mu.Unlock()

	return ok, bad, nil
}

// Stats returns the results of the last completed scrub.
func (s *Scrubber) Stats() (lastScrub time.Time, chunksOK, chunksBad int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastScrub, s.chunksOK, s.chunksBad
}
