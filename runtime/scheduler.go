// Package runtime provides the interpreter and event-loop execution engine.
package runtime

import (
	"statecraft/core"
	"sync"
	"sync/atomic"
	"time"
)

// TimedFire is the value written to the scheduler's output channel when a
// timer fires. The ID matches the timerID passed to Start().
type TimedFire struct {
	Event core.Event
	ID    string
}

// Scheduler manages state-scoped timers. Every timer is identified by a
// string ID. When a state is exited, its timers are cancelled by ID —
// preventing stale after-events from reaching the interpreter.
//
// Scheduler is NOT safe for concurrent use from multiple goroutines:
// Start() and Cancel() must be called only from the interpreter goroutine.
// The timer callback goroutines write to the output channel independently,
// but that path is synchronized through the channel and the cancelled flag.
type Scheduler struct {
	clock   core.Clock
	outCh   chan TimedFire
	entries map[string]*timerEntry
	mu      sync.Mutex // protects entries only during callback writes
}

type timerEntry struct {
	timer     core.Timer
	cancelled atomic.Bool
}

const schedulerBufSize = 64

func newScheduler(clock core.Clock) *Scheduler {
	return &Scheduler{
		clock:   clock,
		outCh:   make(chan TimedFire, schedulerBufSize),
		entries: make(map[string]*timerEntry),
	}
}

// Start schedules ev to fire after delay d with the given timerID.
// If a timer with the same ID already exists it is cancelled first.
// Callers must not call Start from the timer callback goroutine.
func (s *Scheduler) Start(id string, d time.Duration, ev core.Event) {
	// Cancel any existing timer with this ID (idempotent re-entry).
	s.cancel(id)

	entry := &timerEntry{}

	// Lock only while writing to the map. The callback goroutine reads
	// entry.cancelled atomically, so the map does not need to be locked
	// during the callback.
	s.mu.Lock()
	s.entries[id] = entry
	s.mu.Unlock()

	entry.timer = s.clock.AfterFunc(d, func() {
		// This runs in a goroutine spawned by the runtime — NOT the
		// interpreter goroutine. Only the atomic and channel ops here.
		if entry.cancelled.Load() {
			return
		}
		select {
		case s.outCh <- TimedFire{Event: ev, ID: id}:
		default:
			// Buffer full means the interpreter is overwhelmed.
			// The event is dropped. In Phase 6 this will be persisted
			// and retried.
		}
	})
}

// Cancel stops a pending timer and marks it as cancelled so that a concurrently
// firing callback will be a no-op.
func (s *Scheduler) Cancel(id string) { s.cancel(id) }

func (s *Scheduler) cancel(id string) {
	s.mu.Lock()
	entry, ok := s.entries[id]
	if ok {
		delete(s.entries, id)
	}
	s.mu.Unlock()

	if ok {
		entry.cancelled.Store(true)
		entry.timer.Stop()
	}
}

// CancelAll cancels every pending timer. Called during service teardown.
func (s *Scheduler) CancelAll() {
	s.mu.Lock()
	entries := s.entries
	s.entries = make(map[string]*timerEntry)
	s.mu.Unlock()

	for _, entry := range entries {
		entry.cancelled.Store(true)
		entry.timer.Stop()
	}
}

// C returns the read-only channel on which fired events arrive.
// The interpreter selects on this channel in its main loop.
func (s *Scheduler) C() <-chan TimedFire { return s.outCh }
