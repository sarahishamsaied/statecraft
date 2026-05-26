// Package actor provides a named-actor runtime on top of statecraft Services.
// Each actor is a running Service with an ID, a supervision strategy, and a
// typed Ref handle that survives restarts.
//
// Quick start:
//
//	sys := actor.NewSystem()
//	ref := actor.MustSpawn(sys, "counter", counterMachine,
//	    actor.WithStrategy(actor.RestartAlways, 0))
//	ref.Send(core.E("INCREMENT"))
//	sys.Stop()
package actor

import (
	"fmt"
	"sync"
)

// System is a named registry of actors. It coordinates lifecycle:
// spawning, stopping, and (via the supervision loop inside each Ref)
// restarting actors on panic.
//
// A System has no goroutines of its own — all supervision work runs inside
// the goroutine spawned by Spawn for each Ref.
type System struct {
	mu     sync.RWMutex
	actors map[string]stoppable
	once   sync.Once
	done   chan struct{}
}

// stoppable is the type-erased view of a Ref stored inside System.
// It lets System stop actors without knowing their context type C.
type stoppable interface {
	stopAndWait()
}

// NewSystem creates an empty actor system.
func NewSystem() *System {
	return &System{
		actors: make(map[string]stoppable),
		done:   make(chan struct{}),
	}
}

// Stop signals every actor in the system to shut down and blocks until all
// have stopped. Calling Stop more than once is safe (subsequent calls block
// until the first call's shutdown completes).
func (s *System) Stop() {
	s.once.Do(func() {
		s.mu.RLock()
		handles := make([]stoppable, 0, len(s.actors))
		for _, h := range s.actors {
			handles = append(handles, h)
		}
		s.mu.RUnlock()

		var wg sync.WaitGroup
		for _, h := range handles {
			wg.Add(1)
			go func(h stoppable) {
				defer wg.Done()
				h.stopAndWait()
			}(h)
		}
		wg.Wait()
		close(s.done)
	})
	<-s.done
}

// Done returns a channel closed when all actors have stopped.
func (s *System) Done() <-chan struct{} { return s.done }

func (s *System) register(id string, h stoppable) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.actors[id]; exists {
		return fmt.Errorf("actor %q is already registered in this system", id)
	}
	s.actors[id] = h
	return nil
}
