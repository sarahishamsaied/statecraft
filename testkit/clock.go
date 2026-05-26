// Package testkit provides synchronous, deterministic testing utilities for
// statecraft machines. Import it only in _test files or test binaries.
package testkit

import (
	"sort"
	"statecraft/core"
	"sync"
	"time"
)

// MockClock implements core.Clock with manually-advanced time.
// Inject it via runtime.WithClock(clock) to control timer behaviour in tests
// that use a live Service. For fully synchronous tests, use Harness instead.
//
// MockClock is safe for concurrent use.
type MockClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*mockTimer
}

type mockTimer struct {
	fireAt    time.Time
	fn        func()
	cancelled bool
}

func (t *mockTimer) Stop() bool {
	if t.cancelled {
		return false
	}
	t.cancelled = true
	return true
}

// NewMockClock creates a MockClock with its current time set to start.
func NewMockClock(start time.Time) *MockClock {
	return &MockClock{now: start}
}

// Now returns the clock's current (manually-set) time.
func (c *MockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// AfterFunc schedules fn to fire after duration d relative to the mock's
// current time. The function is called synchronously inside Advance().
func (c *MockClock) AfterFunc(d time.Duration, fn func()) core.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &mockTimer{fireAt: c.now.Add(d), fn: fn}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward by d and fires all timers whose deadline
// falls within the new time, in chronological order.
func (c *MockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	deadline := c.now

	sort.Slice(c.timers, func(i, j int) bool {
		return c.timers[i].fireAt.Before(c.timers[j].fireAt)
	})

	var fire []*mockTimer
	remaining := c.timers[:0]
	for _, t := range c.timers {
		if !t.cancelled && !t.fireAt.After(deadline) {
			fire = append(fire, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()

	// Fire outside the lock — timers may call AfterFunc re-entrantly.
	for _, t := range fire {
		t.fn()
	}
}

// Set jumps the clock to an absolute time and fires any timers that have
// elapsed. Panics if t is before the current time.
func (c *MockClock) Set(t time.Time) {
	c.mu.Lock()
	if t.Before(c.now) {
		c.mu.Unlock()
		panic("testkit.MockClock.Set: cannot move clock backwards")
	}
	delta := t.Sub(c.now)
	c.mu.Unlock()
	c.Advance(delta)
}
