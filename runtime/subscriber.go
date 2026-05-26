package runtime

import (
	"sync"
	"sync/atomic"
)

// SubscriberFn is the callback type for state-change observers.
// It is called synchronously from the interpreter goroutine after every
// step that produces a new snapshot, so it must not block or send on
// unbuffered channels owned by the caller — doing so deadlocks.
// Copy any data you need; the snapshot is a value type.
type SubscriberFn[C any] func(Snapshot[C])

// UnsubscribeFn cancels a subscription when called.
// It is idempotent and safe to call from any goroutine.
type UnsubscribeFn func()

// subscriberSet manages a dynamic set of subscriber callbacks.
// Add and Notify may be called from different goroutines concurrently.
type subscriberSet[C any] struct {
	mu   sync.RWMutex
	subs map[uint64]SubscriberFn[C]
	next atomic.Uint64
}

func newSubscriberSet[C any]() *subscriberSet[C] {
	return &subscriberSet[C]{subs: make(map[uint64]SubscriberFn[C])}
}

// Add registers fn and returns an UnsubscribeFn.
// Safe to call from any goroutine.
func (ss *subscriberSet[C]) Add(fn SubscriberFn[C]) UnsubscribeFn {
	id := ss.next.Add(1)
	ss.mu.Lock()
	ss.subs[id] = fn
	ss.mu.Unlock()
	return func() {
		ss.mu.Lock()
		delete(ss.subs, id)
		ss.mu.Unlock()
	}
}

// Notify calls every registered subscriber with snap.
//
// Implementation note: we copy the live subscriber slice under RLock then
// release the lock before invoking callbacks. This means:
//   - A subscriber that calls Unsubscribe during its own callback will not
//     deadlock (Unsubscribe takes the write lock, which is free at call time).
//   - A subscriber added during notification will not be called for this
//     particular snap (it will be called starting from the next snapshot).
//   - A subscriber that was removed concurrently may still be called once
//     after removal — callers should tolerate this edge case.
func (ss *subscriberSet[C]) Notify(snap Snapshot[C]) {
	ss.mu.RLock()
	fns := make([]SubscriberFn[C], 0, len(ss.subs))
	for _, fn := range ss.subs {
		fns = append(fns, fn)
	}
	ss.mu.RUnlock()

	for _, fn := range fns {
		fn(snap)
	}
}

// Len returns the current number of active subscribers.
func (ss *subscriberSet[C]) Len() int {
	ss.mu.RLock()
	n := len(ss.subs)
	ss.mu.RUnlock()
	return n
}
