package actor

import (
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"sync"
	"sync/atomic"
)

// SupervisionStrategy determines how a Ref behaves when its actor panics.
type SupervisionStrategy int

const (
	// NoRestart stops the actor permanently on panic without restarting.
	// This is the default strategy.
	NoRestart SupervisionStrategy = iota

	// RestartAlways restarts the actor unconditionally on each panic.
	RestartAlways

	// RestartN restarts the actor up to MaxRestarts times; stops permanently
	// after the limit is exceeded. Configure the limit with WithStrategy.
	RestartN
)

// SpawnOption configures the behaviour of a spawned actor.
type SpawnOption func(*spawnConfig)

type spawnConfig struct {
	strategy    SupervisionStrategy
	maxRestarts int
	svcOpts     []func(*runtime.ServiceOptions)
}

// WithStrategy sets the supervision strategy for the actor.
// For RestartN, maxRestarts is the inclusive upper bound on the number of
// restarts (e.g. 3 allows up to 3 restarts after the first panic).
func WithStrategy(strategy SupervisionStrategy, maxRestarts int) SpawnOption {
	return func(c *spawnConfig) {
		c.strategy = strategy
		c.maxRestarts = maxRestarts
	}
}

// WithClock injects a custom Clock into the actor's underlying Service.
// Useful for deterministic timer testing via testkit.MockClock.
func WithClock(clock core.Clock) SpawnOption {
	return func(c *spawnConfig) {
		c.svcOpts = append(c.svcOpts, runtime.WithClock(clock))
	}
}

// WithMailboxSize sets the mailbox buffer size for the actor's Service.
func WithMailboxSize(n int) SpawnOption {
	return func(c *spawnConfig) {
		c.svcOpts = append(c.svcOpts, runtime.WithMailboxSize(n))
	}
}

// ─── Ref ─────────────────────────────────────────────────────────────────────

// Ref[C] is a typed, persistent handle to an actor registered in a System.
// It survives actor restarts — the underlying Service is transparently swapped
// by the supervision loop on each restart.
//
// All public methods are safe to call from any goroutine.
type Ref[C any] struct {
	id          string
	machine     *model.Machine[C]
	svcOpts     []func(*runtime.ServiceOptions)
	strategy    SupervisionStrategy
	maxRestarts int

	svcPtr atomic.Pointer[runtime.Service[C]] // current service; swapped on restart

	stopOnce sync.Once
	stopCh   chan struct{} // closed to signal the supervision loop to stop
	doneCh   chan struct{} // closed when the supervision loop exits
}

// ID returns the actor's name within its System.
func (r *Ref[C]) ID() string { return r.id }

// Send delivers an event to the actor's current mailbox.
// Returns ErrActorStopped if the actor has been stopped.
func (r *Ref[C]) Send(ev core.Event) error {
	return r.svcPtr.Load().Send(ev)
}

// Snapshot returns the latest point-in-time state of the actor.
// Safe to call from any goroutine; never blocks.
func (r *Ref[C]) Snapshot() runtime.Snapshot[C] {
	return r.svcPtr.Load().Snapshot()
}

// Subscribe registers a callback fired after every state-changing snapshot.
// The subscription is on the current Service instance; if the actor restarts,
// the callback will not be called for the new instance — re-subscribe in
// response to the Done() channel or use a higher-level observation pattern.
func (r *Ref[C]) Subscribe(fn func(runtime.Snapshot[C])) runtime.UnsubscribeFn {
	return r.svcPtr.Load().Subscribe(fn)
}

// Stop requests the actor to shut down and blocks until it has fully stopped.
// Calling Stop more than once is safe.
func (r *Ref[C]) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
	<-r.doneCh
}

// Done returns a channel closed when the actor and its supervision loop have
// fully stopped. Useful for waiting without calling Stop().
func (r *Ref[C]) Done() <-chan struct{} { return r.doneCh }

// Err returns the panic error from the current (or last) service instance,
// or nil if it stopped cleanly. After a restart the new service has Err()==nil.
func (r *Ref[C]) Err() error { return r.svcPtr.Load().Err() }

// stopAndWait implements the stoppable interface used by System.
func (r *Ref[C]) stopAndWait() { r.Stop() }

// ─── supervision loop ─────────────────────────────────────────────────────────

// supervise runs the supervision loop. It is started as a goroutine by Spawn
// and runs until the actor is stopped intentionally or the restart limit is hit.
func (r *Ref[C]) supervise() {
	defer close(r.doneCh)
	restarts := 0

	for {
		svc := r.svcPtr.Load()
		select {
		case <-r.stopCh:
			// Intentional stop — shut down the current service and exit.
			svc.Stop()
			return

		case <-svc.Done():
			if svc.Err() == nil {
				// Service stopped cleanly (e.g. Stop() called directly on svc).
				return
			}
			// Service stopped due to a panic — decide whether to restart.
			if !r.shouldRestart(restarts) {
				return
			}
			restarts++
			newSvc := runtime.Start(r.machine, r.svcOpts...)
			r.svcPtr.Store(newSvc)
			// Loop again to supervise the new instance.
		}
	}
}

func (r *Ref[C]) shouldRestart(count int) bool {
	switch r.strategy {
	case RestartAlways:
		return true
	case RestartN:
		return count < r.maxRestarts
	default: // NoRestart
		return false
	}
}

// ─── Spawn ────────────────────────────────────────────────────────────────────

// Spawn creates a new actor in sys with the given id and starts it immediately.
// Returns an error if an actor with the same id is already registered.
//
// The returned Ref is the persistent handle to the actor. Hold onto it to send
// events, read snapshots, or subscribe to state changes.
func Spawn[C any](sys *System, id string, m *model.Machine[C], opts ...SpawnOption) (*Ref[C], error) {
	cfg := &spawnConfig{strategy: NoRestart}
	for _, o := range opts {
		o(cfg)
	}

	r := &Ref[C]{
		id:          id,
		machine:     m,
		svcOpts:     cfg.svcOpts,
		strategy:    cfg.strategy,
		maxRestarts: cfg.maxRestarts,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	svc := runtime.Start(m, cfg.svcOpts...)
	r.svcPtr.Store(svc)

	if err := sys.register(id, r); err != nil {
		svc.Stop() // clean up the started service on registration failure
		return nil, err
	}

	go r.supervise()
	return r, nil
}

// MustSpawn is like Spawn but panics on error. Useful for package-level
// initialisation where a duplicate id is a programming error.
func MustSpawn[C any](sys *System, id string, m *model.Machine[C], opts ...SpawnOption) *Ref[C] {
	r, err := Spawn(sys, id, m, opts...)
	if err != nil {
		panic("actor.MustSpawn: " + err.Error())
	}
	return r
}
