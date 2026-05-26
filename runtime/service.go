package runtime

import (
	"context"
	"statecraft/core"
	"statecraft/model"
	"sync/atomic"
	"time"
)

const (
	defaultMailboxSize = 256

	statusCreated int32 = 0
	statusRunning int32 = 1
	statusStopped int32 = 2
)

// Service is a running instance of a compiled Machine.
// One Machine can spawn many concurrent Services.
//
// The event loop runs in a dedicated goroutine. All mutable state
// (current state ID, context, internal queue) is owned exclusively by
// that goroutine — no locking is needed for them. The only shared objects
// are the mailbox channel and the atomic snapshot value.
type Service[C any] struct {
	machine *model.Machine[C]

	// ── Interpreter state (owned by the run goroutine) ────────────────────
	state         core.StateID
	previousState core.StateID // last state before the most recent transition
	ctx           C
	internal      []core.Event // internal queue — drained before the mailbox

	// ── Concurrency primitives ────────────────────────────────────────────
	mailbox   chan core.Envelope   // external event channel
	done      chan struct{}         // closed when the run goroutine exits
	cancelFn  context.CancelFunc   // stops the run goroutine
	status    atomic.Int32         // statusCreated / statusRunning / statusStopped

	// ── Observability ─────────────────────────────────────────────────────
	snap atomic.Value        // stores Snapshot[C]; read from any goroutine
	subs *subscriberSet[C]

	// ── Scheduler ─────────────────────────────────────────────────────────
	scheduler *Scheduler

	// ── Options ───────────────────────────────────────────────────────────
	clock       core.Clock
	mailboxSize int
}

// ServiceOptions holds configuration for a Service.
// Use the With* option functions to set individual fields.
type ServiceOptions struct {
	mailboxSize int
	clock       core.Clock
}

// WithMailboxSize sets the mailbox channel buffer size (default: 256).
// Larger values absorb bursts; smaller values surface backpressure sooner.
func WithMailboxSize(n int) func(*ServiceOptions) {
	return func(o *ServiceOptions) { o.mailboxSize = n }
}

// WithClock injects a custom Clock for deterministic testing.
func WithClock(c core.Clock) func(*ServiceOptions) {
	return func(o *ServiceOptions) { o.clock = c }
}

// Start compiles-to-runtime: creates a Service from a compiled Machine and
// begins processing events in a background goroutine.
//
// The returned Service is ready to receive events immediately.
// Call Stop() (or cancel the context) to shut down.
func Start[C any](m *model.Machine[C], opts ...func(*ServiceOptions)) *Service[C] {
	o := &ServiceOptions{
		mailboxSize: defaultMailboxSize,
		clock:       core.System,
	}
	for _, opt := range opts {
		opt(o)
	}

	runCtx, cancel := context.WithCancel(context.Background())

	svc := &Service[C]{
		machine:     m,
		state:       m.InitialState(),
		ctx:         m.InitialContext(),
		mailbox:     make(chan core.Envelope, o.mailboxSize),
		done:        make(chan struct{}),
		cancelFn:    cancel,
		subs:        newSubscriberSet[C](),
		scheduler:   newScheduler(o.clock),
		clock:       o.clock,
		mailboxSize: o.mailboxSize,
	}

	// Perform initial state setup synchronously so that callers using a
	// MockClock can advance time immediately after Start() returns.
	// Entry actions, timer registration, and the initial snapshot are all
	// visible to the caller before the run goroutine starts.
	svc.doEntry(svc.state, core.Init)
	svc.startStateTimers(svc.state)
	svc.storeSnapshot(core.Init, false)

	svc.status.Store(statusRunning)
	go svc.run(runCtx)
	return svc
}

// ─── Public API ───────────────────────────────────────────────────────────────

// Send enqueues an event for processing. It blocks if the mailbox is full,
// and returns ErrActorStopped if the service has been stopped.
//
// Backpressure: callers block until the interpreter drains enough of the
// mailbox to accept the event. This is the natural, safe default. Use
// TrySend for non-blocking delivery.
func (s *Service[C]) Send(ev core.Event) error {
	if s.status.Load() != statusRunning {
		return core.ErrActorStopped
	}
	select {
	case s.mailbox <- core.Envelope{Event: ev}:
		return nil
	case <-s.done:
		return core.ErrActorStopped
	}
}

// TrySend enqueues an event without blocking.
// Returns ErrMailboxFull if the mailbox is at capacity, ErrActorStopped
// if the service has stopped.
func (s *Service[C]) TrySend(ev core.Event) error {
	if s.status.Load() != statusRunning {
		return core.ErrActorStopped
	}
	select {
	case s.mailbox <- core.Envelope{Event: ev}:
		return nil
	case <-s.done:
		return core.ErrActorStopped
	default:
		return core.ErrMailboxFull
	}
}

// Snapshot returns the latest point-in-time view of the service.
// Safe to call from any goroutine; never blocks.
func (s *Service[C]) Snapshot() Snapshot[C] {
	return s.snap.Load().(Snapshot[C])
}

// Subscribe registers a callback that fires after every step.
// Returns an UnsubscribeFn; call it to deregister.
// The callback runs in the interpreter goroutine — keep it fast and
// non-blocking.
func (s *Service[C]) Subscribe(fn func(Snapshot[C])) UnsubscribeFn {
	return s.subs.Add(fn)
}

// Stop signals the service to shut down and blocks until the interpreter
// goroutine has exited and all resources are released.
// Calling Stop more than once is safe (subsequent calls are no-ops).
func (s *Service[C]) Stop() {
	if !s.status.CompareAndSwap(statusRunning, statusStopped) {
		<-s.done // already stopping — wait for completion
		return
	}
	s.cancelFn()
	<-s.done
}

// Done returns a channel that is closed when the service has fully stopped.
// Use it to wait for shutdown without calling Stop().
func (s *Service[C]) Done() <-chan struct{} { return s.done }

// State returns the currently active state ID.
// Sugar for s.Snapshot().State.
func (s *Service[C]) State() core.StateID { return s.Snapshot().State }

// ─── Event loop ───────────────────────────────────────────────────────────────

func (s *Service[C]) run(ctx context.Context) {
	defer func() {
		s.scheduler.CancelAll()
		s.status.Store(statusStopped)
		close(s.done)
	}()

	for {
		// ── Priority 1: internal events (SCXML §5.2) ────────────────────
		// Raised events are processed before any external event, ensuring
		// action-driven state chains appear atomic to outside observers.
		if len(s.internal) > 0 {
			ev := s.internal[0]
			s.internal = s.internal[1:]
			s.step(ev)
			continue
		}

		// ── Priority 2: always transitions ──────────────────────────────
		// Evaluated after the internal queue empties. If one fires it may
		// raise internal events, so we go back to the top of the loop.
		if s.tryAlways() {
			continue
		}

		// ── Priority 3: external events (mailbox + scheduler) ───────────
		select {
		case <-ctx.Done():
			return

		case env, ok := <-s.mailbox:
			if !ok {
				return
			}
			s.step(env.Event)

		case tf := <-s.scheduler.C():
			// Timer event types are state-scoped. If the machine has since
			// left the state that started this timer, ResolveTransition
			// returns false and the event is silently dropped.
			s.step(tf.Event)
		}
	}
}

// step processes a single event: resolve transition → exit → transition
// actions → update state → entry → timers → notify.
func (s *Service[C]) step(ev core.Event) {
	target, tActions, ok := s.machine.ResolveTransition(s.state, s.ctx, ev)
	if !ok {
		// No enabled transition — event is silently ignored.
		// Subscribers are NOT notified (nothing changed).
		return
	}
	s.doTransition(ev, target, tActions)
}

// tryAlways evaluates always transitions for the current state.
// Returns true if one fired (caller should re-check internal queue).
// Panics if always transitions loop more than maxAlwaysIter times without
// an external event — indicates a missing guard.
func (s *Service[C]) tryAlways() bool {
	const maxAlwaysIter = 1000
	for range maxAlwaysIter {
		target, tActions, ok := s.machine.ResolveAlways(s.state, s.ctx)
		if !ok {
			return false
		}
		s.doTransition(model.AlwaysEvent{}, target, tActions)
		// If internal events were raised, stop here — the main loop will
		// drain them before checking always again.
		if len(s.internal) > 0 {
			return true
		}
	}
	panic("statecraft: always-transition loop — " + string(s.state) +
		" has an unguarded always transition that keeps re-entering itself")
}

// doTransition executes the mechanical steps of any transition type:
// exit, transition actions, state update, entry, timers, snapshot.
func (s *Service[C]) doTransition(ev core.Event, target core.StateID, tActions []model.ActionFn[C]) {
	from := s.state
	ac := &model.ActionContext{}

	// 1. Exit current state.
	s.doExit(s.state, ev)

	// 2. Cancel timers scoped to the exited state.
	s.stopStateTimers(s.state)

	// 3. Execute transition actions.
	for _, a := range tActions {
		s.ctx = a(s.ctx, ev, ac)
	}

	// 4. Update active state.
	s.previousState = s.state
	s.state = target

	// 5. Enter new state.
	s.doEntry(s.state, ev)

	// 6. Start timers scoped to the entered state.
	s.startStateTimers(s.state)

	// 7. Inject raised events.
	if raised := ac.Drain(); len(raised) > 0 {
		s.internal = append(s.internal, raised...)
	}

	// 8. Publish snapshot.
	s.storeAndNotify(ev, s.state != from)
}

// ─── Lifecycle helpers ────────────────────────────────────────────────────────

func (s *Service[C]) doEntry(id core.StateID, ev core.Event) {
	actions := s.machine.EntryActions(id)
	if len(actions) == 0 {
		return
	}
	ac := &model.ActionContext{}
	for _, a := range actions {
		s.ctx = a(s.ctx, ev, ac)
	}
	s.internal = append(s.internal, ac.Drain()...)
}

func (s *Service[C]) doExit(id core.StateID, ev core.Event) {
	actions := s.machine.ExitActions(id)
	if len(actions) == 0 {
		return
	}
	ac := &model.ActionContext{}
	for _, a := range actions {
		s.ctx = a(s.ctx, ev, ac)
	}
	s.internal = append(s.internal, ac.Drain()...)
}

// ─── Timer helpers ────────────────────────────────────────────────────────────

func (s *Service[C]) startStateTimers(id core.StateID) {
	for _, conf := range s.machine.AfterConfs(id) {
		ev := core.E(string(conf.EventType))
		s.scheduler.Start(conf.TimerID, conf.Delay, ev)
	}
}

func (s *Service[C]) stopStateTimers(id core.StateID) {
	for _, conf := range s.machine.AfterConfs(id) {
		s.scheduler.Cancel(conf.TimerID)
	}
}

// ─── Snapshot helpers ─────────────────────────────────────────────────────────

func (s *Service[C]) storeSnapshot(ev core.Event, changed bool) {
	s.snap.Store(Snapshot[C]{
		State:         s.state,
		PreviousState: s.previousState,
		Context:       s.ctx,
		Event:         ev,
		Changed:       changed,
		Final:         s.machine.IsFinal(s.state),
		At:            time.Now(),
	})
}

func (s *Service[C]) storeAndNotify(ev core.Event, changed bool) {
	s.storeSnapshot(ev, changed)
	// Only notify subscribers when a transition fired. Callers that want the
	// initial state (before any event) should call Snapshot() directly.
	if changed {
		s.subs.Notify(s.snap.Load().(Snapshot[C]))
	}
}
