package runtime

import (
	"context"
	"fmt"
	"sort"
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
	// leaves holds all active leaf states: exactly one entry for flat/compound
	// machines, one per region for parallel machines.
	leaves       []core.StateID
	previousLeaf core.StateID // leaves[0] before the most recent transition
	ctx          C
	internal     []core.Event // internal queue — drained before the mailbox

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

	// ── Active invocations ────────────────────────────────────────────────
	// keyed by state ID so hierarchical transitions only cancel the invokes
	// for the states actually being exited, leaving sibling/ancestor invokes intact.
	invokes map[core.StateID][]context.CancelFunc

	// ── Fault capture ─────────────────────────────────────────────────────
	panicErr atomic.Value // stores panicVal; non-nil if run goroutine panicked

	// ── Options ───────────────────────────────────────────────────────────
	clock       core.Clock
	mailboxSize int
}

// panicVal wraps a recovered panic so it can be stored in an atomic.Value
// (which requires a concrete non-nil type, not a bare interface).
type panicVal struct{ err error }

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

	// Resolve the declared initial state to all active initial leaves
	// (one for flat/compound machines, one per region for parallel).
	initialLeaves := m.InitialLeaves()

	svc := &Service[C]{
		machine:     m,
		leaves:      initialLeaves,
		ctx:         m.InitialContext(),
		mailbox:     make(chan core.Envelope, o.mailboxSize),
		done:        make(chan struct{}),
		cancelFn:    cancel,
		subs:        newSubscriberSet[C](),
		scheduler:   newScheduler(o.clock),
		clock:       o.clock,
		mailboxSize: o.mailboxSize,
		invokes:     make(map[core.StateID][]context.CancelFunc),
	}

	// Enter the full initial configuration: collect the union of entry paths
	// for all initial leaves, deduplicated and in definition order.
	initEntry := svc.unifiedEntryPaths("", initialLeaves)
	for _, id := range initEntry {
		svc.doEntry(id, core.Init)
		svc.startStateTimers(id)
	}
	svc.raiseDoneEvents(initEntry)
	svc.storeSnapshot(core.Init, false)

	// Set running before starting invokes so that the send callback they
	// receive can call svc.Send() without getting ErrActorStopped.
	svc.status.Store(statusRunning)
	for _, id := range initEntry {
		svc.doInvokeEntry(id, core.Init)
	}

	go svc.run(runCtx)
	return svc
}

// Restore creates a Service whose initial configuration is taken from leaves
// and ctx rather than the machine's declared initial state. Use this to resume
// a service after a process restart.
//
// Entry actions are NOT re-executed — the provided ctx already reflects them.
// Timers and invokes are started fresh since they are ephemeral.
//
// Returns ErrInvalidCheckpoint if any leaf is unknown or is a compound state.
func Restore[C any](m *model.Machine[C], leaves []core.StateID, ctx C, opts ...func(*ServiceOptions)) (*Service[C], error) {
	if len(leaves) == 0 {
		return nil, fmt.Errorf("%w: leaves must not be empty", core.ErrInvalidCheckpoint)
	}
	for _, leaf := range leaves {
		if !m.Has(leaf) {
			return nil, fmt.Errorf("%w: unknown state %q", core.ErrInvalidCheckpoint, leaf)
		}
		if m.IsCompound(leaf) {
			return nil, fmt.Errorf("%w: state %q is compound — only leaf states are valid", core.ErrInvalidCheckpoint, leaf)
		}
	}

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
		leaves:      append([]core.StateID(nil), leaves...),
		ctx:         ctx,
		mailbox:     make(chan core.Envelope, o.mailboxSize),
		done:        make(chan struct{}),
		cancelFn:    cancel,
		subs:        newSubscriberSet[C](),
		scheduler:   newScheduler(o.clock),
		clock:       o.clock,
		mailboxSize: o.mailboxSize,
		invokes:     make(map[core.StateID][]context.CancelFunc),
	}

	// Restart timers and invokes for every state in the active configuration.
	// Entry actions are intentionally skipped — the restored context already
	// reflects them.
	for _, id := range m.ConfigurationOf(leaves) {
		svc.startStateTimers(id)
	}
	svc.storeSnapshot(core.Init, false)

	svc.status.Store(statusRunning)
	for _, id := range m.ConfigurationOf(leaves) {
		svc.doInvokeEntry(id, core.Init)
	}

	go svc.run(runCtx)
	return svc, nil
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

// Err returns the recovered panic value if the service stopped due to an
// unhandled panic in the run goroutine. Returns nil if the service was stopped
// cleanly via Stop() or context cancellation. Safe to call from any goroutine
// after Done() is closed.
func (s *Service[C]) Err() error {
	if v := s.panicErr.Load(); v != nil {
		return v.(panicVal).err
	}
	return nil
}

// State returns the first active leaf state ID. For flat and compound
// machines this is always the single active state. For parallel machines,
// use Snapshot().ActiveStates to see all active regions.
func (s *Service[C]) State() core.StateID { return s.Snapshot().State }

// MachineID returns the ID of the compiled machine this service runs.
func (s *Service[C]) MachineID() string { return s.machine.ID() }

// ─── Event loop ───────────────────────────────────────────────────────────────

func (s *Service[C]) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.panicErr.Store(panicVal{fmt.Errorf("%v", r)})
		}
		s.scheduler.CancelAll()
		s.stopInvokes()
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

// pendingTrans is an enabled transition collected during step().
type pendingTrans[C any] struct {
	source  core.StateID
	target  core.StateID
	actions []model.ActionFn[C]
}

// step collects enabled transitions from every active leaf and applies them
// in a single SCXML macrostep. For non-parallel machines this is always
// zero or one transitions; for parallel machines each region contributes
// independently.
func (s *Service[C]) step(ev core.Event) {
	var pending []pendingTrans[C]
	covered := make(map[core.StateID]bool)
	for _, leaf := range s.leaves {
		if covered[leaf] {
			continue
		}
		target, actions, ok := s.machine.ResolveTransition(leaf, s.ctx, ev)
		if !ok {
			continue
		}
		pending = append(pending, pendingTrans[C]{leaf, target, actions})
		covered[leaf] = true
	}
	if len(pending) == 0 {
		return
	}
	s.applyTransitions(ev, pending)
}

// tryAlways evaluates always transitions for every active leaf.
// Returns true if any fired (caller should re-check internal queue).
// Panics after maxAlwaysIter iterations to catch infinite always loops.
func (s *Service[C]) tryAlways() bool {
	const maxAlwaysIter = 1000
	for range maxAlwaysIter {
		var pending []pendingTrans[C]
		covered := make(map[core.StateID]bool)
		for _, leaf := range s.leaves {
			if covered[leaf] {
				continue
			}
			target, actions, ok := s.machine.ResolveAlways(leaf, s.ctx)
			if !ok {
				continue
			}
			pending = append(pending, pendingTrans[C]{leaf, target, actions})
			covered[leaf] = true
		}
		if len(pending) == 0 {
			return false
		}
		s.applyTransitions(model.AlwaysEvent{}, pending)
		if len(s.internal) > 0 {
			return true
		}
	}
	panic("statecraft: always-transition loop — " + string(s.leaves[0]) +
		" has an unguarded always transition that keeps re-entering itself")
}

// applyTransitions executes one SCXML macrostep for one or more enabled
// transitions. Computes the unified exit/entry sets relative to each
// transition's LCCA so shared ancestors are never re-entered.
func (s *Service[C]) applyTransitions(ev core.Event, pending []pendingTrans[C]) {
	prevLeaves := append([]core.StateID(nil), s.leaves...)
	ac := &model.ActionContext{}

	// ── Per-transition metadata ────────────────────────────────────────────
	type transInfo struct {
		source      core.StateID
		leafTargets []core.StateID // one for atomic/OR, many for parallel
		lcca        core.StateID
		exitPath    []core.StateID   // innermost → outermost
		entryPaths  [][]core.StateID // one path per leaf target
	}
	infos := make([]transInfo, len(pending))
	for i, p := range pending {
		lt := s.machine.LeafTargets(p.target)
		// Use the first leaf target for LCCA (they're always under the same parent).
		lcca := s.machine.LCCA(p.source, lt[0])
		exitPath := s.machine.ExitPath(p.source, lcca)
		entryPaths := make([][]core.StateID, len(lt))
		for j, leafTarget := range lt {
			entryPaths[j] = s.machine.EntryPath(lcca, leafTarget)
		}
		infos[i] = transInfo{p.source, lt, lcca, exitPath, entryPaths}
	}

	// ── Unified exit set (innermost first, deduplicated) ──────────────────
	// Collect all states to exit across all transitions, then sort by depth
	// descending so that shared ancestors (e.g. a parallel state exited by
	// all regions) are always exited after all their descendants.
	exitSeen := make(map[core.StateID]bool)
	var exitOrder []core.StateID
	for _, info := range infos {
		for _, id := range info.exitPath {
			if !exitSeen[id] {
				exitSeen[id] = true
				exitOrder = append(exitOrder, id)
			}
		}
	}
	sort.SliceStable(exitOrder, func(i, j int) bool {
		return s.machine.Depth(exitOrder[i]) > s.machine.Depth(exitOrder[j])
	})

	// ── 1. Exit states ─────────────────────────────────────────────────────
	for _, id := range exitOrder {
		s.doExit(id, ev)
		s.stopStateTimers(id)
		s.stopStateInvokes(id)
	}

	// ── 2. Execute all transition actions ──────────────────────────────────
	for _, p := range pending {
		for _, a := range p.actions {
			s.ctx = a(s.ctx, ev, ac)
		}
	}

	// ── 3. Compute new leaf set ─────────────────────────────────────────────
	// Replace each exited leaf with its new targets in-place so that region
	// ordering is preserved (leaves[0] always tracks the first region).
	sourceToInfo := make(map[core.StateID]*transInfo, len(infos))
	for i := range infos {
		sourceToInfo[infos[i].source] = &infos[i]
	}
	newLeaves := make([]core.StateID, 0, len(s.leaves))
	for _, leaf := range s.leaves {
		if info := sourceToInfo[leaf]; info != nil {
			newLeaves = append(newLeaves, info.leafTargets...)
		} else {
			newLeaves = append(newLeaves, leaf)
		}
	}
	s.previousLeaf = prevLeaves[0]
	s.leaves = newLeaves

	// ── 4. Unified entry set (outermost first, deduplicated) ──────────────
	entrySeen := make(map[core.StateID]bool)
	var entryOrder []core.StateID
	for _, info := range infos {
		for _, ep := range info.entryPaths {
			for _, id := range ep {
				if !entrySeen[id] {
					entrySeen[id] = true
					entryOrder = append(entryOrder, id)
				}
			}
		}
	}

	// ── 5. Enter states ────────────────────────────────────────────────────
	for _, id := range entryOrder {
		s.doEntry(id, ev)
		s.startStateTimers(id)
		s.doInvokeEntry(id, ev)
	}

	// ── 6. Inject events raised by transition actions ─────────────────────
	if raised := ac.Drain(); len(raised) > 0 {
		s.internal = append(s.internal, raised...)
	}

	// ── 6.5. Raise done events for compound/parallel states that just completed ─
	s.raiseDoneEvents(entryOrder)

	// ── 7. Publish snapshot ────────────────────────────────────────────────
	// Always notify: applyTransitions is only called when a transition fired.
	// Self-transitions (A→A) leave leaves unchanged but still update context.
	s.storeAndNotify(ev, true)
}

func leavesEqual(a, b []core.StateID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ─── Path helpers ─────────────────────────────────────────────────────────────

// unifiedEntryPaths returns the deduplicated, outermost-first list of states
// to enter when starting from lcca toward a set of leaf targets.
// Used for initial entry and for entering parallel-state targets.
func (s *Service[C]) unifiedEntryPaths(lcca core.StateID, leaves []core.StateID) []core.StateID {
	seen := make(map[core.StateID]bool)
	var result []core.StateID
	for _, leaf := range leaves {
		for _, id := range s.machine.EntryPath(lcca, leaf) {
			if !seen[id] {
				seen[id] = true
				result = append(result, id)
			}
		}
	}
	return result
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

// ─── Invoke helpers ───────────────────────────────────────────────────────────

// doInvokeEntry starts all invoke functions registered for state id.
// Each invoke receives a fresh cancellable context and a send callback that
// routes events through the service mailbox.
func (s *Service[C]) doInvokeEntry(id core.StateID, ev core.Event) {
	fns := s.machine.InvokeFns(id)
	for _, fn := range fns {
		invokeCtx, cancel := context.WithCancel(context.Background())
		s.invokes[id] = append(s.invokes[id], cancel)
		fn(invokeCtx, s.ctx, ev, func(sendEv core.Event) {
			_ = s.Send(sendEv) // best-effort: ignore ErrActorStopped on shutdown
		})
	}
}

// stopStateInvokes cancels invocations scoped to state id only.
// Used during hierarchical transitions to leave ancestor invokes running.
func (s *Service[C]) stopStateInvokes(id core.StateID) {
	for _, cancel := range s.invokes[id] {
		cancel()
	}
	delete(s.invokes, id)
}

// stopInvokes cancels every active invocation (used on shutdown).
func (s *Service[C]) stopInvokes() {
	for id, cancels := range s.invokes {
		for _, cancel := range cancels {
			cancel()
		}
		delete(s.invokes, id)
	}
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

// ─── Done-event helpers ───────────────────────────────────────────────────────

// raiseDoneEvents checks each newly-entered state's compound ancestors and
// injects a "done.state.X" internal event for any that just completed.
// Scoping to `entered` (rather than all leaves) ensures done events only fire
// once per macrostep — when a relevant descendant was actually entered.
func (s *Service[C]) raiseDoneEvents(entered []core.StateID) {
	checked := make(map[core.StateID]bool)
	for _, id := range entered {
		for cur := id; ; {
			par, ok := s.machine.Parent(cur)
			if !ok {
				break
			}
			if checked[par] {
				break
			}
			checked[par] = true
			if s.compoundIsDone(par) {
				s.internal = append(s.internal, core.E("done.state."+string(par)))
			}
			cur = par
		}
	}
}

// compoundIsDone reports whether compound state id has completed:
//   - compound OR: the active leaf in its subtree is a final state
//   - parallel AND: every region contains at least one active final leaf
func (s *Service[C]) compoundIsDone(id core.StateID) bool {
	if !s.machine.IsCompound(id) {
		return false
	}
	if s.machine.IsParallel(id) {
		for _, regionID := range s.machine.Children(id) {
			regionDone := false
			for _, leaf := range s.leaves {
				if (leaf == regionID || s.machine.IsDescendantOf(leaf, regionID)) && s.machine.IsFinal(leaf) {
					regionDone = true
					break
				}
			}
			if !regionDone {
				return false
			}
		}
		return true
	}
	// Compound OR: find the active leaf inside this subtree.
	for _, leaf := range s.leaves {
		if s.machine.IsDescendantOf(leaf, id) {
			return s.machine.IsFinal(leaf)
		}
	}
	return false
}

// ─── Snapshot helpers ─────────────────────────────────────────────────────────

func (s *Service[C]) storeSnapshot(ev core.Event, changed bool) {
	// Final = true only when ALL active leaves are in final states.
	allFinal := len(s.leaves) > 0
	for _, leaf := range s.leaves {
		if !s.machine.IsFinal(leaf) {
			allFinal = false
			break
		}
	}
	leaves := make([]core.StateID, len(s.leaves))
	copy(leaves, s.leaves)
	s.snap.Store(Snapshot[C]{
		State:         s.leaves[0],
		Leaves:        leaves,
		ActiveStates:  s.machine.ConfigurationOf(s.leaves),
		PreviousState: s.previousLeaf,
		Context:       s.ctx,
		Event:         ev,
		Changed:       changed,
		Final:         allFinal,
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
