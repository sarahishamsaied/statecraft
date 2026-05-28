package actor_test

import (
	"context"
	"statecraft/actor"
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"sync"
	"testing"
	"time"
)

// ─── fixtures ─────────────────────────────────────────────────────────────────

type lightCtx struct{ Cycles int }

var trafficLight = model.New[lightCtx]("traffic").
	Context(lightCtx{}).
	Initial("red").
	State("red", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "green") }).
	State("green", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "yellow") }).
	State("yellow", func(s *model.StateBuilder[lightCtx]) { s.On("TIMER", "red") }).
	MustBuild()

// panicMachine has:
//   - "idle" → BOOM → "boom" via a panicking action
//   - "idle" → OK  → "done" (final)
func panicMachine() *model.Machine[struct{}] {
	panicAction := model.ActionFn[struct{}](func(_ struct{}, _ core.Event, _ *model.ActionContext) struct{} {
		panic("intentional test panic")
	})
	return model.New[struct{}]("panicky").
		Initial("idle").
		State("idle", func(s *model.StateBuilder[struct{}]) {
			s.On("BOOM", "boom", model.Do(panicAction))
			s.On("OK", "done")
		}).
		State("boom").
		State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()
}

// ─── basic spawn / send / stop ────────────────────────────────────────────────

func TestSpawn_BasicSendAndStop(t *testing.T) {
	sys := actor.NewSystem()
	ref := actor.MustSpawn(sys, "light", trafficLight)

	if !ref.Snapshot().Is("red") {
		t.Fatalf("initial state = %q, want red", ref.Snapshot().State)
	}

	ch := awaitState(ref, "green")
	mustSend(t, ref, core.E("TIMER"))
	waitSnap(t, ch, "green")

	sys.Stop()
}

func TestSpawn_DuplicateIDReturnsError(t *testing.T) {
	sys := actor.NewSystem()
	actor.MustSpawn(sys, "light", trafficLight)
	defer sys.Stop()

	_, err := actor.Spawn(sys, "light", trafficLight)
	if err == nil {
		t.Fatal("expected error for duplicate actor id, got nil")
	}
}

func TestSpawn_StopRef(t *testing.T) {
	sys := actor.NewSystem()
	ref := actor.MustSpawn(sys, "light", trafficLight)

	ref.Stop()

	select {
	case <-ref.Done():
	case <-time.After(time.Second):
		t.Fatal("ref.Done() not closed after Stop()")
	}
}

func TestSystem_StopAllActors(t *testing.T) {
	sys := actor.NewSystem()
	refs := [3]*actor.Ref[lightCtx]{
		actor.MustSpawn(sys, "a", trafficLight),
		actor.MustSpawn(sys, "b", trafficLight),
		actor.MustSpawn(sys, "c", trafficLight),
	}

	sys.Stop()

	for _, r := range refs {
		select {
		case <-r.Done():
		case <-time.After(time.Second):
			t.Fatalf("actor %q not stopped after system stop", r.ID())
		}
	}
}

func TestSystem_StopIdempotent(t *testing.T) {
	sys := actor.NewSystem()
	actor.MustSpawn(sys, "light", trafficLight)
	sys.Stop()
	sys.Stop() // must not deadlock or panic
}

// ─── supervision: NoRestart ───────────────────────────────────────────────────

func TestSupervision_NoRestart_StopsOnPanic(t *testing.T) {
	sys := actor.NewSystem()
	ref := actor.MustSpawn(sys, "panicky", panicMachine()) // default: NoRestart
	defer sys.Stop()

	mustSend(t, ref, core.E("BOOM"))

	select {
	case <-ref.Done():
	case <-time.After(time.Second):
		t.Fatal("actor not stopped after panic with NoRestart strategy")
	}

	if ref.Err() == nil {
		t.Error("Ref.Err() should be non-nil after panic with NoRestart")
	}
}

// ─── supervision: RestartAlways ───────────────────────────────────────────────

func TestSupervision_RestartAlways_RestartsOnPanic(t *testing.T) {
	sys := actor.NewSystem()
	ref := actor.MustSpawn(sys, "panicky", panicMachine(),
		actor.WithStrategy(actor.RestartAlways, 0))
	defer sys.Stop()

	mustSend(t, ref, core.E("BOOM"))

	// ref.Done() must NOT close , the actor should restart, not stop permanently.
	select {
	case <-ref.Done():
		t.Fatal("actor stopped instead of restarting with RestartAlways")
	case <-time.After(100 * time.Millisecond):
		// Good , still running
	}

	// Wait for restart then subscribe on the new service instance and verify.
	time.Sleep(20 * time.Millisecond)
	ch := awaitState(ref, "done")
	mustSend(t, ref, core.E("OK"))
	waitSnap(t, ch, "done")
}

// ─── supervision: RestartN ────────────────────────────────────────────────────

func TestSupervision_RestartN_RespectsLimit(t *testing.T) {
	sys := actor.NewSystem()
	// Allow 2 restarts total (3 lives: initial + 2 restarts).
	ref := actor.MustSpawn(sys, "panicky", panicMachine(),
		actor.WithStrategy(actor.RestartN, 2))
	defer sys.Stop()

	// Trigger 3 panics , the third exhausts the restart budget.
	for range 3 {
		mustSend(t, ref, core.E("BOOM"))
		time.Sleep(40 * time.Millisecond)
	}

	select {
	case <-ref.Done():
	case <-time.After(time.Second):
		t.Fatal("actor not stopped after exceeding RestartN limit")
	}
}

// ─── actor-to-actor messaging ─────────────────────────────────────────────────

func TestActorToActor_InvokeMessaging(t *testing.T) {
	// "dst" waits for a PING event then moves to "done".
	// "src" on entry to its initial state invokes a function that immediately
	// sends PING to dst , demonstrating actor-to-actor communication.
	type srcCtx struct{}
	type dstCtx struct{}

	dstMachine := model.New[dstCtx]("dst").
		Initial("waiting").
		State("waiting", func(s *model.StateBuilder[dstCtx]) {
			s.On("PING", "done")
		}).
		State("done", func(s *model.StateBuilder[dstCtx]) { s.Final() }).
		MustBuild()

	sys := actor.NewSystem()
	dstRef := actor.MustSpawn(sys, "dst", dstMachine)
	defer sys.Stop()

	ch := awaitState(dstRef, "done")

	// Build srcMachine after dstRef is ready so the closure captures a live ref.
	srcMachine := model.New[srcCtx]("src").
		Initial("sending").
		State("sending", func(s *model.StateBuilder[srcCtx]) {
			s.Invoke(func(_ context.Context, _ srcCtx, _ core.Event, _ func(core.Event)) {
				// Send directly to dstRef via the captured typed Ref.
				_ = dstRef.Send(core.E("PING"))
			})
		}).
		MustBuild()

	actor.MustSpawn(sys, "src", srcMachine)

	waitSnap(t, ch, "done")
}

// ─── Ref.Subscribe ────────────────────────────────────────────────────────────

func TestRef_Subscribe(t *testing.T) {
	sys := actor.NewSystem()
	ref := actor.MustSpawn(sys, "light", trafficLight)
	defer sys.Stop()

	var mu sync.Mutex
	var seen []string

	unsub := ref.Subscribe(func(snap runtime.Snapshot[lightCtx]) {
		mu.Lock()
		seen = append(seen, string(snap.State))
		mu.Unlock()
	})
	defer unsub()

	for range 3 {
		mustSend(t, ref, core.E("TIMER"))
	}
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	n := len(seen)
	mu.Unlock()
	if n < 3 {
		t.Errorf("subscriber received %d snapshots, want at least 3", n)
	}
}

// ─── runtime: panic recovery (Service.Err) ───────────────────────────────────

func TestService_PanicRecovery_ErrNotNil(t *testing.T) {
	svc := runtime.Start(panicMachine())

	if err := svc.Send(core.E("BOOM")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case <-svc.Done():
	case <-time.After(time.Second):
		t.Fatal("service did not stop after panic")
	}

	if svc.Err() == nil {
		t.Error("Service.Err() should be non-nil after a panic in the run goroutine")
	}
}

func TestService_CleanStop_ErrNil(t *testing.T) {
	m := model.New[struct{}]("m").Initial("a").State("a").MustBuild()
	svc := runtime.Start(m)
	svc.Stop()
	if err := svc.Err(); err != nil {
		t.Errorf("Service.Err() = %v after clean stop, want nil", err)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func mustSend(t *testing.T, ref interface{ Send(core.Event) error }, ev core.Event) {
	t.Helper()
	if err := ref.Send(ev); err != nil {
		t.Fatalf("Send(%q): %v", ev.Type(), err)
	}
}

func awaitState[C any](ref *actor.Ref[C], id string) <-chan runtime.Snapshot[C] {
	ch := make(chan runtime.Snapshot[C], 1)
	var once sync.Once
	ref.Subscribe(func(snap runtime.Snapshot[C]) {
		if snap.Is(id) {
			once.Do(func() { ch <- snap })
		}
	})
	return ch
}

func waitSnap[C any](t *testing.T, ch <-chan runtime.Snapshot[C], want string) {
	t.Helper()
	select {
	case snap := <-ch:
		if !snap.Is(want) {
			t.Errorf("state = %q, want %q", snap.State, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for state %q", want)
	}
}
