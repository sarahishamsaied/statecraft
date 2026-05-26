// Package bench measures the performance of core statecraft operations.
//
// Run with:
//
//	go test -bench=. -benchmem ./bench/
package bench

import (
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"testing"
)

// ─── Machines ─────────────────────────────────────────────────────────────────

var flatToggle = model.New[struct{}]("toggle").
	Initial("off").
	State("off", func(s *model.StateBuilder[struct{}]) { s.On("T", "on") }).
	State("on", func(s *model.StateBuilder[struct{}]) { s.On("T", "off") }).
	MustBuild()

var guardedChain = func() *model.Machine[int] {
	// 5 guarded transitions on the same event — tests guard evaluation cost.
	b := model.New[int]("guarded").Initial("s0")
	prev := "s0"
	b.State("s0")
	for i := 1; i <= 5; i++ {
		next := "s0"
		if i < 5 {
			next = prev
		}
		target := prev
		threshold := i - 1
		b.State(prev, func(s *model.StateBuilder[int]) {
			_ = target
			_ = threshold
			_ = next
		})
		prev = prev // avoid capture confusion
	}
	// Simpler: just build a 5-state linear chain.
	return model.New[int]("chain").
		Initial("a").
		State("a", func(s *model.StateBuilder[int]) {
			s.On("NEXT", "b", model.When[int](func(c int, _ core.Event) bool { return c >= 1 }))
			s.On("NEXT", "a") // fallback
		}).
		State("b", func(s *model.StateBuilder[int]) { s.On("NEXT", "a") }).
		MustBuild()
}()

// ─── Benchmarks ───────────────────────────────────────────────────────────────

// BenchmarkCompile measures machine compilation (definition → compiled tree).
// This runs once at startup; the cost matters for large machines.
func BenchmarkCompile(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = model.New[struct{}]("light").
			Initial("red").
			State("red", func(s *model.StateBuilder[struct{}]) { s.On("T", "green") }).
			State("green", func(s *model.StateBuilder[struct{}]) { s.On("T", "yellow") }).
			State("yellow", func(s *model.StateBuilder[struct{}]) { s.On("T", "red") }).
			MustBuild()
	}
}

// BenchmarkResolveTransition measures only the transition-resolution hot path —
// no goroutines, no channels. This is the algorithmic baseline.
func BenchmarkResolveTransition(b *testing.B) {
	m := flatToggle
	ctx := struct{}{}
	ev := core.E("T")
	state := m.InitialState()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		target, _, ok := m.ResolveTransition(state, ctx, ev)
		if ok {
			state = target
		}
	}
}

// BenchmarkSendThroughput measures end-to-end event delivery:
// external goroutine → channel → interpreter goroutine → state update.
// This includes goroutine scheduling overhead on top of pure transition cost.
func BenchmarkSendThroughput(b *testing.B) {
	svc := runtime.Start(flatToggle)
	b.Cleanup(svc.Stop)

	ev := core.E("T")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = svc.Send(ev)
	}
}

// BenchmarkSendParallel measures throughput under concurrent senders.
// Because the interpreter is single-goroutine, this benchmark isolates
// channel contention cost.
func BenchmarkSendParallel(b *testing.B) {
	svc := runtime.Start(flatToggle, runtime.WithMailboxSize(1024))
	b.Cleanup(svc.Stop)

	ev := core.E("T")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = svc.Send(ev)
		}
	})
}

// BenchmarkSnapshot measures snapshot reads from outside the interpreter
// goroutine. This exercises the atomic.Value load path.
func BenchmarkSnapshot(b *testing.B) {
	svc := runtime.Start(flatToggle)
	b.Cleanup(svc.Stop)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = svc.Snapshot()
	}
}

// BenchmarkContextAssign measures Assign action cost: allocation + copy.
func BenchmarkContextAssign(b *testing.B) {
	type BigCtx struct {
		A, B, C, D, E int
		Name           string
	}
	m := model.New[BigCtx]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[BigCtx]) {
			s.On("T", "b", model.Do(
				model.Assign(func(c BigCtx, _ core.Event) BigCtx {
					c.A++
					return c
				}),
			))
		}).
		State("b", func(s *model.StateBuilder[BigCtx]) {
			s.On("T", "a")
		}).
		MustBuild()

	svc := runtime.Start(m)
	b.Cleanup(svc.Stop)
	ev := core.E("T")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = svc.Send(ev)
	}
}
