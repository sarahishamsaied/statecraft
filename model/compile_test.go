package model_test

import (
	"errors"
	"statecraft/core"
	"statecraft/model"
	"testing"
)

func TestCompile_Valid(t *testing.T) {
	m, err := model.New[struct{}]("light").
		Initial("red").
		State("red", func(s *model.StateBuilder[struct{}]) {
			s.On("TIMER", "green")
		}).
		State("green", func(s *model.StateBuilder[struct{}]) {
			s.On("TIMER", "red")
		}).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ID() != "light" {
		t.Errorf("ID = %q, want %q", m.ID(), "light")
	}
	if m.InitialState() != "red" {
		t.Errorf("InitialState = %q, want %q", m.InitialState(), "red")
	}
	ids := m.StateIDs()
	if len(ids) != 2 {
		t.Errorf("len(StateIDs) = %d, want 2", len(ids))
	}
}

func TestCompile_EmptyID(t *testing.T) {
	_, err := model.New[struct{}]("").Initial("a").State("a").Build()
	assertErr(t, err, core.ErrInvalidMachine)
}

func TestCompile_NoStates(t *testing.T) {
	_, err := model.New[struct{}]("m").Initial("a").Build()
	assertErr(t, err, core.ErrInvalidMachine)
}

func TestCompile_NoInitial(t *testing.T) {
	_, err := model.New[struct{}]("m").State("a").Build()
	assertErr(t, err, core.ErrNoInitialState)
}

func TestCompile_UnknownInitial(t *testing.T) {
	_, err := model.New[struct{}]("m").Initial("missing").State("a").Build()
	assertErr(t, err, core.ErrUnknownState)
}

func TestCompile_DuplicateState(t *testing.T) {
	_, err := model.New[struct{}]("m").
		Initial("a").
		State("a").
		State("a").
		Build()
	assertErr(t, err, core.ErrDuplicateState)
}

func TestCompile_UnknownTarget(t *testing.T) {
	_, err := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.On("GO", "missing")
		}).
		Build()
	assertErr(t, err, core.ErrUnknownTarget)
}

func TestCompile_AfterUnknownTarget(t *testing.T) {
	_, err := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.After(5e8, "missing") // 500ms
		}).
		Build()
	assertErr(t, err, core.ErrUnknownTarget)
}

func TestResolveTransition_FirstGuardWins(t *testing.T) {
	type Ctx struct{ Count int }

	m := model.New[Ctx]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[Ctx]) {
			// Two transitions on the same event — guarded chain.
			s.On("GO", "b", model.When[Ctx](func(c Ctx, _ core.Event) bool { return c.Count > 5 }))
			s.On("GO", "c") // fallback (no guard)
		}).
		State("b").
		State("c").
		MustBuild()

	low := Ctx{Count: 3}
	high := Ctx{Count: 10}

	target, _, ok := m.ResolveTransition("a", low, core.E("GO"))
	if !ok || target != "c" {
		t.Errorf("low count: got (%q, %v), want (c, true)", target, ok)
	}

	target, _, ok = m.ResolveTransition("a", high, core.E("GO"))
	if !ok || target != "b" {
		t.Errorf("high count: got (%q, %v), want (b, true)", target, ok)
	}
}

func TestResolveTransition_NoMatch(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("a").
		State("a").
		MustBuild()

	_, _, ok := m.ResolveTransition("a", struct{}{}, core.E("UNKNOWN"))
	if ok {
		t.Error("expected no match for unregistered event")
	}
}

func TestIsFinal(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("a").
		State("a", func(s *model.StateBuilder[struct{}]) {
			s.On("DONE", "end")
		}).
		State("end", func(s *model.StateBuilder[struct{}]) {
			s.Final()
		}).
		MustBuild()

	if m.IsFinal("a") {
		t.Error("a should not be final")
	}
	if !m.IsFinal("end") {
		t.Error("end should be final")
	}
}

func TestMustBuild_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from MustBuild on invalid machine")
		}
	}()
	model.New[struct{}]("").MustBuild()
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func assertErr(t *testing.T, err error, target error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error wrapping %v, got nil", target)
	}
	if !errors.Is(err, target) {
		t.Fatalf("expected error wrapping %v, got: %v", target, err)
	}
}
