package viz_test

import (
	"statecraft/model"
	"statecraft/viz"
	"strings"
	"testing"
)

var lightMachine = model.New[struct{}]("traffic-light").
	Initial("red").
	State("red", func(s *model.StateBuilder[struct{}]) { s.On("TIMER", "green") }).
	State("green", func(s *model.StateBuilder[struct{}]) { s.On("TIMER", "yellow") }).
	State("yellow", func(s *model.StateBuilder[struct{}]) { s.On("TIMER", "red") }).
	MustBuild()

func TestToMermaid_ContainsInitialArrow(t *testing.T) {
	out := viz.ToMermaid(lightMachine)
	if !strings.Contains(out, "[*] --> red") {
		t.Errorf("missing initial arrow in:\n%s", out)
	}
}

func TestToMermaid_ContainsTransitions(t *testing.T) {
	out := viz.ToMermaid(lightMachine)
	for _, want := range []string{
		"red --> green : TIMER",
		"green --> yellow : TIMER",
		"yellow --> red : TIMER",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestToMermaid_FinalState(t *testing.T) {
	m := model.New[struct{}]("m").
		Initial("active").
		State("active", func(s *model.StateBuilder[struct{}]) { s.On("DONE", "end") }).
		State("end", func(s *model.StateBuilder[struct{}]) { s.Final() }).
		MustBuild()

	out := viz.ToMermaid(m)
	if !strings.Contains(out, "end --> [*]") {
		t.Errorf("missing final state arrow in:\n%s", out)
	}
}

func TestToGraphviz_ContainsExpectedLines(t *testing.T) {
	out := viz.ToGraphviz(lightMachine)
	for _, want := range []string{
		`digraph "traffic-light"`,
		`rankdir=LR`,
		`__start -> "red"`,
		`"red" -> "green"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in DOT output:\n%s", want, out)
		}
	}
}

// ─── compound states ──────────────────────────────────────────────────────────

var compoundMachine = model.New[struct{}]("m").
	Initial("active").
	State("active", func(s *model.StateBuilder[struct{}]) {
		s.State("idle", func(s *model.StateBuilder[struct{}]) {
			s.On("RUN", "running")
		})
		s.State("running", func(s *model.StateBuilder[struct{}]) {
			s.On("STOP", "idle")
		})
		s.On("CANCEL", "done")
	}).
	State("done", func(s *model.StateBuilder[struct{}]) { s.Final() }).
	MustBuild()

func TestToMermaid_CompoundStateBlock(t *testing.T) {
	out := viz.ToMermaid(compoundMachine)

	for _, want := range []string{
		"state active {",
		"[*] --> idle",
		"}",
		"idle --> running : RUN",
		"active --> done : CANCEL",
		"done --> [*]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestToGraphviz_CompoundCluster(t *testing.T) {
	out := viz.ToGraphviz(compoundMachine)

	for _, want := range []string{
		`subgraph "cluster_active"`,
		`label="active"`,
		`"idle" [shape=circle]`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Transition from compound state uses ltail.
	if !strings.Contains(out, `ltail="cluster_active"`) {
		t.Errorf("missing ltail for compound source in:\n%s", out)
	}
}

// ─── parallel states ──────────────────────────────────────────────────────────

var parallelMachine = model.New[struct{}]("form").
	Initial("par").
	Parallel("par", func(s *model.StateBuilder[struct{}]) {
		s.State("field1", func(s *model.StateBuilder[struct{}]) {
			s.State("f1pristine", func(s *model.StateBuilder[struct{}]) {
				s.On("EDIT1", "f1dirty")
			})
			s.State("f1dirty")
		})
		s.State("field2", func(s *model.StateBuilder[struct{}]) {
			s.State("f2pristine", func(s *model.StateBuilder[struct{}]) {
				s.On("EDIT2", "f2dirty")
			})
			s.State("f2dirty")
		})
		s.On("SUBMIT", "submitted")
	}).
	State("submitted", func(s *model.StateBuilder[struct{}]) { s.Final() }).
	MustBuild()

func TestToMermaid_ParallelRegionSeparator(t *testing.T) {
	out := viz.ToMermaid(parallelMachine)

	for _, want := range []string{
		"state par {",
		"state field1 {",
		"[*] --> f1pristine",
		"--",
		"state field2 {",
		"[*] --> f2pristine",
		"par --> submitted : SUBMIT",
		"submitted --> [*]",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestToGraphviz_ParallelCluster(t *testing.T) {
	out := viz.ToGraphviz(parallelMachine)

	for _, want := range []string{
		`subgraph "cluster_par"`,
		`subgraph "cluster_field1"`,
		`subgraph "cluster_field2"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// ─── snapshot tests ───────────────────────────────────────────────────────────

func TestToMermaid_Snapshot(t *testing.T) {
	out := viz.ToMermaid(lightMachine)
	t.Logf("Mermaid output:\n%s", out)
}

func TestToGraphviz_Snapshot(t *testing.T) {
	out := viz.ToGraphviz(lightMachine)
	t.Logf("Graphviz DOT output:\n%s", out)
}

func TestToMermaid_CompoundSnapshot(t *testing.T) {
	out := viz.ToMermaid(compoundMachine)
	t.Logf("Compound Mermaid output:\n%s", out)
}

func TestToMermaid_ParallelSnapshot(t *testing.T) {
	out := viz.ToMermaid(parallelMachine)
	t.Logf("Parallel Mermaid output:\n%s", out)
}

func TestToGraphviz_ParallelSnapshot(t *testing.T) {
	out := viz.ToGraphviz(parallelMachine)
	t.Logf("Parallel Graphviz DOT output:\n%s", out)
}
