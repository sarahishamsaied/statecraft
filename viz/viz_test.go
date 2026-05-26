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

func TestToMermaid_Snapshot(t *testing.T) {
	out := viz.ToMermaid(lightMachine)
	t.Logf("Mermaid output:\n%s", out)
}

func TestToGraphviz_Snapshot(t *testing.T) {
	out := viz.ToGraphviz(lightMachine)
	t.Logf("Graphviz DOT output:\n%s", out)
}
