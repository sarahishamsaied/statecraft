// Package viz generates visual representations of compiled machines.
// It depends only on the model layer — never on the runtime.
package viz

import (
	"fmt"
	"statecraft/model"
	"strings"
)

// ToMermaid returns a Mermaid stateDiagram-v2 string for the given machine.
//
// Paste the output into https://mermaid.live or any Mermaid-aware renderer.
//
// Example for a traffic-light machine:
//
//	stateDiagram-v2
//	    direction LR
//	    [*] --> red
//	    red --> green : TIMER
//	    green --> yellow : TIMER
//	    yellow --> red : TIMER
func ToMermaid[C any](m *model.Machine[C]) string {
	var b strings.Builder

	b.WriteString("stateDiagram-v2\n")
	b.WriteString("    direction LR\n")

	// Initial pseudo-state.
	fmt.Fprintf(&b, "    [*] --> %s\n", m.InitialState())

	// Final states.
	for _, id := range m.StateIDs() {
		if m.IsFinal(id) {
			fmt.Fprintf(&b, "    %s --> [*]\n", id)
		}
	}

	// All transitions (regular + after).
	for _, t := range m.Transitions() {
		var label string
		if t.IsAfter {
			label = fmt.Sprintf("after(%s)", t.Delay)
		} else {
			label = t.Event
			if t.HasGuard {
				label += " [guard]"
			}
		}
		fmt.Fprintf(&b, "    %s --> %s : %s\n", t.From, t.To, label)
	}

	return b.String()
}

// ToGraphviz returns a Graphviz DOT graph for the given machine.
//
// Render with: dot -Tsvg -o out.svg <(statecraft viz graphviz)
// or paste into https://dreampuf.github.io/GraphvizOnline/
func ToGraphviz[C any](m *model.Machine[C]) string {
	var b strings.Builder

	fmt.Fprintf(&b, "digraph %q {\n", m.ID())
	b.WriteString("    rankdir=LR;\n")
	b.WriteString("    node [shape=circle fontname=\"Helvetica\"];\n")
	b.WriteString(`    __start [shape=point];` + "\n")
	fmt.Fprintf(&b, "    __start -> %q;\n", m.InitialState())

	// State nodes — double circle for final states.
	for _, id := range m.StateIDs() {
		shape := "circle"
		if m.IsFinal(id) {
			shape = "doublecircle"
		}
		fmt.Fprintf(&b, "    %q [shape=%s];\n", id, shape)
	}

	// Transitions.
	for _, t := range m.Transitions() {
		var label string
		if t.IsAfter {
			label = fmt.Sprintf("after(%s)", t.Delay)
		} else {
			label = t.Event
			if t.HasGuard {
				label += "\\n[guard]"
			}
		}
		fmt.Fprintf(&b, "    %q -> %q [label=%q];\n", t.From, t.To, label)
	}

	b.WriteString("}\n")
	return b.String()
}
