// Package viz generates visual representations of compiled machines.
// It depends only on the model layer — never on the runtime.
package viz

import (
	"fmt"
	"statecraft/core"
	"statecraft/model"
	"strings"
)

// ToMermaid returns a Mermaid stateDiagram-v2 string for the given machine.
// Compound states are rendered as nested blocks; parallel states use the
// Mermaid `--` region separator.
//
// Paste the output into https://mermaid.live or any Mermaid-aware renderer.
func ToMermaid[C any](m *model.Machine[C]) string {
	var b strings.Builder

	b.WriteString("stateDiagram-v2\n")
	b.WriteString("    direction LR\n")

	// Top-level initial arrow (points to the declared initial state which may
	// itself be compound; Mermaid renders the arrow into the state block).
	fmt.Fprintf(&b, "    [*] --> %s\n", m.InitialState())

	// Compound/parallel state structure blocks (top-level only; children are
	// rendered recursively).
	for _, id := range m.StateIDs() {
		if _, hasParent := m.Parent(id); hasParent {
			continue
		}
		if m.IsCompound(id) {
			mermaidStateBlock(&b, m, id, "    ")
		}
	}

	// Final state arrows.
	for _, id := range m.StateIDs() {
		if m.IsFinal(id) {
			fmt.Fprintf(&b, "    %s --> [*]\n", id)
		}
	}

	// All transitions (regular, after, always) at the top level — Mermaid
	// resolves nested state IDs regardless of where the line is declared.
	for _, t := range m.Transitions() {
		label := mermaidLabel(t)
		fmt.Fprintf(&b, "    %s --> %s : %s\n", t.From, t.To, label)
	}

	return b.String()
}

// mermaidStateBlock emits a `state X { ... }` block with correct inner content
// for both compound OR states (single-active child) and parallel AND states
// (concurrent regions separated by `--`).
func mermaidStateBlock[C any](b *strings.Builder, m *model.Machine[C], id core.StateID, indent string) {
	inner := indent + "    "
	fmt.Fprintf(b, "%sstate %s {\n", indent, id)

	if m.IsParallel(id) {
		children := m.Children(id)
		for i, child := range children {
			if m.IsCompound(child) {
				mermaidStateBlock(b, m, child, inner)
			}
			if i < len(children)-1 {
				fmt.Fprintf(b, "%s--\n", inner)
			}
		}
	} else {
		if init, ok := m.InitialChild(id); ok {
			fmt.Fprintf(b, "%s[*] --> %s\n", inner, init)
		}
		for _, child := range m.Children(id) {
			if m.IsCompound(child) {
				mermaidStateBlock(b, m, child, inner)
			}
		}
	}

	fmt.Fprintf(b, "%s}\n", indent)
}

func mermaidLabel(t model.TransitionInfo) string {
	var label string
	switch {
	case t.IsAlways:
		label = "always"
	case t.IsAfter:
		label = fmt.Sprintf("after(%s)", t.Delay)
	default:
		label = t.Event
	}
	if t.HasGuard {
		label += " [guard]"
	}
	return label
}

// ToGraphviz returns a Graphviz DOT graph for the given machine.
// Compound and parallel states are rendered as named subgraph clusters.
// Edges that cross cluster boundaries use `lhead`/`ltail` to attach to the
// cluster bounding box rather than the inner leaf node.
//
// Render with: dot -Tsvg -o out.svg <(statecraft viz graphviz)
// or paste into https://dreampuf.github.io/GraphvizOnline/
func ToGraphviz[C any](m *model.Machine[C]) string {
	var b strings.Builder

	fmt.Fprintf(&b, "digraph %q {\n", m.ID())
	b.WriteString("    compound=true;\n")
	b.WriteString("    rankdir=LR;\n")
	b.WriteString("    node [shape=circle fontname=\"Helvetica\"];\n")
	b.WriteString("    __start [shape=point];\n")

	// State structure: atomic states as nodes, compound states as clusters.
	for _, id := range m.StateIDs() {
		if _, hasParent := m.Parent(id); hasParent {
			continue
		}
		graphvizStateNode(&b, m, id, "    ")
	}

	// Initial arrow — if the initial state is compound, attach to its cluster.
	initID := m.InitialState()
	initLeaf := string(m.LeafTarget(initID))
	if m.IsCompound(initID) {
		fmt.Fprintf(&b, "    __start -> %q [lhead=%q];\n", initLeaf, "cluster_"+string(initID))
	} else {
		fmt.Fprintf(&b, "    __start -> %q;\n", initLeaf)
	}

	// Transitions.
	for _, t := range m.Transitions() {
		label := graphvizLabel(t)

		fromNode, fromCluster := graphvizEdgeEndpoint(m, core.StateID(t.From))
		toNode, toCluster := graphvizEdgeEndpoint(m, core.StateID(t.To))

		var attrs []string
		attrs = append(attrs, fmt.Sprintf("label=%q", label))
		if fromCluster != "" {
			attrs = append(attrs, fmt.Sprintf("ltail=%q", fromCluster))
		}
		if toCluster != "" {
			attrs = append(attrs, fmt.Sprintf("lhead=%q", toCluster))
		}
		fmt.Fprintf(&b, "    %q -> %q [%s];\n", fromNode, toNode, strings.Join(attrs, " "))
	}

	b.WriteString("}\n")
	return b.String()
}

// graphvizStateNode emits either an atomic state node or a compound/parallel
// subgraph cluster, recursing into children.
func graphvizStateNode[C any](b *strings.Builder, m *model.Machine[C], id core.StateID, indent string) {
	if !m.IsCompound(id) {
		shape := "circle"
		if m.IsFinal(id) {
			shape = "doublecircle"
		}
		fmt.Fprintf(b, "%s%q [shape=%s];\n", indent, string(id), shape)
		return
	}

	fillColor := "\"#e8e8e8\""
	if m.IsParallel(id) {
		fillColor = "\"#fffacd\""
	}
	fmt.Fprintf(b, "%ssubgraph %q {\n", indent, "cluster_"+string(id))
	fmt.Fprintf(b, "%s    label=%q;\n", indent, string(id))
	fmt.Fprintf(b, "%s    style=filled;\n", indent)
	fmt.Fprintf(b, "%s    fillcolor=%s;\n", indent, fillColor)
	for _, child := range m.Children(id) {
		graphvizStateNode(b, m, child, indent+"    ")
	}
	fmt.Fprintf(b, "%s}\n", indent)
}

// graphvizEdgeEndpoint returns the node name and optional cluster attribute
// for an edge endpoint. Compound states resolve to their initial leaf (the
// actual node Graphviz draws), with the cluster name supplied separately so
// the caller can add lhead/ltail.
func graphvizEdgeEndpoint[C any](m *model.Machine[C], id core.StateID) (node, cluster string) {
	if m.IsCompound(id) {
		return string(m.LeafTarget(id)), "cluster_" + string(id)
	}
	return string(id), ""
}

func graphvizLabel(t model.TransitionInfo) string {
	switch {
	case t.IsAlways:
		return "always"
	case t.IsAfter:
		return fmt.Sprintf("after(%s)", t.Delay)
	default:
		label := t.Event
		if t.HasGuard {
			label += "\\n[guard]"
		}
		return label
	}
}
