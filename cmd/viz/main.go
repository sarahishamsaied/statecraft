// cmd/viz generates SVG diagrams for all statecraft example machines.
//
// Usage:
//
//	go run ./cmd/viz
//
// SVGs are written to docs/diagrams/<machine-id>.svg.
// Requires graphviz: brew install graphviz
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"statecraft"
	"statecraft/model"
	"statecraft/viz"
	"strings"
	"time"
)

func main() {
	if _, err := exec.LookPath("dot"); err != nil {
		fmt.Fprintln(os.Stderr, "error: graphviz not found — install with: brew install graphviz")
		os.Exit(1)
	}

	outDir := "docs/diagrams"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", outDir, err)
		os.Exit(1)
	}

	machines := []struct {
		dot string
		id  string
	}{
		{viz.ToGraphviz(trafficLight()), "traffic-light"},
		{viz.ToGraphviz(authFlow()), "auth-flow"},
		{viz.ToGraphviz(docEditor()), "doc-editor"},
	}

	for _, m := range machines {
		out := filepath.Join(outDir, m.id+".svg")
		if err := renderSVG(m.dot, out); err != nil {
			fmt.Fprintf(os.Stderr, "render %s: %v\n", m.id, err)
			os.Exit(1)
		}
		fmt.Printf("wrote %s\n", out)
	}
}

func renderSVG(dot, outPath string) error {
	cmd := exec.Command("dot", "-Tsvg", "-o", outPath)
	cmd.Stdin = strings.NewReader(dot)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ─── machine definitions ──────────────────────────────────────────────────────

type lightCtx struct{ Cycles int }

func trafficLight() *model.Machine[lightCtx] {
	return statecraft.New[lightCtx]("traffic-light").
		Context(lightCtx{}).
		Initial("red").
		State("red", func(s *statecraft.StateBuilder[lightCtx]) {
			s.On("TIMER", "green",
				statecraft.Do(statecraft.Assign(func(c lightCtx, _ statecraft.Event) lightCtx {
					c.Cycles++
					return c
				})),
			)
		}).
		State("green", func(s *statecraft.StateBuilder[lightCtx]) {
			s.On("TIMER", "yellow")
		}).
		State("yellow", func(s *statecraft.StateBuilder[lightCtx]) {
			s.On("TIMER", "red")
		}).
		MustBuild()
}

type authCtx struct {
	Attempts int
	User     string
}

func authFlow() *model.Machine[authCtx] {
	tooManyAttempts := statecraft.When[authCtx](func(c authCtx, _ statecraft.Event) bool {
		return c.Attempts >= 3
	})
	return statecraft.New[authCtx]("auth-flow").
		Context(authCtx{}).
		Initial("idle").
		State("idle", func(s *statecraft.StateBuilder[authCtx]) {
			s.On("LOGIN", "authenticating")
		}).
		State("authenticating", func(s *statecraft.StateBuilder[authCtx]) {
			s.On("SUCCESS", "dashboard")
			s.On("FAILURE", "locked", tooManyAttempts,
				statecraft.Do(statecraft.Assign(func(c authCtx, _ statecraft.Event) authCtx {
					c.Attempts++
					return c
				})),
			)
			s.On("FAILURE", "idle",
				statecraft.Do(statecraft.Assign(func(c authCtx, _ statecraft.Event) authCtx {
					c.Attempts++
					return c
				})),
			)
			s.After(30*time.Second, "idle")
		}).
		State("dashboard", func(s *statecraft.StateBuilder[authCtx]) {
			s.On("LOGOUT", "idle")
		}).
		State("locked", func(s *statecraft.StateBuilder[authCtx]) {
			s.After(5*time.Minute, "idle",
				statecraft.Do(statecraft.Assign(func(c authCtx, _ statecraft.Event) authCtx {
					c.Attempts = 0
					return c
				})),
			)
		}).
		MustBuild()
}

type editorCtx struct {
	Words     int
	SaveCount int
}

func docEditor() *model.Machine[editorCtx] {
	return statecraft.New[editorCtx]("doc-editor").
		Context(editorCtx{}).
		Initial("doc").
		Parallel("doc", func(s *statecraft.StateBuilder[editorCtx]) {
			s.State("edit", func(s *statecraft.StateBuilder[editorCtx]) {
				s.Initial("idle")
				s.State("idle", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("TYPE", "typing")
				})
				s.State("typing", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("WORDS", "typing",
						statecraft.Do(statecraft.Assign(func(c editorCtx, _ statecraft.Event) editorCtx {
							c.Words += 10
							return c
						})),
					)
					s.On("FORMAT", "formatting")
					s.On("PAUSE", "idle")
				})
				s.State("formatting", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("DONE", "idle")
				})
			})
			s.State("sync", func(s *statecraft.StateBuilder[editorCtx]) {
				s.Initial("clean")
				s.State("clean", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("TYPE", "dirty")
				})
				s.State("dirty", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("SAVE", "saving")
				})
				s.State("saving", func(s *statecraft.StateBuilder[editorCtx]) {
					s.On("SAVED", "clean",
						statecraft.Do(statecraft.Assign(func(c editorCtx, _ statecraft.Event) editorCtx {
							c.SaveCount++
							return c
						})),
					)
					s.On("ERR", "dirty")
				})
			})
		}).
		MustBuild()
}
