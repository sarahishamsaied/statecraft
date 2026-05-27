// Package main demonstrates compound states, parallel regions, and persistence.
//
// The machine models a text editor with two independent parallel regions:
//
//	edit   — tracks whether the user is idle, typing, or formatting
//	sync   — tracks whether the document is clean, dirty, saving, or errored
//
// Events flow through both regions simultaneously.  The document is
// checkpointed after the first save and then restored into a fresh service,
// which resumes accepting events from where it left off.
//
// Run with:
//
//	go run ./examples/doceditor
package main

import (
	"fmt"
	"os"
	"statecraft"
	"statecraft/viz"
	"time"
)

// ─── context ─────────────────────────────────────────────────────────────────

type EditorCtx struct {
	Words     int
	SaveCount int
}

// ─── machine definition ───────────────────────────────────────────────────────

var machine = statecraft.New[EditorCtx]("doc-editor").
	Context(EditorCtx{}).
	Initial("doc").
	Parallel("doc", func(s *statecraft.StateBuilder[EditorCtx]) {

		// ── Region 1: editing mode ────────────────────────────────────────────
		s.State("edit", func(s *statecraft.StateBuilder[EditorCtx]) {
			s.Initial("idle")

			s.State("idle", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.On("TYPE", "typing")
			})

			s.State("typing", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.Entry(statecraft.Log(func(c EditorCtx, _ statecraft.Event) string {
					return "  [edit]  ✍  typing…"
				}))
				s.On("WORDS", "typing",
					statecraft.Do(statecraft.Assign(func(c EditorCtx, _ statecraft.Event) EditorCtx {
						c.Words += 10
						return c
					})),
				)
				s.On("FORMAT", "formatting")
				s.On("PAUSE", "idle")
			})

			s.State("formatting", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.Entry(statecraft.Log(func(c EditorCtx, _ statecraft.Event) string {
					return "  [edit]  ¶  formatting…"
				}))
				s.On("DONE", "idle")
			})
		})

		// ── Region 2: sync / save status ─────────────────────────────────────
		s.State("sync", func(s *statecraft.StateBuilder[EditorCtx]) {
			s.Initial("clean")

			s.State("clean", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.On("TYPE", "dirty")
			})

			s.State("dirty", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.On("SAVE", "saving")
			})

			s.State("saving", func(s *statecraft.StateBuilder[EditorCtx]) {
				s.Entry(statecraft.Log(func(c EditorCtx, _ statecraft.Event) string {
					return "  [sync]  💾 saving…"
				}))
				s.On("SAVED", "clean",
					statecraft.Do(statecraft.Assign(func(c EditorCtx, _ statecraft.Event) EditorCtx {
						c.SaveCount++
						return c
					})),
				)
				s.On("ERR", "dirty")
			})
		})
	}).
	MustBuild()

// ─── helpers ──────────────────────────────────────────────────────────────────

func send(svc *statecraft.Service[EditorCtx], ev string) {
	if err := svc.Send(statecraft.E(ev)); err != nil {
		fmt.Fprintf(os.Stderr, "send %q: %v\n", ev, err)
		os.Exit(1)
	}
	time.Sleep(20 * time.Millisecond)
}

func printSnap(label string, svc *statecraft.Service[EditorCtx]) {
	snap := svc.Snapshot()
	fmt.Printf("%-20s leaves=%-30v  words=%d  saves=%d\n",
		label, snap.Leaves, snap.Context.Words, snap.Context.SaveCount)
}

// ─── main ─────────────────────────────────────────────────────────────────────

func main() {
	fmt.Println("=== doc-editor state machine ===")
	fmt.Println()

	// Print the Mermaid diagram so you can paste it into mermaid.live.
	fmt.Println("--- mermaid diagram ---")
	fmt.Println(viz.ToMermaid(machine))

	fmt.Println("--- trace ---")

	svc := statecraft.Start(machine)
	printSnap("start", svc)

	send(svc, "TYPE") // edit: idle→typing   sync: clean→dirty
	printSnap("TYPE", svc)

	send(svc, "WORDS") // edit stays typing, words += 10
	send(svc, "WORDS")
	printSnap("WORDS×2", svc)

	send(svc, "FORMAT") // edit: typing→formatting
	printSnap("FORMAT", svc)

	send(svc, "DONE") // edit: formatting→idle
	printSnap("DONE", svc)

	send(svc, "SAVE") // sync: dirty→saving
	printSnap("SAVE", svc)

	send(svc, "SAVED") // sync: saving→clean, SaveCount++
	printSnap("SAVED", svc)

	// ── checkpoint ────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("--- checkpoint ---")

	data, err := statecraft.Save(svc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "save: %v\n", err)
		os.Exit(1)
	}
	svc.Stop()
	fmt.Printf("serialised %d bytes\n", len(data))

	// ── restore ───────────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("--- restore ---")

	svc2, err := statecraft.Restore(machine, data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore: %v\n", err)
		os.Exit(1)
	}
	defer svc2.Stop()

	printSnap("restored", svc2)

	// Continue editing after restore.
	send(svc2, "TYPE")
	send(svc2, "WORDS")
	send(svc2, "SAVE")
	send(svc2, "SAVED")
	printSnap("after restore", svc2)

	fmt.Println()
	fmt.Printf("total saves: %d\n", svc2.Snapshot().Context.SaveCount)
}
