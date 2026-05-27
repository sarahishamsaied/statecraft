// Package persist provides JSON-based snapshot persistence for statecraft services.
//
// A Checkpoint captures the active leaf states and serialised context of a
// running Service. It can be written to any storage backend and later used to
// restore the service to exactly that configuration without re-executing entry
// actions — the stored context already reflects them.
//
// Quick start:
//
//	// Save
//	data, err := persist.Save(svc)
//	os.WriteFile("checkpoint.json", data, 0o644)
//
//	// Restore
//	data, _ = os.ReadFile("checkpoint.json")
//	svc, err = persist.Restore(machine, data)
package persist

import (
	"encoding/json"
	"fmt"
	"statecraft/core"
	"statecraft/model"
	"statecraft/runtime"
	"time"
)

// Checkpoint is the serialised form of a Service's state. It captures enough
// information to reconstruct the service via Restore.
type Checkpoint struct {
	// MachineID is the ID of the machine that produced this checkpoint.
	// Restore validates that the target machine has the same ID.
	MachineID string `json:"machine_id"`

	// Leaves contains the active leaf state IDs — one per parallel region.
	// For flat and compound machines this is always a single entry.
	Leaves []string `json:"leaves"`

	// Context holds the JSON-encoded context value at the time of saving.
	Context json.RawMessage `json:"context"`

	// SavedAt is the wall-clock time when Save was called.
	SavedAt time.Time `json:"saved_at"`
}

// Save serialises the current snapshot of svc to JSON.
// The context type C must be JSON-marshallable.
func Save[C any](svc *runtime.Service[C]) ([]byte, error) {
	snap := svc.Snapshot()

	ctxBytes, err := json.Marshal(snap.Context)
	if err != nil {
		return nil, fmt.Errorf("persist: marshal context: %w", err)
	}

	leaves := make([]string, len(snap.Leaves))
	for i, l := range snap.Leaves {
		leaves[i] = string(l)
	}

	cp := Checkpoint{
		MachineID: svc.MachineID(),
		Leaves:    leaves,
		Context:   ctxBytes,
		SavedAt:   time.Now(),
	}
	data, err := json.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("persist: marshal checkpoint: %w", err)
	}
	return data, nil
}

// Restore creates a new running Service whose configuration matches the
// serialised Checkpoint. The context type C must match the machine's context
// type and be JSON-unmarshallable.
//
// Entry actions are not re-executed — the stored context already reflects them.
// Timers and invokes are started fresh (they are ephemeral).
//
// Returns ErrInvalidCheckpoint if the checkpoint is malformed, references the
// wrong machine, or contains unknown/compound state IDs.
func Restore[C any](m *model.Machine[C], data []byte, opts ...func(*runtime.ServiceOptions)) (*runtime.Service[C], error) {
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrInvalidCheckpoint, err)
	}
	return RestoreCheckpoint(m, &cp, opts...)
}

// RestoreCheckpoint creates a Service from an already-decoded Checkpoint.
// Useful when the caller manages deserialisation separately.
func RestoreCheckpoint[C any](m *model.Machine[C], cp *Checkpoint, opts ...func(*runtime.ServiceOptions)) (*runtime.Service[C], error) {
	if cp.MachineID != m.ID() {
		return nil, fmt.Errorf("%w: checkpoint machine %q does not match %q",
			core.ErrInvalidCheckpoint, cp.MachineID, m.ID())
	}
	if len(cp.Leaves) == 0 {
		return nil, fmt.Errorf("%w: checkpoint has no leaves", core.ErrInvalidCheckpoint)
	}

	leaves := make([]core.StateID, len(cp.Leaves))
	for i, l := range cp.Leaves {
		leaves[i] = core.StateID(l)
	}

	var ctx C
	if err := json.Unmarshal(cp.Context, &ctx); err != nil {
		return nil, fmt.Errorf("%w: unmarshal context: %v", core.ErrInvalidCheckpoint, err)
	}

	return runtime.Restore(m, leaves, ctx, opts...)
}
