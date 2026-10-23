// Package migrate is Cortex's schema-migration framework for the Weaviate store.
//
// Weaviate forbids many schema changes in place (a property's tokenization, for
// one). A Migration therefore usually rebuilds a class: create the next physical
// version, copy the data WITH its vectors (no re-embedding), and repoint the
// class's alias (see store.SwapToVersion and docs/DESIGN.md "Schema migrations").
//
// Applied state lives in the NATS KV bucket cortex_meta, so every replica agrees
// on it. The server runs pending migrations at boot, before serving, under a
// distributed lease; the worker never migrates, it waits until the schema is at
// the version it was built for. While data is being copied a maintenance flag
// stops writers (worker pauses fetching, server write RPCs answer Unavailable).
package migrate

import (
	"context"
	"fmt"
)

// Migration is one ordered, one-way schema/data change.
type Migration struct {
	// ID is the position in the history: strictly increasing, starting at 1, never
	// reused or renumbered once released.
	ID          int
	Name        string
	Description string
	// Up applies the change. It MUST be idempotent and resumable: after a crash
	// the runner calls it again from the start (nothing is recorded until it
	// returns nil), so it must detect "already done" parts and finish the rest.
	Up func(ctx context.Context, env *Env) error
}

// registry is the ordered history. Append new migrations at the END with the next
// ID; TestRegistry enforces the ordering. Never edit a released migration's ID.
var registry = []Migration{
	tagsFieldTokenization,
}

// All returns the registered migrations in order.
func All() []Migration { return append([]Migration(nil), registry...) }

// Latest is the highest registered migration ID: the schema version this build
// expects (0 if none). The worker waits until the store reaches it.
func Latest() int { return latest(registry) }

func latest(ms []Migration) int {
	if len(ms) == 0 {
		return 0
	}
	return ms[len(ms)-1].ID
}

// validate checks a registry: IDs start at 1... strictly increase, and every
// entry is complete.
func validate(ms []Migration) error {
	prev := 0
	for _, m := range ms {
		switch {
		case m.ID <= prev:
			return fmt.Errorf("migration %d %q: ID must be greater than the previous one (%d)", m.ID, m.Name, prev)
		case m.Name == "":
			return fmt.Errorf("migration %d: empty Name", m.ID)
		case m.Up == nil:
			return fmt.Errorf("migration %d %q: nil Up", m.ID, m.Name)
		}
		prev = m.ID
	}
	return nil
}
