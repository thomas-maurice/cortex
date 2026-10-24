package store

// Durable store-level metadata in the CortexMeta class: facts ABOUT the store
// (schema version, applied migrations, usage-tracking epoch) that must survive a
// wipe of NATS, so they live next to the data they describe. Coordination state
// (locks, maintenance flag, worker heartbeats) deliberately stays in the NATS KV.
//
// The class is always single-tenant (even with CORTEX_MULTI_TENANT=true), has no
// vectors and is a plain class (not alias-backed, never rebuilt by a migration).
// It is NOT dropped by DeleteClass / DeleteAllClasses; tests that need a clean
// slate call DeleteMetaClass.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// metaNamespace is the fixed UUIDv5 namespace for CortexMeta object ids
// (randomly generated once; NEVER change it, ids of existing entries derive from it).
var metaNamespace = uuid.MustParse("6f1c2b0e-9d3a-4c57-8e41-2a7b5d90c3f8")

// MetaKeyUsageTrackingSince holds the RFC3339 instant usage tracking first ran.
const MetaKeyUsageTrackingSince = "usage_tracking_since"

// MetaEntry is one CortexMeta record.
type MetaEntry struct {
	Key       string
	Value     []byte
	UpdatedAt time.Time
}

func metaID(key string) string { return uuid.NewSHA1(metaNamespace, []byte(key)).String() }

func metaProperties() []*models.Property {
	no := false
	return []*models.Property{
		// key is an exact-match identifier -> field tokenization (see memoryProperties).
		{Name: "key", DataType: []string{"text"}, Tokenization: "field"},
		// value is an opaque JSON payload, only ever read by id or listed.
		{Name: "value", DataType: []string{"text"}, IndexFilterable: &no, IndexSearchable: &no},
		{Name: "updatedAt", DataType: []string{"date"}},
	}
}

func metaClass() *models.Class {
	return &models.Class{
		Class:             memory.MetaClassName,
		Description:       "Durable store-level metadata (schema version, applied migrations)",
		Vectorizer:        "none",
		VectorIndexConfig: map[string]interface{}{"skip": true},
		Properties:        metaProperties(),
	}
}

// EnsureMetaClass creates CortexMeta if absent, or additively adds missing properties.
func (s *Store) EnsureMetaClass(ctx context.Context) error {
	exists, err := s.client.Schema().ClassExistenceChecker().WithClassName(memory.MetaClassName).Do(ctx)
	if err != nil {
		return fmt.Errorf("check class %s: %w", memory.MetaClassName, err)
	}
	if !exists {
		if err := s.client.Schema().ClassCreator().WithClass(metaClass()).Do(ctx); err != nil {
			// A concurrent replica may have created it first.
			if ok, cerr := s.client.Schema().ClassExistenceChecker().WithClassName(memory.MetaClassName).Do(ctx); cerr != nil || !ok {
				return fmt.Errorf("create class %s: %w", memory.MetaClassName, err)
			}
		}
		return nil
	}
	return s.ensureProps(ctx, memory.MetaClassName, metaProperties())
}

// DeleteMetaClass drops CortexMeta. Nothing in production calls it (the class
// describes the store and outlives DeleteAllClasses); tests use it for a clean slate.
func (s *Store) DeleteMetaClass(ctx context.Context) error {
	return s.dropLogical(ctx, memory.MetaClassName)
}

func metaProps(key string, value []byte) map[string]interface{} {
	return map[string]interface{}{
		"key": key, "value": string(value), "updatedAt": time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// GetMeta returns the value stored under key.
func (s *Store) GetMeta(ctx context.Context, key string) (value []byte, found bool, err error) {
	id := metaID(key)
	exists, err := s.client.Data().Checker().WithClassName(memory.MetaClassName).WithID(id).Do(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("check meta %q: %w", key, err)
	}
	if !exists {
		return nil, false, nil
	}
	objs, err := s.client.Data().ObjectsGetter().WithClassName(memory.MetaClassName).WithID(id).Do(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("get meta %q: %w", key, err)
	}
	if len(objs) == 0 {
		return nil, false, nil
	}
	p, _ := objs[0].Properties.(map[string]interface{})
	return []byte(restString(p, "value")), true, nil
}

// DeleteMeta removes key (absent is fine).
func (s *Store) DeleteMeta(ctx context.Context, key string) error {
	err := s.client.Data().Deleter().WithClassName(memory.MetaClassName).WithID(metaID(key)).Do(ctx)
	if err != nil {
		if ok, cerr := s.client.Data().Checker().WithClassName(memory.MetaClassName).WithID(metaID(key)).Do(ctx); cerr == nil && !ok {
			return nil
		}
		return fmt.Errorf("delete meta %q: %w", key, err)
	}
	return nil
}

// PutMeta upserts key. There is deliberately no compare-and-set: the only
// contended key (schema_version) is written exclusively under the migration lease
// (NATS lock), which already serialises writers.
func (s *Store) PutMeta(ctx context.Context, key string, value []byte) error {
	id := metaID(key)
	props := metaProps(key, value)
	exists, err := s.client.Data().Checker().WithClassName(memory.MetaClassName).WithID(id).Do(ctx)
	if err != nil {
		return fmt.Errorf("check meta %q: %w", key, err)
	}
	if !exists {
		if _, err := s.client.Data().Creator().WithClassName(memory.MetaClassName).WithID(id).WithProperties(props).Do(ctx); err == nil {
			return nil
		}
		// Lost a creation race: fall through to the replace.
	}
	if err := s.client.Data().Updater().WithClassName(memory.MetaClassName).WithID(id).WithProperties(props).Do(ctx); err != nil {
		return fmt.Errorf("put meta %q: %w", key, err)
	}
	return nil
}

// CreateMetaIfAbsent stores value only if key is absent. It returns the value that
// is stored after the call and whether this call created it. The deterministic id
// makes a duplicate create fail, but Weaviate's existence check is not atomic across
// truly simultaneous creators, so after a successful create the value is read back
// and a loser reports created=false with the winning value. A residual
// interleaving can still let two callers see different values; acceptable for its
// only user (usage_tracking_since: values seconds apart). Do NOT use it for a lock.
func (s *Store) CreateMetaIfAbsent(ctx context.Context, key string, value []byte) (existing []byte, created bool, err error) {
	_, cerr := s.client.Data().Creator().WithClassName(memory.MetaClassName).WithID(metaID(key)).WithProperties(metaProps(key, value)).Do(ctx)
	if cerr == nil {
		cur, found, gerr := s.GetMeta(ctx, key)
		if gerr != nil || !found || string(cur) == string(value) {
			return value, true, nil
		}
		return cur, false, nil
	}
	cur, found, gerr := s.GetMeta(ctx, key)
	if gerr != nil {
		return nil, false, errors.Join(fmt.Errorf("create meta %q: %w", key, cerr), gerr)
	}
	if !found {
		return nil, false, fmt.Errorf("create meta %q: %w", key, cerr)
	}
	return cur, false, nil
}

// ListMeta returns every entry whose key starts with prefix ("" = all), sorted by key.
func (s *Store) ListMeta(ctx context.Context, prefix string) ([]MetaEntry, error) {
	res, err := s.client.Experimental().Search().
		WithCollection(memory.MetaClassName).
		WithProperties("key", "value", "updatedAt").
		WithLimit(allCount).
		Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("list meta: %w", err)
	}
	var out []MetaEntry
	for _, r := range res {
		k := propString(r.Properties, "key")
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		e := MetaEntry{Key: k, Value: []byte(propString(r.Properties, "value"))}
		e.UpdatedAt, _ = time.Parse(time.RFC3339Nano, propString(r.Properties, "updatedAt")) // ignored: best-effort
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// EnsureUsageTrackingSince returns the instant usage tracking started, creating the
// record if absent. A new record is seeded from legacy (the pre-CortexMeta NATS KV
// value, so the stale-review epoch does not reset) when it reports one, else now.
// An existing record is never overwritten; replicas starting together agree on one.
func (s *Store) EnsureUsageTrackingSince(ctx context.Context, now time.Time, legacy func(context.Context) (time.Time, bool, error)) (time.Time, error) {
	cur, found, err := s.GetMeta(ctx, MetaKeyUsageTrackingSince)
	if err != nil {
		return time.Time{}, err
	}
	if !found {
		seed := now
		if legacy != nil {
			lt, ok, lerr := legacy(ctx)
			if lerr != nil {
				return time.Time{}, fmt.Errorf("read legacy usage tracking start: %w", lerr)
			}
			if ok {
				seed = lt
			}
		}
		cur, _, err = s.CreateMetaIfAbsent(ctx, MetaKeyUsageTrackingSince, []byte(seed.UTC().Format(time.RFC3339)))
		if err != nil {
			return time.Time{}, err
		}
	}
	t, err := time.Parse(time.RFC3339, string(cur))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse usage tracking start %q: %w", cur, err)
	}
	return t, nil
}
