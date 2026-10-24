package migrate

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/thomas-maurice/cortex/internal/store"
)

// fakeMeta is an in-memory MetaStore for tests that exercise ordering and state
// logic without Weaviate.
type fakeMeta struct {
	mu sync.Mutex
	m  map[string][]byte
	// failAfter, when >= 0, makes every PutMeta after that many successful puts fail.
	failAfter int
	puts      int
}

func newFakeMeta() *fakeMeta { return &fakeMeta{m: map[string][]byte{}, failAfter: -1} }

func (f *fakeMeta) GetMeta(_ context.Context, k string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[k]
	return v, ok, nil
}

func (f *fakeMeta) PutMeta(_ context.Context, k string, v []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAfter >= 0 && f.puts >= f.failAfter {
		return errors.New("injected put failure")
	}
	f.puts++
	f.m[k] = append([]byte(nil), v...)
	return nil
}

func (f *fakeMeta) ListMeta(_ context.Context, prefix string) ([]store.MetaEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.MetaEntry
	for k, v := range f.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, store.MetaEntry{Key: k, Value: v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakeMeta) DeleteMeta(_ context.Context, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, k)
	return nil
}

// heal lifts the injected failure and resets the put counter.
func (f *fakeMeta) heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfter, f.puts = -1, 0
}
