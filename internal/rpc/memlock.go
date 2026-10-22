package rpc

import (
	"context"
	"errors"
	"fmt"
	"os"

	"connectrpc.com/connect"

	"github.com/thomas-maurice/cortex/internal/dlock"
	"github.com/thomas-maurice/cortex/internal/store"
)

// lockMemories takes the SAME per-memory distributed locks the worker takes
// (dlock.Key("mem", tenant, id) in the shared KV bucket), for every id, in sorted
// order, so a server-side read-modify-write of a memory's non-embedded fields
// excludes the worker's index/link commits on ANY replica. The returned context is
// bounded inside the first lease: a long critical section fails loudly (deadline)
// instead of running without the lock. release is idempotent.
//
// tenant is the authenticated user id ("" when there is no identity), matching the
// UserID the worker reads off the queue payload.
func (s *Service) lockMemories(ctx context.Context, ids ...string) (context.Context, func(), error) {
	if s.memLocks != nil {
		return s.memLocks(ctx, gateKey(ctx), ids)
	}
	l, err := s.getLocker(ctx)
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("lock service unavailable: %w", err))
	}
	keys := make([]string, len(ids))
	tenant := gateKey(ctx)
	for i, id := range ids {
		keys[i] = dlock.Key("mem", tenant, id)
	}
	hctx, release, err := l.AcquireAll(ctx, keys, l.Wait())
	if err != nil {
		return nil, nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("could not lock memories (busy, retry): %w", err))
	}
	return hctx, release, nil
}

// getLocker lazily creates the one server-side Locker (NewService stays
// infallible). Tests may preset s.locker.
func (s *Service) getLocker(ctx context.Context) (*dlock.Locker, error) {
	s.lockerMu.Lock()
	defer s.lockerMu.Unlock()
	if s.locker != nil {
		return s.locker, nil
	}
	if s.js == nil {
		return nil, errors.New("lock service unavailable")
	}
	host, _ := os.Hostname()
	l, err := dlock.New(ctx, s.js, dlock.DefaultBucket, dlock.DefaultTTL, fmt.Sprintf("server/%s/%d", host, os.Getpid()))
	if err != nil {
		return nil, err
	}
	s.locker = l
	return l, nil
}

// memoryLocker adapts lockMemories to the single-id store.MemoryLocker the tag
// operations take, so tag rewrites, MergeInto and DismissDuplicate all share one
// lock implementation (same bucket, TTL and key as the worker). It fails early
// (Unavailable) when no lock service can exist, before a rewrite starts.
func (s *Service) memoryLocker(ctx context.Context) (store.MemoryLocker, error) {
	if s.memLocks == nil {
		if _, err := s.getLocker(ctx); err != nil {
			return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("lock service unavailable: %w", err))
		}
	}
	return func(c context.Context, id string) (context.Context, func(), error) {
		return s.lockMemories(c, id)
	}, nil
}
