package rpc

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// SetMetaKV wires the NATS cortex_meta bucket (coordination state) shown by ListMeta.
func (s *Service) SetMetaKV(kv jetstream.KeyValue) { s.metaKV = kv }

// ListMeta is the read-only admin debug view of store metadata: durable CortexMeta
// entries (Weaviate) next to the NATS cortex_meta keys (coordination + legacy
// leftovers). Admin only; values can describe the deployment's internals.
func (s *Service) ListMeta(ctx context.Context, _ *connect.Request[cortexv1.ListMetaRequest]) (*connect.Response[cortexv1.ListMetaResponse], error) {
	if err := requireAdmin(ctx); err != nil {
		return nil, err
	}
	out := &cortexv1.ListMetaResponse{}
	entries, err := s.store.ListMeta(ctx, "")
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list durable meta: %w", err))
	}
	for _, e := range entries {
		m := &cortexv1.MetaEntry{Key: e.Key, Value: string(e.Value)}
		if !e.UpdatedAt.IsZero() {
			m.UpdatedAt = timestamppb.New(e.UpdatedAt)
		}
		out.Durable = append(out.Durable, m)
	}
	if s.metaKV == nil {
		return connect.NewResponse(out), nil
	}
	keys, err := s.metaKV.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list coordination keys: %w", err))
	}
	sort.Strings(keys)
	for _, k := range keys {
		e, err := s.metaKV.Get(ctx, k)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue // expired or deleted between Keys and Get
		}
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read coordination key %q: %w", k, err))
		}
		out.Coordination = append(out.Coordination, &cortexv1.KVMetaEntry{
			Key: k, Value: string(e.Value()), Revision: e.Revision(), CreatedAt: timestamppb.New(e.Created()),
		})
	}
	return connect.NewResponse(out), nil
}
