package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/thomas-maurice/cortex/internal/memory"
)

// resolveExpiry turns the two ways a client can ask for an expiry — an absolute
// valid_until or a relative expires_in — into one time. set is false when neither
// was given. Both at once is ambiguous and rejected. An absolute time must lie in
// the future (a past expiry would hide the memory the moment it is saved) and
// within memory.MaxExpiry; a relative one is validated by memory.ParseExpiresIn.
func resolveExpiry(validUntil *timestamppb.Timestamp, expiresIn string, now time.Time) (until time.Time, set bool, err error) {
	if validUntil != nil && expiresIn != "" {
		return time.Time{}, false, connect.NewError(connect.CodeInvalidArgument, errors.New("set only one of valid_until and expires_in"))
	}
	switch {
	case validUntil != nil:
		if err := validUntil.CheckValid(); err != nil {
			return time.Time{}, false, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid valid_until: %w", err))
		}
		until = validUntil.AsTime()
		if !until.After(now) {
			return time.Time{}, false, connect.NewError(connect.CodeInvalidArgument, errors.New("valid_until must be in the future"))
		}
		if until.After(now.Add(memory.MaxExpiry)) {
			return time.Time{}, false, connect.NewError(connect.CodeInvalidArgument, errors.New("valid_until is more than 10 years away"))
		}
		return until, true, nil
	case expiresIn != "":
		d, err := memory.ParseExpiresIn(expiresIn)
		if err != nil {
			return time.Time{}, false, connect.NewError(connect.CodeInvalidArgument, err)
		}
		return now.Add(d).UTC(), true, nil
	}
	return time.Time{}, false, nil
}

// validUntilProto renders an expiry for the wire: nil for "never expires".
func validUntilProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// SweepExpired is the expiry sweep: across all tenants it moves memories that
// expired more than cfg.ExpiryGrace ago to Trash (reason "expired"). The grace
// keeps a just-expired memory around (hidden from recall, still listable with
// include_expired and extendable) in case the expiry was a mistake; 0 moves a
// memory as soon as it has expired.
func (s *Service) SweepExpired(ctx context.Context) (int, error) {
	return s.store.SweepExpired(ctx, time.Now().Add(-s.cfg.ExpiryGrace))
}

// skipExpired wraps a by-id getter so expired records read as not found, unless
// includeExpired. Consolidate's cluster expansion fetches linked/duplicate
// neighbours by id (bypassing the store's expiry filter), and the cluster becomes
// the supersede manifest, so an expired neighbour must not slip in and be merged
// away.
func skipExpired(get func(string) (memory.Record, bool, error), includeExpired bool, now time.Time) func(string) (memory.Record, bool, error) {
	if includeExpired {
		return get
	}
	return func(id string) (memory.Record, bool, error) {
		r, found, err := get(id)
		if err != nil || !found || r.Expired(now) {
			return memory.Record{}, false, err
		}
		return r, true, nil
	}
}
