package outbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// expiryMargin: an entry whose valid_until is this close to now (or past) is
// dropped instead of sent, since the server rejects a non-future valid_until.
const expiryMargin = time.Minute

// ErrExpired is returned by SendWithDeps for an entry whose valid_until lapsed
// while it was queued; the entry is deleted without being sent.
var ErrExpired = errors.New("outbox: entry expired before delivery")

// ErrCorrupt is returned by SendWithDeps for an undecodable entry. It is left
// in place (visible, droppable) and never sent.
var ErrCorrupt = errors.New("outbox: entry is corrupt")

// Sender is the slice of the Cortex client the outbox needs.
type Sender interface {
	Save(context.Context, *connect.Request[cortexv1.SaveRequest]) (*connect.Response[cortexv1.SaveResponse], error)
	SummarizeSession(context.Context, *connect.Request[cortexv1.SummarizeSessionRequest]) (*connect.Response[cortexv1.SummarizeSessionResponse], error)
}

// IsTransient reports whether err means "the save may succeed later" (queue it)
// as opposed to "the server rejected it" (return it). Unavailable, deadline
// exceeded, raw network errors and Internal/Unknown (server up, backend hiccup;
// replay is safe because the worker skips an id that already landed) are
// transient. Everything else is permanent.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var ce *connect.Error
	if errors.As(err, &ce) {
		switch ce.Code() {
		case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeInternal, connect.CodeUnknown:
			return true
		}
		return false
	}
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne)
}

// SendDeps sends, first, every queued entry named by ids (any conversation),
// skipping ids already in visited. Missing and corrupt entries are skipped
// (nothing to send); an entry that expired is dropped. The first send failure
// is returned and later ids are left queued.
func SendDeps(ctx context.Context, path string, c Sender, ids []string, visited map[string]bool) error {
	if visited == nil {
		visited = map[string]bool{}
	}
	for _, id := range ids {
		if visited[id] {
			continue
		}
		dep, ok, err := Get(path, id)
		if err != nil {
			return err
		}
		if !ok || dep.Corrupt {
			continue
		}
		if err := SendWithDeps(ctx, path, c, dep, visited); err != nil && !errors.Is(err, ErrExpired) {
			return err
		}
	}
	return nil
}

// SendWithDeps sends e after any still-queued entry its link_to / supersedes
// name (recursively, so the target reaches the server before the link that
// needs it), and deletes each entry on success only. visited breaks cycles.
func SendWithDeps(ctx context.Context, path string, c Sender, e Entry, visited map[string]bool) error {
	if visited == nil {
		visited = map[string]bool{}
	}
	visited[e.ID] = true
	if e.Corrupt {
		return ErrCorrupt
	}
	req := &cortexv1.SaveRequest{}
	if err := proto.Unmarshal(e.Request, req); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if vu := req.GetValidUntil(); vu != nil && !vu.AsTime().After(time.Now().Add(expiryMargin)) {
		if _, _, err := Delete(path, e.ID); err != nil {
			return err
		}
		return ErrExpired
	}
	if err := SendDeps(ctx, path, c, append(append([]string{}, req.GetLinkTo()...), req.GetSupersedes()...), visited); err != nil {
		return err
	}
	if _, err := c.Save(ctx, connect.NewRequest(req)); err != nil {
		return err
	}
	_, _, err := Delete(path, e.ID)
	return err
}

// Outcome of one drained entry.
type Outcome string

const (
	OutcomeSent    Outcome = "sent"
	OutcomeExpired Outcome = "expired" // deleted unsent: valid_until lapsed
	OutcomeFailed  Outcome = "failed"  // left queued; Err says why
	OutcomeCorrupt Outcome = "corrupt" // left queued; cannot be sent
	OutcomeGone    Outcome = "gone"    // already sent (as a dependency) or removed meanwhile
)

// Result is the per-entry drain outcome.
type Result struct {
	ID      string // memory id, or the conversation id for a summary
	Outcome Outcome
	Err     error
	Summary bool // ID is a conversation id: a session-summary slot
}

// Drain sends queued entries in enqueue order. convFilter nil drains all
// conversations, otherwise only entries of that conversation start a send
// (dependencies of other conversations are still pulled in). perEntry bounds
// each entry's send (0 = ctx only). After the memory entries, the pending
// session-summary slots (same conversation filter) are sent, so a summary never
// reaches the server before the facts it digests. An entry is deleted only on success or
// expiry; any failure leaves it queued. A per-entry (permanent) failure moves on
// to the next entry; a transient one means the server is unreachable, so the
// drain stops instead of waiting out perEntry once per remaining entry.
func Drain(ctx context.Context, path string, c Sender, convFilter *string, perEntry time.Duration) ([]Result, error) {
	entries, err := ListAll(path)
	if err != nil {
		return nil, err
	}
	var results []Result
	for _, e := range entries {
		if convFilter != nil && e.ConversationID != *convFilter {
			continue
		}
		if e.Corrupt {
			results = append(results, Result{ID: e.ID, Outcome: OutcomeCorrupt, Err: ErrCorrupt})
			continue
		}
		// An earlier entry's dependency pass may already have sent this one.
		cur, ok, err := Get(path, e.ID)
		if err != nil {
			results = append(results, Result{ID: e.ID, Outcome: OutcomeFailed, Err: err})
			continue
		}
		if !ok {
			results = append(results, Result{ID: e.ID, Outcome: OutcomeGone})
			continue
		}
		sctx, cancel := ctx, context.CancelFunc(func() {})
		if perEntry > 0 {
			sctx, cancel = context.WithTimeout(ctx, perEntry)
		}
		err = SendWithDeps(sctx, path, c, cur, nil)
		cancel()
		switch {
		case err == nil:
			results = append(results, Result{ID: e.ID, Outcome: OutcomeSent})
		case errors.Is(err, ErrExpired):
			results = append(results, Result{ID: e.ID, Outcome: OutcomeExpired})
		case errors.Is(err, ErrCorrupt):
			results = append(results, Result{ID: e.ID, Outcome: OutcomeCorrupt, Err: err})
		default:
			results = append(results, Result{ID: e.ID, Outcome: OutcomeFailed, Err: err})
			if IsTransient(err) {
				return results, nil
			}
		}
	}
	sums, err := drainSummaries(ctx, path, c, convFilter, perEntry)
	return append(results, sums...), err
}

// drainSummaries sends the summary slots, deleting each on success only (and only
// if no newer summary replaced it meanwhile). Stops at the first transient error.
func drainSummaries(ctx context.Context, path string, c Sender, convFilter *string, perEntry time.Duration) ([]Result, error) {
	slots, err := ListSummaries(path)
	if err != nil {
		return nil, err
	}
	var results []Result
	for _, s := range slots {
		if convFilter != nil && s.ConversationID != *convFilter {
			continue
		}
		res := Result{ID: s.ConversationID, Summary: true}
		req := &cortexv1.SummarizeSessionRequest{}
		if s.Corrupt || proto.Unmarshal(s.Request, req) != nil {
			res.Outcome, res.Err = OutcomeCorrupt, ErrCorrupt
			results = append(results, res)
			continue
		}
		sctx, cancel := ctx, context.CancelFunc(func() {})
		if perEntry > 0 {
			sctx, cancel = context.WithTimeout(ctx, perEntry)
		}
		_, err := c.SummarizeSession(sctx, connect.NewRequest(req))
		cancel()
		if err == nil {
			_, _, err = DeleteSummary(path, s.ConversationID, s.EnqueuedAt)
		}
		if err != nil {
			res.Outcome, res.Err = OutcomeFailed, err
			results = append(results, res)
			if IsTransient(err) {
				return results, nil
			}
			continue
		}
		res.Outcome = OutcomeSent
		results = append(results, res)
	}
	return results, nil
}
