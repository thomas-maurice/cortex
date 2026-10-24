package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/internal/outbox"
)

// Model-facing wording for offline saves (O6). Kept in one place so tests assert it.
// Reading the outbox is best-effort: on any error the hint is omitted, a tool call
// never fails because of it.

// maxListedPending caps the queued entries named in an "unreachable" error.
const maxListedPending = 10

// queuedStatus marks a SaveOut / SearchHit that lives only in the local outbox.
const queuedStatus = "queued (offline)"

// pendingCounts returns the outbox total and this session's share. ok=false when
// the outbox is disabled or unreadable (logged).
func (d *deps) pendingCounts() (total, mine int, ok bool) {
	if d.outboxPath == "" {
		return 0, 0, false
	}
	st, err := outbox.ReadStats(d.outboxPath)
	if err != nil {
		slog.Warn("outbox stats unreadable; omitting pending hint", "err", err)
		return 0, 0, false
	}
	return st.Total, st.ByConversation[d.conversationID], true
}

// pendingNote is the footer appended to search / recall results, plus the count
// for the structured pendingOffline field. "" / 0 when nothing is pending.
func (d *deps) pendingNote() (string, int) {
	total, mine, ok := d.pendingCounts()
	if !ok || total == 0 {
		return "", 0
	}
	verb := "saves are"
	if total == 1 {
		verb = "save is"
	}
	return fmt.Sprintf("Note: %d %s queued offline and not searchable yet (%d from this session).", total, verb, mine), total
}

// savedOfflineMessage is the text of a save that went to the outbox.
func (d *deps) savedOfflineMessage(id, ns string) string {
	msg := fmt.Sprintf("Memory stored in the local offline outbox (id=%s, namespace=%s), NOT searchable or visible to other sessions until synced. "+
		"It syncs automatically when this session next reaches the Cortex server, or via `cortex outbox flush`.", id, ns)
	if total, mine, ok := d.pendingCounts(); ok {
		msg += fmt.Sprintf(" Pending: %d (%d this session).", total, mine)
	}
	return msg
}

// unreachableErr turns a transient failure of a read tool into an error that can
// not be mistaken for an empty result, listing this session's queued saves.
// Non-transient errors pass through untouched.
func (d *deps) unreachableErr(err error) error {
	if !outbox.IsTransient(err) {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Cortex is unreachable (%v) — no results ≠ nothing stored: this says nothing about what memories exist.", err)
	if d.outboxPath == "" {
		return fmt.Errorf("%s", b.String())
	}
	all, lerr := outbox.ListAll(d.outboxPath)
	if lerr != nil {
		slog.Warn("outbox unreadable; omitting pending list", "err", lerr)
		return fmt.Errorf("%s", b.String())
	}
	var mine []outbox.Entry
	for _, e := range all {
		if e.ConversationID == d.conversationID {
			mine = append(mine, e)
		}
	}
	if len(mine) > 0 {
		fmt.Fprintf(&b, " This session has %d save(s) queued offline (stored locally, not searchable until synced):", len(mine))
		for i, e := range mine {
			if i == maxListedPending {
				fmt.Fprintf(&b, "\n+%d more", len(mine)-maxListedPending)
				break
			}
			fmt.Fprintf(&b, "\n- id=%s %s", e.ID, e.Preview)
		}
	}
	if other := len(all) - len(mine); other > 0 {
		fmt.Fprintf(&b, "\n%d more queued by other sessions.", other)
	}
	return fmt.Errorf("%s", b.String())
}

// queuedHit renders an outbox entry as a hit. Falls back to the preview when the
// stored request does not decode.
func queuedHit(e outbox.Entry) SearchHit {
	h := SearchHit{ID: e.ID, Text: e.Preview, Status: queuedStatus}
	var req cortexv1.SaveRequest
	if !e.Corrupt && proto.Unmarshal(e.Request, &req) == nil {
		h.Text, h.Namespace, h.Tags = req.GetText(), req.GetNamespace(), req.GetTags()
		h.LinkedIDs = req.GetLinkTo()
	}
	return h
}

// startupPendingNotice is appended to the MCP instructions when earlier sessions
// left saves in the outbox. "" when none (or unreadable).
func startupPendingNotice(path string, now time.Time) string {
	if path == "" {
		return ""
	}
	st, err := outbox.ReadStats(path)
	if err != nil {
		return ""
	}
	sums, err := outbox.ListSummaries(path)
	if err != nil {
		sums = nil // best-effort hint: the memory count above is still worth reporting
	}
	if st.Total == 0 && len(sums) == 0 {
		return ""
	}
	oldest := st.OldestEnqueuedAt
	for _, s := range sums {
		if !s.Corrupt && s.EnqueuedAt > 0 && (oldest == 0 || s.EnqueuedAt < oldest) {
			oldest = s.EnqueuedAt
		}
	}
	var parts []string
	switch st.Total {
	case 0:
	case 1:
		parts = append(parts, "1 memory")
	default:
		parts = append(parts, fmt.Sprintf("%d memories", st.Total))
	}
	switch len(sums) {
	case 0:
	case 1:
		parts = append(parts, "1 session summary")
	default:
		parts = append(parts, fmt.Sprintf("%d session summaries", len(sums)))
	}
	age := ""
	if oldest > 0 {
		age = fmt.Sprintf(" (oldest %s old)", humanAge(now.Sub(time.UnixMicro(oldest))))
	}
	from := "from earlier sessions are"
	if st.Total+len(sums) == 1 {
		from = "from an earlier session is"
	}
	return fmt.Sprintf("\n\n%s %s queued locally and not yet in Cortex%s. "+
		"Tell the user, and suggest `cortex outbox flush` when the server is reachable.",
		strings.Join(parts, " and "), from, age)
}

func humanAge(d time.Duration) string {
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%d min", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h", int(d.Hours()))
	}
	return fmt.Sprintf("%d days", int(d.Hours()/24))
}

// queuedEntry looks id up in the outbox; unreadable or disabled = not found.
func (d *deps) queuedEntry(id string) (outbox.Entry, bool) {
	if d.outboxPath == "" {
		return outbox.Entry{}, false
	}
	e, found, err := outbox.Get(d.outboxPath, id)
	if err != nil {
		slog.Warn("outbox get failed", "id", id, "err", err)
		return outbox.Entry{}, false
	}
	return e, found
}
