// `cortex outbox` — inspect and manage the local offline outbox (saves queued
// by the MCP server while Cortex was unreachable). status/drop/purge are purely
// local; flush replays queued saves through the Save RPC.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/thomas-maurice/cortex/internal/outbox"
)

// flushPerEntry bounds one entry's send (with its dependencies) so a hung
// server cannot stall the whole flush.
const flushPerEntry = 30 * time.Second

func outboxCmd() *cobra.Command {
	var dbPath string
	cmd := &cobra.Command{
		Use:   "outbox",
		Short: "Show the offline outbox (saves queued while Cortex was unreachable)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return outboxStatus(os.Stdout, dbPath, time.Now())
		},
	}
	cmd.PersistentFlags().StringVar(&dbPath, "outbox-db", outbox.DefaultPath(), "bbolt database holding the offline outbox")

	flush := &cobra.Command{
		Use:   "flush",
		Short: "Send every queued save (all conversations) to the server now",
		Long: "Sends queued saves in enqueue order through the Save RPC, serially, replaying each stored\n" +
			"request verbatim. Entries are deleted only on success (or when their valid_until lapsed\n" +
			"while queued); failures stay queued. Manual only: nothing flushes automatically from the CLI.\n" +
			"There is no claim step: a live MCP session drains only its own conversation and saves are\n" +
			"create-only, so a flush racing a session that sends the same entry cannot duplicate it.\n" +
			"Exits non-zero if anything remains queued.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return outboxFlush(cmd.Context(), os.Stdout, dbPath, client())
		},
	}

	drop := &cobra.Command{
		Use:   "drop <id>...",
		Short: "Remove specific queued saves by id (local only; leaves pending summaries, see purge)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return outboxDrop(os.Stdout, dbPath, args)
		},
	}

	var conv string
	var yes bool
	purge := &cobra.Command{
		Use:   "purge [--conversation <id>] [--yes]",
		Short: "Permanently delete queued saves and pending summaries (all, or one conversation; local only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var c *string
			if cmd.Flags().Changed("conversation") {
				c = &conv
			}
			return outboxPurge(os.Stdin, os.Stdout, os.Stderr, dbPath, c, yes)
		},
	}
	purge.Flags().StringVar(&conv, "conversation", "", "only this conversation's entries")
	purge.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt")

	cmd.AddCommand(flush, drop, purge)
	return cmd
}

func printOutboxEntry(w io.Writer, e outbox.Entry) {
	if e.Corrupt {
		pf(w, "  %s  [corrupt: cannot be sent; drop it]\n", e.ID)
		return
	}
	pf(w, "  %s  %s\n", e.ID, e.Preview)
}

func outboxStatus(w io.Writer, path string, now time.Time) error {
	entries, err := outbox.ListAll(path)
	if err != nil {
		return err
	}
	sums, err := outbox.ListSummaries(path)
	if err != nil {
		return err
	}
	if len(entries) == 0 && len(sums) == 0 {
		pf(w, "%s\n", "nothing pending")
		return nil
	}
	byConv := map[string]int{}
	var oldest int64
	for _, e := range entries {
		byConv[e.ConversationID]++
		if e.EnqueuedAt > 0 && (oldest == 0 || e.EnqueuedAt < oldest) {
			oldest = e.EnqueuedAt
		}
	}
	pf(w, "%d pending", len(entries))
	if oldest > 0 {
		pf(w, ", oldest %s ago", now.Sub(time.UnixMicro(oldest)).Round(time.Second))
	}
	pf(w, "\n")
	convs := make([]string, 0, len(byConv))
	for c := range byConv {
		convs = append(convs, c)
	}
	sort.Strings(convs)
	for _, c := range convs {
		name := c
		if name == "" {
			name = "(no conversation)"
		}
		pf(w, "  %s: %d\n", name, byConv[c])
	}
	pf(w, "\n")
	for _, e := range entries {
		printOutboxEntry(w, e)
	}
	printSummaries(w, sums)
	return nil
}

// printSummaries lists the pending session-summary slots (one per conversation).
func printSummaries(w io.Writer, sums []outbox.SummaryEntry) {
	if len(sums) == 0 {
		return
	}
	pf(w, "\n%d pending session summaries:\n", len(sums))
	for _, s := range sums {
		if s.Corrupt {
			pf(w, "  %s  [corrupt: cannot be sent; purge it]\n", s.ConversationID)
			continue
		}
		pf(w, "  %s  %s\n", s.ConversationID, s.Preview)
	}
}

func outboxFlush(ctx context.Context, w io.Writer, path string, c outbox.Sender) error {
	results, err := outbox.Drain(ctx, path, c, nil, flushPerEntry)
	if err != nil {
		return err
	}
	var sent, expired, failed int
	var errs []outbox.Result
	for _, r := range results {
		switch r.Outcome {
		case outbox.OutcomeSent:
			sent++
		case outbox.OutcomeExpired:
			expired++
		case outbox.OutcomeFailed, outbox.OutcomeCorrupt:
			failed++
			errs = append(errs, r)
		}
	}
	left, err := outbox.ListAll(path)
	if err != nil {
		return err
	}
	leftSums, err := outbox.ListSummaries(path)
	if err != nil {
		return err
	}
	remaining := len(left) + len(leftSums)
	pf(w, "sent %d, expired %d, failed %d, remaining %d\n", sent, expired, failed, remaining)
	for _, r := range errs {
		if r.Summary {
			pf(w, "  summary %s: %v\n", r.ID, r.Err)
			continue
		}
		pf(w, "  %s: %v\n", r.ID, r.Err)
	}
	if n := len(results); n > 0 && results[n-1].Outcome == outbox.OutcomeFailed && outbox.IsTransient(results[n-1].Err) {
		pf(w, "stopped: server unreachable, later entries were not attempted\n")
	}
	if remaining > 0 {
		return fmt.Errorf("%d entries remain queued", remaining)
	}
	return nil
}

func outboxDrop(w io.Writer, path string, ids []string) error {
	var errs []error
	for _, id := range ids {
		e, found, err := outbox.Delete(path, id)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", id, err))
			continue
		}
		if !found {
			pf(w, "not queued: %s\n", id)
			errs = append(errs, fmt.Errorf("%s: not queued", id))
			continue
		}
		pf(w, "dropped %s %s\n", e.ID, e.Preview)
	}
	return errors.Join(errs...)
}

// The confirmation prompt goes to ew (stderr), like the other CLI prompts, so
// piped output only carries the listing and results.
func outboxPurge(in io.Reader, w, ew io.Writer, path string, conv *string, yes bool) error {
	var entries []outbox.Entry
	var err error
	if conv != nil {
		entries, err = outbox.ListByConversation(path, *conv)
	} else {
		entries, err = outbox.ListAll(path)
	}
	if err != nil {
		return err
	}
	sums, err := outbox.ListSummaries(path)
	if err != nil {
		return err
	}
	if conv != nil {
		var mine []outbox.SummaryEntry
		for _, s := range sums {
			if s.ConversationID == *conv {
				mine = append(mine, s)
			}
		}
		sums = mine
	}
	if len(entries) == 0 && len(sums) == 0 {
		pf(w, "%s\n", "nothing to purge")
		return nil
	}
	pf(w, "%d queued saves:\n", len(entries))
	for _, e := range entries {
		printOutboxEntry(w, e)
	}
	printSummaries(w, sums)
	if !yes {
		pf(ew, "Permanently delete %d queued saves and %d pending summaries? This cannot be undone. Type 'yes' to continue: ", len(entries), len(sums))
		var answer string
		if _, err := fmt.Fscan(in, &answer); err != nil || answer != "yes" {
			return fmt.Errorf("aborted")
		}
	}
	var errs []error
	n := 0
	for _, e := range entries {
		if _, found, err := outbox.Delete(path, e.ID); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.ID, err))
		} else if found {
			n++
			pf(w, "purged %s\n", e.ID)
		}
	}
	ns := 0
	for _, s := range sums {
		if _, found, err := outbox.DeleteSummary(path, s.ConversationID, 0); err != nil {
			errs = append(errs, fmt.Errorf("summary %s: %w", s.ConversationID, err))
		} else if found {
			ns++
			pf(w, "purged summary %s\n", s.ConversationID)
		}
	}
	pf(w, "purged %d saves, %d summaries\n", n, ns)
	return errors.Join(errs...)
}

// pf writes to w; output errors (closed stdout) are not actionable here.
func pf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
