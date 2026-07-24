// `cortex state` — inspect and maintain the local per-conversation state
// database (~/.cache/cortex/cortex.db) shared by the recall prompt hook and the
// MCP server. Purely local: no RPC, works with no server reachable.
package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/thomas-maurice/cortex/internal/recallstate"
)

func stateCmd() *cobra.Command {
	var dbPath string
	cmd := &cobra.Command{
		Use:   "state",
		Short: "Inspect/maintain the local per-conversation state db (recall dedup)",
		Args:  cobra.NoArgs,
	}
	cmd.PersistentFlags().StringVar(&dbPath, "state-db", recallstate.DefaultPath(), "bbolt database holding per-conversation state")

	ls := &cobra.Command{
		Use:   "ls",
		Short: "List conversations with state, most recently active first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			convos, err := recallstate.Conversations(dbPath)
			if err != nil {
				return err
			}
			if len(convos) == 0 {
				fmt.Println("No conversation state.")
				return nil
			}
			for _, c := range convos {
				last := "never"
				if !c.LastSeen.IsZero() {
					last = c.LastSeen.Format(time.RFC3339)
				}
				fmt.Printf("%s  last-seen=%s  recalled=%d\n", c.ID, last, c.Recalled)
			}
			return nil
		},
	}

	show := &cobra.Command{
		Use:   "show <conversation-id>",
		Short: "Show the memories already delivered to a conversation's context",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			recalled, err := recallstate.Recalled(dbPath, args[0])
			if err != nil {
				return err
			}
			if len(recalled) == 0 {
				fmt.Println("No memories delivered yet.")
				return nil
			}
			for _, m := range recalled {
				fmt.Printf("%s  delivered=%s\n", m.ID, m.DeliveredAt.Format(time.RFC3339))
			}
			return nil
		},
	}

	clear := &cobra.Command{
		Use:   "clear <conversation-id>",
		Short: "Drop a conversation's state (its memories become injectable again)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := recallstate.Clear(dbPath, args[0]); err != nil {
				return err
			}
			fmt.Printf("Cleared state for conversation %s\n", args[0])
			return nil
		},
	}

	var olderThan time.Duration
	purge := &cobra.Command{
		Use:   "purge",
		Short: "Delete state of conversations idle for longer than --older-than",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			purged, err := recallstate.Purge(dbPath, olderThan)
			if err != nil {
				return err
			}
			if len(purged) == 0 {
				fmt.Printf("Nothing idle for more than %s.\n", olderThan)
				return nil
			}
			for _, id := range purged {
				fmt.Println(id)
			}
			fmt.Printf("Purged %d conversation(s).\n", len(purged))
			return nil
		},
	}
	purge.Flags().DurationVar(&olderThan, "older-than", recallstate.PruneAge, "idle threshold; conversations untouched for longer are deleted")

	cmd.AddCommand(ls, show, clear, purge)
	return cmd
}
