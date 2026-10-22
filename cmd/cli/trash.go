package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

func trashCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trash",
		Short: "List, restore or purge deleted memories (soft-delete trash)",
		Long: "Deleting a memory (cortex delete, supersedes, namespace delete) moves it to the trash\n" +
			"instead of destroying it. Entries stay restorable until the server's TRASH_RETENTION\n" +
			"expires (default 7 days; 0 = forever) or you purge them.",
	}
	cmd.AddCommand(trashLsCmd(), trashRestoreCmd(), trashPurgeCmd())
	return cmd
}

func trashLsCmd() *cobra.Command {
	var namespace string
	var limit int
	cmd := &cobra.Command{
		Use:   "ls [-n namespace]",
		Short: "List trashed memories, newest deletion first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().ListTrash(cmd.Context(), connect.NewRequest(&cortexv1.ListTrashRequest{Namespace: namespace, Limit: int32(limit)}))
			if err != nil {
				return err
			}
			entries := resp.Msg.GetEntries()
			if len(entries) == 0 {
				fmt.Println("Trash is empty.")
				return nil
			}
			for _, e := range entries {
				text := strings.Join(strings.Fields(e.GetText()), " ")
				if r := []rune(text); len(r) > 100 {
					text = string(r[:100]) + "…"
				}
				fmt.Printf("[%s] %s\n   id=%s  deleted=%s  reason=%s\n", e.GetNamespace(), text, e.GetId(),
					e.GetDeletedAt().AsTime().Local().Format(time.RFC3339), e.GetReason())
			}
			fmt.Printf("\n%d of %d trashed", len(entries), resp.Msg.GetTotal())
			if s := resp.Msg.GetRetentionSeconds(); s > 0 {
				fmt.Printf(" (purged automatically %s after deletion)", (time.Duration(s) * time.Second).String())
			}
			fmt.Println()
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "only this namespace (default: all)")
	cmd.Flags().IntVarP(&limit, "limit", "l", 100, "max entries to list")
	return cmd
}

func trashRestoreCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "restore <id>...",
		Short: "Restore trashed memories (re-queued for indexing under the same id)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().RestoreFromTrash(cmd.Context(), connect.NewRequest(&cortexv1.RestoreFromTrashRequest{Ids: args}))
			if err != nil {
				return err
			}
			fmt.Printf("restored %d memories (queued for indexing)\n", resp.Msg.GetQueued())
			return nil
		},
	}
}

func trashPurgeCmd() *cobra.Command {
	var all, yes bool
	var namespace string
	cmd := &cobra.Command{
		Use:   "purge <id>... | --all [-n namespace] [--yes]",
		Short: "Permanently delete trashed memories",
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == (len(args) > 0) {
				return fmt.Errorf("give either ids or --all")
			}
			if !yes {
				what := fmt.Sprintf("%d trashed memories", len(args))
				if all {
					what = "ALL trashed memories"
				}
				fmt.Fprintf(os.Stderr, "Permanently delete %s? This cannot be undone. Type 'yes' to continue: ", what)
				var answer string
				if _, err := fmt.Fscan(os.Stdin, &answer); err != nil || answer != "yes" {
					return fmt.Errorf("aborted")
				}
			}
			resp, err := client().PurgeTrash(cmd.Context(), connect.NewRequest(&cortexv1.PurgeTrashRequest{Ids: args, All: all, Namespace: namespace}))
			if err != nil {
				return err
			}
			fmt.Printf("purged %d memories\n", resp.Msg.GetPurged())
			return nil
		},
	}
	cmd.Flags().BoolVar(&all, "all", false, "purge every trashed memory")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "with --all: only this namespace")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt")
	return cmd
}
