package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
)

// tagsCmd lists tags with usage; `rename` and `delete` fix drifted spellings.
func tagsCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "tags",
		Short: "List tags with memory counts; rename/merge or delete them",
		Long: "Lists every tag in use with its memory count and the date of its newest memory.\n" +
			"Use `tags rename` to fix drifted spellings (k8s / kubernetes / K8s): renaming into\n" +
			"an existing tag merges the two. Tags are matched exactly (case-sensitive).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client().ListTags(cmd.Context(), connect.NewRequest(&cortexv1.ListTagsRequest{Namespace: namespace}))
			if err != nil {
				return err
			}
			tags := resp.Msg.GetTags()
			if len(tags) == 0 {
				fmt.Println("No tags found.")
				return nil
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "TAG\tMEMORIES\tNEWEST MEMORY")
			for _, t := range tags {
				last := "-"
				if t.GetLastActivity() != nil {
					last = t.GetLastActivity().AsTime().Format("2006-01-02 15:04")
				}
				count := fmt.Sprint(t.GetMemoryCount())
				if n := t.GetExpiredCount(); n > 0 {
					count += fmt.Sprintf(" (+%d expired)", n)
				}
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", displayTag(t.GetName()), count, last)
			}
			return w.Flush()
		},
	}
	cmd.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", `limit to one namespace; "*" or empty for all`)
	cmd.AddCommand(tagsRenameCmd(&namespace), tagsDeleteCmd(&namespace))
	return cmd
}

func tagsRenameCmd(namespace *string) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "rename <from> <to>",
		Short: "Rename a tag on every memory (merges if <to> already exists)",
		Long: "Replaces <from> with <to> on every memory (and its search chunks). Metadata-only:\n" +
			"nothing is re-embedded. If a memory already has <to> the result is deduplicated.\n" +
			"Renaming from or to the reserved `preference` tag requires --yes.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			confirm, err := confirmTagChange(os.Stdin, os.Stderr, yes, false, args[0], args[1])
			if err != nil {
				return err
			}
			resp, err := client().RenameTag(cmd.Context(), connect.NewRequest(&cortexv1.RenameTagRequest{
				From: args[0], To: args[1], Namespace: *namespace, ConfirmPreference: confirm,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("renamed %q -> %q on %d memories\n", args[0], strings.TrimSpace(args[1]), resp.Msg.GetMemoriesUpdated())
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm changing the reserved `preference` tag")
	return cmd
}

func tagsDeleteCmd(namespace *string) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "delete <tag>",
		Short: "Remove a tag from every memory (the memories are kept)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			confirm, err := confirmTagChange(os.Stdin, os.Stderr, yes, true, args[0])
			if err != nil {
				return err
			}
			resp, err := client().DeleteTag(cmd.Context(), connect.NewRequest(&cortexv1.DeleteTagRequest{
				Tag: args[0], Namespace: *namespace, ConfirmPreference: confirm,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("removed %q from %d memories\n", args[0], resp.Msg.GetMemoriesUpdated())
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt (also confirms deleting the reserved `preference` tag)")
	return cmd
}

func isPreferenceTag(t string) bool { return strings.EqualFold(strings.TrimSpace(t), "preference") }

// confirmTagChange decides whether the change may proceed and whether the
// reserved-tag confirmation should be sent. A generic "yes" never confirms the
// `preference` tag: it controls the standing preferences injected into every
// session, so it needs --yes or typing the tag name. promptGeneric adds the plain
// yes/no prompt for non-reserved deletes (rename needs none).
func confirmTagChange(in io.Reader, out io.Writer, yes, promptGeneric bool, tags ...string) (confirmPref bool, err error) {
	pref := false
	for _, t := range tags {
		pref = pref || isPreferenceTag(t)
	}
	if yes {
		return pref, nil
	}
	read := func(prompt string) string {
		_, _ = fmt.Fprint(out, prompt)
		var answer string
		_, _ = fmt.Fscan(in, &answer)
		return answer
	}
	if pref {
		_, _ = fmt.Fprintln(out, `WARNING: "preference" controls the standing preferences injected into EVERY session`)
		_, _ = fmt.Fprintln(out, "(the cortex-prefs hook). Renaming or deleting it changes that for all future sessions.")
		if read("Type 'preference' to continue: ") != "preference" {
			return false, fmt.Errorf("aborted")
		}
		return true, nil
	}
	if promptGeneric {
		_, _ = fmt.Fprintf(out, "This removes the tag %q from every memory (memories are kept).\n", tags[0])
		if read("Type 'yes' to continue: ") != "yes" {
			return false, fmt.Errorf("aborted")
		}
	}
	return false, nil
}

// displayTag quotes tags with leading/trailing whitespace so look-alikes
// ("k8s" vs "k8s ") can be told apart; plain tags print as-is.
func displayTag(t string) string {
	if strings.TrimSpace(t) != t || t == "" {
		return fmt.Sprintf("%q", t)
	}
	return t
}
