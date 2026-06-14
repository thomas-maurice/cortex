// Command cortex is the host-side CLI for the second-brain memory store. It is
// a thin client of the Cortex Connect RPC server: every command is one RPC, and
// it holds no NATS/Weaviate/Ollama connection of its own. Point it at a server
// with --server / CORTEX_SERVER_URL and authenticate with --token /
// CORTEX_AUTH_TOKEN.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/alexedwards/argon2id"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/rpc"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// version is the build version, injected at release time via
// -ldflags "-X main.version=...". Defaults to "dev" for un-stamped builds.
var version = "dev"

var (
	serverURL      string
	authToken      string
	defaultNS      string
	source         string
	conversationID string
)

// client builds an RPC client from the resolved global flags.
func client() cortexv1connect.MemoryServiceClient {
	return rpc.NewClient(serverURL, authToken)
}

// hashPasswordCmd generates an argon2id PHC hash for use as CORTEX_UI_PASSWORD,
// so the plaintext password need not be stored in the environment/compose file.
func hashPasswordCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "hash-password",
		Short: "Generate an argon2id hash for CORTEX_UI_PASSWORD",
		Long: "Read a password (no-echo prompt, or piped on stdin) and print an argon2id\n" +
			"PHC hash. Set the result as CORTEX_UI_PASSWORD so the server stores only the\n" +
			"hash, never the plaintext.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pw, err := readSecret()
			if err != nil {
				return err
			}
			if pw == "" {
				return fmt.Errorf("empty password")
			}
			hash, err := argon2id.CreateHash(pw, argon2id.DefaultParams)
			if err != nil {
				return err
			}
			fmt.Println(hash)
			return nil
		},
	}
}

// readSecret reads a password without echoing when stdin is a terminal, or reads
// the first line when piped (e.g. `echo -n pw | cortex hash-password`).
func readSecret() (string, error) {
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		fmt.Fprint(os.Stderr, "Password: ")
		b, err := term.ReadPassword(fd)
		fmt.Fprintln(os.Stderr)
		return strings.TrimSpace(string(b)), err
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func main() {
	root := &cobra.Command{
		Use:           "cortex",
		Short:         "Command-line access to the Cortex second-brain memory store",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringVar(&serverURL, "server", env("CORTEX_SERVER_URL", "http://localhost:8080"), "Cortex RPC server URL")
	pf.StringVar(&authToken, "token", os.Getenv("CORTEX_AUTH_TOKEN"), "bearer token for the Cortex server")
	pf.StringVar(&defaultNS, "namespace-default", env("DEFAULT_NAMESPACE", "global"), "namespace used when none is given")
	pf.StringVar(&source, "source", env("MEMORY_SOURCE", "cli"), "source tag recorded on saved memories")
	pf.StringVar(&conversationID, "conversation", os.Getenv("CLAUDE_CODE_SESSION_ID"), "conversation/session ID stamped on saved memories")

	root.AddCommand(saveCmd(), listCmd(), searchCmd(), deleteCmd(), exportCmd(), reindexCmd(), deadCmd(), statusCmd(), doctorCmd(), summarizeCmd(), summariesCmd(), recallCmd(), hashPasswordCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func saveCmd() *cobra.Command {
	var namespace string
	var tags []string
	cmd := &cobra.Command{
		Use:   "save <text>",
		Short: "Save a memory (queued on the server for async indexing)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			text := strings.TrimSpace(args[0])
			if text == "" {
				return fmt.Errorf("text must not be empty")
			}
			ns := namespace
			if ns == "" {
				ns = defaultNS
			}
			resp, err := client().Save(cmd.Context(), connect.NewRequest(&cortexv1.SaveRequest{
				Text:           text,
				Namespace:      ns,
				Tags:           tags,
				Source:         source,
				ConversationId: conversationID,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("queued %s (namespace=%s)\n", resp.Msg.GetId(), ns)
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace to scope the memory (default: configured default)")
	cmd.Flags().StringSliceVarP(&tags, "tag", "t", nil, "tag to attach (repeatable)")
	return cmd
}

func listCmd() *cobra.Command {
	var namespace string
	var limit int
	var tags, excludeTags []string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List stored memories, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().List(cmd.Context(), connect.NewRequest(&cortexv1.ListRequest{
				Namespace:   namespace,
				Limit:       int32(limit),
				Tags:        tags,
				ExcludeTags: excludeTags,
			}))
			if err != nil {
				return err
			}
			mems := resp.Msg.GetMemories()
			if len(mems) == 0 {
				fmt.Println("No memories found.")
				return nil
			}
			for _, m := range mems {
				printMemory(m)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "max memories to list")
	cmd.Flags().StringSliceVarP(&tags, "tag", "t", nil, "require this tag (repeatable; memory must have all)")
	cmd.Flags().StringSliceVarP(&excludeTags, "exclude-tag", "x", nil, "drop memories with this tag (repeatable)")
	return cmd
}

func searchCmd() *cobra.Command {
	var namespace string
	var limit int
	var maxDistance float32
	var autocut int
	var tags, excludeTags []string
	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Semantic search over stored memories",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.TrimSpace(args[0])
			if query == "" {
				return fmt.Errorf("query must not be empty")
			}
			resp, err := client().Search(cmd.Context(), connect.NewRequest(&cortexv1.SearchRequest{
				Query:       query,
				Namespace:   namespace,
				Limit:       int32(limit),
				MaxDistance: maxDistance,
				Autocut:     int32(autocut),
				Tags:        tags,
				ExcludeTags: excludeTags,
			}))
			if err != nil {
				return err
			}
			hits := resp.Msg.GetHits()
			if len(hits) == 0 {
				fmt.Println("No memories found.")
				return nil
			}
			for i, h := range hits {
				fmt.Printf("%d. (dist %.3f) ", i+1, h.GetDistance())
				printMemory(h.GetMemory())
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 5, "max results")
	cmd.Flags().Float32VarP(&maxDistance, "max-distance", "d", 0.6, "relevance cutoff; drop matches farther than this (model/query-dependent; 0 disables)")
	cmd.Flags().IntVar(&autocut, "autocut", 0, "adaptive cutoff: keep results before the Nth distance jump (0 disables)")
	cmd.Flags().StringSliceVarP(&tags, "tag", "t", nil, "require this tag (repeatable; memory must have all)")
	cmd.Flags().StringSliceVarP(&excludeTags, "exclude-tag", "x", nil, "drop memories with this tag (repeatable)")
	return cmd
}

func deleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a memory by ID (get the ID from list or search)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := strings.TrimSpace(args[0])
			if id == "" {
				return fmt.Errorf("id must not be empty")
			}
			if _, err := client().Delete(cmd.Context(), connect.NewRequest(&cortexv1.DeleteRequest{Id: id})); err != nil {
				return err
			}
			fmt.Println("deleted", id)
			return nil
		},
	}
	return cmd
}

// exportRecord is the stable JSON shape for `cortex export` (no vectors).
type exportRecord struct {
	ID             string   `json:"id"`
	Text           string   `json:"text"`
	Namespace      string   `json:"namespace"`
	Tags           []string `json:"tags,omitempty"`
	Source         string   `json:"source"`
	CreatedAt      string   `json:"createdAt"`
	Model          string   `json:"model,omitempty"`
	Dims           int32    `json:"dims,omitempty"`
	ConversationID string   `json:"conversationId,omitempty"`
}

func exportCmd() *cobra.Command {
	var namespace, out string
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Dump stored memories (text + metadata, no vectors) to JSON",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().List(cmd.Context(), connect.NewRequest(&cortexv1.ListRequest{
				Namespace: namespace,
				Limit:     allLimit,
			}))
			if err != nil {
				return err
			}
			mems := resp.Msg.GetMemories()
			recs := make([]exportRecord, 0, len(mems))
			for _, m := range mems {
				recs = append(recs, toExportRecord(m))
			}
			data, err := json.MarshalIndent(recs, "", "  ")
			if err != nil {
				return err
			}
			if out == "" || out == "-" {
				fmt.Println(string(data))
				return nil
			}
			if err := os.WriteFile(out, data, 0o644); err != nil {
				return err
			}
			fmt.Printf("exported %d memories to %s\n", len(recs), out)
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "*", `namespace to export; "*" for all (default)`)
	cmd.Flags().StringVarP(&out, "out", "o", "", "output file (default stdout)")
	return cmd
}

func reindexCmd() *cobra.Command {
	var namespace string
	var yes bool
	cmd := &cobra.Command{
		Use:   "reindex",
		Short: "Re-embed memories through the worker (e.g. after an embedding-model change)",
		Long: "Asks the server to snapshot every memory to a backup, then republish each onto NATS\n" +
			"so the worker re-embeds it with its currently configured model and re-stamps provenance.\n" +
			"If the new model's vector dimension differs from what is stored, the Memory class must be\n" +
			"dropped and recreated first (requires --yes). The worker must already point at the target\n" +
			"model before you run this.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().Reindex(cmd.Context(), connect.NewRequest(&cortexv1.ReindexRequest{
				Namespace:    namespace,
				ForceRebuild: yes,
			}))
			if err != nil {
				return err
			}
			msg := resp.Msg
			if msg.GetBackupPath() != "" {
				fmt.Printf("server backed up memories to %s\n", msg.GetBackupPath())
			}
			if msg.GetRebuilt() {
				fmt.Printf("dimension change %d -> %d: server dropped and recreated the Memory class\n", msg.GetOldDims(), msg.GetNewDims())
			}
			fmt.Println(msg.GetMessage())
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "*", `namespace to reindex; "*" for all (default)`)
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm a destructive class rebuild on dimension change")
	return cmd
}

func deadCmd() *cobra.Command {
	var requeue, purge bool
	cmd := &cobra.Command{
		Use:   "dead",
		Short: "List memories that failed indexing and were dead-lettered",
		Long: "Memories that fail to index after the worker's retry limit are preserved on a\n" +
			"dead-letter subject instead of being dropped. This lists them; --requeue resubmits\n" +
			"them for another indexing attempt (then clears them); --purge discards them.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if requeue && purge {
				return fmt.Errorf("--requeue and --purge are mutually exclusive")
			}
			action := cortexv1.DeadAction_DEAD_ACTION_LIST
			switch {
			case requeue:
				action = cortexv1.DeadAction_DEAD_ACTION_REQUEUE
			case purge:
				action = cortexv1.DeadAction_DEAD_ACTION_PURGE
			}
			resp, err := client().Dead(cmd.Context(), connect.NewRequest(&cortexv1.DeadRequest{Action: action}))
			if err != nil {
				return err
			}
			switch action {
			case cortexv1.DeadAction_DEAD_ACTION_REQUEUE:
				fmt.Printf("requeued %d memories for indexing\n", resp.Msg.GetAffected())
				return nil
			case cortexv1.DeadAction_DEAD_ACTION_PURGE:
				fmt.Printf("purged %d dead-lettered memories\n", resp.Msg.GetAffected())
				return nil
			}
			dls := resp.Msg.GetDeadLetters()
			if len(dls) == 0 {
				fmt.Println("No dead-lettered memories.")
				return nil
			}
			for _, dl := range dls {
				printMemory(dl.GetRecord())
				fmt.Printf("   failed=%s deliveries=%d\n   error: %s\n",
					dl.GetFailedAt().AsTime().Format(time.RFC3339), dl.GetDeliveries(), dl.GetError())
			}
			fmt.Printf("\n%d dead-lettered memories (fix the cause, then `cortex dead --requeue`)\n", len(dls))
			return nil
		},
	}
	cmd.Flags().BoolVar(&requeue, "requeue", false, "resubmit all dead-lettered memories for indexing, then clear them")
	cmd.Flags().BoolVar(&purge, "purge", false, "discard all dead-lettered memories")
	return cmd
}

func statusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show server health and store size",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().Status(cmd.Context(), connect.NewRequest(&cortexv1.StatusRequest{}))
			if err != nil {
				return err
			}
			m := resp.Msg
			fmt.Printf("server   %s\n", m.GetVersion())
			fmt.Printf("nats     %s\n", okStr(m.GetNatsOk()))
			fmt.Printf("weaviate %s\n", okStr(m.GetWeaviateOk()))
			fmt.Printf("ollama   %s (model=%s dims=%d)\n", okStr(m.GetOllamaOk()), m.GetModel(), m.GetDims())
			fmt.Printf("memories %d\n", m.GetMemoryCount())
			return nil
		},
	}
	return cmd
}

func doctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run server-side diagnostics and print a per-check breakdown",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().Doctor(cmd.Context(), connect.NewRequest(&cortexv1.DoctorRequest{}))
			if err != nil {
				return err
			}
			for _, c := range resp.Msg.GetChecks() {
				fmt.Printf("%s %-12s %s\n", okStr(c.GetOk()), c.GetName(), c.GetDetail())
			}
			if !resp.Msg.GetHealthy() {
				return fmt.Errorf("one or more checks failed")
			}
			return nil
		},
	}
	return cmd
}

func summarizeCmd() *cobra.Command {
	var namespace string
	cmd := &cobra.Command{
		Use:   "summarize <text>",
		Short: "Save/update the summary of a conversation (unique per --conversation)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			text := strings.TrimSpace(args[0])
			if text == "" {
				return fmt.Errorf("text must not be empty")
			}
			if conversationID == "" {
				return fmt.Errorf("no conversation ID (set --conversation or CLAUDE_CODE_SESSION_ID)")
			}
			resp, err := client().SummarizeSession(cmd.Context(), connect.NewRequest(&cortexv1.SummarizeSessionRequest{
				ConversationId: conversationID,
				Text:           text,
				Namespace:      namespace,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("summary queued (conversation=%s)\n", resp.Msg.GetConversationId())
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace to scope the summary (default: configured default)")
	return cmd
}

func summariesCmd() *cobra.Command {
	var namespace string
	var limit int
	cmd := &cobra.Command{
		Use:   "summaries",
		Short: "List conversation summaries, most-recently-updated first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().ListSummaries(cmd.Context(), connect.NewRequest(&cortexv1.ListSummariesRequest{
				Namespace: namespace,
				Limit:     int32(limit),
			}))
			if err != nil {
				return err
			}
			sums := resp.Msg.GetSummaries()
			if len(sums) == 0 {
				fmt.Println("No summaries found.")
				return nil
			}
			for _, s := range sums {
				printSummary(s)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 20, "max summaries to list")
	return cmd
}

func recallCmd() *cobra.Command {
	var namespace string
	var factLimit int
	var maxDistance float32
	cmd := &cobra.Command{
		Use:   "recall <query>",
		Short: "Recall a past session: best-matching summary + its facts",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := strings.TrimSpace(args[0])
			if query == "" {
				return fmt.Errorf("query must not be empty")
			}
			resp, err := client().RecallSession(cmd.Context(), connect.NewRequest(&cortexv1.RecallSessionRequest{
				Query:       query,
				Namespace:   namespace,
				FactLimit:   int32(factLimit),
				MaxDistance: maxDistance,
			}))
			if err != nil {
				return err
			}
			if !resp.Msg.GetMatched() {
				fmt.Println("No matching past session found.")
				return nil
			}
			sum := resp.Msg.GetSummary()
			fmt.Printf("=== session %s (dist %.3f) ===\n%s\n", sum.GetConversationId(), sum.GetDistance(), sum.GetText())
			facts := resp.Msg.GetFacts()
			if len(facts) > 0 {
				fmt.Printf("\n--- %d facts from that session ---\n", len(facts))
			}
			for _, f := range facts {
				printMemory(f)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVar(&factLimit, "fact-limit", 50, "max facts to return for the matched session")
	cmd.Flags().Float32VarP(&maxDistance, "max-distance", "d", 0, "relevance cutoff on the summary match (0 = server default)")
	return cmd
}

// allLimit caps a full-store fetch (export). Matches the server-side cap.
const allLimit = 10000

func okStr(ok bool) string {
	if ok {
		return "OK "
	}
	return "DOWN"
}

func toExportRecord(m *cortexv1.Memory) exportRecord {
	return exportRecord{
		ID:             m.GetId(),
		Text:           m.GetText(),
		Namespace:      m.GetNamespace(),
		Tags:           m.GetTags(),
		Source:         m.GetSource(),
		CreatedAt:      m.GetCreatedAt().AsTime().UTC().Format(time.RFC3339),
		Model:          m.GetModel(),
		Dims:           m.GetDims(),
		ConversationID: m.GetConversationId(),
	}
}

func printSummary(s *cortexv1.ConversationSummary) {
	updated := s.GetUpdatedAt().AsTime().UTC().Format(time.RFC3339)
	fmt.Printf("[%s] %s\n   conversation=%s updated=%s\n", s.GetNamespace(), s.GetText(), s.GetConversationId(), updated)
}

func printMemory(m *cortexv1.Memory) {
	line := fmt.Sprintf("[%s] %s", m.GetNamespace(), m.GetText())
	if tags := m.GetTags(); len(tags) > 0 {
		line += "  #" + strings.Join(tags, " #")
	}
	meta := "id=" + m.GetId()
	if m.GetModel() != "" {
		meta += fmt.Sprintf("  model=%s dims=%d", m.GetModel(), m.GetDims())
	}
	fmt.Printf("%s\n   %s\n", line, meta)
}
