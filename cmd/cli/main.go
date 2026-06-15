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
	"google.golang.org/protobuf/types/known/timestamppb"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/config"
	"github.com/thomas-maurice/cortex/internal/rpc"
)

// settings maps each persistent flag to its environment variable and built-in
// default. The same set drives flag registration, viper env binding, and the
// generated sample config, so there is one source of truth for the precedence
// chain: explicit flag > env var > config file > built-in default.
var settings = []struct {
	key, env, def, usage string
	target               *string
}{
	{"server", "CORTEX_SERVER_URL", "http://localhost:8080", "Cortex RPC server URL", &serverURL},
	{"token", "CORTEX_AUTH_TOKEN", "", "bearer token for the Cortex server", &authToken},
	{"namespace-default", "DEFAULT_NAMESPACE", "global", "namespace used when none is given", &defaultNS},
	{"source", "MEMORY_SOURCE", "cli", "source tag recorded on saved memories", &source},
	{"conversation", "CLAUDE_CODE_SESSION_ID", "", "conversation/session ID stamped on saved memories", &conversationID},
}

// initConfig layers the config file under the flags/env via viper and writes the
// resolved values back into the global flag variables the commands read.
func initConfig(cmd *cobra.Command) error {
	v, err := config.New(configFile)
	if err != nil {
		return err
	}
	pf := cmd.Root().PersistentFlags()
	for _, s := range settings {
		_ = v.BindPFlag(s.key, pf.Lookup(s.key))
		_ = v.BindEnv(s.key, s.env)
		*s.target = v.GetString(s.key)
	}
	v.SetDefault("save.hostname-tag", false) // opt-in: stamp host:<hostname> on saves
	saveTags = v.GetStringSlice("save.tags")
	hostnameTag = v.GetBool("save.hostname-tag")
	return nil
}

// version is the build version, injected at release time via
// -ldflags "-X main.version=...". Defaults to "dev" for un-stamped builds.
var version = "dev"

var (
	configFile     string
	serverURL      string
	authToken      string
	defaultNS      string
	source         string
	conversationID string
	// saveTags / hostnameTag drive the client-side tags stamped on every saved
	// memory (config keys save.tags and save.hostname-tag). Resolved in
	// initConfig; not flag-backed (string-slice/bool, unlike the settings table).
	saveTags    []string
	hostnameTag bool
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

// sampleConfig is the starter file written by `cortex config init`. Keys mirror
// the persistent flags (shared with the MCP server); the mcp.* block holds the
// MCP tool defaults applied when a call omits the field.
const sampleConfig = `# Cortex configuration, shared by the CLI and the MCP server.
# Resolution order: command-line flag > environment variable > this file > built-in default.

# --- client settings ---
server: http://localhost:8080   # Cortex RPC server URL (env: CORTEX_SERVER_URL)
token: ""                       # bearer token (env: CORTEX_AUTH_TOKEN)
namespace-default: global       # namespace used when none is given (env: DEFAULT_NAMESPACE)
source: cli                     # source tag recorded on saved memories (env: MEMORY_SOURCE)

# --- save-time tags (stamped client-side on every memory this host saves; save-only, never used to query) ---
save:
  tags: []                      # static tags added to every save, e.g. [personal] or [work] to mark this profile
  hostname-tag: false           # when true, also stamp host:<hostname> on every save (opt-in)

# --- MCP server defaults (applied when a tool call omits the field; 0 = defer to server) ---
mcp:
  search-limit: 10              # default max results for cortex_memory_search
  fact-limit: 50                # default max facts for cortex_recall_session
  max-distance: 0.45            # relevance cutoff, cosine distance; 0 = no cutoff (env: MAX_DISTANCE)
`

func configCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Inspect or scaffold the cortex.yaml config file",
		Args:  cobra.NoArgs,
	}

	show := &cobra.Command{
		Use:   "show",
		Short: "Print the effective configuration (flags + env + file merged)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path := configFile
			if path == "" {
				path = config.FilePath()
			}
			fmt.Printf("config file:       %s\n", path)
			fmt.Printf("server:            %s\n", serverURL)
			fmt.Printf("token:             %s\n", maskToken(authToken))
			fmt.Printf("namespace-default: %s\n", defaultNS)
			fmt.Printf("source:            %s\n", source)
			fmt.Printf("save auto-tags:    %v\n", config.AutoTags(saveTags, hostnameTag))
			return nil
		},
	}

	var force bool
	initc := &cobra.Command{
		Use:   "init",
		Short: "Write a starter config file (does not overwrite without --force)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path := configFile
			if path == "" {
				path = config.FilePath()
			}
			if _, err := os.Stat(path); err == nil && !force {
				return fmt.Errorf("%s already exists; pass --force to overwrite", path)
			}
			if err := os.MkdirAll(config.Dir(), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte(sampleConfig), 0o600); err != nil {
				return err
			}
			fmt.Printf("wrote %s\n", path)
			return nil
		},
	}
	initc.Flags().BoolVar(&force, "force", false, "overwrite an existing config file")

	cmd.AddCommand(show, initc)
	return cmd
}

// maskToken hides all but the last 4 characters of a bearer token so `config
// show` is safe to paste into a bug report.
func maskToken(t string) string {
	if t == "" {
		return "(unset)"
	}
	if len(t) <= 4 {
		return "****"
	}
	return "****" + t[len(t)-4:]
}

func main() {
	root := &cobra.Command{
		Use:           "cortex",
		Short:         "Command-line access to the Cortex second-brain memory store",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return initConfig(cmd)
		},
	}
	pf := root.PersistentFlags()
	pf.StringVar(&configFile, "config", os.Getenv("CORTEX_CONFIG"), "path to config file (default "+config.FilePath()+")")
	// Flag defaults are the built-in fallbacks only; env vars and the config file
	// are layered in by initConfig via viper, so they must not be baked in here.
	for _, s := range settings {
		pf.StringVar(s.target, s.key, s.def, s.usage)
	}

	root.AddCommand(saveCmd(), editCmd(), listCmd(), searchCmd(), deleteCmd(), exportCmd(), importCmd(), reindexCmd(), deadCmd(), statusCmd(), doctorCmd(), summarizeCmd(), summariesCmd(), recallCmd(), candidatesCmd(), consolidateCmd(), hashPasswordCmd(), configCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func saveCmd() *cobra.Command {
	var namespace string
	var tags, linkTo, supersedes []string
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
				Tags:           config.MergeTags(tags, config.AutoTags(saveTags, hostnameTag)),
				LinkTo:         linkTo,
				Supersedes:     supersedes,
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
	cmd.Flags().StringSliceVarP(&linkTo, "link-to", "L", nil, "ID of an existing memory to link this one to (repeatable; applied after indexing)")
	cmd.Flags().StringSliceVarP(&supersedes, "supersedes", "S", nil, "ID of an existing memory this one replaces (repeatable; the server deletes it once this memory is indexed)")
	return cmd
}

func editCmd() *cobra.Command {
	var namespace string
	var tags []string
	var replaceTags bool
	cmd := &cobra.Command{
		Use:   "edit <id> <text>",
		Short: "Edit an existing memory's text (re-embedded; keeps id, links, history)",
		Long: "Replace a memory's text and re-embed it through the worker, preserving its ID, creation time,\n" +
			"links and dedup decisions. Tags are kept unless you pass --tag (which sets --replace-tags); the\n" +
			"namespace is kept unless you pass --namespace. The edit is queued for async re-indexing.",
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := strings.TrimSpace(args[0])
			text := strings.TrimSpace(args[1])
			if id == "" {
				return fmt.Errorf("id must not be empty")
			}
			if text == "" {
				return fmt.Errorf("text must not be empty")
			}
			// Passing --tag implies the user wants those tags applied.
			if cmd.Flags().Changed("tag") {
				replaceTags = true
			}
			resp, err := client().UpdateMemory(cmd.Context(), connect.NewRequest(&cortexv1.UpdateMemoryRequest{
				Id:          id,
				Text:        text,
				Tags:        tags,
				ReplaceTags: replaceTags,
				Namespace:   namespace,
			}))
			if err != nil {
				return err
			}
			fmt.Printf("queued %s for re-indexing\n", resp.Msg.GetId())
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", "move the memory to this namespace (default: keep current)")
	cmd.Flags().StringSliceVarP(&tags, "tag", "t", nil, "replace the memory's tags with these (repeatable; implies --replace-tags)")
	cmd.Flags().BoolVar(&replaceTags, "replace-tags", false, "replace tags even with an empty set (clears all tags)")
	return cmd
}

func listCmd() *cobra.Command {
	var namespace string
	var limit int
	var tags, anyTags, excludeTags []string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List stored memories, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().List(cmd.Context(), connect.NewRequest(&cortexv1.ListRequest{
				Namespace:   namespace,
				Limit:       int32(limit),
				Tags:        tags,
				AnyTags:     anyTags,
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
	cmd.Flags().StringSliceVarP(&anyTags, "any-tag", "T", nil, "require at least one of these tags (repeatable)")
	cmd.Flags().StringSliceVarP(&excludeTags, "exclude-tag", "x", nil, "drop memories with this tag (repeatable)")
	return cmd
}

func searchCmd() *cobra.Command {
	var namespace string
	var limit int
	var maxDistance float32
	var autocut int
	var tags, anyTags, excludeTags []string
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
				AnyTags:     anyTags,
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
	cmd.Flags().StringSliceVarP(&anyTags, "any-tag", "T", nil, "require at least one of these tags (repeatable)")
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
	LinkedIDs      []string `json:"linkedIds,omitempty"`
	NotDuplicateOf []string `json:"notDuplicateOf,omitempty"`
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

func importCmd() *cobra.Command {
	var batch int
	cmd := &cobra.Command{
		Use:   "import <file>",
		Short: "Restore memories from a `cortex export` JSON dump via the normal NATS ingest queue",
		Long: "Reads a JSON dump (from `cortex export`) and republishes every memory onto the server's NATS\n" +
			"index queue — the SAME path a save takes — so the worker re-embeds and upserts each one. Ids,\n" +
			"namespace, tags, createdAt, links and not-duplicate decisions are preserved; vectors are NOT\n" +
			"carried but recomputed by the target worker's model, so a restore is safe across model changes.\n" +
			"An existing id is overwritten (upsert). Point --server/--token at the TARGET (e.g. a dev stack).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var recs []exportRecord
			if err := json.Unmarshal(data, &recs); err != nil {
				return fmt.Errorf("parse %s: %w", args[0], err)
			}
			if len(recs) == 0 {
				fmt.Println("nothing to import")
				return nil
			}

			mems := make([]*cortexv1.Memory, 0, len(recs))
			for _, r := range recs {
				m := &cortexv1.Memory{
					Id:             r.ID,
					Text:           r.Text,
					Namespace:      r.Namespace,
					Tags:           r.Tags,
					Source:         r.Source,
					Model:          r.Model,
					Dims:           r.Dims,
					ConversationId: r.ConversationID,
					LinkedIds:      r.LinkedIDs,
					NotDuplicateOf: r.NotDuplicateOf,
				}
				if r.CreatedAt != "" {
					if t, err := time.Parse(time.RFC3339, r.CreatedAt); err == nil {
						m.CreatedAt = timestamppb.New(t)
					}
				}
				mems = append(mems, m)
			}

			total := 0
			for start := 0; start < len(mems); start += batch {
				end := start + batch
				if end > len(mems) {
					end = len(mems)
				}
				resp, err := client().RestoreMemories(cmd.Context(), connect.NewRequest(&cortexv1.RestoreMemoriesRequest{
					Memories: mems[start:end],
				}))
				if err != nil {
					return fmt.Errorf("restore batch [%d:%d]: %w", start, end, err)
				}
				total += int(resp.Msg.GetQueued())
			}
			fmt.Printf("queued %d/%d memories for re-indexing\n", total, len(recs))
			return nil
		},
	}
	cmd.Flags().IntVar(&batch, "batch", 500, "memories per request")
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

func candidatesCmd() *cobra.Command {
	var namespace string
	var limit int
	cmd := &cobra.Command{
		Use:   "candidates",
		Short: "List memories flagged as likely duplicates, for review",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().ListDuplicateCandidates(cmd.Context(), connect.NewRequest(&cortexv1.ListDuplicateCandidatesRequest{
				Namespace: namespace,
				Limit:     int32(limit),
			}))
			if err != nil {
				return err
			}
			groups := resp.Msg.GetGroups()
			if len(groups) == 0 {
				fmt.Println("No duplicate candidates flagged.")
				return nil
			}
			for _, g := range groups {
				printMemory(g.GetMemory())
				for _, c := range g.GetCandidates() {
					fmt.Printf("   ~ likely duplicate of id=%s: %s\n", c.GetId(), c.GetText())
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 50, "max flagged memories to list")
	cmd.AddCommand(dismissCandidateCmd())
	return cmd
}

func dismissCandidateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dismiss <id> <target-id>",
		Short: "Mark two flagged memories as NOT duplicates (stops re-flagging)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := client().DismissDuplicate(cmd.Context(), connect.NewRequest(&cortexv1.DismissDuplicateRequest{
				Id:       strings.TrimSpace(args[0]),
				TargetId: strings.TrimSpace(args[1]),
			}))
			if err != nil {
				return err
			}
			fmt.Printf("Dismissed. %s is now marked not-a-duplicate of %d memor%s.\n",
				args[0], len(resp.Msg.GetNotDuplicateOf()), map[bool]string{true: "y", false: "ies"}[len(resp.Msg.GetNotDuplicateOf()) == 1])
			return nil
		},
	}
}

func consolidateCmd() *cobra.Command {
	var namespace string
	var limit int
	var maxDistance float32
	var tags, anyTags, excludeTags []string
	cmd := &cobra.Command{
		Use:   "consolidate <topic>",
		Short: "Gather the cluster of memories about a topic, for review/merging",
		Long: "Print the related memories about a topic — the vector matches plus their linked and likely-\n" +
			"duplicate neighbours — and the manifest of their ids. This is the read-only gather step; merging is\n" +
			"done by an LLM (the MCP cortex_consolidate tool), which saves the compiled memories with\n" +
			"--supersedes set to the ids they replace. Nothing is written or deleted by this command.\n\n" +
			"Tag flags scope the cluster. With no tag flags there is NO tag filtering — the whole topic cluster\n" +
			"in the namespace is gathered, across every tag (this does NOT mean only untagged memories).",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := strings.TrimSpace(args[0])
			if topic == "" {
				return fmt.Errorf("topic must not be empty")
			}
			resp, err := client().Consolidate(cmd.Context(), connect.NewRequest(&cortexv1.ConsolidateRequest{
				Topic:       topic,
				Namespace:   namespace,
				Limit:       int32(limit),
				MaxDistance: maxDistance,
				Tags:        tags,
				AnyTags:     anyTags,
				ExcludeTags: excludeTags,
			}))
			if err != nil {
				return err
			}
			cluster := resp.Msg.GetCluster()
			if len(cluster) == 0 {
				fmt.Println("No memories found for that topic; nothing to consolidate.")
				return nil
			}
			for _, m := range cluster {
				printMemory(m)
			}
			fmt.Printf("\nmanifest (%d supersede-able ids): %s\n", len(resp.Msg.GetManifest()), strings.Join(resp.Msg.GetManifest(), " "))
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespace, "namespace", "n", "", `namespace filter; "*" for all namespaces`)
	cmd.Flags().IntVarP(&limit, "limit", "l", 25, "max memories to gather into the cluster")
	cmd.Flags().Float32VarP(&maxDistance, "max-distance", "d", 0, "relevance cutoff on the topic match (<=0 = server default)")
	cmd.Flags().StringSliceVarP(&tags, "tag", "t", nil, "only consolidate memories with all of these tags (repeatable; omit = no tag filter)")
	cmd.Flags().StringSliceVarP(&anyTags, "any-tag", "T", nil, "only consolidate memories with at least one of these tags (repeatable)")
	cmd.Flags().StringSliceVarP(&excludeTags, "exclude-tag", "x", nil, "drop memories with this tag from the cluster (repeatable)")
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
		LinkedIDs:      m.GetLinkedIds(),
		NotDuplicateOf: m.GetNotDuplicateOf(),
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
