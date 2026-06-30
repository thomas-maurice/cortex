// Command mcp is the MCP server Claude connects to over stdio. It is a thin
// client of the Cortex Connect RPC server: every tool call is a single RPC.
// It holds no NATS/Weaviate/Ollama connection of its own.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	cortexv1 "github.com/thomas-maurice/cortex/gen/cortex/v1"
	"github.com/thomas-maurice/cortex/gen/cortex/v1/cortexv1connect"
	"github.com/thomas-maurice/cortex/internal/config"
	"github.com/thomas-maurice/cortex/internal/rpc"
)

// version is the build version, injected at release time via
// -ldflags "-X main.version=...". Defaults to "dev" for un-stamped builds.
var version = "dev"

// resolveDefaultNamespace picks the namespace used when a tool call omits one.
// An explicit DEFAULT_NAMESPACE env always wins (per-project override). Otherwise
// it is detected from the launch directory: the full git origin remote URL (e.g.
// "git@github.com:thomas-maurice/cortex.git") if the cwd is a repo with an origin,
// else the directory basename, else "global". This gives each project its own
// memory scope automatically.
func resolveDefaultNamespace() string {
	if ns := os.Getenv("DEFAULT_NAMESPACE"); ns != "" {
		return ns
	}
	wd, err := os.Getwd()
	if err != nil {
		return "global"
	}
	if url, err := gitRemoteURL(wd); err == nil && url != "" {
		return url
	}
	if base := filepath.Base(wd); base != "" && base != "." && base != string(os.PathSeparator) {
		return base
	}
	return "global"
}

// nsOrDefault applies the per-project default namespace when the caller omitted
// one. "*" (all namespaces) and any explicit value pass through unchanged. This
// makes an omitted namespace consistently mean "this project" across every tool
// (Save always did); without it the server would fall back to its own global
// default, scattering a tool's reads/writes away from the project's namespace.
func (d *deps) nsOrDefault(ns string) string {
	if ns == "" {
		return d.defaultNamespace
	}
	return ns
}

// resolveConversationID picks the ID that ties this session's saves and summary
// together, in priority order:
//
//  1. CORTEX_CONVERSATION_ID — an explicit override, the deterministic injection
//     point for a hook/wrapper that knows the real session ID.
//  2. CLAUDE_CODE_SESSION_ID — the real Claude Code session ID, IF it reaches the
//     MCP server's env. Note: Claude Code does NOT reliably inject this into MCP
//     subprocesses (empirically absent for a project .mcp.json server), so it
//     may not be present even under Claude Code.
//  3. A per-process UUID fallback — summaries still work within this process's
//     lifetime, but won't survive an MCP restart and won't match the real session.
//
// A value that arrives unexpanded as a literal "${...}" (failed .mcp.json
// expansion) is treated as absent. Either way the summary and the facts saved
// during the session share one ID, so recall links them.
func resolveConversationID(log *slog.Logger) string {
	if cid := firstUsableEnv("CORTEX_CONVERSATION_ID", "CLAUDE_CODE_SESSION_ID"); cid != "" {
		log.Info("conversation id resolved from env", "conversationId", cid)
		return cid
	}
	cid := uuid.NewString()
	log.Warn("no conversation id in env (set CORTEX_CONVERSATION_ID, or forward "+
		"CLAUDE_CODE_SESSION_ID); using a per-process id that won't survive an MCP restart",
		"conversationId", cid)
	return cid
}

// firstUsableEnv returns the first env var that is set and not an unexpanded
// "${...}" placeholder.
func firstUsableEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" && !strings.HasPrefix(v, "${") {
			return v
		}
	}
	return ""
}

// gitRemoteURL returns the origin remote URL for the repo at dir, verbatim, or an
// error if dir is not a repo / has no origin / git is unavailable.
func gitRemoteURL(dir string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "-C", dir, "remote", "get-url", "origin").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// deps holds the RPC client and request defaults shared by all tool handlers.
type deps struct {
	client             cortexv1connect.MemoryServiceClient
	defaultNamespace   string
	source             string
	conversationID     string
	defaultMaxDistance float32
	defaultSearchLimit int
	defaultFactLimit   int
	// autoSaveTags are stamped on every memory this client saves (the static
	// save.tags list plus, opt-in, a host:<hostname> tag). Save-only, never used
	// to filter searches.
	autoSaveTags []string
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// The MCP server has no flags (Claude launches it over stdio), so the config
	// file sits directly under the env vars. viper resolves each key in the order
	// env > cortex.yaml > built-in default.
	cfg, err := config.New(os.Getenv("CORTEX_CONFIG"))
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}
	cfg.SetDefault("server", "http://localhost:8080")
	cfg.SetDefault("source", "claude-code")
	cfg.SetDefault("mcp.search-limit", 0) // 0 = defer to the server's own default
	cfg.SetDefault("mcp.fact-limit", 0)
	cfg.SetDefault("save.hostname-tag", false) // opt-in: stamp host:<hostname> on saves
	_ = cfg.BindEnv("server", "CORTEX_SERVER_URL")
	_ = cfg.BindEnv("token", "CORTEX_AUTH_TOKEN")
	_ = cfg.BindEnv("source", "MEMORY_SOURCE")
	_ = cfg.BindEnv("mcp.max-distance", "MAX_DISTANCE")

	var (
		serverURL   = cfg.GetString("server")
		authToken   = cfg.GetString("token")
		defaultNS   = resolveDefaultNamespace()
		source      = cfg.GetString("source")
		maxDistance = float32(cfg.GetFloat64("mcp.max-distance"))
	)

	conversationID := resolveConversationID(log)

	autoSaveTags := config.AutoTags(cfg.GetStringSlice("save.tags"), cfg.GetBool("save.hostname-tag"))

	d := &deps{
		client:             rpc.NewClient(serverURL, authToken),
		defaultNamespace:   defaultNS,
		source:             source,
		conversationID:     conversationID,
		defaultMaxDistance: maxDistance,
		defaultSearchLimit: cfg.GetInt("mcp.search-limit"),
		defaultFactLimit:   cfg.GetInt("mcp.fact-limit"),
		autoSaveTags:       autoSaveTags,
	}

	server := mcp.NewServer(&mcp.Implementation{
		Name:    "cortex",
		Version: version,
	}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_memory_save",
		Description: "Save a memory to the user's second brain. Use this PROACTIVELY and OFTEN — " +
			"whenever the user states a fact, preference, decision, plan, or piece of context worth " +
			"recalling in a future session. Do not wait to be asked. Write a self-contained note of " +
			"one to a few sentences that captures enough surrounding context to be understood months " +
			"later on its own (who/what/why, not just a keyword). Memories can be as long as they need " +
			"to be — a rich, detailed note is better than a lossy one. Strongly prefer writing the text " +
			"as Markdown: use headings, bullet lists, code fences, and links where they make the note " +
			"clearer, since the UI renders it as formatted Markdown. Summarising a discussion or a " +
			"conclusion into one rich memory is encouraged. Scope it with a namespace (e.g. the " +
			"project or topic) and tags. To connect this memory to related ones in the same call, pass " +
			"their IDs in the linkTo field — links are bidirectional and queued, applied once both this " +
			"memory and each target are indexed, so you do not need a separate cortex_memory_link call and " +
			"the targets need not be indexed yet. Indexing is asynchronous and durable; saving is cheap, so " +
			"err on the side of saving more.",
	}, d.save)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_memory_search",
		Description: "Semantic search over the user's stored memories — their second brain of facts, " +
			"preferences, decisions, conventions, and past work. SEARCH IT BEFORE YOU ANSWER OR ACT: it is " +
			"the FIRST step of a task, not a fallback. Default to searching at the start of essentially every " +
			"non-trivial task, and ALWAYS the moment the user references anything that could carry prior " +
			"context — a system/service/host, a project or repo, a person, a tool or library choice, a past " +
			"decision/convention/preference, an error or symptom, or phrasing like 'how did we', 'last time', " +
			"'as usual', 'the normal way', 'remember', or a name/term you don't recognise. When unsure whether " +
			"something is in memory, SEARCH ANYWAY — a cheap empty result beats answering without the user's own " +
			"context or contradicting a stored decision/preference. Only skip it for a pure greeting or a fully " +
			"self-contained mechanical step. Returns the most relevant memories for a natural-language query, " +
			"optionally filtered by namespace, required/excluded tags, and a relevance cutoff (maxDistance) that " +
			"drops weak matches.",
	}, d.search)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "cortex_memory_delete",
		Description: "Delete a memory by its ID (as returned by cortex_memory_search).",
	}, d.del)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_memory_edit",
		Description: "Correct or update an EXISTING memory by its ID (as returned by cortex_memory_search). " +
			"Pass the FULL new text — it replaces the old text and is re-embedded, so the corrected wording is " +
			"what future searches match. Use this when a stored memory is wrong, outdated, or should be reworded " +
			"or enriched, rather than deleting and re-saving (editing keeps the memory's ID, links, and history). " +
			"Strongly prefer Markdown for the text, as the UI renders it. Tags are left untouched unless you set " +
			"replaceTags=true, in which case the tags field becomes the memory's new tag set (an empty list clears " +
			"them). Namespace is left untouched unless you pass a non-empty namespace. Editing is asynchronous: the " +
			"updated memory is re-indexed shortly after the call returns.",
	}, d.edit)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_memory_link",
		Description: "Create an explicit, durable link between two existing memories so they are connected " +
			"in the user's knowledge graph and can be traversed together. When you notice two memories are " +
			"strongly, meaningfully related — e.g. a decision and the bug that motivated it, a preference and " +
			"the project it applies to, or two facts about the same system — link them AUTOMATICALLY without " +
			"asking first; only pause to ask on borderline or weakly-related pairs. Do not link merely " +
			"topically-adjacent memories — that produces a useless hairball graph; the bar is a real " +
			"relationship, not surface similarity. The link is bidirectional and applied asynchronously: it is " +
			"queued and takes effect once both memories are indexed, so you may pass an ID you just got back " +
			"from cortex_memory_save this turn (still being indexed) as well as IDs from cortex_memory_search " +
			"or cortex_recall_session — the link waits for both endpoints to exist. The linkTo field of " +
			"cortex_memory_save remains a convenient shortcut for linking a new memory at save time.",
	}, d.link)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "cortex_memory_unlink",
		Description: "Remove the explicit link between two memories (by their IDs). The inverse of cortex_memory_link.",
	}, d.unlink)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_session_summarize",
		Description: "Save/update a running summary of the CURRENT conversation so it can be recalled later " +
			"by meaning (e.g. \"the session where we patched the router\"). Call this PROACTIVELY and " +
			"FREQUENTLY — after each meaningful step or topic shift, and again before the session ends — " +
			"NOT just once. There is exactly ONE summary per conversation; each call REPLACES it, so always " +
			"pass the full, current summary (covering what the session is about, what was " +
			"done, and key outcomes), not a delta. Strongly prefer writing the summary as Markdown — use " +
			"headings, bullet lists, and code fences to structure it, since the UI renders it as formatted " +
			"Markdown. Keeping it fresh is what makes later recall accurate; a " +
			"stale summary means the session is remembered wrong. You do not provide an ID — the server ties " +
			"the summary to this session automatically.",
	}, d.summarize)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_recall_session",
		Description: "Recall a PAST conversation by describing it in natural language (e.g. \"when we debugged " +
			"the WireGuard MTU on my router\"). Returns the best-matching conversation summary AND the " +
			"individual facts/memories saved during that session — it reconstructs a whole prior effort, not " +
			"isolated facts. Use it PROACTIVELY whenever the user resumes or references earlier work: " +
			"\"remember when\", \"last time\", \"we were working on\", \"continue\", \"pick up where we left " +
			"off\", \"the X we set up\", or any time you are resuming a project and broader prior context would " +
			"help. Prefer it over cortex_memory_search when the user points at a whole conversation rather than " +
			"a single fact. When unsure whether a past session is relevant, recall it.",
	}, d.recall)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_review_candidates",
		Description: "Review likely-duplicate memories the system flagged automatically. Returns groups, each " +
			"a flagged memory plus the existing memories it closely resembles (by vector similarity). These are " +
			"HEURISTIC hints, NOT confirmed duplicates — the system never deletes or merges on its own. Read each " +
			"group and decide PER PAIR: if two memories say the same thing, delete the redundant one with " +
			"cortex_memory_delete (keep the richer/newer wording); if one SUPERSEDES the other (e.g. a value " +
			"changed), delete the stale one; if they are merely related but distinct, leave them and optionally " +
			"connect them with cortex_memory_link; if they are genuinely separate, call cortex_dismiss_duplicate so " +
			"the pair is never flagged again. Use this when " +
			"the user asks to clean up / deduplicate their memory, or proactively at the start of a session to keep " +
			"the store tidy. Scope with namespace (omit for default, \"*\" for all).",
	}, d.reviewCandidates)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_dismiss_duplicate",
		Description: "Record that two memories are NOT duplicates of each other, so the system stops flagging the " +
			"pair in cortex_review_candidates on future re-indexing. Use this when reviewing duplicate candidates " +
			"and you decide two flagged memories are genuinely distinct (related but not redundant). The decision " +
			"is bidirectional and durable. Pass the two memory IDs (as returned by cortex_review_candidates). This " +
			"does NOT delete or link anything — it only suppresses the false-positive duplicate flag.",
	}, d.dismissDuplicate)

	mcp.AddTool(server, &mcp.Tool{
		Name: "cortex_consolidate",
		Description: "Consolidate the user's memories about a topic into fewer, richer ones. Use when the user " +
			"asks to consolidate / merge / clean up / deduplicate what's stored about something, or when a search " +
			"surfaces overlapping memories worth merging. This returns a CLUSTER of related memories — the topic's " +
			"best vector matches plus their linked and likely-duplicate neighbours — for YOU to analyse; it does NOT " +
			"merge or delete anything itself. After reading the cluster, write the FEWEST faithful memories that " +
			"capture everything important (lossless on facts, lossy on redundancy), and save each with " +
			"cortex_memory_save passing the `supersedes` field set to the ids (from this response's `manifest`) that " +
			"the new memory replaces. The server deletes those superseded sources automatically once the new memory " +
			"is durably indexed, so do NOT call cortex_memory_delete on them yourself — that would risk losing " +
			"content if indexing hasn't finished. Only supersede ids that appear in the manifest. " +
			"SCOPE: OMIT namespace to consolidate ONLY the current project (the default — almost always what you " +
			"want; it gathers fewer, more relevant memories and costs far fewer tokens). Pass \"*\" to span ALL " +
			"projects ONLY when the user explicitly asks to consolidate across everything — it pulls in much more " +
			"and is expensive.",
	}, d.consolidate)

	log.Info("cortex mcp server starting on stdio", "namespace", defaultNS, "server", serverURL, "autoSaveTags", autoSaveTags)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Error("server run", "err", err)
		os.Exit(1)
	}
}

// ---- memory_save ----

type SaveIn struct {
	Text       string   `json:"text" jsonschema:"the memory content to store; a self-contained note that captures the fact AND enough context to be understood on its own later. Can be as long as needed; strongly prefer Markdown formatting (headings, bullets, code fences) as the UI renders it as Markdown"`
	Namespace  string   `json:"namespace,omitempty" jsonschema:"optional namespace to scope the memory (e.g. a project name); defaults to the current project's namespace (the MCP-derived default)"`
	Tags       []string `json:"tags,omitempty" jsonschema:"optional free-form tags for later filtering"`
	LinkTo     []string `json:"linkTo,omitempty" jsonschema:"optional IDs of related memories to bidirectionally link this new memory to (e.g. from a prior memory_search). Linking is queued and applied once both memories are indexed, so the targets do not need to be indexed yet — they just need to be real memory IDs that exist or are being saved"`
	Supersedes []string `json:"supersedes,omitempty" jsonschema:"optional IDs of EXISTING memories this new memory replaces (e.g. the sources from a cortex_consolidate manifest that this merged memory absorbs); the server deletes them automatically once this memory is durably indexed, so do not also call cortex_memory_delete on them. Only pass IDs you actually merged into this text"`
}

type SaveOut struct {
	ID     string `json:"id" jsonschema:"the assigned memory ID"`
	Status string `json:"status" jsonschema:"queued once accepted for async indexing"`
}

func (d *deps) save(ctx context.Context, _ *mcp.CallToolRequest, in SaveIn) (*mcp.CallToolResult, SaveOut, error) {
	text := strings.TrimSpace(in.Text)
	if text == "" {
		return nil, SaveOut{}, fmt.Errorf("text must not be empty")
	}
	ns := d.nsOrDefault(in.Namespace)

	resp, err := d.client.Save(ctx, connect.NewRequest(&cortexv1.SaveRequest{
		Text:           text,
		Namespace:      ns,
		Tags:           config.MergeTags(in.Tags, d.autoSaveTags),
		LinkTo:         in.LinkTo,
		Supersedes:     in.Supersedes,
		Source:         d.source,
		ConversationId: d.conversationID,
	}))
	if err != nil {
		return nil, SaveOut{}, err
	}

	out := SaveOut{ID: resp.Msg.GetId(), Status: resp.Msg.GetStatus()}
	msg := fmt.Sprintf("Memory queued for indexing (id=%s, namespace=%s)", out.ID, ns)
	return text2result(msg), out, nil
}

// ---- memory_search ----

type SearchIn struct {
	Query       string   `json:"query" jsonschema:"natural-language query to semantically match against stored memories"`
	Namespace   string   `json:"namespace,omitempty" jsonschema:"optional namespace filter; omit to search the current project, pass \"*\" to search across all namespaces"`
	Limit       int      `json:"limit,omitempty" jsonschema:"max results to return (default 5)"`
	Tags        []string `json:"tags,omitempty" jsonschema:"only return memories carrying ALL of these tags"`
	AnyTags     []string `json:"anyTags,omitempty" jsonschema:"only return memories carrying AT LEAST ONE of these tags"`
	ExcludeTags []string `json:"excludeTags,omitempty" jsonschema:"drop memories carrying ANY of these tags"`
	MaxDistance float32  `json:"maxDistance,omitempty" jsonschema:"relevance cutoff (cosine distance, ~0=identical, larger=less related); results farther than this are dropped; omit to use the server default"`
}

type SearchHit struct {
	ID            string   `json:"id"`
	Text          string   `json:"text"`
	Namespace     string   `json:"namespace"`
	Tags          []string `json:"tags,omitempty"`
	Distance      float32  `json:"distance"`
	Model         string   `json:"model,omitempty"`
	LinkedIDs     []string `json:"linkedIds,omitempty"`
	DupCandidates []string `json:"dupCandidates,omitempty"`
}

type SearchOut struct {
	Hits []SearchHit `json:"hits"`
}

func (d *deps) search(ctx context.Context, _ *mcp.CallToolRequest, in SearchIn) (*mcp.CallToolResult, SearchOut, error) {
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, SearchOut{}, fmt.Errorf("query must not be empty")
	}

	maxDist := in.MaxDistance
	if maxDist <= 0 {
		maxDist = d.defaultMaxDistance
	}

	limit := in.Limit
	if limit <= 0 {
		limit = d.defaultSearchLimit
	}

	resp, err := d.client.Search(ctx, connect.NewRequest(&cortexv1.SearchRequest{
		Query:       query,
		Namespace:   d.nsOrDefault(in.Namespace), // "" -> this project, "*" -> all namespaces
		Limit:       int32(limit),
		Tags:        in.Tags,
		AnyTags:     in.AnyTags,
		ExcludeTags: in.ExcludeTags,
		MaxDistance: maxDist,
	}))
	if err != nil {
		return nil, SearchOut{}, err
	}

	hits := resp.Msg.GetHits()
	out := SearchOut{Hits: make([]SearchHit, 0, len(hits))}
	var b strings.Builder
	if len(hits) == 0 {
		b.WriteString("No memories found.")
	}
	flagged := 0
	for i, h := range hits {
		m := h.GetMemory()
		dups := m.GetDupCandidates()
		out.Hits = append(out.Hits, SearchHit{
			ID:            m.GetId(),
			Text:          m.GetText(),
			Namespace:     m.GetNamespace(),
			Tags:          m.GetTags(),
			Distance:      h.GetDistance(),
			Model:         m.GetModel(),
			LinkedIDs:     m.GetLinkedIds(),
			DupCandidates: dups,
		})
		fmt.Fprintf(&b, "%d. id=%s [%s] (dist %.3f) %s\n", i+1, m.GetId(), m.GetNamespace(), h.GetDistance(), m.GetText())
		if len(dups) > 0 {
			flagged++
			fmt.Fprintf(&b, "   ⚠ %d likely duplicate(s) flagged — run cortex_consolidate to merge\n", len(dups))
		}
	}
	if flagged > 0 {
		fmt.Fprintf(&b, "\n%d of %d result(s) have duplicate candidates; cortex_consolidate can gather and merge the cluster.\n", flagged, len(hits))
	}
	return text2result(strings.TrimSpace(b.String())), out, nil
}

// ---- memory_delete ----

type DeleteIn struct {
	ID string `json:"id" jsonschema:"the memory ID to delete"`
}

type DeleteOut struct {
	Status string `json:"status"`
}

func (d *deps) del(ctx context.Context, _ *mcp.CallToolRequest, in DeleteIn) (*mcp.CallToolResult, DeleteOut, error) {
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return nil, DeleteOut{}, fmt.Errorf("id must not be empty")
	}
	resp, err := d.client.Delete(ctx, connect.NewRequest(&cortexv1.DeleteRequest{Id: id}))
	if err != nil {
		return nil, DeleteOut{}, err
	}
	return text2result("deleted " + id), DeleteOut{Status: resp.Msg.GetStatus()}, nil
}

// ---- memory_edit ----

type EditIn struct {
	ID          string   `json:"id" jsonschema:"the ID of the existing memory to edit (from cortex_memory_search)"`
	Text        string   `json:"text" jsonschema:"the new FULL memory text; replaces the existing text and is re-embedded. Prefer Markdown as the UI renders it"`
	Tags        []string `json:"tags,omitempty" jsonschema:"the memory's new tag set; applied only when replaceTags is true (an empty list then clears all tags)"`
	ReplaceTags bool     `json:"replaceTags,omitempty" jsonschema:"when true, replace the memory's tags with the tags field; when false/omitted, keep the existing tags"`
	Namespace   string   `json:"namespace,omitempty" jsonschema:"optional new namespace to move the memory to; omit to keep the current one"`
}

type EditOut struct {
	ID     string `json:"id"`
	Status string `json:"status" jsonschema:"queued once accepted for async re-indexing"`
}

func (d *deps) edit(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, EditOut, error) {
	id := strings.TrimSpace(in.ID)
	if id == "" {
		return nil, EditOut{}, fmt.Errorf("id must not be empty")
	}
	text := strings.TrimSpace(in.Text)
	if text == "" {
		return nil, EditOut{}, fmt.Errorf("text must not be empty")
	}
	resp, err := d.client.UpdateMemory(ctx, connect.NewRequest(&cortexv1.UpdateMemoryRequest{
		Id:          id,
		Text:        text,
		Tags:        in.Tags,
		ReplaceTags: in.ReplaceTags,
		Namespace:   in.Namespace,
	}))
	if err != nil {
		return nil, EditOut{}, err
	}
	out := EditOut{ID: resp.Msg.GetId(), Status: resp.Msg.GetStatus()}
	return text2result("Memory queued for re-indexing (id=" + out.ID + ")"), out, nil
}

// ---- memory_link / memory_unlink ----

type LinkIn struct {
	ID       string `json:"id" jsonschema:"first memory ID to link (from cortex_memory_search, cortex_recall_session, or just returned by cortex_memory_save)"`
	TargetID string `json:"targetId" jsonschema:"second memory ID to link"`
}

type LinkOut struct {
	LinkedIDs []string `json:"linkedIds" jsonschema:"empty: linking is queued and applied asynchronously once both memories are indexed, so the resulting link set is not returned here"`
}

func (d *deps) link(ctx context.Context, _ *mcp.CallToolRequest, in LinkIn) (*mcp.CallToolResult, LinkOut, error) {
	id := strings.TrimSpace(in.ID)
	target := strings.TrimSpace(in.TargetID)
	if id == "" || target == "" {
		return nil, LinkOut{}, fmt.Errorf("id and targetId must not be empty")
	}
	resp, err := d.client.Link(ctx, connect.NewRequest(&cortexv1.LinkRequest{Id: id, TargetId: target}))
	if err != nil {
		return nil, LinkOut{}, err
	}
	return text2result(fmt.Sprintf("queued link %s <-> %s (applied once both memories are indexed)", id, target)), LinkOut{LinkedIDs: resp.Msg.GetLinkedIds()}, nil
}

type UnlinkIn struct {
	ID       string `json:"id" jsonschema:"first memory ID"`
	TargetID string `json:"targetId" jsonschema:"second memory ID"`
}

type UnlinkOut struct {
	LinkedIDs []string `json:"linkedIds" jsonschema:"empty: unlinking is queued and applied asynchronously, so the remaining link set is not returned here"`
}

func (d *deps) unlink(ctx context.Context, _ *mcp.CallToolRequest, in UnlinkIn) (*mcp.CallToolResult, UnlinkOut, error) {
	id := strings.TrimSpace(in.ID)
	target := strings.TrimSpace(in.TargetID)
	if id == "" || target == "" {
		return nil, UnlinkOut{}, fmt.Errorf("id and targetId must not be empty")
	}
	resp, err := d.client.Unlink(ctx, connect.NewRequest(&cortexv1.UnlinkRequest{Id: id, TargetId: target}))
	if err != nil {
		return nil, UnlinkOut{}, err
	}
	return text2result(fmt.Sprintf("queued unlink %s <-> %s", id, target)), UnlinkOut{LinkedIDs: resp.Msg.GetLinkedIds()}, nil
}

// ---- session_summarize ----

type SummarizeIn struct {
	Text      string `json:"text" jsonschema:"the full, current summary of THIS conversation (what it's about, what was done, key outcomes); replaces any prior summary for the session. Strongly prefer Markdown formatting (headings, bullets, code fences) as the UI renders it as Markdown"`
	Namespace string `json:"namespace,omitempty" jsonschema:"optional namespace to scope the summary; defaults to the current project's namespace (the MCP-derived default)"`
}

type SummarizeOut struct {
	ConversationID string `json:"conversationId"`
	Status         string `json:"status"`
}

func (d *deps) summarize(ctx context.Context, _ *mcp.CallToolRequest, in SummarizeIn) (*mcp.CallToolResult, SummarizeOut, error) {
	text := strings.TrimSpace(in.Text)
	if text == "" {
		return nil, SummarizeOut{}, fmt.Errorf("text must not be empty")
	}
	if d.conversationID == "" {
		return nil, SummarizeOut{}, fmt.Errorf("no conversation ID available (CLAUDE_CODE_SESSION_ID is unset), cannot summarize the session")
	}
	// Like Save, an omitted namespace means "this project" (nsOrDefault), so a
	// session summary lands in the same namespace as the facts saved during it
	// rather than the server's global fallback.
	ns := d.nsOrDefault(in.Namespace)
	resp, err := d.client.SummarizeSession(ctx, connect.NewRequest(&cortexv1.SummarizeSessionRequest{
		ConversationId: d.conversationID,
		Text:           text,
		Namespace:      ns,
	}))
	if err != nil {
		return nil, SummarizeOut{}, err
	}
	out := SummarizeOut{ConversationID: resp.Msg.GetConversationId(), Status: resp.Msg.GetStatus()}
	return text2result("session summary updated (conversation=" + out.ConversationID + ")"), out, nil
}

// ---- recall_session ----

type RecallIn struct {
	Query       string  `json:"query" jsonschema:"natural-language description of the past conversation to recall (e.g. 'when we patched the router')"`
	Namespace   string  `json:"namespace,omitempty" jsonschema:"optional namespace filter; omit for default, pass \"*\" for all namespaces"`
	FactLimit   int     `json:"factLimit,omitempty" jsonschema:"max facts to return for the matched conversation (default 50)"`
	MaxDistance float32 `json:"maxDistance,omitempty" jsonschema:"relevance cutoff on the summary match; omit to use the server default"`
}

type RecallOut struct {
	Matched bool        `json:"matched"`
	Summary string      `json:"summary,omitempty"`
	Facts   []SearchHit `json:"facts,omitempty"`
}

func (d *deps) recall(ctx context.Context, _ *mcp.CallToolRequest, in RecallIn) (*mcp.CallToolResult, RecallOut, error) {
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, RecallOut{}, fmt.Errorf("query must not be empty")
	}
	factLimit := in.FactLimit
	if factLimit <= 0 {
		factLimit = d.defaultFactLimit
	}

	resp, err := d.client.RecallSession(ctx, connect.NewRequest(&cortexv1.RecallSessionRequest{
		Query:       query,
		Namespace:   d.nsOrDefault(in.Namespace), // "" -> this project, "*" -> all namespaces
		FactLimit:   int32(factLimit),
		MaxDistance: in.MaxDistance,
	}))
	if err != nil {
		return nil, RecallOut{}, err
	}

	if !resp.Msg.GetMatched() {
		return text2result("No matching past session found."), RecallOut{Matched: false}, nil
	}

	sum := resp.Msg.GetSummary()
	facts := resp.Msg.GetFacts()
	out := RecallOut{Matched: true, Summary: sum.GetText(), Facts: make([]SearchHit, 0, len(facts))}

	var b strings.Builder
	fmt.Fprintf(&b, "Session summary (conversation=%s):\n%s\n", sum.GetConversationId(), sum.GetText())
	if len(facts) > 0 {
		b.WriteString("\nFacts from that session:\n")
	}
	for i, f := range facts {
		out.Facts = append(out.Facts, SearchHit{
			ID:        f.GetId(),
			Text:      f.GetText(),
			Namespace: f.GetNamespace(),
			Tags:      f.GetTags(),
			Model:     f.GetModel(),
		})
		fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, f.GetNamespace(), f.GetText())
	}
	return text2result(strings.TrimSpace(b.String())), out, nil
}

// ---- review_candidates ----

type ReviewIn struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"optional namespace filter; omit for default, pass \"*\" for all namespaces"`
	Limit     int    `json:"limit,omitempty" jsonschema:"max flagged memories to return (default 50)"`
}

type CandidateGroup struct {
	Memory     SearchHit   `json:"memory"`
	Candidates []SearchHit `json:"candidates"`
}

type ReviewOut struct {
	Groups []CandidateGroup `json:"groups"`
}

func memToHit(m *cortexv1.Memory) SearchHit {
	return SearchHit{
		ID:            m.GetId(),
		Text:          m.GetText(),
		Namespace:     m.GetNamespace(),
		Tags:          m.GetTags(),
		Model:         m.GetModel(),
		LinkedIDs:     m.GetLinkedIds(),
		DupCandidates: m.GetDupCandidates(),
	}
}

func (d *deps) reviewCandidates(ctx context.Context, _ *mcp.CallToolRequest, in ReviewIn) (*mcp.CallToolResult, ReviewOut, error) {
	resp, err := d.client.ListDuplicateCandidates(ctx, connect.NewRequest(&cortexv1.ListDuplicateCandidatesRequest{
		Namespace: d.nsOrDefault(in.Namespace), // "" -> this project, "*" -> all namespaces
		Limit:     int32(in.Limit),
	}))
	if err != nil {
		return nil, ReviewOut{}, err
	}

	groups := resp.Msg.GetGroups()
	if len(groups) == 0 {
		return text2result("No duplicate candidates flagged."), ReviewOut{Groups: []CandidateGroup{}}, nil
	}

	out := ReviewOut{Groups: make([]CandidateGroup, 0, len(groups))}
	var b strings.Builder
	fmt.Fprintf(&b, "%d flagged memor%s with duplicate candidates:\n", len(groups), plural(len(groups), "y", "ies"))
	for i, g := range groups {
		m := g.GetMemory()
		cg := CandidateGroup{Memory: memToHit(m), Candidates: make([]SearchHit, 0, len(g.GetCandidates()))}
		fmt.Fprintf(&b, "\n%d. [%s] (%s) %s\n", i+1, m.GetNamespace(), m.GetId(), m.GetText())
		for _, c := range g.GetCandidates() {
			cg.Candidates = append(cg.Candidates, memToHit(c))
			fmt.Fprintf(&b, "   ~ likely duplicate of (%s) %s\n", c.GetId(), c.GetText())
		}
		out.Groups = append(out.Groups, cg)
	}
	return text2result(strings.TrimSpace(b.String())), out, nil
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// ---- dismiss_duplicate ----

type DismissIn struct {
	ID       string `json:"id" jsonschema:"first memory ID (from cortex_review_candidates)"`
	TargetID string `json:"targetId" jsonschema:"second memory ID; confirmed NOT a duplicate of the first"`
}

type DismissOut struct {
	NotDuplicateOf []string `json:"notDuplicateOf" jsonschema:"updated confirmed-not-duplicate set of the first memory"`
}

func (d *deps) dismissDuplicate(ctx context.Context, _ *mcp.CallToolRequest, in DismissIn) (*mcp.CallToolResult, DismissOut, error) {
	id := strings.TrimSpace(in.ID)
	target := strings.TrimSpace(in.TargetID)
	if id == "" || target == "" {
		return nil, DismissOut{}, fmt.Errorf("id and targetId must not be empty")
	}
	resp, err := d.client.DismissDuplicate(ctx, connect.NewRequest(&cortexv1.DismissDuplicateRequest{
		Id:       id,
		TargetId: target,
	}))
	if err != nil {
		return nil, DismissOut{}, err
	}
	out := DismissOut{NotDuplicateOf: resp.Msg.GetNotDuplicateOf()}
	return text2result(fmt.Sprintf("Marked %s and %s as not duplicates; they won't be flagged again.", id, target)), out, nil
}

// ---- consolidate ----

type ConsolidateIn struct {
	Topic       string   `json:"topic" jsonschema:"natural-language description of the topic/subject whose memories to gather and consolidate"`
	Namespace   string   `json:"namespace,omitempty" jsonschema:"optional namespace filter; OMIT to consolidate only the current project (the default — fewer, more relevant memories, far cheaper in tokens). Pass \"*\" for ALL namespaces only when explicitly asked to span every project, as it gathers much more and is expensive"`
	Limit       int      `json:"limit,omitempty" jsonschema:"max memories to gather into the cluster (default 25)"`
	MaxDistance float32  `json:"maxDistance,omitempty" jsonschema:"relevance cutoff on the topic match; omit to use the server default"`
	Tags        []string `json:"tags,omitempty" jsonschema:"only consolidate memories carrying ALL of these tags; omit to NOT filter by tag (gathers the whole topic cluster across every tag, NOT only untagged memories)"`
	AnyTags     []string `json:"anyTags,omitempty" jsonschema:"only consolidate memories carrying AT LEAST ONE of these tags"`
	ExcludeTags []string `json:"excludeTags,omitempty" jsonschema:"drop memories carrying ANY of these tags from the cluster"`
}

type ConsolidateOut struct {
	Cluster  []SearchHit `json:"cluster" jsonschema:"the related memories to analyse and merge"`
	Manifest []string    `json:"manifest" jsonschema:"the ids present in the cluster; the ONLY ids eligible to pass to cortex_memory_save's supersedes field"`
}

func (d *deps) consolidate(ctx context.Context, _ *mcp.CallToolRequest, in ConsolidateIn) (*mcp.CallToolResult, ConsolidateOut, error) {
	topic := strings.TrimSpace(in.Topic)
	if topic == "" {
		return nil, ConsolidateOut{}, fmt.Errorf("topic must not be empty")
	}
	resp, err := d.client.Consolidate(ctx, connect.NewRequest(&cortexv1.ConsolidateRequest{
		Topic:       topic,
		Namespace:   d.nsOrDefault(in.Namespace), // "" -> this project, "*" -> all namespaces
		Limit:       int32(in.Limit),
		MaxDistance: in.MaxDistance,
		Tags:        in.Tags,
		AnyTags:     in.AnyTags,
		ExcludeTags: in.ExcludeTags,
	}))
	if err != nil {
		return nil, ConsolidateOut{}, err
	}

	cluster := resp.Msg.GetCluster()
	out := ConsolidateOut{
		Cluster:  make([]SearchHit, 0, len(cluster)),
		Manifest: resp.Msg.GetManifest(),
	}
	if len(cluster) == 0 {
		return text2result("No memories found for that topic; nothing to consolidate."), out, nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Cluster of %d memor%s about %q. Merge into the FEWEST faithful memories (keep every distinct fact, drop redundancy), "+
		"then cortex_memory_save each new memory with supersedes set to the ids it absorbs (only ids from the manifest below).\n",
		len(cluster), plural(len(cluster), "y", "ies"), topic)
	for i, m := range cluster {
		out.Cluster = append(out.Cluster, memToHit(m))
		fmt.Fprintf(&b, "\n%d. id=%s [%s] %s\n", i+1, m.GetId(), m.GetNamespace(), m.GetText())
	}
	fmt.Fprintf(&b, "\nmanifest (supersede-able ids): %s\n", strings.Join(out.Manifest, " "))
	return text2result(strings.TrimSpace(b.String())), out, nil
}

func text2result(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: msg}},
	}
}
