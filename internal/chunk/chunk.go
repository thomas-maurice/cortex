// Package chunk splits a memory's text into overlapping, token-bounded chunks
// for vector indexing. Long memories embed poorly as a single vector — a specific
// fact buried in 2000+ tokens gets averaged away — so search is run against these
// smaller, focused chunks and the hits are resolved back to their parent memory.
//
// Token counting uses the cl100k_base BPE (via tiktoken-go, offline). That is the
// embedding model qwen3's tokenizer is not available in Go, so cl100k is a
// documented PROXY: chunk *sizing* is approximate, which is fine — the target
// (512 tokens) is far below qwen3's 32K context, so the proxy only affects chunk
// granularity, never correctness.
package chunk

import (
	"fmt"
	"strings"
	"sync"

	tiktoken "github.com/pkoukk/tiktoken-go"
	loader "github.com/pkoukk/tiktoken-go-loader"
)

// Defaults: 512-token chunks with 64-token (12.5%) overlap. Overlap carries
// context across a boundary so a fact split across two chunks still resolves.
const (
	DefaultMaxTokens = 512
	DefaultOverlap   = 64
)

// loaderOnce installs the OFFLINE BPE loader exactly once. Without it tiktoken-go
// fetches the vocab over the network on first use, which would make the worker's
// indexing depend on internet egress from inside the container — unacceptable.
var loaderOnce sync.Once

// Chunker splits text into token-bounded, overlapping chunks. It is safe for
// concurrent use (tiktoken.Tiktoken's Encode is read-only after construction).
type Chunker struct {
	maxTokens int
	overlap   int
	enc       *tiktoken.Tiktoken
}

// New builds a Chunker. maxTokens<=0 and overlap<0 fall back to the defaults;
// overlap is clamped below maxTokens so a chunk always makes forward progress.
func New(maxTokens, overlap int) (*Chunker, error) {
	if maxTokens <= 0 {
		maxTokens = DefaultMaxTokens
	}
	if overlap < 0 {
		overlap = DefaultOverlap
	}
	if overlap >= maxTokens {
		overlap = maxTokens / 8
	}
	loaderOnce.Do(func() { tiktoken.SetBpeLoader(loader.NewOfflineLoader()) })
	enc, err := tiktoken.GetEncoding("cl100k_base")
	if err != nil {
		return nil, fmt.Errorf("init tokenizer: %w", err)
	}
	return &Chunker{maxTokens: maxTokens, overlap: overlap, enc: enc}, nil
}

// Count returns the token count of s under the configured encoding.
func (c *Chunker) Count(s string) int {
	return len(c.enc.Encode(s, nil, nil))
}

// Split returns the chunks for text. A text at or under maxTokens yields a single
// chunk (its whole self). Otherwise sentences are greedily packed up to maxTokens,
// and each new chunk is primed with the trailing sentences of the previous one up
// to `overlap` tokens. A single sentence longer than maxTokens is hard-split on
// token boundaries (the only case a chunk boundary can fall mid-sentence).
func (c *Chunker) Split(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if c.Count(text) <= c.maxTokens {
		return []string{text}
	}

	sentences := splitSentences(text)
	var chunks []string
	var cur []string
	curTokens := 0
	flush := func() {
		if len(cur) > 0 {
			chunks = append(chunks, strings.TrimSpace(strings.Join(cur, " ")))
		}
	}

	for _, s := range sentences {
		st := c.Count(s)
		if st > c.maxTokens {
			// A single oversized sentence: flush what we have, then hard-split it.
			flush()
			cur, curTokens = nil, 0
			chunks = append(chunks, c.hardSplit(s)...)
			continue
		}
		if curTokens+st > c.maxTokens && len(cur) > 0 {
			flush()
			cur, curTokens = c.overlapTail(cur)
		}
		cur = append(cur, s)
		curTokens += st
	}
	flush()
	return chunks
}

// overlapTail returns the trailing sentences of cur whose cumulative token count
// is within the overlap budget, to seed the next chunk with shared context.
func (c *Chunker) overlapTail(cur []string) ([]string, int) {
	if c.overlap <= 0 {
		return nil, 0
	}
	tail := []string{}
	tokens := 0
	for i := len(cur) - 1; i >= 0; i-- {
		t := c.Count(cur[i])
		if tokens+t > c.overlap {
			break
		}
		tail = append([]string{cur[i]}, tail...)
		tokens += t
	}
	return tail, tokens
}

// hardSplit breaks a single over-long sentence into token windows of maxTokens
// with `overlap` tokens shared between consecutive windows, decoding each window
// back to text. This is the fallback for prose with no sentence breaks.
func (c *Chunker) hardSplit(s string) []string {
	toks := c.enc.Encode(s, nil, nil)
	step := c.maxTokens - c.overlap
	if step <= 0 {
		step = c.maxTokens
	}
	var out []string
	for start := 0; start < len(toks); start += step {
		end := min(start+c.maxTokens, len(toks))
		out = append(out, strings.TrimSpace(c.enc.Decode(toks[start:end])))
		if end == len(toks) {
			break
		}
	}
	return out
}

// splitSentences breaks text into sentences. It splits on sentence-terminating
// punctuation (. ! ?) followed by whitespace, and on blank lines (paragraph
// breaks), keeping the terminator with its sentence. Go's regexp has no
// lookbehind, so this is a small hand-rolled scan rather than a regex.
func splitSentences(text string) []string {
	var out []string
	var b strings.Builder
	runes := []rune(text)
	for i, r := range runes {
		b.WriteRune(r)
		if r == '.' || r == '!' || r == '?' {
			// boundary if the next rune is whitespace or end of text
			if i+1 >= len(runes) || isSpace(runes[i+1]) {
				if s := strings.TrimSpace(b.String()); s != "" {
					out = append(out, s)
				}
				b.Reset()
			}
		} else if r == '\n' && i+1 < len(runes) && runes[i+1] == '\n' {
			if s := strings.TrimSpace(b.String()); s != "" {
				out = append(out, s)
			}
			b.Reset()
		}
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	return out
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}
