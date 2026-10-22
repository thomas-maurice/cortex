// Package embed turns text into vectors using a local Ollama instance.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ollamaClientTimeout is the HTTP client deadline for embed requests. Long
// enough for a cold model load; callers provide a shorter context when needed.
const ollamaClientTimeout = 60 * time.Second

// Client calls Ollama's /api/embed endpoint (Ollama >= 0.3.4).
type Client struct {
	baseURL string
	model   string
	http    *http.Client
	observe func(d time.Duration, err error)
}

// New returns an embedder. baseURL e.g. "http://localhost:11434",
// model e.g. "qwen3-embedding:0.6b".
func New(baseURL, model string) *Client {
	return &Client{
		baseURL: baseURL,
		model:   model,
		http:    &http.Client{Timeout: ollamaClientTimeout},
	}
}

// SetObserver registers a callback invoked after every embed HTTP request (one per
// <=embedMaxBatch inputs) with its total duration (retries included) and final error. Used by the worker for metrics;
// call before the client is shared between goroutines.
func (c *Client) SetObserver(fn func(d time.Duration, err error)) { c.observe = fn }

// Model returns the embedding model this client uses.
func (c *Client) Model() string { return c.model }

type tagsResp struct {
	Models []struct {
		Name string `json:"name"`
	} `json:"models"`
}

// Reachable reports whether the Ollama server answers at all (independent of
// whether the configured model is pulled).
func (c *Client) Reachable(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ollama tags request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama tags: status %d", resp.StatusCode)
	}
	return nil
}

// HasModel reports whether the configured model is present in Ollama's local
// model list. A model configured without an explicit tag is matched against
// its ":latest" form, as Ollama stores it.
func (c *Client) HasModel(ctx context.Context) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/tags", nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("ollama tags request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("ollama tags: status %d", resp.StatusCode)
	}
	var out tagsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, fmt.Errorf("ollama tags decode: %w", err)
	}
	want := c.model
	wantLatest := want
	if !strings.Contains(want, ":") {
		wantLatest = want + ":latest"
	}
	for _, m := range out.Models {
		if m.Name == want || m.Name == wantLatest {
			return true, nil
		}
	}
	return false, nil
}

type pullReq struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

// Pull asks Ollama to download the configured model and blocks until the pull
// completes. The pull can take minutes, so the caller's context governs the
// deadline rather than the short embed-request timeout.
func (c *Client) Pull(ctx context.Context) error {
	body, err := json.Marshal(pullReq{Model: c.model, Stream: false})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/pull", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return fmt.Errorf("ollama pull request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama pull: status %d", resp.StatusCode)
	}

	var out struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("ollama pull decode: %w", err)
	}
	if out.Error != "" {
		return fmt.Errorf("ollama pull: %s", out.Error)
	}
	return nil
}

type embedReq struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResp struct {
	Embeddings [][]float32 `json:"embeddings"`
}

// embedMaxAttempts bounds how many times a request is re-issued on a
// transient failure. Embedding is on the SYNCHRONOUS search path — every
// cortex_memory_search and every auto-recall embeds its query first — so a single
// Ollama hiccup should not fail a whole recall when a quick retry would succeed.
const embedMaxAttempts = 3

// embedRetryBackoff is the base delay between embed retries; it grows linearly
// with the retry number. Kept short — the caller's context still bounds total time.
const embedRetryBackoff = 200 * time.Millisecond

// embedMaxBatch caps the number of inputs sent in one /api/embed request, so a
// huge reindex payload cannot blow the HTTP timeout or Ollama's memory. Larger
// batches are split into sequential requests.
const embedMaxBatch = 32

// Embed returns the vector for a single piece of text. It is EmbedBatch with one
// input, so it shares the same endpoint, retry behaviour and observer.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := c.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// EmbedBatch returns one vector per input, in input order. Batches larger than
// embedMaxBatch are split into sequential requests and concatenated in order.
// Transient failures (network/transport errors and HTTP 5xx) are retried up to
// embedMaxAttempts per request, with a short backoff, within the caller's context
// deadline. A 4xx, a decode failure, a wrong embedding count, or an empty vector
// are NOT retried: re-issuing an identical request would not change the outcome.
// The observer fires once per HTTP request (all retries of that request included),
// not once per EmbedBatch call.
func (c *Client) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += embedMaxBatch {
		end := min(start+embedMaxBatch, len(texts))
		t0 := time.Now()
		vecs, err := c.embed(ctx, texts[start:end])
		if c.observe != nil {
			c.observe(time.Since(t0), err)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, vecs...)
	}
	return out, nil
}

func (c *Client) embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(embedReq{Model: c.model, Input: texts})
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= embedMaxAttempts; attempt++ {
		if attempt > 1 {
			if err := sleepCtx(ctx, embedRetryBackoff*time.Duration(attempt-1)); err != nil {
				return nil, err
			}
		}
		vecs, retryable, err := c.embedOnce(ctx, body, len(texts))
		if err == nil {
			return vecs, nil
		}
		lastErr = err
		// Stop early on a non-transient failure or a cancelled/expired context
		// (a network error caused by the context is not worth retrying).
		if !retryable || ctx.Err() != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("ollama embed: giving up after %d attempts: %w", embedMaxAttempts, lastErr)
}

// embedOnce issues a single embed request for want inputs. The bool reports
// whether a failure is worth retrying: network/transport errors and HTTP 5xx are
// transient; a 4xx, a decode error, a count mismatch and an empty vector are not.
// The count check guarantees callers never misalign inputs and vectors.
func (c *Client) embedOnce(ctx context.Context, body []byte, want int) ([][]float32, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, true, fmt.Errorf("ollama embed request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode >= 500, fmt.Errorf("ollama embed: status %d", resp.StatusCode)
	}

	var out embedResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false, fmt.Errorf("ollama embed decode: %w", err)
	}
	if len(out.Embeddings) != want {
		return nil, false, fmt.Errorf("ollama embed: got %d embeddings for %d inputs (is model %q pulled?)", len(out.Embeddings), want, c.model)
	}
	for i, v := range out.Embeddings {
		if len(v) == 0 {
			return nil, false, fmt.Errorf("ollama embed: empty vector at index %d (is model %q pulled?)", i, c.model)
		}
	}
	return out.Embeddings, false, nil
}

// sleepCtx waits for d, or returns early with ctx.Err() if the context is done
// first — so a retry backoff never outlives the caller's deadline.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
