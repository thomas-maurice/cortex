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

// Client calls Ollama's /api/embeddings endpoint.
type Client struct {
	baseURL string
	model   string
	http    *http.Client
}

// New returns an embedder. baseURL e.g. "http://localhost:11434",
// model e.g. "qwen3-embedding:0.6b".
func New(baseURL, model string) *Client {
	return &Client{
		baseURL: baseURL,
		model:   model,
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

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
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type embedResp struct {
	Embedding []float32 `json:"embedding"`
}

// Embed returns the vector for a single piece of text.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(embedReq{Model: c.model, Prompt: text})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama embed request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama embed: status %d", resp.StatusCode)
	}

	var out embedResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("ollama embed decode: %w", err)
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("ollama embed: empty vector (is model %q pulled?)", c.model)
	}
	return out.Embedding, nil
}
