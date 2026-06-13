// Package ui embeds the built Vue single-page app and serves it. The Go server
// mounts Handler() as the catch-all route, so the same binary that serves the
// Connect RPC API also serves the web UI — nothing extra to deploy.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

// dist holds the built SPA. `make ui` (or the Docker node stage) populates
// ui/dist; a committed .gitkeep keeps this embeddable on a fresh checkout so
// `go build` never fails for lack of assets.
//
//go:embed all:dist
var dist embed.FS

// Handler serves the embedded SPA, falling back to index.html for any path that
// is not a real asset so client-side routing (vue-router history mode) works.
func Handler() http.Handler {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // dist is embedded at compile time; this cannot fail at runtime.
	}
	return &spaHandler{fs: http.FS(sub)}
}

type spaHandler struct {
	fs http.FileSystem
}

const indexPath = "index.html"

func (h *spaHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if p == "" || p == "/" {
		p = indexPath
	}

	f, err := h.fs.Open(p)
	if err != nil {
		h.serveIndex(w, r)
		return
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil || stat.IsDir() {
		h.serveIndex(w, r)
		return
	}
	http.ServeContent(w, r, p, stat.ModTime(), f)
}

func (h *spaHandler) serveIndex(w http.ResponseWriter, r *http.Request) {
	f, err := h.fs.Open(indexPath)
	if err != nil {
		http.Error(w, "web UI not built", http.StatusNotFound)
		return
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil {
		http.Error(w, "web UI not built", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, indexPath, stat.ModTime(), f)
}
