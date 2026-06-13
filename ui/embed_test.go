package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The SPA handler must serve index.html for unknown routes so vue-router
// history-mode deep links (e.g. /status) resolve instead of 404ing.
func TestHandlerServesIndexForUnknownRoute(t *testing.T) {
	rr := httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/status", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "<div id=\"app\">") {
		t.Fatalf("expected SPA index.html, got: %s", rr.Body.String())
	}
}
