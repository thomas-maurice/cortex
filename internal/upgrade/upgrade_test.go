package upgrade

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTarGz creates a gzip-compressed tar archive containing the given files.
// If dirPrefix is non-empty, each file is placed inside that directory in the
// archive — matching goreleaser's default single-top-directory layout.
func buildTarGz(t *testing.T, files map[string][]byte, dirPrefix string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	for name, data := range files {
		entryName := name
		if dirPrefix != "" {
			entryName = dirPrefix + "/" + name
		}
		hdr := &tar.Header{
			Name:     entryName,
			Mode:     0755,
			Size:     int64(len(data)),
			Typeflag: tar.TypeReg,
		}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gw.Close())
	return buf.Bytes()
}

// sha256Hex returns the hex-encoded SHA-256 digest of data.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ── Decide tests ─────────────────────────────────────────────────────────────

func TestDecide(t *testing.T) {
	// Each case documents WHY the expected Action matters to the upgrade contract.
	tests := []struct {
		name          string
		client        string
		server        string
		wantAction    Action
		wantTargetSet bool
		wantTarget    string
	}{
		{
			// Dev client has no GitHub release — blocking is the only safe option;
			// attempting a download would 404 and confuse the user.
			name:       "dev_client_blocked",
			client:     "dev",
			server:     "0.0.13",
			wantAction: Blocked,
		},
		{
			// Dev server means the deployment is a snapshot; no release to download.
			name:       "dev_server_blocked",
			client:     "0.0.13",
			server:     "dev",
			wantAction: Blocked,
		},
		{
			// Goreleaser snapshot version template produces "<ver>-dev-<short-commit>".
			// These snapshots are not uploaded to GitHub Releases.
			name:       "snapshot_client_blocked",
			client:     "0.0.14-dev-abc1234",
			server:     "0.0.13",
			wantAction: Blocked,
		},
		{
			// Snapshot server: same reason as snapshot client.
			name:       "snapshot_server_blocked",
			client:     "0.0.13",
			server:     "0.0.14-dev-abc1234",
			wantAction: Blocked,
		},
		{
			// An empty client version means the binary was not stamped at build time.
			name:       "empty_client_blocked",
			client:     "",
			server:     "0.0.13",
			wantAction: Blocked,
		},
		{
			// An empty server version means the server was not stamped; we cannot
			// determine what release to install.
			name:       "empty_server_blocked",
			client:     "0.0.13",
			server:     "",
			wantAction: Blocked,
		},
		{
			// Identical versions without any 'v' prefix — nothing to do.
			name:       "equal_no_v_up_to_date",
			client:     "0.0.13",
			server:     "0.0.13",
			wantAction: UpToDate,
		},
		{
			// The client binary may be stamped with a leading 'v' from ldflags while
			// the server may report without one (or vice versa). Both represent the
			// same release and must not trigger a no-op upgrade loop.
			name:       "equal_v_prefix_client_up_to_date",
			client:     "v0.0.13",
			server:     "0.0.13",
			wantAction: UpToDate,
		},
		{
			// Same as above, server carries the 'v'.
			name:       "equal_v_prefix_server_up_to_date",
			client:     "0.0.13",
			server:     "v0.0.13",
			wantAction: UpToDate,
		},
		{
			// Both carry 'v' — still equal after stripping.
			name:       "equal_both_v_up_to_date",
			client:     "v1.2.3",
			server:     "v1.2.3",
			wantAction: UpToDate,
		},
		{
			// Different major version (client ahead): protocol incompatibility is
			// possible; require manual intervention so the operator can validate.
			name:       "major_mismatch_client_ahead_blocked",
			client:     "2.0.0",
			server:     "1.0.0",
			wantAction: Blocked,
		},
		{
			// Different major version (server ahead): same incompatibility concern.
			name:       "major_mismatch_server_ahead_blocked",
			client:     "1.0.0",
			server:     "2.0.0",
			wantAction: Blocked,
		},
		{
			// Normal patch upgrade: client is behind, converge upward to server.
			name:          "patch_upgrade",
			client:        "0.0.12",
			server:        "0.0.13",
			wantAction:    Upgrade,
			wantTargetSet: true,
			wantTarget:    "0.0.13",
		},
		{
			// Minor version upgrade.
			name:          "minor_upgrade",
			client:        "0.0.13",
			server:        "0.1.0",
			wantAction:    Upgrade,
			wantTargetSet: true,
			wantTarget:    "0.1.0",
		},
		{
			// The server is OLDER than the client: the server is the deployment
			// source of truth, so the client must converge downward to it. A newer
			// client against an older server is still skew.
			name:          "downgrade_to_server",
			client:        "0.1.0",
			server:        "0.0.13",
			wantAction:    Upgrade,
			wantTargetSet: true,
			wantTarget:    "0.0.13",
		},
		{
			// The 'v' prefix on the server version must be stripped from Target,
			// because Options.Version must not carry a leading 'v' (the URL builder
			// in Apply prepends it for the tag path segment).
			name:          "server_v_prefix_stripped_in_target",
			client:        "1.2.3",
			server:        "v1.2.4",
			wantAction:    Upgrade,
			wantTargetSet: true,
			wantTarget:    "1.2.4",
		},
		{
			// Garbage client version must not panic — Decide must always return a
			// well-formed Decision with a non-empty Reason.
			name:       "garbage_client_blocked",
			client:     "not-a-version",
			server:     "0.0.13",
			wantAction: Blocked,
		},
		{
			// Garbage server version: same requirement.
			name:       "garbage_server_blocked",
			client:     "0.0.13",
			server:     "not-a-version",
			wantAction: Blocked,
		},
		{
			// Completely unparseable strings on both sides.
			name:       "both_garbage_blocked",
			client:     "???",
			server:     "???",
			wantAction: Blocked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Decide(tt.client, tt.server)
			assert.Equal(t, tt.wantAction, d.Action, "unexpected Action")
			assert.NotEmpty(t, d.Reason, "Reason must always be set regardless of outcome")
			if tt.wantTargetSet {
				assert.Equal(t, tt.wantTarget, d.Target, "unexpected Target")
			}
		})
	}
}

// ── Apply tests ───────────────────────────────────────────────────────────────

// TestApplyHappyPath verifies the core upgrade flow end-to-end: archive is
// served by a local httptest server, checksum is verified, both binaries are
// extracted, written with executable permissions, and atomically renamed over
// the old files. Result.Replaced must list both binaries and Skipped must be
// empty.
func TestApplyHappyPath(t *testing.T) {
	const version = "0.0.13"
	archiveName := fmt.Sprintf("cortex_%s_%s_%s.tar.gz", version, runtime.GOOS, runtime.GOARCH)
	// Goreleaser default: binaries are inside a directory named after the archive.
	dirPrefix := fmt.Sprintf("cortex_%s_%s_%s", version, runtime.GOOS, runtime.GOARCH)

	newCortex := []byte("new-cortex-binary-content")
	newMCP := []byte("new-cortex-mcp-binary-content")

	archiveData := buildTarGz(t, map[string][]byte{
		"cortex":     newCortex,
		"cortex-mcp": newMCP,
	}, dirPrefix)
	checksum := sha256Hex(archiveData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case fmt.Sprintf("/v%s/%s", version, archiveName):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(archiveData)
		case fmt.Sprintf("/v%s/checksums.txt", version):
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "%s  %s\n", checksum, archiveName)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cortex"), []byte("old-cortex"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cortex-mcp"), []byte("old-cortex-mcp"), 0755))

	result, err := Apply(t.Context(), Options{
		Version: version,
		Dir:     dir,
		BaseURL: srv.URL,
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"cortex", "cortex-mcp"}, result.Replaced)
	assert.Empty(t, result.Skipped)

	// Verify new contents were written.
	gotCortex, err := os.ReadFile(filepath.Join(dir, "cortex"))
	require.NoError(t, err)
	assert.Equal(t, newCortex, gotCortex, "cortex must contain the new content from the archive")

	gotMCP, err := os.ReadFile(filepath.Join(dir, "cortex-mcp"))
	require.NoError(t, err)
	assert.Equal(t, newMCP, gotMCP, "cortex-mcp must contain the new content from the archive")

	// Verify executable bit is set on each replaced binary.
	for _, name := range []string{"cortex", "cortex-mcp"} {
		info, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		assert.True(t, info.Mode()&0111 != 0, "binary %s must have the executable bit set after replacement", name)
	}
}

// TestApplyChecksumMismatch verifies the anti-supply-chain / anti-corruption
// invariant: a tampered or corrupted archive is rejected before any binary on
// disk is modified. The error message must mention "checksum" so callers can
// surface a meaningful diagnostic.
func TestApplyChecksumMismatch(t *testing.T) {
	const version = "0.0.13"
	archiveName := fmt.Sprintf("cortex_%s_%s_%s.tar.gz", version, runtime.GOOS, runtime.GOARCH)

	archiveData := buildTarGz(t, map[string][]byte{
		"cortex": []byte("tampered-content"),
	}, "")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case fmt.Sprintf("/v%s/%s", version, archiveName):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(archiveData)
		case fmt.Sprintf("/v%s/checksums.txt", version):
			w.WriteHeader(http.StatusOK)
			// Deliberately wrong checksum to simulate a tampered release.
			_, _ = fmt.Fprintf(w, "%s  %s\n",
				"0000000000000000000000000000000000000000000000000000000000000000",
				archiveName)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	oldContent := []byte("original-cortex-must-not-change")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cortex"), oldContent, 0755))

	_, err := Apply(t.Context(), Options{
		Version:  version,
		Dir:      dir,
		BaseURL:  srv.URL,
		Binaries: []string{"cortex"},
	})

	require.Error(t, err, "Apply must fail when the checksum does not match")
	assert.Contains(t, err.Error(), "checksum",
		"error message must mention checksum so callers can surface a meaningful diagnostic")

	// The pre-existing binary must be completely untouched — this is the
	// checksum-before-touch invariant.
	got, readErr := os.ReadFile(filepath.Join(dir, "cortex"))
	require.NoError(t, readErr)
	assert.Equal(t, oldContent, got,
		"checksum mismatch must leave the existing binary on disk untouched")
}

// TestApplyMissingBinaryInDir verifies that a binary listed in Binaries but
// absent from Dir is reported in Result.Skipped and never created. A user who
// has only the CLI installed (not the MCP binary) must not end up with a
// spurious cortex-mcp file after an upgrade.
func TestApplyMissingBinaryInDir(t *testing.T) {
	const version = "0.0.13"
	archiveName := fmt.Sprintf("cortex_%s_%s_%s.tar.gz", version, runtime.GOOS, runtime.GOARCH)

	newCortex := []byte("new-cortex-content")
	archiveData := buildTarGz(t, map[string][]byte{
		"cortex":     newCortex,
		"cortex-mcp": []byte("new-mcp-content"),
	}, "")
	checksum := sha256Hex(archiveData)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case fmt.Sprintf("/v%s/%s", version, archiveName):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(archiveData)
		case fmt.Sprintf("/v%s/checksums.txt", version):
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, "%s  %s\n", checksum, archiveName)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	// Only cortex is present; cortex-mcp is absent.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cortex"), []byte("old-cortex"), 0755))

	result, err := Apply(t.Context(), Options{
		Version:  version,
		Dir:      dir,
		BaseURL:  srv.URL,
		Binaries: []string{"cortex", "cortex-mcp"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"cortex"}, result.Replaced,
		"only the binary that pre-exists in Dir should be replaced")
	assert.Equal(t, []string{"cortex-mcp"}, result.Skipped,
		"absent binary must be reported in Skipped")

	// Verify cortex was actually replaced with new content.
	got, err := os.ReadFile(filepath.Join(dir, "cortex"))
	require.NoError(t, err)
	assert.Equal(t, newCortex, got)

	// Verify cortex-mcp was NOT created in Dir.
	_, statErr := os.Stat(filepath.Join(dir, "cortex-mcp"))
	assert.True(t, os.IsNotExist(statErr),
		"Apply must not create cortex-mcp when it was not present in Dir before upgrade")
}
