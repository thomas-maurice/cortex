// Package upgrade provides self-upgrade logic for the cortex CLI and MCP
// binaries. It downloads a GitHub release matching the server's version,
// verifies the archive checksum, extracts the binaries, and atomically
// replaces local ones. Only the Go standard library is used — no new module
// dependencies are introduced.
package upgrade

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Action describes the outcome of a version comparison.
type Action int

const (
	// UpToDate means the client version already matches the server's version.
	UpToDate Action = iota
	// Upgrade means the client should converge to Target (may be a downgrade
	// if the server is older — the server is the deployment source of truth).
	Upgrade
	// Blocked means upgrade cannot proceed: dev/snapshot builds, major version
	// mismatch, or unparseable version strings.
	Blocked
)

// Decision is the result of comparing client and server versions.
type Decision struct {
	Action Action // UpToDate | Upgrade | Blocked
	Target string // semver to install (server version, no leading "v"), set when Action==Upgrade
	Reason string // human-readable explanation, always set
}

// Options controls what Apply does.
type Options struct {
	// Version to install, no leading "v" (from Decision.Target).
	Version string
	// Dir whose binaries get replaced. If empty, the directory of the current
	// executable (os.Executable, symlinks resolved).
	Dir string
	// BaseURL for release downloads; defaults to
	// "https://github.com/thomas-maurice/cortex/releases/download".
	// Tests override this with an httptest server URL.
	BaseURL string
	// Binaries to replace; defaults to []string{"cortex", "cortex-mcp"}.
	// Only binaries that ALREADY EXIST in Dir are replaced; missing ones are
	// reported in Result.Skipped and never created.
	Binaries []string
}

// Result reports what Apply did.
type Result struct {
	Replaced []string // binaries actually replaced
	Skipped  []string // requested binaries not present in Dir (or not in archive)
}

const (
	defaultBaseURL = "https://github.com/thomas-maurice/cortex/releases/download"
	// maxBinarySize caps per-file extraction to resist decompression bombs.
	maxBinarySize = int64(200 * 1024 * 1024) // 200 MB
	// downloadTimeout is generous for ~30 MB archives.
	downloadTimeout = 5 * time.Minute
)

var defaultBinaries = []string{"cortex", "cortex-mcp"}

// semver holds parsed version components for numeric comparison.
type semver struct {
	major, minor, patch int
	suffix              string
}

// isDevVersion reports whether v looks like an unstamped or snapshot build that
// has no corresponding GitHub release. Covers three documented patterns: empty
// string, literal "dev", and goreleaser snapshot versions that embed "-dev-".
func isDevVersion(v string) bool {
	v = strings.TrimPrefix(v, "v")
	return v == "" || v == "dev" || strings.Contains(v, "-dev-")
}

// parseSemver parses "MAJOR.MINOR.PATCH[-suffix]" after stripping a leading 'v'.
// It returns false if the string does not begin with three dot-separated
// non-negative decimal integers.
func parseSemver(v string) (semver, bool) {
	v = strings.TrimPrefix(v, "v")

	suffix := ""
	if idx := strings.IndexByte(v, '-'); idx != -1 {
		suffix = v[idx+1:]
		v = v[:idx]
	}

	parts := strings.SplitN(v, ".", 3)
	if len(parts) != 3 {
		return semver{}, false
	}

	var nums [3]int
	for i, p := range parts {
		if p == "" {
			return semver{}, false
		}
		n := 0
		for _, c := range p {
			if c < '0' || c > '9' {
				return semver{}, false
			}
			n = n*10 + int(c-'0')
		}
		nums[i] = n
	}

	return semver{major: nums[0], minor: nums[1], patch: nums[2], suffix: suffix}, true
}

// Decide compares clientVersion against serverVersion and returns what should
// happen. Rules:
//   - Either side "dev", empty, or containing "-dev-" (snapshot builds) => Blocked.
//     Unstamped/dev builds have no corresponding GitHub release.
//   - Equal (ignoring a leading "v") => UpToDate.
//   - Different MAJOR version => Blocked (incompatible; manual upgrade required).
//   - Otherwise => Upgrade with Target set to the server's version (no leading
//     "v"). This includes the server being OLDER than the client: clients converge
//     to the server's version in both directions, because the server is the
//     deployment source of truth and a newer client against an older server is
//     still skew.
func Decide(clientVersion, serverVersion string) Decision {
	if isDevVersion(clientVersion) {
		return Decision{
			Action: Blocked,
			Reason: fmt.Sprintf("client version %q is a dev/snapshot build; self-upgrade is not supported for unstamped builds", clientVersion),
		}
	}
	if isDevVersion(serverVersion) {
		return Decision{
			Action: Blocked,
			Reason: fmt.Sprintf("server version %q is a dev/snapshot build; no GitHub release exists to download", serverVersion),
		}
	}

	cv, cok := parseSemver(clientVersion)
	sv, sok := parseSemver(serverVersion)

	if !cok {
		return Decision{
			Action: Blocked,
			Reason: fmt.Sprintf("client version %q is not a valid semver string", clientVersion),
		}
	}
	if !sok {
		return Decision{
			Action: Blocked,
			Reason: fmt.Sprintf("server version %q is not a valid semver string", serverVersion),
		}
	}

	// Numeric equality handles "v1.2.3" == "1.2.3" because we strip 'v' before
	// parsing. Suffix must also match (e.g. "1.2.3-rc1" != "1.2.3").
	if cv.major == sv.major && cv.minor == sv.minor && cv.patch == sv.patch && cv.suffix == sv.suffix {
		return Decision{
			Action: UpToDate,
			Reason: fmt.Sprintf("client version %s matches server version %s", clientVersion, serverVersion),
		}
	}

	// Different major versions are incompatible at the protocol level; require
	// manual intervention so the operator can validate compatibility.
	if cv.major != sv.major {
		return Decision{
			Action: Blocked,
			Reason: fmt.Sprintf("major version mismatch: client is v%d.x.x, server is v%d.x.x; manual upgrade required", cv.major, sv.major),
		}
	}

	// Minor or patch differs (including server older than client): converge to
	// the server's version. Strip the leading 'v' because Options.Version must
	// not have one.
	return Decision{
		Action: Upgrade,
		Target: strings.TrimPrefix(serverVersion, "v"),
		Reason: fmt.Sprintf("client version %s differs from server version %s", clientVersion, serverVersion),
	}
}

// Apply downloads cortex_<Version>_<GOOS>_<GOARCH>.tar.gz and checksums.txt
// from BaseURL/v<Version>/, verifies the archive's sha256 against checksums.txt
// (checksum is verified BEFORE any binary on disk is modified — this is the
// anti-supply-chain / anti-corruption invariant), extracts the binaries, and
// atomically replaces each target that already exists in Dir.
func Apply(ctx context.Context, opts Options) (Result, error) {
	if opts.BaseURL == "" {
		opts.BaseURL = defaultBaseURL
	}
	if len(opts.Binaries) == 0 {
		opts.Binaries = defaultBinaries
	}
	if opts.Dir == "" {
		exe, err := os.Executable()
		if err != nil {
			return Result{}, fmt.Errorf("resolving executable path: %w", err)
		}
		resolved, err := filepath.EvalSymlinks(exe)
		if err != nil {
			return Result{}, fmt.Errorf("resolving symlinks for executable: %w", err)
		}
		opts.Dir = filepath.Dir(resolved)
	}

	// Classify requested binaries: only those that exist in Dir will be replaced.
	// We never create a binary that is not already installed.
	var toReplace []string
	var skipped []string
	for _, b := range opts.Binaries {
		if _, err := os.Stat(filepath.Join(opts.Dir, b)); err == nil {
			toReplace = append(toReplace, b)
		} else {
			skipped = append(skipped, b)
		}
	}

	if len(toReplace) == 0 {
		return Result{Skipped: skipped}, nil
	}

	archiveName := fmt.Sprintf("cortex_%s_%s_%s.tar.gz", opts.Version, runtime.GOOS, runtime.GOARCH)
	tagBase := fmt.Sprintf("%s/v%s", strings.TrimRight(opts.BaseURL, "/"), opts.Version)
	archiveURL := tagBase + "/" + archiveName
	checksumURL := tagBase + "/checksums.txt"

	client := &http.Client{Timeout: downloadTimeout}

	// Fetch checksums.txt (small) to learn the expected sha256 of the archive.
	expectedChecksum, err := fetchChecksum(ctx, client, checksumURL, archiveName)
	if err != nil {
		return Result{}, fmt.Errorf("fetching checksums: %w", err)
	}

	// Download the archive fully into memory before touching any file on disk.
	// Checksum verification happens next — this ordering is the
	// anti-supply-chain / anti-corruption invariant.
	archiveData, err := fetchBody(ctx, client, archiveURL)
	if err != nil {
		return Result{}, fmt.Errorf("downloading archive %s: %w", archiveURL, err)
	}

	// Verify checksum BEFORE any binary is written.
	gotSum := sha256.Sum256(archiveData)
	gotHex := hex.EncodeToString(gotSum[:])
	if gotHex != expectedChecksum {
		return Result{}, fmt.Errorf("checksum mismatch for %s: got %s, want %s", archiveName, gotHex, expectedChecksum)
	}

	// Extract only the binaries we intend to replace.
	wantSet := make(map[string]bool, len(toReplace))
	for _, b := range toReplace {
		wantSet[b] = true
	}
	extracted, err := extractBinaries(archiveData, wantSet)
	if err != nil {
		return Result{}, fmt.Errorf("extracting archive: %w", err)
	}

	// Atomically replace each binary on disk.
	var replaced []string
	for _, b := range toReplace {
		data, ok := extracted[b]
		if !ok {
			// Binary exists in Dir but is absent from the archive; skip it.
			skipped = append(skipped, b)
			continue
		}
		if err := atomicReplace(filepath.Join(opts.Dir, b), data); err != nil {
			return Result{}, fmt.Errorf("replacing binary %s: %w", b, err)
		}
		replaced = append(replaced, b)
	}

	return Result{Replaced: replaced, Skipped: skipped}, nil
}

// fetchChecksum downloads url, parses goreleaser's checksums.txt format
// ("<sha256hex>  <filename>" per line), and returns the expected hex digest for
// filename.
func fetchChecksum(ctx context.Context, client *http.Client, url, filename string) (string, error) {
	body, err := fetchBody(ctx, client, url)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] == filename {
			return fields[0], nil
		}
	}
	return "", fmt.Errorf("checksum for %q not found in %s", filename, url)
}

// fetchBody issues a GET request with the given context and returns the response
// body bytes. It returns an error for non-200 HTTP status codes.
func fetchBody(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("building request for %s: %w", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: unexpected status %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// extractBinaries decompresses and unpacks the gzip-compressed tar archive in
// data, returning the contents of files whose base names are in want. It handles
// both flat archives and single-top-directory archives (goreleaser wraps binaries
// in a directory named after the archive by default). Path traversal is rejected
// and per-file reads are capped to resist decompression bombs.
func extractBinaries(data []byte, want map[string]bool) (result map[string][]byte, err error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("opening gzip stream: %w", err)
	}
	defer func() {
		if cerr := gr.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing gzip stream: %w", cerr)
		}
	}()

	tr := tar.NewReader(gr)
	result = make(map[string][]byte)

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading tar entry: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		// Guard against path traversal: reject absolute paths and ".." components.
		// Use the tar-native path package (forward-slash) rather than filepath.
		if path.IsAbs(hdr.Name) || strings.Contains(hdr.Name, "..") {
			return nil, fmt.Errorf("rejecting unsafe tar entry %q", hdr.Name)
		}

		// path.Base strips an optional single top-level directory prefix so that
		// both "cortex" (flat) and "cortex_0.0.13_linux_amd64/cortex" (wrapped)
		// resolve to the same base name.
		base := path.Base(hdr.Name)
		if !want[base] {
			continue
		}

		// Pre-flight size check (fast path; the LimitReader below is the real guard
		// against decompression bombs when the declared size is 0 or wrong).
		if hdr.Size > maxBinarySize {
			return nil, fmt.Errorf("tar entry %q declared size %d exceeds %d byte limit", hdr.Name, hdr.Size, maxBinarySize)
		}

		// Read up to maxBinarySize+1 bytes; receiving more than the limit means
		// the archive is either corrupt or a decompression bomb.
		content, err := io.ReadAll(io.LimitReader(tr, maxBinarySize+1))
		if err != nil {
			return nil, fmt.Errorf("reading tar entry %q: %w", hdr.Name, err)
		}
		if int64(len(content)) > maxBinarySize {
			return nil, fmt.Errorf("tar entry %q exceeds %d byte size limit", hdr.Name, maxBinarySize)
		}

		result[base] = content
	}

	return result, nil
}

// atomicReplace writes data to a temporary file in the same directory as target,
// sets its mode to 0755, then renames it over target. rename(2) on POSIX systems
// is atomic with respect to the final path name, so readers of target always see
// either the old or the new content, never a partially-written file.
func atomicReplace(target string, data []byte) error {
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".upgrade-*")
	if err != nil {
		return fmt.Errorf("creating temp file in %s: %w", dir, err)
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing to temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Chmod(tmpName, 0755); err != nil {
		return fmt.Errorf("setting permissions on temp file: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("renaming temp file to %s: %w", target, err)
	}
	ok = true
	return nil
}
