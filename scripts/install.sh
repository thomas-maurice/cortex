#!/usr/bin/env bash
#
# Install or update the Cortex client binaries (cortex-mcp + cortex CLI) from a
# published GitHub release. Safe to re-run: it just overwrites with the chosen
# version, so the same command installs and updates.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/thomas-maurice/cortex/master/scripts/install.sh | bash
#
# Env overrides:
#   CORTEX_VERSION       version/tag to install (default: latest release; e.g. v0.0.3)
#   CORTEX_INSTALL_DIR   where to put the binaries (default: ~/bin if it exists, else ~/.local/bin)
#   CORTEX_BINS          space-separated binaries to install (default: "cortex-mcp cortex")
#
set -euo pipefail

REPO="thomas-maurice/cortex"
BINS="${CORTEX_BINS:-cortex-mcp cortex}"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mwarning:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- platform detection -----------------------------------------------------
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$os" in
  darwin|linux) ;;
  *) die "unsupported OS '$os' (only darwin and linux have release binaries)";;
esac

arch="$(uname -m)"
case "$arch" in
  x86_64|amd64) arch="amd64";;
  arm64|aarch64) arch="arm64";;
  *) die "unsupported arch '$arch'";;
esac

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v tar  >/dev/null 2>&1 || die "tar is required"

# --- resolve version --------------------------------------------------------
version="${CORTEX_VERSION:-}"
if [ -z "$version" ]; then
  log "resolving latest release..."
  # Follow the /releases/latest redirect to read the tag without the API (no rate limit, no jq).
  effective="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest")" \
    || die "could not reach GitHub to resolve the latest release"
  version="${effective##*/tag/}"
  [ -n "$version" ] && [ "$version" != "$effective" ] || die "could not parse latest release tag from '$effective'"
fi
# Archive names use the version without the leading 'v'.
ver_no_v="${version#v}"
log "installing cortex $version ($os/$arch)"

# --- install dir ------------------------------------------------------------
install_dir="${CORTEX_INSTALL_DIR:-}"
if [ -z "$install_dir" ]; then
  if [ -d "$HOME/bin" ]; then install_dir="$HOME/bin"; else install_dir="$HOME/.local/bin"; fi
fi
mkdir -p "$install_dir"

# --- download + verify ------------------------------------------------------
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

archive="cortex_${ver_no_v}_${os}_${arch}.tar.gz"
base="https://github.com/${REPO}/releases/download/${version}"
log "downloading $archive"
curl -fsSL "${base}/${archive}"     -o "$tmp/$archive"   || die "download failed: ${base}/${archive}"
curl -fsSL "${base}/checksums.txt"  -o "$tmp/checksums.txt" || warn "no checksums.txt for $version — skipping verification"

if [ -f "$tmp/checksums.txt" ]; then
  log "verifying checksum"
  want="$(grep " ${archive}\$" "$tmp/checksums.txt" | awk '{print $1}')"
  [ -n "$want" ] || die "no checksum entry for $archive"
  if command -v sha256sum >/dev/null 2>&1; then
    got="$(sha256sum "$tmp/$archive" | awk '{print $1}')"
  else
    got="$(shasum -a 256 "$tmp/$archive" | awk '{print $1}')"
  fi
  [ "$want" = "$got" ] || die "checksum mismatch for $archive (want $want, got $got)"
fi

# --- extract + install ------------------------------------------------------
tar -xzf "$tmp/$archive" -C "$tmp"
for bin in $BINS; do
  [ -f "$tmp/$bin" ] || die "release archive has no '$bin'"
  install -m 0755 "$tmp/$bin" "$install_dir/$bin"
  # macOS quarantines downloaded binaries; clear it so they run (and can be
  # launched as an MCP subprocess) without a Gatekeeper prompt.
  if [ "$os" = "darwin" ]; then xattr -d com.apple.quarantine "$install_dir/$bin" 2>/dev/null || true; fi
  log "installed $install_dir/$bin"
done

# --- post-install hints -----------------------------------------------------
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) warn "$install_dir is not on your PATH — add it, e.g.: export PATH=\"$install_dir:\$PATH\"";;
esac

echo
log "done. Versions:"
for bin in $BINS; do
  "$install_dir/$bin" --version 2>/dev/null || "$install_dir/$bin" version 2>/dev/null || true
done
cat <<EOF

Next:
  - Point your MCP client at $install_dir/cortex-mcp (restart/reconnect it to pick up a new build).
  - The CLI/MCP talk to your server via CORTEX_SERVER_URL + CORTEX_AUTH_TOKEN.
EOF
