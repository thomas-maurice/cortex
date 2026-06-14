package config

import "os"

// HostnameTagPrefix prefixes the auto-added host tag, e.g. "host:my-laptop", so
// it is visually distinct from ordinary tags and easy to filter or exclude.
const HostnameTagPrefix = "host:"

// AutoTags returns the tags that should be stamped, client-side, on every memory
// saved from THIS machine: the static list (staticTags, e.g. a "work"/"personal"
// marker) plus, when hostnameTag is true, "host:<hostname>". These are applied
// only at save time, never on query, so each host or profile can mark its
// memories without affecting search. A hostname lookup failure drops the host
// tag rather than erroring.
func AutoTags(staticTags []string, hostnameTag bool) []string {
	out := append([]string(nil), staticTags...)
	if hostnameTag {
		if h, err := os.Hostname(); err == nil && h != "" {
			out = append(out, HostnameTagPrefix+h)
		}
	}
	return out
}

// MergeTags returns base with every tag from extra that is not already present
// appended, preserving order and dropping empties. Used to fold the auto
// save-tags into a request's explicit tags without introducing duplicates.
func MergeTags(base, extra []string) []string {
	seen := make(map[string]bool, len(base)+len(extra))
	out := make([]string, 0, len(base)+len(extra))
	for _, t := range base {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	for _, t := range extra {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}
