package memory

import "strings"

// NormalizeTags trims every tag, drops empties and removes duplicates, keeping
// first-seen order. Applied wherever tags are written so the stored form is
// canonical (tag rename/delete match tags exactly, so stray whitespace would
// otherwise create look-alike tags that cannot be told apart in the UI).
func NormalizeTags(tags []string) []string {
	if len(tags) == 0 {
		return tags
	}
	out := make([]string, 0, len(tags))
	seen := make(map[string]bool, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}
