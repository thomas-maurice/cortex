package memory

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MaxExpiry is the furthest in the future a memory's expiry may be set. Anything
// longer is almost certainly a typo (or an attempt to dodge the sweep); a fact
// that is meant to live forever simply has no expiry.
const MaxExpiry = 10 * 365 * 24 * time.Hour

// ParseExpiresIn parses a relative expiry such as "7d" or "36h" into a duration.
// It accepts everything time.ParseDuration does plus a whole-number "d" (days)
// suffix, which Go's parser lacks. Zero, negative and over-MaxExpiry values are
// rejected.
func ParseExpiresIn(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("expires_in is empty")
	}
	var d time.Duration
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseInt(days, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid expires_in %q: want e.g. 7d or 36h", s)
		}
		if n > int64(MaxExpiry/(24*time.Hour)) {
			return 0, fmt.Errorf("expires_in %q is longer than the 10 year maximum", s)
		}
		d = time.Duration(n) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("invalid expires_in %q: want e.g. 7d or 36h", s)
		}
	}
	if d <= 0 {
		return 0, fmt.Errorf("expires_in %q must be positive", s)
	}
	if d > MaxExpiry {
		return 0, fmt.Errorf("expires_in %q is longer than the 10 year maximum", s)
	}
	return d, nil
}
