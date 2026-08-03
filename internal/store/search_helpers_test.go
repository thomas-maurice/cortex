package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// effectiveDistance is the single point where the two search paths (chunk-level
// and whole-memory) agree on how a result's distance and the maxDistance cutoff
// are computed. These tests pin that contract so the paths can't diverge.
func TestEffectiveDistance(t *testing.T) {
	tests := []struct {
		name        string
		dist, score float32
		hybrid      bool
		maxDistance float32
		wantDist    float32
		wantOK      bool
	}{
		{
			name: "vector result trusts server distance and always passes",
			dist: 0.42, hybrid: false, maxDistance: 0.1, // cutoff already enforced server-side
			wantDist: 0.42, wantOK: true,
		},
		{
			name: "hybrid maps score to 1-score",
			score: 0.9, hybrid: true, maxDistance: 0,
			wantDist: 0.1, wantOK: true, // hybridDistance(0.9) = 0.1
		},
		{
			name: "hybrid within cutoff passes",
			score: 0.8, hybrid: true, maxDistance: 0.5,
			wantDist: 0.2, wantOK: true,
		},
		{
			name: "hybrid beyond cutoff is dropped",
			score: 0.3, hybrid: true, maxDistance: 0.5,
			wantDist: 0.7, wantOK: false, // hybridDistance(0.3)=0.7 > 0.5
		},
		{
			name: "hybrid with no cutoff keeps everything",
			score: 0.0, hybrid: true, maxDistance: 0,
			wantDist: 1.0, wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDist, gotOK := effectiveDistance(tt.dist, tt.score, tt.hybrid, tt.maxDistance)
			assert.InDelta(t, tt.wantDist, gotDist, 1e-6)
			assert.Equal(t, tt.wantOK, gotOK)
		})
	}
}
