package latency

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}

	tests := []struct {
		name  string
		input []time.Duration
		p     float64
		want  float64
	}{
		{"p0", sorted, 0.0, float64(10 * time.Millisecond)},
		{"p50", sorted, 0.50, float64(30 * time.Millisecond)},
		{"p95", sorted, 0.95, float64(40 * time.Millisecond)},
		{"empty", nil, 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := percentile(tc.input, tc.p)
			if got != tc.want {
				t.Errorf("percentile(%v, %v) = %v, want %v", tc.input, tc.p, got, tc.want)
			}
		})
	}
}
