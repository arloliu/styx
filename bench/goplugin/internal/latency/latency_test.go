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
		name string
		p    float64
		want float64
	}{
		{"p50", 0.50, float64(30 * time.Millisecond)},
		{"p95", 0.95, float64(40 * time.Millisecond)},
		{"empty", 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := sorted
			if tc.name == "empty" {
				input = nil
			}
			got := percentile(input, tc.p)
			if got != tc.want {
				t.Errorf("percentile(%v, %v) = %v, want %v", input, tc.p, got, tc.want)
			}
		})
	}
}
