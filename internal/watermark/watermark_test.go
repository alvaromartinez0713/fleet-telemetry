package watermark_test

import (
	"testing"

	"fleet-telemetry/internal/watermark"
)

func TestAdvance(t *testing.T) {
	tests := []struct {
		name    string
		current uint64
		maxSeq  uint64
		present int64
		want    uint64
		why     string
	}{
		{
			name:    "contiguous run advances to maxSeq",
			current: 3, maxSeq: 5, present: 2, want: 5,
			why: "seqs 4 and 5 are both present, so the run is complete",
		},
		{
			name:    "gap blocks the watermark",
			current: 3, maxSeq: 6, present: 2, want: 3,
			why: "only two of the three seqs in 4..6 exist, so 4 is not durable",
		},
		{
			name:    "never regresses",
			current: 5, maxSeq: 3, present: 1, want: 5,
			why: "a stale replay must not move the watermark backwards",
		},
		{
			name:    "no events above the watermark",
			current: 5, maxSeq: 5, present: 0, want: 5,
			why: "maxSeq equals current, nothing to advance",
		},
		{
			name:    "empty present count blocks",
			current: 0, maxSeq: 4, present: 0, want: 0,
			why: "counting zero rows cannot prove contiguity",
		},
		{
			name:    "maxSeq below current is ignored",
			current: 5, maxSeq: 4, present: 3, want: 5,
			why: "present rows below current never move the watermark",
		},
		{
			name:    "first run from zero advances",
			current: 0, maxSeq: 3, present: 3, want: 3,
			why: "a freshly registered device starts at 0 and drains 1..3",
		},
		{
			name:    "missing first seq blocks everything",
			current: 0, maxSeq: 3, present: 2, want: 0,
			why: "seq 1 is missing, so no local row may be deleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := watermark.Advance(tt.current, tt.maxSeq, tt.present)
			if got != tt.want {
				t.Fatalf("Advance(%d, %d, %d) = %d, want %d (%s)",
					tt.current, tt.maxSeq, tt.present, got, tt.want, tt.why)
			}
		})
	}
}
