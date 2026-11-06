package adapter_test

import (
	"context"
	"testing"
	"time"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/telemetry/journal/adapter"
)

// TestWatermarkScanWindowBoundsThePerFlushWork pins the availability rule the
// native review found: the contiguity scan must not grow with the device's
// history. With a small window, a permanent gap leaves the watermark where it
// is instead of scanning every row above it, and the watermark still advances as
// far as the window proves is contiguous.
func TestWatermarkScanWindowBoundsThePerFlushWork(t *testing.T) {
	requireIntegration(t)

	ctx := context.Background()
	st, err := adapter.OpenWithOptions(ctx, timescaleDSN(t), adapter.Options{WatermarkScanWindow: 3})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	device := "dev-window"
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	at := func(seq uint64) contracts.Envelope {
		return contracts.Envelope{
			Event: contracts.Event{
				DeviceID:      device,
				Seq:           seq,
				CapturedAt:    base.Add(time.Duration(seq) * time.Second),
				Lat:           4.65,
				Lon:           -74.05,
				Mode:          contracts.ModeMoving,
				SchemaVersion: contracts.SchemaVersion,
			},
			ReceivedAt: base.Add(time.Hour),
		}
	}

	// 1 and 2 arrive, 3 never does, and the device keeps sending 5..9. Only the
	// window (0, 3], i.e. seq 1..3, may be examined: the watermark advances to the
	// last contiguous seq inside it and no further.
	if _, err := st.PersistBatch(ctx, []contracts.Envelope{
		at(1), at(2), at(5), at(6), at(7), at(8), at(9),
	}); err != nil {
		t.Fatalf("persist: %v", err)
	}
	wm, err := st.Watermark(ctx, device)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if wm != 2 {
		t.Fatalf("watermark = %d, want 2 (the window proves 1..2 and seq 3 is missing)", wm)
	}

	// With the gap filled, the window moves and the run completes as far as the
	// window reaches: (2, 5] proves 3..5, so the watermark lands on 5.
	if _, err := st.PersistBatch(ctx, []contracts.Envelope{at(3), at(4)}); err != nil {
		t.Fatalf("persist the gap: %v", err)
	}
	wm, err = st.Watermark(ctx, device)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if wm != 5 {
		t.Fatalf("watermark = %d, want 5 (the window (2,5] proves 3..5)", wm)
	}

	// The next batch for the device slides the window again: (5, 8] proves 6..8.
	// A device that is behind therefore converges in bounded steps instead of
	// forcing one unbounded scan.
	if _, err := st.PersistBatch(ctx, []contracts.Envelope{at(5)}); err != nil {
		t.Fatalf("persist a replay to slide the window: %v", err)
	}
	wm, err = st.Watermark(ctx, device)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if wm != 8 {
		t.Fatalf("watermark = %d, want 8 (the window (5,8] proves 6..8)", wm)
	}
}

// TestWatermarkDefaultWindowIsBounded keeps the default explicit: the adapter
// must always carry a positive window.
func TestWatermarkDefaultWindowIsBounded(t *testing.T) {
	if adapter.DefaultWatermarkScanWindow <= 0 {
		t.Fatalf("DefaultWatermarkScanWindow = %d, want a positive bound", adapter.DefaultWatermarkScanWindow)
	}
}
