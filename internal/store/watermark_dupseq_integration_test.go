package store_test

import (
	"context"
	"testing"
	"time"

	"fleet-telemetry/internal/contracts"
)

// TestWatermarkNotAdvancedByDuplicateSeqMaskingAGap pins the defect the
// independent verifier found: the dedup key is (device_id, captured_at, seq), so
// the same seq can legally be stored twice under a different captured_at.
// Counting rows instead of distinct seq values would let that duplicate stand in
// for a missing event and push the watermark past a seq that was never stored —
// the device would then delete local events the server does not have (ADR-003).
func TestWatermarkNotAdvancedByDuplicateSeqMaskingAGap(t *testing.T) {
	requireIntegration(t)

	st := startStore(t)
	ctx := context.Background()
	device := "dev-dup-gap"
	base := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	received := base.Add(time.Hour)

	at := func(seq uint64, offset time.Duration) contracts.Envelope {
		return contracts.Envelope{
			Event: contracts.Event{
				DeviceID:      device,
				Seq:           seq,
				CapturedAt:    base.Add(offset),
				Lat:           4.65,
				Lon:           -74.05,
				Mode:          contracts.ModeMoving,
				SchemaVersion: contracts.SchemaVersion,
			},
			ReceivedAt: received,
		}
	}

	// seq 2 stored twice under different captured_at values; seq 3 never arrives.
	batch := []contracts.Envelope{
		at(1, time.Second),
		at(2, 2*time.Second),
		at(2, 30*time.Second),
		at(4, 4*time.Second),
	}
	res, err := st.PersistBatch(ctx, batch)
	if err != nil {
		t.Fatalf("persist batch: %v", err)
	}
	if res.Inserted != 4 {
		t.Fatalf("inserted = %d, want 4 (the duplicate keeps its own captured_at)", res.Inserted)
	}

	wm, err := st.Watermark(ctx, device)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if wm != 0 {
		t.Fatalf("watermark = %d with seq 3 missing, want 0: the device would delete the never-stored seq 3", wm)
	}

	// Once the gap is filled the watermark completes the run, including 4.
	third, err := st.PersistBatch(ctx, []contracts.Envelope{at(3, 3*time.Second)})
	if err != nil {
		t.Fatalf("persist the missing event: %v", err)
	}
	if third.Inserted != 1 {
		t.Fatalf("inserted = %d, want 1", third.Inserted)
	}
	wm, err = st.Watermark(ctx, device)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if wm != 4 {
		t.Fatalf("watermark = %d after filling the gap, want 4", wm)
	}
}
