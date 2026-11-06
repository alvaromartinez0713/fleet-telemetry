package contracts_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"fleet-telemetry/internal/contracts"
)

var testNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

func validEvent() contracts.Event {
	return contracts.Event{
		DeviceID:      "dev-1",
		Seq:           1,
		CapturedAt:    testNow.Add(-2 * time.Second),
		Lat:           4.65,
		Lon:           -74.05,
		Mode:          contracts.ModeMoving,
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		SchemaVersion: contracts.SchemaVersion,
	}
}

func TestEventValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*contracts.Event)
		want   error
	}{
		{
			name:   "valid event",
			mutate: func(*contracts.Event) {},
		},
		{
			name:   "heartbeat mode is valid",
			mutate: func(e *contracts.Event) { e.Mode = contracts.ModeHeartbeat },
		},
		{
			name:   "captured_at exactly at the skew limit is accepted",
			mutate: func(e *contracts.Event) { e.CapturedAt = testNow.Add(5 * time.Minute) },
		},
		{
			name:   "empty device id",
			mutate: func(e *contracts.Event) { e.DeviceID = "" },
			want:   contracts.ErrEmptyDeviceID,
		},
		{
			name:   "device id longer than 64 bytes",
			mutate: func(e *contracts.Event) { e.DeviceID = strings.Repeat("d", 65) },
			want:   contracts.ErrDeviceIDTooLong,
		},
		{
			name:   "device id of exactly 64 bytes is accepted",
			mutate: func(e *contracts.Event) { e.DeviceID = strings.Repeat("d", 64) },
		},
		{
			name:   "zero seq",
			mutate: func(e *contracts.Event) { e.Seq = 0 },
			want:   contracts.ErrSeqNotPositive,
		},
		{
			name:   "zero captured_at",
			mutate: func(e *contracts.Event) { e.CapturedAt = time.Time{} },
			want:   contracts.ErrCapturedAtZero,
		},
		{
			name:   "captured_at beyond the clock skew",
			mutate: func(e *contracts.Event) { e.CapturedAt = testNow.Add(5*time.Minute + time.Second) },
			want:   contracts.ErrCapturedInFuture,
		},
		{
			name:   "latitude above the range",
			mutate: func(e *contracts.Event) { e.Lat = 90.1 },
			want:   contracts.ErrLatitudeRange,
		},
		{
			name:   "latitude below the range",
			mutate: func(e *contracts.Event) { e.Lat = -90.1 },
			want:   contracts.ErrLatitudeRange,
		},
		{
			name:   "longitude above the range",
			mutate: func(e *contracts.Event) { e.Lon = 180.1 },
			want:   contracts.ErrLongitudeRange,
		},
		{
			name:   "longitude below the range",
			mutate: func(e *contracts.Event) { e.Lon = -180.1 },
			want:   contracts.ErrLongitudeRange,
		},
		{
			name:   "unknown mode",
			mutate: func(e *contracts.Event) { e.Mode = "idle" },
			want:   contracts.ErrBadMode,
		},
		{
			name:   "empty mode",
			mutate: func(e *contracts.Event) { e.Mode = "" },
			want:   contracts.ErrBadMode,
		},
		{
			name:   "unsupported schema version",
			mutate: func(e *contracts.Event) { e.SchemaVersion = 2 },
			want:   contracts.ErrSchemaVersion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := validEvent()
			tt.mutate(&e)
			err := e.Validate(testNow)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("Validate() = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestBatchValidate(t *testing.T) {
	events := func(n int) []contracts.Event {
		out := make([]contracts.Event, n)
		for i := range out {
			e := validEvent()
			e.Seq = uint64(i + 1)
			e.CapturedAt = testNow.Add(-time.Duration(n-i) * time.Second)
			out[i] = e
		}
		return out
	}

	t.Run("valid batch", func(t *testing.T) {
		idx, err := contracts.Batch{Events: events(2)}.Validate(testNow)
		if err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
		if idx != -1 {
			t.Fatalf("index = %d, want -1", idx)
		}
	})

	t.Run("batch of exactly MaxBatchSize is accepted", func(t *testing.T) {
		idx, err := contracts.Batch{Events: events(contracts.MaxBatchSize)}.Validate(testNow)
		if err != nil {
			t.Fatalf("Validate() = %v, want nil", err)
		}
		if idx != -1 {
			t.Fatalf("index = %d, want -1", idx)
		}
	})

	t.Run("empty batch", func(t *testing.T) {
		idx, err := contracts.Batch{}.Validate(testNow)
		if !errors.Is(err, contracts.ErrEmptyBatch) {
			t.Fatalf("Validate() = %v, want %v", err, contracts.ErrEmptyBatch)
		}
		if idx != -1 {
			t.Fatalf("index = %d, want -1", idx)
		}
	})

	t.Run("oversized batch", func(t *testing.T) {
		idx, err := contracts.Batch{Events: events(contracts.MaxBatchSize + 1)}.Validate(testNow)
		if !errors.Is(err, contracts.ErrBatchTooLarge) {
			t.Fatalf("Validate() = %v, want %v", err, contracts.ErrBatchTooLarge)
		}
		if idx != -1 {
			t.Fatalf("index = %d, want -1", idx)
		}
	})

	t.Run("index points at the offending event", func(t *testing.T) {
		evs := events(3)
		evs[1].Seq = 0
		idx, err := contracts.Batch{Events: evs}.Validate(testNow)
		if !errors.Is(err, contracts.ErrSeqNotPositive) {
			t.Fatalf("Validate() = %v, want %v", err, contracts.ErrSeqNotPositive)
		}
		if idx != 1 {
			t.Fatalf("index = %d, want 1", idx)
		}
	})
}
