package contracts_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"fleet-telemetry/internal/contracts"
)

// TestValidateRejectsSeqBeyondSignedRange pins that a seq which cannot be
// stored as a signed bigint is rejected at the door: stored negative it would
// block that device's watermark forever.
func TestValidateRejectsSeqBeyondSignedRange(t *testing.T) {
	e := contracts.Event{
		DeviceID:      "dev-1",
		Seq:           uint64(math.MaxInt64) + 1,
		CapturedAt:    time.Now().UTC(),
		Lat:           4.65,
		Lon:           -74.05,
		Mode:          contracts.ModeMoving,
		SchemaVersion: contracts.SchemaVersion,
	}
	if err := e.Validate(time.Now()); !errors.Is(err, contracts.ErrSeqTooLarge) {
		t.Fatalf("err = %v, want ErrSeqTooLarge", err)
	}
}

// TestValidateAcceptsLargestStorableSeq is the boundary the previous case
// guards: the value itself must still be accepted.
func TestValidateAcceptsLargestStorableSeq(t *testing.T) {
	e := contracts.Event{
		DeviceID:      "dev-1",
		Seq:           uint64(math.MaxInt64),
		CapturedAt:    time.Now().UTC(),
		Lat:           4.65,
		Lon:           -74.05,
		Mode:          contracts.ModeHeartbeat,
		SchemaVersion: contracts.SchemaVersion,
	}
	if err := e.Validate(time.Now()); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}

// TestEnvelopeValidateShape covers the bus-side guard: a payload that decodes
// but carries no device, no seq, no captured_at or no received_at must be
// treated as undecodable, never persisted as an empty row.
func TestEnvelopeValidateShape(t *testing.T) {
	now := time.Now().UTC()
	valid := contracts.Envelope{
		Event: contracts.Event{
			DeviceID:      "dev-1",
			Seq:           1,
			CapturedAt:    now.Add(-time.Hour),
			Mode:          contracts.ModeMoving,
			SchemaVersion: contracts.SchemaVersion,
		},
		ReceivedAt: now,
	}
	if err := valid.ValidateShape(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}

	tests := map[string]struct {
		mutate  func(*contracts.Envelope)
		wantErr error
	}{
		"empty device":     {func(e *contracts.Envelope) { e.DeviceID = "" }, contracts.ErrEmptyDeviceID},
		"zero seq":         {func(e *contracts.Envelope) { e.Seq = 0 }, contracts.ErrSeqNotPositive},
		"seq beyond range": {func(e *contracts.Envelope) { e.Seq = uint64(math.MaxInt64) + 1 }, contracts.ErrSeqTooLarge},
		"zero captured_at": {func(e *contracts.Envelope) { e.CapturedAt = time.Time{} }, contracts.ErrCapturedAtZero},
		"zero received_at": {func(e *contracts.Envelope) { e.ReceivedAt = time.Time{} }, contracts.ErrMissingReceivedAt},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			e := valid
			tc.mutate(&e)
			err := e.ValidateShape()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestEnvelopeValidateShapeIgnoresClockSkew documents why the bus guard is not
// the wire validation: a queued event may be old, so age alone is not a defect.
func TestEnvelopeValidateShapeIgnoresClockSkew(t *testing.T) {
	e := contracts.Envelope{
		Event: contracts.Event{
			DeviceID:      "dev-1",
			Seq:           1,
			CapturedAt:    time.Now().UTC().Add(-30 * 24 * time.Hour),
			Mode:          contracts.ModeMoving,
			SchemaVersion: contracts.SchemaVersion,
		},
		ReceivedAt: time.Now().UTC(),
	}
	if err := e.ValidateShape(); err != nil {
		t.Fatalf("old but structurally valid envelope rejected: %v", err)
	}
	if strings.Contains(contracts.ErrMissingReceivedAt.Error(), "skew") {
		t.Fatal("unexpected message coupling")
	}
}
