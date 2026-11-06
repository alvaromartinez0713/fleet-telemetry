package ingest_test

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestIntakeStampsReceivedAtAndIgnoresClientTimestamps pins the timestamp
// contract (ADR-002): captured_at is the device's and must survive verbatim,
// while received_at and stored_at belong to the pipeline. A device that sends
// its own received_at or stored_at must not be able to backdate the intake or
// the storage time.
func TestIntakeStampsReceivedAtAndIgnoresClientTimestamps(t *testing.T) {
	producer := &fakeProducer{}
	handler := newHandler(t, producer)

	const capturedAt = "2026-11-05T11:59:30Z"
	body := `{"events":[{
		"device_id":"dev-1",
		"seq":1,
		"captured_at":"` + capturedAt + `",
		"lat":4.65,
		"lon":-74.05,
		"mode":"moving",
		"schema_version":1,
		"received_at":"2001-01-01T00:00:00Z",
		"stored_at":"1999-01-01T00:00:00Z"
	}]}`

	rec := post(handler, strings.NewReader(body), deviceToken(t, "dev-1"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	produced := producer.produced()
	if len(produced) != 1 || len(produced[0]) != 1 {
		t.Fatalf("produced = %d calls, want 1 call with 1 event", len(produced))
	}
	got := produced[0][0]

	wantCaptured, err := time.Parse(time.RFC3339, capturedAt)
	if err != nil {
		t.Fatalf("parse captured_at: %v", err)
	}
	if !got.CapturedAt.Equal(wantCaptured) {
		t.Fatalf("captured_at = %s, want the device value %s", got.CapturedAt, wantCaptured)
	}
	if !got.ReceivedAt.Equal(serverNow) {
		t.Fatalf("received_at = %s, want the intake clock %s (the client value must be ignored)", got.ReceivedAt, serverNow)
	}
}
