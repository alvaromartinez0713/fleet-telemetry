package ingest_test

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"fleet-telemetry/internal/ingest"
)

func newHandlerWithLimit(t *testing.T, p *fakeProducer, maxBatch int) http.Handler {
	t.Helper()
	auth := ingest.NewAuthenticator([]byte(testSecret), func() time.Time { return serverNow })
	return ingest.NewServer(auth, p, ingest.Options{
		Now:      func() time.Time { return serverNow },
		MaxBatch: maxBatch,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// TestServerEnforcesConfiguredBatchLimit pins that an operator-configured
// MAX_BATCH is a real boundary: a batch above it is rejected without touching
// the bus, and a batch at the limit is still accepted.
func TestServerEnforcesConfiguredBatchLimit(t *testing.T) {
	producer := &fakeProducer{}
	handler := newHandlerWithLimit(t, producer, 2)
	token := deviceToken(t, "dev-1")

	over := post(handler, batchBody(t,
		event("dev-1", 1), event("dev-1", 2), event("dev-1", 3),
	), token)
	if over.Code != http.StatusBadRequest {
		t.Fatalf("status = %d for a batch above the configured limit, want %d (body %s)",
			over.Code, http.StatusBadRequest, over.Body.String())
	}
	if producer.callCount() != 0 {
		t.Fatalf("producer calls = %d, want 0: a rejected batch must never reach the bus", producer.callCount())
	}

	atLimit := post(handler, batchBody(t, event("dev-1", 1), event("dev-1", 2)), token)
	if atLimit.Code != http.StatusAccepted {
		t.Fatalf("status = %d for a batch at the configured limit, want %d (body %s)",
			atLimit.Code, http.StatusAccepted, atLimit.Body.String())
	}
}
