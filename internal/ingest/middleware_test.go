package ingest_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/ingest"
)

// oversizedBody is a body that cannot fit within the intake's 1 MiB limit.
func oversizedBody() io.Reader {
	big := strings.Repeat("a", 2<<20)
	return bytes.NewReader([]byte(`{"events":[{"device_id":"` + big + `"}]}`))
}

// TestAuthenticationRunsBeforeBodyLimit pins the middleware order: an invalid
// token must be answered with 401 before the body is read, so the size of the
// body cannot change the answer and a caller that cannot authenticate never
// makes the intake read its bytes.
func TestAuthenticationRunsBeforeBodyLimit(t *testing.T) {
	producer := &fakeProducer{}
	handler := newHandler(t, producer)

	rec := post(handler, oversizedBody(), "Bearer not-a-jwt")

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d for an invalid token with an oversized body, want %d (body %s)",
			rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	doc := decodeProblem(t, rec)
	if !strings.Contains(doc.Detail, "token") {
		t.Fatalf("detail %q does not name the rejected token", doc.Detail)
	}
	if producer.callCount() != 0 {
		t.Fatalf("producer calls = %d, want 0", producer.callCount())
	}
}

// TestRequestEmitsOneLogLine pins that a request produces exactly one INFO line
// through the injected logger, and that the line carries the facts an operator
// needs: method, path, status, latency and the authenticated device.
func TestRequestEmitsOneLogLine(t *testing.T) {
	var logs bytes.Buffer
	handler := ingest.NewServer(
		ingest.NewAuthenticator([]byte(testSecret), func() time.Time { return serverNow }),
		&fakeProducer{},
		ingest.Options{
			Now:    func() time.Time { return serverNow },
			Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		},
	)

	rec := post(handler, batchBody(t, event("dev-1", 1)), deviceToken(t, "dev-1"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (body %s)", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var lines []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("logged %d lines for one accepted request, want exactly 1:\n%s", len(lines), logs.String())
	}

	for _, want := range []string{
		"level=INFO",
		http.MethodPost,
		contracts.RouteTelemetry,
		"status=202",
		"latency=",
		"device=dev-1",
	} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("log line %q does not carry %q", lines[0], want)
		}
	}
}
