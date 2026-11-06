package intakehttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"fleet-telemetry/internal/contracts"
	intakehttp "fleet-telemetry/internal/telemetry/intake/http"
)

var serverNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

const testSecret = "dev-secret"

type fakeProducer struct {
	mu      sync.Mutex
	batches [][]contracts.Envelope
	calls   int
	err     error
}

func (p *fakeProducer) Produce(_ context.Context, envs []contracts.Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.err != nil {
		return p.err
	}
	p.batches = append(p.batches, envs)
	return nil
}

func (p *fakeProducer) produced() [][]contracts.Envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batches
}

func (p *fakeProducer) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

type problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	EventIndex *int   `json:"event_index"`
}

func newHandler(t *testing.T, p *fakeProducer) http.Handler {
	t.Helper()
	auth := intakehttp.NewAuthenticator([]byte(testSecret), func() time.Time { return serverNow })
	return intakehttp.NewServer(auth, p, intakehttp.Options{
		Now:    func() time.Time { return serverNow },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func deviceToken(t *testing.T, device string) string {
	t.Helper()
	claims := jwt.RegisteredClaims{
		Subject:   device,
		ExpiresAt: jwt.NewNumericDate(serverNow.Add(time.Hour)),
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return "Bearer " + token
}

func event(device string, seq uint64) contracts.Event {
	return contracts.Event{
		DeviceID:      device,
		Seq:           seq,
		CapturedAt:    serverNow.Add(-time.Duration(seq) * time.Second),
		Lat:           4.65,
		Lon:           -74.05,
		Mode:          contracts.ModeMoving,
		SchemaVersion: contracts.SchemaVersion,
	}
}

func batchBody(t *testing.T, events ...contracts.Event) *bytes.Reader {
	t.Helper()
	raw, err := json.Marshal(contracts.Batch{Events: events})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return bytes.NewReader(raw)
}

func post(h http.Handler, body io.Reader, authHeader string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, contracts.RouteTelemetry, body)
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) problem {
	t.Helper()
	if got := rec.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", got)
	}
	var p problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem document %q: %v", rec.Body.String(), err)
	}
	if p.Type == "" || p.Title == "" || p.Detail == "" || p.Status == 0 {
		t.Fatalf("incomplete problem document: %+v", p)
	}
	if p.Status != rec.Code {
		t.Fatalf("problem status = %d, response status = %d", p.Status, rec.Code)
	}
	return p
}

func TestServerAcceptsBatch(t *testing.T) {
	p := &fakeProducer{}
	h := newHandler(t, p)

	rec := post(h, batchBody(t, event("dev-1", 1), event("dev-1", 2)), deviceToken(t, "dev-1"))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"accepted":2}` {
		t.Fatalf("body = %s, want {\"accepted\":2}", got)
	}

	batches := p.produced()
	if len(batches) != 1 {
		t.Fatalf("produced %d batches, want 1", len(batches))
	}
	if len(batches[0]) != 2 {
		t.Fatalf("produced %d envelopes, want 2", len(batches[0]))
	}
	for i, env := range batches[0] {
		if env.ReceivedAt.IsZero() {
			t.Errorf("envelope %d has no received_at", i)
		}
		if !env.ReceivedAt.Equal(serverNow) {
			t.Errorf("envelope %d received_at = %s, want %s", i, env.ReceivedAt, serverNow)
		}
		if !env.CapturedAt.Equal(event("dev-1", uint64(i+1)).CapturedAt) {
			t.Errorf("envelope %d captured_at was altered: %s", i, env.CapturedAt)
		}
		if env.DeviceID != "dev-1" {
			t.Errorf("envelope %d device = %q, want dev-1", i, env.DeviceID)
		}
	}
}

func TestServerRejectsWithoutValidToken(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "no authorization header", header: ""},
		{name: "bad token", header: "Bearer not-a-jwt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProducer{}
			h := newHandler(t, p)

			rec := post(h, batchBody(t, event("dev-1", 1)), tt.header)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 (body %s)", rec.Code, rec.Body.String())
			}
			decodeProblem(t, rec)
			if p.callCount() != 0 {
				t.Fatalf("producer was called %d times, want 0", p.callCount())
			}
		})
	}
}

func TestServerRejectsContractViolation(t *testing.T) {
	p := &fakeProducer{}
	h := newHandler(t, p)

	bad := event("dev-1", 2)
	bad.Seq = 0

	rec := post(h, batchBody(t, event("dev-1", 1), bad, event("dev-1", 3)), deviceToken(t, "dev-1"))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	doc := decodeProblem(t, rec)
	if doc.EventIndex == nil {
		t.Fatalf("problem document has no event_index: %s", rec.Body.String())
	}
	if *doc.EventIndex != 1 {
		t.Fatalf("event_index = %d, want 1", *doc.EventIndex)
	}
	if !strings.Contains(doc.Detail, "event 1") {
		t.Fatalf("detail %q does not name the offending event", doc.Detail)
	}
	if p.callCount() != 0 {
		t.Fatalf("producer was called %d times, want 0", p.callCount())
	}
}

func TestServerRejectsDeviceMismatch(t *testing.T) {
	p := &fakeProducer{}
	h := newHandler(t, p)

	rec := post(h, batchBody(t, event("dev-1", 1), event("dev-2", 1)), deviceToken(t, "dev-1"))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	doc := decodeProblem(t, rec)
	if doc.EventIndex != nil && *doc.EventIndex != 1 {
		t.Fatalf("event_index = %d, want 1 (the foreign event)", *doc.EventIndex)
	}
	if p.callCount() != 0 {
		t.Fatalf("producer was called %d times, want 0", p.callCount())
	}
}

func TestServerReportsBusFailure(t *testing.T) {
	p := &fakeProducer{err: errors.New("kafka: dial tcp: connection refused")}
	h := newHandler(t, p)

	rec := post(h, batchBody(t, event("dev-1", 1)), deviceToken(t, "dev-1"))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (body %s)", rec.Code, rec.Body.String())
	}
	doc := decodeProblem(t, rec)
	if strings.Contains(doc.Detail, "connection refused") {
		t.Fatalf("detail leaks internal error text: %q", doc.Detail)
	}
	if strings.Contains(rec.Body.String(), "connection refused") {
		t.Fatalf("response leaks internal error text: %s", rec.Body.String())
	}
}

func TestServerRejectsOversizedBody(t *testing.T) {
	p := &fakeProducer{}
	h := newHandler(t, p)

	big := strings.Repeat("a", 2<<20)
	body := bytes.NewReader([]byte(`{"events":[{"device_id":"` + big + `"}]}`))

	rec := post(h, body, deviceToken(t, "dev-1"))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %s)", rec.Code, rec.Body.String())
	}
	decodeProblem(t, rec)
	if p.callCount() != 0 {
		t.Fatalf("producer was called %d times, want 0", p.callCount())
	}
}

func TestServerRejectsMalformedJSON(t *testing.T) {
	p := &fakeProducer{}
	h := newHandler(t, p)

	rec := post(h, strings.NewReader(`{"events":`), deviceToken(t, "dev-1"))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
	}
	decodeProblem(t, rec)
	if p.callCount() != 0 {
		t.Fatalf("producer was called %d times, want 0", p.callCount())
	}
}

func TestServerMethodNotAllowed(t *testing.T) {
	h := newHandler(t, &fakeProducer{})

	req := httptest.NewRequest(http.MethodGet, contracts.RouteTelemetry, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
