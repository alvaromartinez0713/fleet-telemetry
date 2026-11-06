package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"fleet-telemetry/internal/contracts"
)

// maxBodyBytes bounds one request body. A batch of MaxBatchSize events fits
// comfortably; anything larger is a client bug, not telemetry we owe storage.
const maxBodyBytes = 1 << 20 // 1 MiB

// Problem type URIs. They identify the failure class, so a client can branch on
// the type instead of parsing prose (RFC 9457).
const (
	problemUnauthorized   = "urn:fleet:ingest:unauthorized"
	problemInvalidBatch   = "urn:fleet:ingest:invalid-batch"
	problemDeviceMismatch = "urn:fleet:ingest:device-mismatch"
	problemBodyTooLarge   = "urn:fleet:ingest:body-too-large"
	problemBusUnavailable = "urn:fleet:ingest:bus-unavailable"
)

// Producer queues the accepted batch on the bus. *bus.Producer implements it.
type Producer interface {
	Produce(context.Context, []contracts.Envelope) error
}

// Options are the intake's boundaries. Zero values fall back to the defaults.
type Options struct {
	// Now stamps received_at and bounds the accepted device clock skew.
	Now func() time.Time
	// MaxBodyBytes bounds the request body.
	MaxBodyBytes int64
	Logger       *slog.Logger
}

// Server is the HTTP intake for POST /v1/telemetry.
type Server struct {
	auth     *Authenticator
	producer Producer
	now      func() time.Time
	maxBytes int64
	log      *slog.Logger
}

// NewServer builds the handler. The mux owns routing, so method and path
// mismatches are answered by net/http (405/404) and never by hand-rolled code.
func NewServer(auth *Authenticator, producer Producer, opts Options) http.Handler {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = maxBodyBytes
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	s := &Server{
		auth:     auth,
		producer: producer,
		now:      opts.Now,
		maxBytes: opts.MaxBodyBytes,
		log:      opts.Logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST "+contracts.RouteTelemetry, s.handleTelemetry)
	return mux
}

// accepted is the 202 body: it counts what was durably queued, never what was
// persisted (ADR-002).
type accepted struct {
	Accepted int `json:"accepted"`
}

// problem is an RFC 9457 problem document. EventIndex is an extension member:
// it points at the offending event of a batch so a client can drop just that one.
type problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	EventIndex *int   `json:"event_index,omitempty"`
}

func writeProblem(w http.ResponseWriter, p problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// handleTelemetry accepts a batch: authenticate, decode, validate the contract,
// enforce that a device submits only its own events, stamp received_at and
// queue it. The 202 answer means "durably queued", never "persisted".
func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	deviceID, err := s.auth.DeviceID(r.Header.Get("Authorization"))
	if err != nil {
		if errors.Is(err, ErrMissingToken) {
			writeProblem(w, problem{
				Type:   problemUnauthorized,
				Title:  "Missing bearer token",
				Status: http.StatusUnauthorized,
				Detail: "an Authorization: Bearer <token> header is required",
			})
			return
		}
		s.log.Warn("rejected device token", "error", err)
		writeProblem(w, problem{
			Type:   problemUnauthorized,
			Title:  "Invalid bearer token",
			Status: http.StatusUnauthorized,
			Detail: "the device token is invalid or expired",
		})
		return
	}

	now := s.now()

	body := http.MaxBytesReader(w, r.Body, s.maxBytes)
	defer func() { _ = body.Close() }()

	var batch contracts.Batch
	if err := json.NewDecoder(body).Decode(&batch); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.log.Warn("rejected oversized batch", "device", deviceID, "limit_bytes", s.maxBytes)
			writeProblem(w, problem{
				Type:   problemBodyTooLarge,
				Title:  "Request body too large",
				Status: http.StatusRequestEntityTooLarge,
				Detail: fmt.Sprintf("the body exceeds the %d byte limit", s.maxBytes),
			})
			return
		}
		s.log.Warn("rejected undecodable batch", "device", deviceID, "error", err)
		writeProblem(w, problem{
			Type:   problemInvalidBatch,
			Title:  "Invalid request body",
			Status: http.StatusBadRequest,
			Detail: "the body is not a valid telemetry batch",
		})
		return
	}

	if idx, err := batch.Validate(now); err != nil {
		if idx < 0 {
			s.log.Warn("rejected invalid batch", "device", deviceID, "error", err)
			writeProblem(w, problem{
				Type:   problemInvalidBatch,
				Title:  "Invalid telemetry batch",
				Status: http.StatusBadRequest,
				Detail: err.Error(),
			})
			return
		}
		s.log.Warn("rejected invalid event", "device", deviceID, "index", idx, "error", err)
		index := idx
		writeProblem(w, problem{
			Type:       problemInvalidBatch,
			Title:      "Invalid telemetry event",
			Status:     http.StatusBadRequest,
			Detail:     fmt.Sprintf("event %d: %s", idx, err),
			EventIndex: &index,
		})
		return
	}

	// A token identifies one device installation, so a batch may only carry
	// that device's events. One foreign event rejects the whole batch: the
	// device cannot know when its own events were accepted otherwise.
	for i, e := range batch.Events {
		if e.DeviceID == deviceID {
			continue
		}
		s.log.Warn("rejected foreign event", "subject", deviceID, "device", e.DeviceID, "index", i)
		index := i
		writeProblem(w, problem{
			Type:       problemDeviceMismatch,
			Title:      "Device mismatch",
			Status:     http.StatusForbidden,
			Detail:     fmt.Sprintf("event %d belongs to another device; a device may only submit its own events", i),
			EventIndex: &index,
		})
		return
	}

	// received_at is intake's stamp, the same for every envelope of the batch:
	// a retry re-stamps it, which is why it is not part of the dedup identity.
	envs := make([]contracts.Envelope, len(batch.Events))
	for i, e := range batch.Events {
		envs[i] = contracts.Envelope{Event: e, ReceivedAt: now}
	}

	if err := s.producer.Produce(r.Context(), envs); err != nil {
		s.log.Error("failed to queue batch", "error", err, "device", deviceID, "events", len(envs))
		writeProblem(w, problem{
			Type:   problemBusUnavailable,
			Title:  "Event bus unavailable",
			Status: http.StatusBadGateway,
			Detail: "the batch could not be queued; retry later",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(accepted{Accepted: len(envs)}); err != nil {
		s.log.Error("failed to write accepted response", "error", err, "device", deviceID)
	}
}
