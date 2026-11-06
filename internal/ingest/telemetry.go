package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/platform/httpx"
)

// accepted is the 202 body: it counts what was durably queued, never what was
// persisted (ADR-002).
type accepted struct {
	Accepted int `json:"accepted"`
}

// handleTelemetry is the intake use case. The device id arrives from the
// authentication middleware and the bounded body from the body-limit middleware;
// what is left is the use case itself: decode, enforce the batch limit, validate
// the contract, refuse foreign events, stamp received_at and queue the batch.
//
// Every failure leaves as an error, never as a written response: the choke point
// is the only place that renders a problem, so this reads as a use case instead
// of as plumbing.
func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) error {
	deviceID := deviceIDFrom(r.Context())
	now := s.now()

	batch, err := decodeBatch(r, s.maxBytes)
	if err != nil {
		return err
	}

	if len(batch.Events) > s.maxBatch {
		return failure(problemInvalidBatch, "Telemetry batch too large", http.StatusBadRequest,
			fmt.Sprintf("the batch has %d events and the configured limit is %d", len(batch.Events), s.maxBatch), nil)
	}

	idx, err := batch.Validate(now)
	if err != nil {
		if idx < 0 {
			return failure(problemInvalidBatch, "Invalid telemetry batch", http.StatusBadRequest, err.Error(), err)
		}
		return eventFailure(problemInvalidBatch, "Invalid telemetry event", http.StatusBadRequest, idx,
			fmt.Sprintf("event %d: %s", idx, err), err)
	}

	// A token identifies one device installation, so a batch may only carry that
	// device's events. One foreign event rejects the whole batch: the device
	// cannot know when its own events were accepted otherwise.
	for i, e := range batch.Events {
		if e.DeviceID == deviceID {
			continue
		}
		return eventFailure(problemDeviceMismatch, "Device mismatch", http.StatusForbidden, i,
			fmt.Sprintf("event %d belongs to another device; a device may only submit its own events", i),
			fmt.Errorf("event %d of %d submitted by device %q", i, len(batch.Events), e.DeviceID))
	}

	// received_at is intake's stamp, the same for every envelope of the batch: a
	// retry re-stamps it, which is why it is not part of the dedup identity.
	envs := make([]contracts.Envelope, len(batch.Events))
	for i, e := range batch.Events {
		envs[i] = contracts.Envelope{Event: e, ReceivedAt: now}
	}

	if err := s.producer.Produce(r.Context(), envs); err != nil {
		return failure(problemBusUnavailable, "Event bus unavailable", http.StatusBadGateway,
			"the batch could not be queued; retry later", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(accepted{Accepted: len(envs)}); err != nil {
		// The 202 is already on the wire: the choke point logs this and does not
		// try to render a problem over it.
		return fmt.Errorf("write accepted response: %w", err)
	}
	return nil
}

// decodeBatch reads the request body as a telemetry batch and translates the two
// ways a body can fail into the problem the client should see: an overflow of the
// body limit becomes a 413, anything else a 400. Both travel as ProblemError, so
// the shared helper keeps the rendering out of the use case.
func decodeBatch(r *http.Request, maxBytes int64) (contracts.Batch, error) {
	var batch contracts.Batch
	err := json.NewDecoder(r.Body).Decode(&batch)
	if err == nil {
		return batch, nil
	}

	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return contracts.Batch{}, failure(problemBodyTooLarge, "Request body too large",
			http.StatusRequestEntityTooLarge, fmt.Sprintf("the body exceeds the %d byte limit", maxBytes), err)
	}
	return contracts.Batch{}, failure(problemInvalidBatch, "Invalid request body",
		http.StatusBadRequest, "the body is not a valid telemetry batch", err)
}

// failure builds a ProblemError for one of the intake's failure classes: kind and
// title identify it, detail is the only text the client sees, and cause is
// log-only. It keeps the problem construction in one place so the use case above
// stays a sequence of steps.
func failure(kind, title string, status int, detail string, cause error) *httpx.ProblemError {
	return httpx.NewProblemError(httpx.Problem{Type: kind, Title: title, Status: status, Detail: detail}, cause)
}

// eventFailure is failure plus the event_index extension member: it points at the
// offending event of a batch so a client can drop just that one.
func eventFailure(kind, title string, status int, index int, detail string, cause error) *httpx.ProblemError {
	return httpx.NewProblemError(httpx.Problem{
		Type: kind, Title: title, Status: status, Detail: detail, EventIndex: &index,
	}, cause)
}
