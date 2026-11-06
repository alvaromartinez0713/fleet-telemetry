// Package contracts defines the contracts shared by every binary: the wire
// shape a device sends, the envelope that travels on the bus, and the routes
// and topic names both sides code against (ADR-001, ADR-002).
package contracts

import (
	"errors"
	"fmt"
	"time"
)

const (
	// TopicTelemetry is produced by ingest and consumed by the consumer and
	// the alerts worker. Partition key is always the device id (ADR-002).
	TopicTelemetry = "telemetry.events"

	// RouteTelemetry accepts a batch of events from a device.
	RouteTelemetry = "/v1/telemetry"

	// SchemaVersion is the only wire version this build accepts.
	SchemaVersion = 1

	// MaxBatchSize bounds one accepted batch.
	MaxBatchSize = 500

	// clockSkewTolerance is how far in the future a device clock may claim an
	// event was captured before the event is rejected.
	clockSkewTolerance = 5 * time.Minute
)

// Mode tells how the device is behaving, which is what makes the difference
// between "stopped" and "no signal" observable (ADR-003).
type Mode string

const (
	ModeMoving    Mode = "moving"
	ModeHeartbeat Mode = "heartbeat"
)

var (
	ErrEmptyDeviceID    = errors.New("device_id is empty")
	ErrDeviceIDTooLong  = errors.New("device_id is longer than 64 bytes")
	ErrSeqNotPositive   = errors.New("seq must be greater than zero")
	ErrCapturedAtZero   = errors.New("captured_at is zero")
	ErrCapturedInFuture = errors.New("captured_at is beyond the accepted clock skew")
	ErrBadMode          = errors.New("mode must be moving or heartbeat")
	ErrLatitudeRange    = errors.New("lat is outside [-90, 90]")
	ErrLongitudeRange   = errors.New("lon is outside [-180, 180]")
	ErrSchemaVersion    = errors.New("unsupported schema_version")
	ErrEmptyBatch       = errors.New("batch has no events")
	ErrBatchTooLarge    = errors.New("batch exceeds MaxBatchSize")
)

// Event is one telemetry sample as captured by a device.
//
// CapturedAt is stamped by the device and is immutable: a retried event is a
// byte-identical replay, which is what makes (DeviceID, CapturedAt, Seq) a
// stable dedup key (ADR-002). ReceivedAt and StoredAt are stamped by the
// pipeline, never by the device.
type Event struct {
	DeviceID      string    `json:"device_id"`
	Seq           uint64    `json:"seq"`
	CapturedAt    time.Time `json:"captured_at"`
	Lat           float64   `json:"lat"`
	Lon           float64   `json:"lon"`
	SpeedKmh      *float64  `json:"speed_kmh,omitempty"`
	HeadingDeg    *float64  `json:"heading_deg,omitempty"`
	AccuracyM     *float64  `json:"accuracy_m,omitempty"`
	Mode          Mode      `json:"mode"`
	Traceparent   string    `json:"traceparent,omitempty"`
	SchemaVersion int       `json:"schema_version"`
}

// Batch is the request body accepted by RouteTelemetry.
type Batch struct {
	Events []Event `json:"events"`
}

// Envelope is what travels on the bus: the device event plus the moment ingest
// accepted it. The consumer adds StoredAt from the database clock.
type Envelope struct {
	Event
	ReceivedAt time.Time `json:"received_at"`
}

// Validate reports the first contract violation of the event. now bounds the
// accepted device clock skew.
func (e Event) Validate(now time.Time) error {
	switch {
	case e.DeviceID == "":
		return ErrEmptyDeviceID
	case len(e.DeviceID) > 64:
		return ErrDeviceIDTooLong
	case e.Seq == 0:
		return ErrSeqNotPositive
	case e.CapturedAt.IsZero():
		return ErrCapturedAtZero
	case e.CapturedAt.After(now.Add(clockSkewTolerance)):
		return ErrCapturedInFuture
	case e.Lat < -90 || e.Lat > 90:
		return ErrLatitudeRange
	case e.Lon < -180 || e.Lon > 180:
		return ErrLongitudeRange
	case e.Mode != ModeMoving && e.Mode != ModeHeartbeat:
		return ErrBadMode
	case e.SchemaVersion != SchemaVersion:
		return fmt.Errorf("%w: got %d", ErrSchemaVersion, e.SchemaVersion)
	}
	return nil
}

// Validate checks the batch envelope rules and every event in it. It returns
// the index of the first invalid event so callers can report it.
func (b Batch) Validate(now time.Time) (int, error) {
	if len(b.Events) == 0 {
		return -1, ErrEmptyBatch
	}
	if len(b.Events) > MaxBatchSize {
		return -1, ErrBatchTooLarge
	}
	for i, e := range b.Events {
		if err := e.Validate(now); err != nil {
			return i, err
		}
	}
	return -1, nil
}
