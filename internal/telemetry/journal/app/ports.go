// Package app is the journal module's core: the use case that moves telemetry
// from the bus to storage, in micro-batches, persisting before it commits
// (ADR-002). Every external interaction crosses a port this package declares, so
// the core imports no driver and no other module.
package app

import (
	"context"

	"fleet-telemetry/internal/contracts"
)

// Result reports what one persisted micro-batch did.
type Result struct {
	Inserted   int64
	Duplicates int64
	// Advanced holds the devices whose watermark moved and their new value.
	Advanced map[string]uint64
}

// Store persists one micro-batch and advances the watermarks of the devices it
// contains in the same transaction. The TimescaleDB adapter implements it.
type Store interface {
	PersistBatch(context.Context, []contracts.Envelope) (Result, error)
}

// Message is one record the source delivered. The core reads the envelope, logs
// the position of an undecodable record and hands the message back unchanged, so
// the driver type stays in the adapter that produced it.
type Message struct {
	Envelope contracts.Envelope
	// Partition and Offset locate the record: they identify the poison record in
	// the log line and they are the unit the commit moves.
	Partition int32
	Offset    int64
	// DecodeErr reports a payload that does not satisfy the envelope contract: the
	// record is committed with its batch but never persisted, so a poison message
	// cannot block the partition.
	DecodeErr error
	// Ref is the source's own handle for the record. The core never inspects it;
	// it only passes it back to Commit, which keeps the Kafka record type out of
	// the core.
	Ref any
}

// Source is the consumed stream the runner drains. The Kafka consumer-group
// adapter implements it.
type Source interface {
	Poll(context.Context) ([]Message, error)
	Commit(context.Context, []Message) error
}
