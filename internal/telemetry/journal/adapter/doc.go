// Package adapter holds the journal module's driven adapters: the Kafka
// consumer group that feeds the use case and the TimescaleDB store that persists
// telemetry idempotently and advances the per-device watermark in one
// transaction (ADR-002, ADR-003). Both implement ports declared in
// internal/telemetry/journal/app and neither knows about another module.
package adapter
