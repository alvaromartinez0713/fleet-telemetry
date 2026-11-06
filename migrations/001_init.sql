-- Telemetry storage for the fleet portal (ADR-001, ADR-002).
-- Idempotent: safe to run on every startup and in tests.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS telemetry_events (
    device_id      text        NOT NULL,
    seq            bigint      NOT NULL,
    captured_at    timestamptz NOT NULL,
    received_at    timestamptz NOT NULL,
    stored_at      timestamptz NOT NULL DEFAULT now(),
    lat            double precision NOT NULL,
    lon            double precision NOT NULL,
    speed_kmh      double precision,
    heading_deg    double precision,
    accuracy_m     double precision,
    mode           text        NOT NULL,
    traceparent    text,
    schema_version integer     NOT NULL DEFAULT 1
);

SELECT create_hypertable('telemetry_events', by_range('captured_at'), if_not_exists => TRUE);

-- Dedup key. A hypertable unique index must contain the partitioning column,
-- so captured_at is part of it; replays keep captured_at immutable, which makes
-- (device_id, captured_at, seq) stable across retries (ADR-002).
CREATE UNIQUE INDEX IF NOT EXISTS telemetry_events_dedup_uq
    ON telemetry_events (device_id, captured_at, seq);

-- Watermark contiguity reads rows of one device by seq.
CREATE INDEX IF NOT EXISTS telemetry_events_device_seq_idx
    ON telemetry_events (device_id, seq);

-- Cumulative FIFO watermark per device (ADR-003). No row means 0.
CREATE TABLE IF NOT EXISTS device_watermark (
    device_id  text        PRIMARY KEY,
    watermark  bigint      NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);
