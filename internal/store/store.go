// Package store is the TimescaleDB adapter: it persists telemetry events
// idempotently and advances the per-device watermark in the same transaction
// (ADR-002, ADR-003).
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/watermark"
	"fleet-telemetry/migrations"
)

// Store owns the connection pool to TimescaleDB.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to TimescaleDB.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to timescale: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping timescale: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// Migrate applies the idempotent schema.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, migrations.Init); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// PersistResult reports what one batch did.
type PersistResult struct {
	Inserted   int64
	Duplicates int64
	// Advanced holds the devices whose watermark moved and their new value.
	Advanced map[string]uint64
}

// insertBatch writes every event of the batch in one statement.
// stored_at is left to the database clock: it is when the row became durable,
// not when the device or the intake handled it.
const insertBatch = `
WITH ins AS (
    INSERT INTO telemetry_events (
        device_id, seq, captured_at, received_at,
        lat, lon, speed_kmh, heading_deg, accuracy_m,
        mode, traceparent, schema_version
    )
    SELECT * FROM UNNEST(
        $1::text[], $2::bigint[], $3::timestamptz[], $4::timestamptz[],
        $5::float8[], $6::float8[], $7::float8[], $8::float8[], $9::float8[],
        $10::text[], $11::text[], $12::int[]
    )
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) FROM ins`

const deviceProgress = `
SELECT COALESCE(MAX(seq), 0), COUNT(*)
FROM telemetry_events
WHERE device_id = $1 AND seq > $2`

const lockWatermark = `SELECT watermark FROM device_watermark WHERE device_id = $1 FOR UPDATE`

const upsertWatermark = `
INSERT INTO device_watermark (device_id, watermark, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (device_id) DO UPDATE
SET watermark = EXCLUDED.watermark, updated_at = now()
WHERE device_watermark.watermark < EXCLUDED.watermark`

// PersistBatch inserts the batch and advances the watermark of every device it
// touches, atomically. A watermark that moved ahead of durability would let a
// device delete local events the server never stored, so both happen in one
// transaction.
func (s *Store) PersistBatch(ctx context.Context, envs []contracts.Envelope) (PersistResult, error) {
	res := PersistResult{Advanced: map[string]uint64{}}
	if len(envs) == 0 {
		return res, nil
	}

	cols := newBatchColumns(len(envs))
	for _, e := range envs {
		cols.add(e)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := tx.QueryRow(ctx, insertBatch,
		cols.deviceIDs, cols.seqs, cols.capturedAts, cols.receivedAts,
		cols.lats, cols.lons, cols.speeds, cols.headings, cols.accuracies,
		cols.modes, cols.traceparents, cols.schemaVersions,
	).Scan(&res.Inserted); err != nil {
		return res, fmt.Errorf("insert batch: %w", err)
	}
	res.Duplicates = int64(len(envs)) - res.Inserted

	for _, deviceID := range cols.devices() {
		next, err := advanceWatermark(ctx, tx, deviceID)
		if err != nil {
			return res, err
		}
		if next > 0 {
			res.Advanced[deviceID] = next
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}
	return res, nil
}

// advanceWatermark locks the device row, computes the contiguous run and moves
// the watermark forward when the run is complete.
func advanceWatermark(ctx context.Context, tx pgx.Tx, deviceID string) (uint64, error) {
	var current int64
	err := tx.QueryRow(ctx, lockWatermark, deviceID).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("lock watermark of %s: %w", deviceID, err)
	}

	var maxSeq, present int64
	if err := tx.QueryRow(ctx, deviceProgress, deviceID, current).Scan(&maxSeq, &present); err != nil {
		return 0, fmt.Errorf("read progress of %s: %w", deviceID, err)
	}

	next := watermark.Advance(uint64(current), uint64(maxSeq), present)
	if next == uint64(current) {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, upsertWatermark, deviceID, int64(next)); err != nil {
		return 0, fmt.Errorf("advance watermark of %s: %w", deviceID, err)
	}
	return next, nil
}

// Watermark returns the stored watermark of a device; 0 when no row exists.
func (s *Store) Watermark(ctx context.Context, deviceID string) (uint64, error) {
	var w int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT watermark FROM device_watermark WHERE device_id = $1), 0)`,
		deviceID).Scan(&w)
	if err != nil {
		return 0, fmt.Errorf("read watermark of %s: %w", deviceID, err)
	}
	return uint64(w), nil
}

// Count returns how many events of a device are stored; used by tests and by
// the pipeline-health tool of the agent.
func (s *Store) Count(ctx context.Context, deviceID string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM telemetry_events WHERE device_id = $1`, deviceID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count events of %s: %w", deviceID, err)
	}
	return n, nil
}
