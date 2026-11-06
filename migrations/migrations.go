// Package migrations embeds the SQL schema so every binary and every test
// applies exactly the same, idempotent DDL.
package migrations

import _ "embed"

// Init is the telemetry schema (hypertable, dedup index, watermark table).
//
//go:embed 001_init.sql
var Init string
