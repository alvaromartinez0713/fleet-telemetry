// Package config loads the runtime configuration of each binary from the
// environment. Configuration is deployment plumbing and stays out of the
// domain: every consumer receives plain values.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Intake is the configuration of the intake module.
type Intake struct {
	KafkaBrokers []string
	Topic        string
	Addr         string
	JWTSecret    []byte
	MaxBatch     int
}

// Journal is the configuration of the journal module.
type Journal struct {
	KafkaBrokers []string
	Topic        string
	DatabaseURL  string
	Group        string
	FlushEvery   time.Duration
	MaxBatchSize int
}

// LoadIntake reads the intake configuration. The intake never touches the
// database, so no DSN is required.
func LoadIntake() (Intake, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return Intake{}, fmt.Errorf("JWT_SECRET is required")
	}
	return Intake{
		KafkaBrokers: brokers(),
		Topic:        env("KAFKA_TOPIC", "telemetry.events"),
		Addr:         env("HTTP_ADDR", ":8080"),
		JWTSecret:    []byte(secret),
		MaxBatch:     envInt("MAX_BATCH", 500),
	}, nil
}

// LoadJournal reads the journal configuration.
func LoadJournal() (Journal, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return Journal{}, fmt.Errorf("DATABASE_URL is required")
	}
	return Journal{
		KafkaBrokers: brokers(),
		Topic:        env("KAFKA_TOPIC", "telemetry.events"),
		DatabaseURL:  dsn,
		Group:        env("CONSUMER_GROUP", "telemetry-consumer"),
		FlushEvery:   envDuration("FLUSH_EVERY", 2*time.Second),
		MaxBatchSize: envInt("MAX_BATCH", 500),
	}, nil
}

func brokers() []string {
	return strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
