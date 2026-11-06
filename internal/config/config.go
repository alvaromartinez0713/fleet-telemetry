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

// Ingest is the configuration of the HTTP intake.
type Ingest struct {
	KafkaBrokers []string
	Topic        string
	Addr         string
	JWTSecret    []byte
	MaxBatch     int
}

// Consumer is the configuration of the bus consumer.
type Consumer struct {
	KafkaBrokers []string
	Topic        string
	DatabaseURL  string
	Group        string
	FlushEvery   time.Duration
	MaxBatchSize int
}

// LoadIngest reads the ingest configuration. The intake never touches the
// database, so no DSN is required.
func LoadIngest() (Ingest, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return Ingest{}, fmt.Errorf("JWT_SECRET is required")
	}
	return Ingest{
		KafkaBrokers: brokers(),
		Topic:        env("KAFKA_TOPIC", "telemetry.events"),
		Addr:         env("HTTP_ADDR", ":8080"),
		JWTSecret:    []byte(secret),
		MaxBatch:     envInt("MAX_BATCH", 500),
	}, nil
}

// LoadConsumer reads the consumer configuration.
func LoadConsumer() (Consumer, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return Consumer{}, fmt.Errorf("DATABASE_URL is required")
	}
	return Consumer{
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
