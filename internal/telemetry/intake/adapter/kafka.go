// Package adapter holds the intake module's driven adapter: the Kafka producer
// that queues accepted telemetry (Redpanda locally, Kafka-shaped by design,
// ADR-001). The partition key of an event is always the device id, so one
// device's events keep their relative order through the bus (ADR-002).
package adapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"fleet-telemetry/internal/contracts"
)

// Producer publishes telemetry envelopes. It implements app.Producer.
type Producer struct {
	client *kgo.Client
	topic  string
}

// NewProducer creates a producer for the given topic.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, topic: topic}, nil
}

// Produce sends the batch and returns once every record is acknowledged.
// A nil error means the events are durably queued, which is exactly what a
// 202 to the device promises (ADR-002).
func (p *Producer) Produce(ctx context.Context, envs []contracts.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(envs))
	for _, e := range envs {
		value, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("encode event of %s seq %d: %w", e.DeviceID, e.Seq, err)
		}
		records = append(records, &kgo.Record{
			Topic: p.topic,
			Key:   []byte(e.DeviceID),
			Value: value,
		})
	}
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce batch: %w", err)
	}
	return nil
}

// Close flushes and releases the producer.
func (p *Producer) Close() { p.client.Close() }
