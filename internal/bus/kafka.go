// Package bus is the Kafka adapter (Redpanda locally, Kafka-shaped by design,
// ADR-001). The partition key of a telemetry event is always the device id, so
// one device's events keep their relative order through the bus (ADR-002).
package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"fleet-telemetry/internal/contracts"
)

// Producer publishes telemetry envelopes.
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

// Message is one consumed record. DecodeErr reports a payload that does not
// satisfy the envelope contract: the record is committed with its batch but
// never persisted, so a poison message cannot block the partition.
type Message struct {
	Envelope  contracts.Envelope
	Record    *kgo.Record
	DecodeErr error
}

// Group consumes the topic as part of a consumer group.
type Group struct {
	client *kgo.Client
}

// NewGroup joins the consumer group. Offsets are committed manually: a message
// is acknowledged only after it is persisted (ADR-002).
func NewGroup(brokers []string, group, topic string) (*Group, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Group{client: client}, nil
}

// Poll returns the messages available within ctx. A deadline expiry is not an
// error: it is the caller's flush tick.
func (g *Group) Poll(ctx context.Context) ([]Message, error) {
	fetches := g.client.PollFetches(ctx)
	if err := fetches.Err(); err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, fmt.Errorf("poll: %w", err)
	}
	var out []Message
	fetches.EachRecord(func(r *kgo.Record) {
		var env contracts.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			out = append(out, Message{Record: r, DecodeErr: err})
			return
		}
		out = append(out, Message{Envelope: env, Record: r})
	})
	return out, nil
}

// Commit marks the offsets of the processed records as consumed.
func (g *Group) Commit(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		records = append(records, m.Record)
	}
	if err := g.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Close leaves the group and releases the client.
func (g *Group) Close() { g.client.Close() }
