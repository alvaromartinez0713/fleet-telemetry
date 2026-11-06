package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"fleet-telemetry/internal/contracts"
	"fleet-telemetry/internal/telemetry/journal/app"
)

// Group consumes the topic as part of a consumer group. It implements
// app.Source.
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
//
// The records are read out of the fetch BEFORE the fetch error is considered.
// A fetch can carry both records and an error, and franz-go has already advanced
// the consumer position for those records: returning early would drop events
// whose offsets a later commit still covers, which is exactly the at-least-once
// guarantee this pipeline promises (ADR-002). Callers must persist the returned
// messages even when err is not nil, then decide what to do with the error.
func (g *Group) Poll(ctx context.Context) ([]app.Message, error) {
	fetches := g.client.PollFetches(ctx)

	var out []app.Message
	fetches.EachRecord(func(r *kgo.Record) {
		msg := app.Message{Partition: r.Partition, Offset: r.Offset, Ref: r}
		var env contracts.Envelope
		if err := json.Unmarshal(r.Value, &env); err != nil {
			msg.DecodeErr = err
			out = append(out, msg)
			return
		}
		// A record can decode and still be structurally unusable; it is handled
		// like an undecodable one so it never becomes a row with an empty device
		// or a zero timestamp.
		if err := env.ValidateShape(); err != nil {
			msg.DecodeErr = err
			out = append(out, msg)
			return
		}
		msg.Envelope = env
		out = append(out, msg)
	})

	if err := fetches.Err(); err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
			return out, nil
		}
		return out, fmt.Errorf("poll: %w", err)
	}
	return out, nil
}

// Commit marks the offsets of the processed records as consumed. The record
// handle is the one Poll attached to each message: rebuilding a record from
// partition and offset alone would drop its leader epoch.
func (g *Group) Commit(ctx context.Context, msgs []app.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	records := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		record, ok := m.Ref.(*kgo.Record)
		if !ok || record == nil {
			return fmt.Errorf("commit offset %d/%d: message carries no kafka record", m.Partition, m.Offset)
		}
		records = append(records, record)
	}
	if err := g.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Close leaves the group and releases the client.
func (g *Group) Close() { g.client.Close() }
