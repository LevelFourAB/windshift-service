package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type PublishConfig struct {
	// Subject to publish event to
	Subject string
	// Data to publish
	Data *anypb.Any
	// ExpectedSubjectSeq is the expected sequence number of the subject.
	ExpectedSubjectSeq *uint64
	// PublishedTime is the time the event was published. If nil, the current time will be used.
	PublishedTime *time.Time
	// IdempotencyKey is the idempotency key for the event. If empty, the event will not be idempotent.
	IdempotencyKey string
	// TraceParent is the trace parent header for the event.
	TraceParent *string
	// TraceState is the trace state header for the event.
	TraceState *string
}

type PublishedEvent struct {
	ID uint64
}

func Publish(ctx context.Context, js nats.JetStreamContext, config *PublishConfig) (*PublishedEvent, error) {
	// Create the message
	msg := &nats.Msg{
		Subject: config.Subject,
		Header:  nats.Header{},
	}

	// Set the message data
	bytes, err := proto.Marshal(config.Data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal message")
	}
	msg.Data = bytes

	publishOpts := []nats.PubOpt{
		nats.Context(ctx),
	}

	// Set the published time
	publishTime := time.Now()
	if config.PublishedTime != nil {
		publishTime = *config.PublishedTime
	}
	msg.Header.Set("WS-Published-Time", publishTime.Format(time.RFC3339Nano))

	// Set the idempotency key
	if config.IdempotencyKey != "" {
		msg.Header.Set("Nats-Msg-Id", config.IdempotencyKey)
	}

	// Set the trace parent
	if config.TraceParent != nil {
		msg.Header.Set("WS-Trace-Parent", *config.TraceParent)
	}

	// Set the trace state
	if config.TraceState != nil {
		msg.Header.Set("WS-Trace-State", *config.TraceState)
	}

	// Set the expected subject sequence
	if config.ExpectedSubjectSeq != nil {
		publishOpts = append(publishOpts, nats.ExpectLastSequencePerSubject(*config.ExpectedSubjectSeq))
	}

	// Publish the message.
	ack, err := js.PublishMsg(msg, publishOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish message")
	}

	return &PublishedEvent{
		ID: ack.Sequence,
	}, nil
}
