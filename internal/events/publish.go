package events

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
)

// PublishConfig is the configuration for publishing an event.
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
}

// PublishedEvent contains information about a published event.
type PublishedEvent struct {
	// ID is the sequence number of the event.
	ID uint64
}

// Publish publishes an event to a stream.
func (m *Manager) Publish(ctx context.Context, config *PublishConfig) (*PublishedEvent, error) {
	ctx, span := m.tracer.Start(
		ctx,
		config.Subject+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystem("nats"),
			semconv.MessagingOperationPublish,
			semconv.MessagingDestinationName(config.Subject),
		),
	)
	defer span.End()

	if !IsValidSubject(config.Subject, false) {
		span.SetStatus(codes.Error, "invalid subject")
		return nil, newValidationError("invalid subject: " + config.Subject)
	}

	if config.Data == nil {
		span.SetStatus(codes.Error, "no data specified")
		return nil, newValidationError("no data specified")
	}

	// Create the message
	msg := &nats.Msg{
		Subject: config.Subject,
		Header:  nats.Header{},
	}

	publishOpts := []jetstream.PublishOpt{}

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

	// Set the expected subject sequence
	if config.ExpectedSubjectSeq != nil {
		publishOpts = append(publishOpts, jetstream.WithExpectLastSequencePerSubject(*config.ExpectedSubjectSeq))
	}

	// Inject the tracing headers
	m.w3cPropagator.Inject(ctx, eventTracingHeaders{
		headers: &msg.Header,
	})

	// Copy data as is
	msg.Header.Set("WS-Data-Type", string(config.Data.MessageName()))
	msg.Data = config.Data.Value

	m.logger.Debug(
		"Publishing event",
		zap.String("subject", config.Subject),
		zap.String("dataType", config.Data.TypeUrl),
		zap.Any("headers", msg.Header),
	)

	// Publish the message.
	f, err := m.js.PublishMsgAsync(msg, publishOpts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return nil, errors.Wrap(err, "failed to publish message")
	}

	select {
	case <-ctx.Done():
		// We don't know if the message was published or not, so the trace
		// will be marked as unset.
		span.SetStatus(codes.Unset, "context canceled")
		return nil, errors.Wrapf(ctx.Err(), "failed to publish message")
	case ack := <-f.Ok():
		span.SetAttributes(
			semconv.MessagingMessageID(fmt.Sprintf("%d", ack.Sequence)),
		)
		span.SetStatus(codes.Ok, "")
		return &PublishedEvent{
			ID: ack.Sequence,
		}, nil
	case err := <-f.Err():
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")

		if errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, nats.ErrNoResponders) {
			return nil, ErrUnboundSubject
		} else if errors.Is(err, nats.ErrTimeout) {
			return nil, ErrPublishTimeout
		}

		var natsErr *jetstream.APIError
		if errors.As(err, &natsErr) {
			if natsErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence {
				return nil, ErrWrongSequence
			}
		}

		return nil, errors.Wrap(err, "failed to publish message")
	}
}
