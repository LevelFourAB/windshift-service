package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Headers struct {
	// PublishedAt is the time the event was published by the producer.
	PublishedAt    time.Time
	IdempotencyKey *string
	TraceParent    *string
	TraceState     *string
}

// Event represents a single event from the event queue. It is received via
// NATS and should be processed within a certain deadline, using Accept() or
// Reject(shouldRetry) to acknowledge the event. If the deadline is exceeded,
// the event will be redelivered. To extend the deadline, use Ping().
type Event struct {
	span   trace.Span
	logger *zap.Logger
	msg    *nats.Msg

	// Context is the context of this event. It will be valid until the event
	// expires, is accepted or rejected.
	Context context.Context

	// Subject is the subject the event was published to.
	Subject string

	// SubscriptionSeq is the sequence number of the event in the queue.
	SubscriptionSeq uint64

	// StreamSeq is the sequence number of the event in the event stream. Can
	// be used for resuming from a certain point in time. For example with an
	// ephemeral queue, the consumer can store the last seen StreamSeq and
	// resume from there on the next run.
	StreamSeq uint64

	// Headers contains the headers of the event.
	Headers *Headers

	// Data is the protobuf message published by the producer.
	Data *anypb.Any
}

func newEvent(
	ctx context.Context,
	span trace.Span,
	logger *zap.Logger,
	msg nats.Msg,
	md *nats.MsgMetadata,
) (*Event, error) {
	// Unmarshal protobuf message from msg.Data
	data := &anypb.Any{}
	err := proto.Unmarshal(msg.Data, data)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal message")
	}

	headers := &Headers{
		PublishedAt: md.Timestamp,
	}

	// Get the published header
	publishTimeHeader := msg.Header.Get("WS-Published-Time")
	if publishTimeHeader != "" {
		publishedTime, err := time.Parse(time.RFC3339Nano, publishTimeHeader)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse header")
		}

		headers.PublishedAt = publishedTime
	}

	// Get the idempotency key header
	idempotencyKeyHeader := msg.Header.Get("Nats-Msg-Id")
	if idempotencyKeyHeader != "" {
		headers.IdempotencyKey = &idempotencyKeyHeader
	}

	// Get the trace parent header
	traceParentHeader := msg.Header.Get("WS-Trace-Parent")
	if traceParentHeader != "" {
		headers.TraceParent = &traceParentHeader
	}

	// Get the trace state header
	traceStateHeader := msg.Header.Get("WS-Trace-State")
	if traceStateHeader != "" {
		headers.TraceState = &traceStateHeader
	}

	// Clear the data of the message
	msg.Data = nil

	return &Event{
		span:            span,
		logger:          logger,
		msg:             &msg,
		Context:         ctx,
		Subject:         msg.Subject,
		SubscriptionSeq: md.Sequence.Stream,
		StreamSeq:       md.Sequence.Consumer,
		Headers:         headers,
		Data:            data,
	}, nil
}

// DiscardData discards the data of the event. This should be called if the
// event data is not needed anymore. Accepting or rejecting the event will
// continue working after this.
func (e *Event) DiscardData() {
	e.Data = nil
}

// Ping extends the deadline of the event. This should be called periodically
// to prevent the event from being redelivered.
func (e *Event) Ping() error {
	e.logger.Debug("Pinging event", zap.Uint64("streamSeq", e.StreamSeq))
	err := e.msg.InProgress()
	if err != nil {
		e.span.RecordError(err)
		return errors.Wrap(err, "could not ping message")
	}
	e.span.AddEvent("pinged")

	return nil
}

// Accept accepts the event. The event will be removed from the queue.
func (e *Event) Accept() error {
	defer e.span.End()

	e.logger.Debug("Accepting event", zap.Uint64("streamSeq", e.StreamSeq))
	err := e.msg.Ack()
	if err != nil {
		e.span.RecordError(err)
		return errors.Wrap(err, "could not accept message")
	}

	e.span.SetStatus(codes.Ok, "")
	return nil
}

// Reject rejects the event. If allowRedeliver is true, the event will be
// redelivered after a certain amount of time. If false, the event will be
// permanently rejected and not redelivered.
func (e *Event) Reject(allowRedeliver bool) error {
	defer e.span.End()

	if allowRedeliver {
		// The event should be redelivered if possible
		e.logger.Debug("Rejecting event", zap.Uint64("streamSeq", e.StreamSeq))
		err := e.msg.Nak()
		if err != nil {
			e.span.RecordError(err)
			return errors.Wrap(err, "could not reject message")
		}

		e.span.SetStatus(codes.Error, "event rejected")
		return nil
	}

	// This is a permanent rejection, terminate the event
	e.logger.Debug("Permanently rejecting event", zap.Uint64("streamSeq", e.StreamSeq))
	err := e.msg.Term()
	if err != nil {
		e.span.RecordError(err)
		return errors.Wrap(err, "could not permanently reject message")
	}

	e.span.SetStatus(codes.Error, "event permanently rejected")
	return nil
}
