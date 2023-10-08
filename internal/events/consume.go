package events

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
	"windshift/service/internal/events/flowcontrol"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// EventConsumeConfig is the configuration for consuming events from a stream.
type EventConsumeConfig struct {
	// Stream is the name of the stream to consume events from.
	Stream string
	// Name is the name of the consumer to consume events from.
	Name string
	// MaxPendingEvents is the maximum number of events that can be pending
	// before the consumer will stop receiving events. If set to 0, a default
	// value of 50 will be used.
	MaxPendingEvents uint
}

// Events is used to receive events from a stream. Events are tied to an
// an already defined consumer which has been previously defined using
// Manager.EnsureConsumer().
type Events struct {
	// manager is the event manager.
	manager *Manager
	// logger is the logger used by the event consumer.
	logger *zap.Logger

	// ctx is the context of the event consumer.
	ctx context.Context
	// ctxCancel is used to cancel the context.
	ctxCancel context.CancelFunc
	// shutdownSignal is used to signal that the actual receiving of events
	// has stopped.
	shutdownSignal chan struct{}
	// closed is used to indicate that the instance has been closed.
	closed int64

	// messages is the NATS subscription used to receive messages.
	messages jetstream.MessagesContext
	// channel is the channel used to send events to the caller.
	channel chan *Event

	// Timeout is the timeout for processing an event. Will be fetched
	// from the consumer configuration.
	Timeout time.Duration
}

// Consume creates a new event consumer for the specified stream and consumer.
// The consumer must have been previously created using Manager.EnsureConsumer().
//
// The returned Events instance must be closed when it is no longer needed.
// This will stop the event consumer and wait for any pending events to be
// processed.
//
// This will honor the context passed in, and will stop the event consumer
// when the context is done.
//
// Example:
//
//	events, err := manager.Consume(ctx, &events.EventConsumeConfig{
//		Stream: "my-stream",
//		Name:   "my-consumer",
//	})
//	if err != nil {
//		// Handle error
//	}
//	defer events.Close()
//
//	for event := range events.Events() {
//		// Handle event
//	}
func (m *Manager) Consume(ctx context.Context, config *EventConsumeConfig) (*Events, error) {
	ctx, span := m.tracer.Start(ctx, config.Stream+" subscribe")
	defer span.End()

	if config.Stream == "" {
		return nil, errors.New("name of stream must be specified")
	}

	if config.Name == "" {
		return nil, errors.New("name of subscription must be specified")
	}

	if config.MaxPendingEvents == 0 {
		// Default to 50 pending events
		config.MaxPendingEvents = 50
	}

	consumer, err := m.js.Consumer(ctx, config.Stream, config.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get consumer info")
	}

	maxEvents := config.MaxPendingEvents / 2
	if maxEvents < 1 {
		maxEvents = 1
	}
	messages, err := consumer.Messages(
		jetstream.PullExpiry(200*time.Millisecond),
		jetstream.PullMaxMessages(maxEvents),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create message subscription")
	}

	logger := m.logger.With(zap.String("stream", config.Stream), zap.String("subscription", config.Name))
	logger.Debug("Created event consumer")

	ctx, cancel := context.WithCancel(ctx)

	q := &Events{
		manager: m,
		logger:  logger,

		ctx:            ctx,
		ctxCancel:      cancel,
		shutdownSignal: make(chan struct{}, 1),

		messages: messages,
		channel:  make(chan *Event),

		Timeout: consumer.CachedInfo().Config.AckWait,
	}

	go q.pump(ctx, config.MaxPendingEvents)
	return q, nil
}

// pump is a helper function that will pump messages from the NATS subscription
// into the channel.
func (q *Events) pump(ctx context.Context, maxPendingEvents uint) {
	fc := flowcontrol.NewFlowControl(ctx, q.logger, q.Timeout, int(maxPendingEvents))
	timeout := q.Timeout

	for {
		if ctx.Err() != nil {
			if atomic.LoadInt64(&q.closed) == 1 {
				q.logger.Debug("Context done, subscriptions already stopped, doing nothing")
				return
			}

			q.logger.Debug("Context done, stopping subscription")
			q.messages.Stop()

			atomic.StoreInt64(&q.closed, 1)
			close(q.channel)
			q.shutdownSignal <- struct{}{}
			return
		}

		fc.WaitUntilAvailable()
		msg, err := q.messages.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			q.logger.Debug("iterator closed, stopping")
			continue
		} else if errors.Is(err, jetstream.ErrNoHeartbeat) {
			q.logger.Debug("no heartbeat received")
			continue
		} else if err != nil {
			q.logger.Error("failed to fetch message", zap.Error(err))
			continue
		}

		now := time.Now()

		if time.Since(now) > timeout {
			// Timeout, reject the event
			q.logger.Debug("Timeout, event should be rejected by NATS")
			continue
		}

		event, err2 := q.createEvent(ctx, fc, msg)
		if err2 != nil {
			continue
		}

		q.logger.Debug(
			"Received event",
			zap.Uint64("streamSeq", event.StreamSeq),
			zap.Uint64("consumerSeq", event.ConsumerSeq),
			zap.String("type", string(event.Data.MessageName())),
		)

		select {
		case q.channel <- event:
			// Event sent to channel
		case <-ctx.Done():
			// Context is done, stop trying to fetch messages
			break
		}
	}
}

// createEvent takes a NATS message, extracts the tracing information and
// creates an Event that can be passed on to the subscriber.
func (q *Events) createEvent(ctx context.Context, fc *flowcontrol.FlowControl, msg jetstream.Msg) (*Event, error) {
	// We may have tracing information stored in the event headers, so we
	// extract them and create our own span indicating that we received the
	// message.
	//
	// Unlike for most tracing the span is only ended in this function if an
	// error occurs, otherwise it is passed into the event and ended when the
	// event is consumed.
	headers := msg.Headers()
	msgCtx := q.manager.w3cPropagator.Extract(ctx, eventTracingHeaders{
		headers: &headers,
	})
	msgCtx, span := q.manager.tracer.Start(
		msgCtx,
		msg.Subject()+" receive", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystem("nats"),
			semconv.MessagingOperationReceive,
			semconv.MessagingDestinationName(msg.Subject()),
		),
	)

	md, err := msg.Metadata()
	if err != nil {
		// Record the error and end the tracing as the span is not passed on
		q.logger.Error("failed to get message metadata", zap.Error(err))
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get message metadata")
		span.End()
		return nil, err
	}

	// Set the message ID as an attribute
	span.SetAttributes(semconv.MessagingMessageID(fmt.Sprintf("%d", md.Sequence.Stream)))

	onProcess := fc.Received(md.Sequence.Consumer)
	event, err := newEvent(msgCtx, span, q.logger, msg, md, onProcess)
	if err != nil {
		q.logger.Error("failed to create event", zap.Error(err))
		span.RecordError(err)

		// If we fail to parse the event data it is most likely an invalid
		// Protobuf message. In this case we terminate the message so it is not
		// redelivered
		err2 := msg.Term()
		if err2 != nil {
			q.logger.Warn("failed to terminate message", zap.Error(err2))
			span.RecordError(err2)
		}

		// Record the error and end the tracing as the span is not passed on
		span.SetStatus(codes.Error, "failed to create event")
		span.End()
		return nil, err
	}

	return event, nil
}

// Close closes the event consumer. Will stop receiving events and wait for
// pending events to be processed.
func (q *Events) Close() error {
	if atomic.LoadInt64(&q.closed) == 1 {
		return nil
	}

	q.messages.Stop()
	q.ctxCancel()
	<-q.shutdownSignal
	return nil
}

// Events returns the channel that events will be sent to.
func (q *Events) Events() <-chan *Event {
	return q.channel
}
