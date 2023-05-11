package events

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type QueueConfig struct {
	Stream string
	Name   string

	BatchSize int
}

type Queue struct {
	manager *Manager
	logger  *zap.Logger

	ctx            context.Context
	ctxCancel      context.CancelFunc
	shutdownSignal chan struct{}

	subscription *nats.Subscription
	channel      chan *Event

	batchSize int
}

func (m *Manager) Subscribe(ctx context.Context, config *QueueConfig) (*Queue, error) {
	ctx, span := m.tracer.Start(ctx, config.Stream+" subscribe")
	defer span.End()

	if config.Stream == "" {
		return nil, errors.New("name of stream must be specified")
	}

	if config.Name == "" {
		return nil, errors.New("name of subscription must be specified")
	}

	ci, err := m.jetStream.ConsumerInfo(config.Stream, config.Name, nats.Context(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get consumer info")
	}

	sub, err := m.jetStream.PullSubscribe(ci.Config.FilterSubject, "", nats.Bind(config.Stream, config.Name))
	if err != nil {
		return nil, errors.Wrap(err, "could not subscribe")
	}

	logger := m.logger.With(zap.String("stream", config.Stream), zap.String("subscription", config.Name))
	logger.Debug("Created queue")

	ctx, cancel := context.WithCancel(ctx)

	q := &Queue{
		manager: m,
		logger:  logger,

		ctx:            ctx,
		ctxCancel:      cancel,
		shutdownSignal: make(chan struct{}, 1),

		subscription: sub,
		channel:      make(chan *Event),
		batchSize:    5,
	}

	if config.BatchSize > 0 {
		q.batchSize = config.BatchSize
	}

	go q.pump(ctx)
	return q, nil
}

// pump is a helper function that will pump messages from the NATS subscription
// into the channel.
func (q *Queue) pump(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			q.logger.Debug("Context done, stopping subscription")
			err := q.subscription.Unsubscribe()
			if err != nil {
				q.logger.Warn("failed to unsubscribe", zap.Error(err))
			}

			close(q.channel)
			q.shutdownSignal <- struct{}{}
			return
		}

		subCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		batch, err := q.subscription.FetchBatch(q.batchSize, nats.Context(subCtx))
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if errors.Is(err, nats.ErrBadSubscription) {
			q.logger.Debug("subscription closed, stopping")
			return
		} else if errors.Is(err, nats.ErrConnectionClosed) {
			q.logger.Debug("connection closed, stopping")
			return
		} else if err != nil {
			q.logger.Error("failed to fetch message", zap.Error(err))
			continue
		}

		// In some cases we may have received some events while the context was
		// being canceled. In this case, we reject the messages and stop the
		// goroutine
		if ctx.Err() != nil {
			q.logger.Debug("Context done, stopping subscription and rejecting messages")
			err = q.subscription.Unsubscribe()
			if err != nil {
				q.logger.Warn("Failed to unsubscribe", zap.Error(err))
			}

			for msg := range batch.Messages() {
				err = msg.Nak()
				if err != nil {
					q.logger.Warn("Failed to reject message", zap.Error(err))
				}
			}

			close(q.channel)
			q.shutdownSignal <- struct{}{}
			return
		}

		for msg := range batch.Messages() {
			msgCtx := q.manager.w3cPropagator.Extract(ctx, eventTracingHeaders{
				headers: &msg.Header,
			})
			msgCtx, span := q.manager.tracer.Start(
				msgCtx,
				msg.Subject+" receive", trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystem("nats"),
					semconv.MessagingOperationReceive,
					semconv.MessagingDestinationName(msg.Subject),
				),
			)

			// Get the metadata for the message
			md, err2 := msg.Metadata()
			if err2 != nil {
				q.logger.Error("failed to get message metadata", zap.Error(err2))
				span.RecordError(err2)
				span.SetStatus(codes.Error, "failed to get message metadata")
				span.End()
				continue
			}

			// Set the message ID as an attribute
			span.SetAttributes(semconv.MessagingMessageID(fmt.Sprintf("%d", md.Sequence.Stream)))

			// Create the event
			event, err2 := newEvent(msgCtx, span, q.logger, *msg, md)
			if err2 != nil {
				q.logger.Error("failed to create event", zap.Error(err2))
				span.RecordError(err2)

				err2 = msg.Term()
				if err2 != nil {
					q.logger.Warn("failed to terminate message", zap.Error(err2))
					span.RecordError(err2)
				}

				span.SetStatus(codes.Error, "failed to create event")
				span.End()
				continue
			}

			q.logger.Debug("Received event", zap.String("type", event.Data.TypeUrl))
			q.channel <- event
		}

		if batch.Error() != nil {
			// Handle the error if an error occurred while fetching messages in
			// the batch
			if errors.Is(err, nats.ErrBadSubscription) {
				q.logger.Debug("subscription closed, stopping")
				return
			} else if errors.Is(err, nats.ErrConnectionClosed) {
				q.logger.Debug("connection closed, stopping")
				return
			} else if err != nil {
				q.logger.Error("failed to fetch message", zap.Error(err))
				continue
			}
		}
	}
}

func (q *Queue) Close() error {
	if q.ctx.Err() != nil {
		return nil
	}

	q.ctxCancel()
	<-q.shutdownSignal
	return nil
}

func (q *Queue) Events() <-chan *Event {
	return q.channel
}
