package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type QueueConfig struct {
	Stream string
	Name   string

	Concurrency int
}

type Queue struct {
	logger *zap.Logger

	jetStream nats.JetStreamContext

	ctx            context.Context
	ctxCancel      context.CancelFunc
	shutdownSignal chan struct{}

	subscription *nats.Subscription
	channel      chan *Event

	concurrency int
}

func newQueue(ctx context.Context, logger *zap.Logger, js nats.JetStreamContext, config *QueueConfig) (*Queue, error) {
	if config.Stream == "" {
		return nil, errors.New("name of stream must be specified")
	}

	if config.Name == "" {
		return nil, errors.New("name of subscription must be specified")
	}

	ci, err := js.ConsumerInfo(config.Stream, config.Name, nats.Context(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "failed to get consumer info")
	}

	sub, err := js.PullSubscribe(ci.Config.FilterSubject, "", nats.Bind(config.Stream, config.Name))
	if err != nil {
		return nil, errors.Wrap(err, "could not subscribe")
	}

	logger = logger.With(zap.String("stream", config.Stream), zap.String("subscription", config.Name))
	logger.Debug("Created queue")

	ctx, cancel := context.WithCancel(ctx)

	q := &Queue{
		logger:    logger,
		jetStream: js,

		ctx:            ctx,
		ctxCancel:      cancel,
		shutdownSignal: make(chan struct{}, 1),

		subscription: sub,
		channel:      make(chan *Event),
		concurrency:  5,
	}

	if config.Concurrency > 0 {
		q.concurrency = config.Concurrency
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
		msgs, err := q.subscription.Fetch(q.concurrency, nats.Context(subCtx))
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			continue
		} else if errors.Is(err, nats.ErrTimeout) {
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
			q.logger.Debug("Context done, stopping subscription and rejecting messages", zap.Int("messageCount", len(msgs)))
			err := q.subscription.Unsubscribe()
			if err != nil {
				q.logger.Warn("Failed to unsubscribe", zap.Error(err))
			}

			for _, msg := range msgs {
				err = msg.Nak()
				if err != nil {
					q.logger.Warn("Failed to reject message", zap.Error(err))
				}
			}

			close(q.channel)
			q.shutdownSignal <- struct{}{}
			return
		}

		for _, msg := range msgs {
			event, err := newEvent(q.logger, *msg)
			if err != nil {
				q.logger.Error("failed to create event", zap.Error(err))
				continue
			}

			q.logger.Debug("Received event", zap.String("type", event.Data.TypeUrl))
			q.channel <- event
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
