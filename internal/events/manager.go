package events

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Manager struct {
	logger    *zap.Logger
	jetStream nats.JetStreamContext
}

func NewManager(
	logger *zap.Logger,
	conn *nats.Conn,
) (*Manager, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create JetStream channel")
	}

	m := &Manager{
		logger:    logger,
		jetStream: js,
	}

	return m, nil
}

func (n *Manager) EnsureStream(ctx context.Context, config *StreamConfig) (*Stream, error) {
	return EnsureStream(ctx, n.jetStream, config)
}

func (n *Manager) EnsureSubscription(ctx context.Context, config *ConsumerConfig) (*Consumer, error) {
	return EnsureConsumer(ctx, n.jetStream, config)
}

// Publish an event.
func (n *Manager) Publish(ctx context.Context, config *PublishConfig) (*PublishedEvent, error) {
	return Publish(ctx, n.jetStream, config)
}

func (n *Manager) Subscribe(ctx context.Context, config *QueueConfig) (*Queue, error) {
	return newQueue(ctx, n.logger, n.jetStream, config)
}
