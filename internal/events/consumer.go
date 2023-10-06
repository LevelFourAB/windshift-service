package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type StreamPointer struct {
	ID    uint64
	Time  time.Time
	First bool
}

type ConsumerConfig struct {
	Name     string
	Stream   string
	Subjects []string

	Timeout time.Duration

	MaxDeliveryAttempts uint

	Pointer *StreamPointer
}

type Consumer struct {
	ID string
}

func (m *Manager) EnsureConsumer(ctx context.Context, config *ConsumerConfig) (*Consumer, error) {
	ctx, span := m.tracer.Start(
		ctx,
		"windshift.events.EnsureConsumer",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.MessagingSystem("nats"),
			attribute.String("stream", config.Stream),
		),
	)
	defer span.End()

	if config.Stream == "" {
		return nil, errors.New("stream must be specified")
	}

	if len(config.Subjects) == 0 {
		return nil, errors.New("one or more subjects must be specified")
	}

	var name string
	var err error
	if config.Name == "" {
		// If the name is not specified, we create an ephemeral consumer
		span.SetAttributes(attribute.String("type", "ephemeral"))

		name, err = m.declareEphemeralConsumer(ctx, config)
		if err != nil {
			return nil, err
		}

		// Update the span with the generated name of the ephemeral consumer
		span.SetAttributes(attribute.String("name", name))
	} else {
		// If the name is specified, we create a durable consumer
		span.SetAttributes(
			attribute.String("type", "durable"),
			attribute.String("name", config.Name),
		)

		name, err = m.declareDurableConsumer(ctx, config)
		if err != nil {
			return nil, err
		}
	}

	return &Consumer{
		ID: name,
	}, nil
}

func (m *Manager) declareEphemeralConsumer(ctx context.Context, config *ConsumerConfig) (string, error) {
	consumerConfig := &jetstream.ConsumerConfig{
		Name:              uuid.New().String(),
		InactiveThreshold: 1 * time.Hour,
	}

	m.setConsumerSettings(consumerConfig, config, false)
	m.logger.Info(
		"Creating ephemeral consumer",
		zap.String("stream", config.Stream),
		zap.Object("config", (*ZapConsumerConfig)(consumerConfig)),
	)

	_, err := m.js.CreateOrUpdateConsumer(ctx, config.Stream, *consumerConfig)
	if err != nil {
		return "", errors.Wrap(err, "could not create consumer")
	}
	return consumerConfig.Name, nil
}

func (m *Manager) declareDurableConsumer(ctx context.Context, config *ConsumerConfig) (string, error) {
	c, err := m.js.Consumer(ctx, config.Stream, config.Name)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			m.logger.Info(
				"Creating durable consumer",
				zap.String("stream", config.Stream),
				zap.String("name", config.Name),
			)

			// Consumer does not exist, create it
			consumerConfig := &jetstream.ConsumerConfig{
				Durable:           config.Name,
				InactiveThreshold: 30 * 24 * time.Hour,
			}

			m.setConsumerSettings(consumerConfig, config, false)

			_, err = m.js.CreateOrUpdateConsumer(ctx, config.Stream, *consumerConfig)
			if err != nil {
				return "", errors.Wrap(err, "could not create consumer")
			}
			return config.Name, nil
		}

		return "", errors.Wrap(err, "could not get consumer info")
	}

	// For updates certain fields can not be set, so we only set what we can
	consumerConfig := c.CachedInfo().Config
	m.setConsumerSettings(&consumerConfig, config, true)

	// Perform the update
	m.logger.Info(
		"Updating durable consumer",
		zap.String("stream", config.Stream),
		zap.String("name", config.Name),
		zap.Object("config", (*ZapConsumerConfig)(&consumerConfig)),
	)
	_, err = m.js.CreateOrUpdateConsumer(ctx, config.Stream, consumerConfig)
	if err != nil {
		return "", errors.Wrap(err, "could not update consumer")
	}
	return config.Name, nil
}

// setConsumerSettings sets the shared settings for both ephemeral and durable
// consumers.
func (m *Manager) setConsumerSettings(c *jetstream.ConsumerConfig, qc *ConsumerConfig, update bool) {
	c.AckPolicy = jetstream.AckExplicitPolicy
	if len(qc.Subjects) == 1 {
		c.FilterSubject = qc.Subjects[0]
	} else {
		c.FilterSubject = ""
		c.FilterSubjects = qc.Subjects
	}

	// If a timeout is specified set it or use the default
	if qc.Timeout > 0 {
		c.AckWait = qc.Timeout
	} else {
		c.AckWait = 30 * time.Second
	}

	// If the max delivery attempts is specified set it
	if qc.MaxDeliveryAttempts > 0 {
		c.MaxDeliver = int(qc.MaxDeliveryAttempts)
	}

	if !update {
		// When creating a consumer we can specify where to start from
		c.DeliverPolicy = jetstream.DeliverNewPolicy
		if qc.Pointer != nil {
			if !qc.Pointer.Time.IsZero() {
				c.DeliverPolicy = jetstream.DeliverByStartTimePolicy
				c.OptStartTime = &qc.Pointer.Time
			} else if qc.Pointer.ID > 0 {
				c.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				c.OptStartSeq = qc.Pointer.ID
			} else if qc.Pointer.First {
				c.DeliverPolicy = jetstream.DeliverAllPolicy
			}
		}
	}
}

type ZapConsumerConfig jetstream.ConsumerConfig

func (c *ZapConsumerConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	err := enc.AddArray("subjects", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
		if c.FilterSubjects == nil {
			enc.AppendString(c.FilterSubject)
		} else {
			for _, subject := range c.FilterSubjects {
				enc.AppendString(subject)
			}
		}
		return nil
	}))
	if err != nil {
		return err
	}

	enc.AddDuration("ackWait", c.AckWait)

	if c.MaxDeliver > 0 {
		enc.AddInt("maxDeliver", c.MaxDeliver)
	}

	switch c.DeliverPolicy {
	case jetstream.DeliverAllPolicy:
		enc.AddString("deliverPolicy", "all")
	case jetstream.DeliverNewPolicy:
		enc.AddString("deliverPolicy", "new")
	case jetstream.DeliverByStartSequencePolicy:
		enc.AddString("deliverPolicy", "byStartSequence")
		enc.AddUint64("startSequence", c.OptStartSeq)
	case jetstream.DeliverByStartTimePolicy:
		enc.AddString("deliverPolicy", "byStartTime")
		enc.AddTime("startTime", *c.OptStartTime)
	case jetstream.DeliverLastPolicy:
		enc.AddString("deliverPolicy", "last")
	case jetstream.DeliverLastPerSubjectPolicy:
		enc.AddString("deliverPolicy", "lastPerSubject")
	}

	return nil
}
