package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DiscardPolicy controls the policy for discarding messages when the
// stream reaches its maximum size.
type DiscardPolicy int

const (
	// DiscardPolicyOld discards old messages when the stream reaches its
	// maximum size.
	DiscardPolicyOld DiscardPolicy = iota
	// DiscardPolicyNew discards new messages when the stream reaches its
	// maximum size.
	DiscardPolicyNew
)

// StorageType controls the type of storage to use for a stream.
type StorageType int

const (
	// StorageTypeFile uses file storage for the stream.
	StorageTypeFile StorageType = iota
	// StorageTypeMemory uses memory storage for the stream.
	StorageTypeMemory
)

// StreamSource defines a source for events in a stream. It is used for
// mirroring and sourcing events from other streams.
type StreamSource struct {
	// Name of the stream to mirror.
	Name string
	// From is the position in the stream to start mirroring from.
	From *StreamPointer
	// FilterSubjects is a list of subjects to filter messages from.
	FilterSubjects []string
}

// StreamConfig is the configuration for creating or updating a stream.
type StreamConfig struct {
	// Name of the stream.
	Name string

	// MaxAge is the maximum age of a message before it is considered stale.
	MaxAge time.Duration
	// MaxMsgs is the maximum number of messages to retain in the stream.
	MaxMsgs uint
	// MaxBytes is the maximum number of bytes to retain in the stream.
	MaxBytes uint
	// MaxMsgsPerSubject is the maximum number of messages to retain per subject.
	MaxMsgsPerSubject uint

	// DiscardPolicy controls the policy for discarding messages when the
	// stream reaches its maximum size.
	DiscardPolicy DiscardPolicy
	// DiscardNewPerSubject controls whether the discard policy applies to
	// each subject individually, or to the stream as a whole. Only used when
	// DiscardPolicy is set to DiscardPolicy.New.
	DiscardNewPerSubject bool

	// Subjects that the stream will receive events from.
	Subjects []string
	// Mirror makes it so this stream mirrors messages from another stream.
	Mirror *StreamSource
	// Sources is a list of streams to receive events from.
	Sources []*StreamSource

	// StorageType is the type of storage to use for the stream.
	StorageType StorageType
	// Replicas is the number of replicas to keep of the stream.
	Replicas *uint

	// DeduplicationWindow is the amount of time to keep idempotency keys.
	DeduplicationWindow *time.Duration
	// MaxEventSize is the maximum size of an event.
	MaxEventSize *uint
}

// Stream contains information about a stream.
type Stream struct{}

// EnsureStream ensures that a JetStream stream exists with the given configuration.
// If the stream already exists, it will be updated with the new configuration.
func (m *Manager) EnsureStream(ctx context.Context, config *StreamConfig) (*Stream, error) {
	_, span := m.tracer.Start(
		ctx,
		"windshift.events.EnsureStream",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.MessagingSystem("nats"),
			attribute.String("stream", config.Name),
		),
	)
	defer span.End()

	if !IsValidStreamName(config.Name) {
		span.SetStatus(codes.Error, "invalid stream name")
		return nil, newValidationError("invalid stream name: " + config.Name)
	}

	natsDiscardPolicy := jetstream.DiscardOld
	switch config.DiscardPolicy {
	case DiscardPolicyOld:
		natsDiscardPolicy = jetstream.DiscardOld
	case DiscardPolicyNew:
		natsDiscardPolicy = jetstream.DiscardNew
	}

	natsStorageType := jetstream.FileStorage
	switch config.StorageType {
	case StorageTypeFile:
		natsStorageType = jetstream.FileStorage
	case StorageTypeMemory:
		natsStorageType = jetstream.MemoryStorage
	}

	streamConfig := jetstream.StreamConfig{
		Name:     config.Name,
		Subjects: config.Subjects,

		MaxMsgs:           int64(config.MaxMsgs),
		MaxMsgsPerSubject: int64(config.MaxMsgsPerSubject),
		MaxBytes:          int64(config.MaxBytes),
		MaxAge:            config.MaxAge,

		Discard:              natsDiscardPolicy,
		DiscardNewPerSubject: config.DiscardNewPerSubject,

		Storage: natsStorageType,

		MaxConsumers: -1,
	}

	if config.Mirror != nil {
		mirror, err := toNatsStreamSource(config.Mirror)
		if err != nil {
			return nil, errors.Wrap(err, "mirror config invalid")
		}

		streamConfig.Mirror = mirror
	}

	if config.Sources != nil && len(config.Sources) > 0 {
		sources := make([]*jetstream.StreamSource, len(config.Sources))
		for i, source := range config.Sources {
			natsSource, err := toNatsStreamSource(source)
			if err != nil {
				return nil, errors.Wrap(err, "source config invalid")
			}

			sources[i] = natsSource
		}

		streamConfig.Sources = sources
	}

	if config.Replicas != nil {
		if *config.Replicas <= 0 {
			return nil, errors.New("replicas must be greater than 0")
		}

		streamConfig.Replicas = int(*config.Replicas)
	} else {
		// TODO: We may want to have a config value in the Windshift server that sets the default value
		streamConfig.Replicas = 1
	}

	if config.DeduplicationWindow != nil {
		streamConfig.Duplicates = *config.DeduplicationWindow
	} else {
		streamConfig.Duplicates = 2 * time.Minute
	}

	if config.MaxEventSize != nil && *config.MaxEventSize > 0 {
		streamConfig.MaxMsgSize = int32(*config.MaxEventSize)
	}

	_, err := m.js.Stream(ctx, config.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		// No stream with this name exists
		m.logger.Info(
			"Creating stream",
			zap.String("name", config.Name),
			zap.Object("config", (*ZapStreamConfig)(&streamConfig)),
		)

		_, err = m.js.CreateStream(ctx, streamConfig)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create JetStream stream")
		}

		return &Stream{}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to get JetStream stream info")
	}

	m.logger.Info(
		"Updating stream",
		zap.String("name", config.Name),
		zap.Object("config", (*ZapStreamConfig)(&streamConfig)),
	)
	_, err = m.js.UpdateStream(ctx, streamConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update JetStream stream")
	}

	return &Stream{}, nil
}

func toNatsStreamSource(source *StreamSource) (*jetstream.StreamSource, error) {
	res := &jetstream.StreamSource{
		Name: source.Name,
	}

	if source.FilterSubjects != nil && len(source.FilterSubjects) > 0 {
		if len(source.FilterSubjects) > 1 {
			return nil, errors.New("only one filter subject can be specified")
		}

		res.FilterSubject = source.FilterSubjects[0]
	}

	if source.From != nil {
		// Pointer has been specified, move away from the default of receiving
		// all events stored in this stream.
		if source.From.ID != 0 {
			res.OptStartSeq = source.From.ID
		} else if !source.From.Time.IsZero() {
			res.OptStartTime = &source.From.Time
		} else if !source.From.First {
			now := time.Now()
			res.OptStartTime = &now
		}
	}

	return res, nil
}

type ZapStreamConfig jetstream.StreamConfig

func (c *ZapStreamConfig) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	err := enc.AddArray("subjects", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
		for _, subject := range c.Subjects {
			enc.AppendString(subject)
		}
		return nil
	}))
	if err != nil {
		return err
	}

	if c.Mirror != nil {
		err = enc.AddObject("mirror", (*ZapStreamSource)(c.Mirror))
		if err != nil {
			return err
		}
	}

	if c.Sources != nil && len(c.Sources) > 0 {
		err = enc.AddArray("sources", zapcore.ArrayMarshalerFunc(func(enc zapcore.ArrayEncoder) error {
			for _, source := range c.Sources {
				err2 := enc.AppendObject((*ZapStreamSource)(source))
				if err2 != nil {
					return err2
				}
			}

			return nil
		}))
		if err != nil {
			return err
		}
	}

	if c.MaxAge != 0 {
		enc.AddDuration("maxAge", c.MaxAge)
	}

	if c.MaxMsgs != 0 {
		enc.AddInt64("maxMsgs", c.MaxMsgs)
	}

	if c.MaxMsgsPerSubject != 0 {
		enc.AddInt64("maxMsgsPerSubject", c.MaxMsgsPerSubject)
	}

	if c.MaxBytes != 0 {
		enc.AddInt64("maxBytes", c.MaxBytes)
	}

	switch c.Discard {
	case jetstream.DiscardOld:
		enc.AddString("discard", "old")
	case jetstream.DiscardNew:
		enc.AddString("discard", "new")
	}

	enc.AddBool("discardNewPerSubject", c.DiscardNewPerSubject)

	switch c.Storage {
	case jetstream.FileStorage:
		enc.AddString("storage", "file")
	case jetstream.MemoryStorage:
		enc.AddString("storage", "memory")
	}

	if c.Replicas != 0 {
		enc.AddInt("replicas", c.Replicas)
	}

	if c.Duplicates != 0 {
		enc.AddDuration("duplicates", c.Duplicates)
	}

	return nil
}

type ZapStreamSource jetstream.StreamSource

func (s *ZapStreamSource) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("name", s.Name)

	if s.FilterSubject != "" {
		enc.AddString("filterSubject", s.FilterSubject)
	}

	if s.OptStartSeq != 0 {
		enc.AddUint64("startSeq", s.OptStartSeq)
	}

	if s.OptStartTime != nil {
		enc.AddTime("startTime", *s.OptStartTime)
	}

	return nil
}
