package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

type DiscardPolicy int

const (
	DiscardOld DiscardPolicy = iota
	DiscardNew
)

type StorageType int

const (
	FileStorage StorageType = iota
	MemoryStorage
)

type StreamSource struct {
	// Name of the stream to mirror.
	Name string
	// Pointer is the position in the stream to start mirroring from.
	Pointer *StreamPointer
	// FilterSubjects is a list of subjects to filter messages from.
	FilterSubjects []string
}

type StreamConfig struct {
	// Name of the stream.
	Name string

	// MaxAge is the maximum age of a message before it is considered stale.
	MaxAge time.Duration
	// MaxMsgs is the maximum number of messages to retain in the stream.
	MaxMsgs uint
	// MaxBytes is the maximum number of bytes to retain in the stream.
	MaxBytes uint

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

type Stream struct{}

// EnsureStream ensures that a JetStream stream exists with the given configuration.
func EnsureStream(_ context.Context, js nats.JetStreamContext, config *StreamConfig) (*Stream, error) {
	streamConfig := &nats.StreamConfig{
		Name:     config.Name,
		Subjects: config.Subjects,

		MaxMsgs:  int64(config.MaxMsgs),
		MaxBytes: int64(config.MaxBytes),
		MaxAge:   config.MaxAge,

		Discard:              nats.DiscardPolicy(config.DiscardPolicy),
		DiscardNewPerSubject: config.DiscardNewPerSubject,

		Storage: nats.StorageType(config.StorageType),

		MaxConsumers: -1,
	}

	if config.Replicas != nil && *config.Replicas == 0 {
		streamConfig.Replicas = int(*config.Replicas)
	} else {
		// TODO: We may want to have a config value in the Windshift server that sets the default value
		streamConfig.Replicas = 1
	}

	if config.DeduplicationWindow != nil {
		streamConfig.Duplicates = *config.DeduplicationWindow
	} else {
		streamConfig.Duplicates = 1 * time.Minute
	}

	if config.MaxEventSize != nil && *config.MaxEventSize > 0 {
		streamConfig.MaxMsgSize = int32(*config.MaxEventSize)
	}

	_, err := js.StreamInfo(config.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		// No stream with this name exists
		_, err = js.AddStream(streamConfig)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create JetStream stream")
		}

		return &Stream{}, nil
	} else if err != nil {
		return nil, errors.Wrap(err, "failed to get JetStream stream info")
	}

	_, err = js.UpdateStream(streamConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update JetStream stream")
	}

	return &Stream{}, nil
}
