package events

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

type StreamConfig struct {
	// The name of the stream.
	Name string

	// The subjects that will be published to this stream.
	Subjects []string

	// The maximum age of a message before it is discarded.
	MaxAge time.Duration
	// The maximum number of messages to retain in the stream.
	MaxMsgs uint
	// The maximum number of bytes to retain in the stream.
	MaxBytes uint
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
