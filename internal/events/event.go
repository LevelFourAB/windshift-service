package events

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Event represents a single event from the event queue. It is received via
// NATS and should be processed within a certain deadline, using Accept() or
// Reject(shouldRetry) to acknowledge the event. If the deadline is exceeded,
// the event will be redelivered. To extend the deadline, use Ping().
type Event struct {
	logger *zap.Logger
	msg    *nats.Msg

	// SubscriptionSeq is the sequence number of the event in the queue.
	SubscriptionSeq uint64

	// StreamSeq is the sequence number of the event in the event stream. Can
	// be used for resuming from a certain point in time. For example with an
	// ephemeral queue, the consumer can store the last seen StreamSeq and
	// resume from there on the next run.
	StreamSeq uint64

	// PublishedAt is the time the event was published by the producer.
	PublishedAt time.Time

	// Data is the protobuf message published by the producer.
	Data *anypb.Any
}

func newEvent(logger *zap.Logger, msg nats.Msg) (*Event, error) {
	md, err := msg.Metadata()
	if err != nil {
		return nil, errors.Wrap(err, "could not get message metadata")
	}

	// Unmarshal protobuf message from msg.Data
	data := &anypb.Any{}
	err = proto.Unmarshal(msg.Data, data)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal message")
	}

	// Get the published header
	publishedTime := md.Timestamp
	header := msg.Header.Get("Published-Time")
	if header != "" {
		publishedTime, err = time.Parse(time.RFC3339Nano, header)
		if err != nil {
			return nil, errors.Wrap(err, "could not parse header")
		}
	}

	// Clear the data of the message
	msg.Data = nil

	return &Event{
		logger:          logger,
		msg:             &msg,
		SubscriptionSeq: md.Sequence.Stream,
		StreamSeq:       md.Sequence.Consumer,
		PublishedAt:     publishedTime,
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
		return errors.Wrap(err, "could not ping message")
	}

	return nil
}

// Accept accepts the event. The event will be removed from the queue.
func (e *Event) Accept() error {
	e.logger.Debug("Accepting event", zap.Uint64("streamSeq", e.StreamSeq))
	err := e.msg.Ack()
	if err != nil {
		return errors.Wrap(err, "could not accept message")
	}

	return nil
}

// Reject rejects the event. If allowRedeliver is true, the event will be
// redelivered after a certain amount of time. If false, the event will be
// permanently rejected and not redelivered.
func (e *Event) Reject(allowRedeliver bool) error {
	if allowRedeliver {
		e.logger.Debug("Rejecting event", zap.Uint64("streamSeq", e.StreamSeq))
		err := e.msg.Nak()
		if err != nil {
			return errors.Wrap(err, "could not reject message")
		}

		return nil
	}

	e.logger.Debug("Permanently rejecting event", zap.Uint64("streamSeq", e.StreamSeq))
	err := e.msg.Term()
	if err != nil {
		return errors.Wrap(err, "could not permanently reject message")
	}

	return nil
}
