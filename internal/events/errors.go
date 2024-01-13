package events

import "github.com/cockroachdb/errors"

// ErrInvalidStreamName is used when the stream name is invalid.
var ErrInvalidStreamName = errors.New("invalid stream name")

// ErrInvalidConsumerName is used when the consumer name is invalid.
var ErrInvalidConsumerName = errors.New("invalid consumer name")

// ErrInvalidSubject is used when the subject is invalid.
var ErrInvalidSubject = errors.New("invalid subject")

// ErrInvalidData is used when the data is invalid.
var ErrInvalidData = errors.New("invalid data")

// ErrPublishTimeout is used when publishing of an event does not meet the deadline.
var ErrPublishTimeout = errors.New("publish timeout")

// ErrUnboundSubject is used when a subject is not bound to a stream.
var ErrUnboundSubject = errors.New("unbound subject, no stream found for subject")

// ErrNoStreamResponse is used when a stream response is expected but none is received.
// This is occurs when publishing events to a stream that does not exist.
var ErrNoStreamResponse = errors.New("no stream response")

// ErrWrongSequence is used when the expected sequence number does not match the
// actual sequence number.
var ErrWrongSequence = errors.New("wrong sequence")
