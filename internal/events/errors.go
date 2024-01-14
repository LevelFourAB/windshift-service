package events

import "github.com/cockroachdb/errors"

// ErrPublishTimeout is used when publishing of an event does not meet the deadline.
var ErrPublishTimeout = errors.New("publish timeout")

// ErrUnboundSubject is used when a subject is not bound to a stream.
var ErrUnboundSubject = errors.New("unbound subject, no stream found for subject")

// ErrWrongSequence is used when the expected sequence number does not match the
// actual sequence number.
var ErrWrongSequence = errors.New("wrong sequence")

type validationError struct {
	err string
}

func (e *validationError) Error() string {
	return e.err
}

func newValidationError(err string) error {
	return &validationError{err: err}
}

func IsValidationError(err error) bool {
	_, ok := err.(*validationError)
	return ok
}
