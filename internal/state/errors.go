package state

import "errors"

// ErrStoreNotFound is returned when a store is not found.
var ErrStoreNotFound = &validationError{err: "store not found"}

// ErrKeyNotFound is returned when a key is not found in a store.
var ErrKeyNotFound = errors.New("key not found")

// ErrKeyAlreadyExists is returned when a key already exists in a store and
// cannot be created.
var ErrKeyAlreadyExists = errors.New("key already exists")

// ErrRevisionMismatch is returned when a revision does not match the current
// revision of a key.
var ErrRevisionMismatch = errors.New("revision mismatch")

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
