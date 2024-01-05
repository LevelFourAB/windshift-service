package state

import "errors"

// ErrStoreRequired is returned when a store is not provided.
var ErrStoreRequired = errors.New("store required")

// ErrStoreNotFound is returned when a store is not found.
var ErrStoreNotFound = errors.New("store not found")

// ErrKeyRequired is returned when a key is not provided.
var ErrKeyRequired = errors.New("missing key")

// ErrKeyNotFound is returned when a key is not found in a store.
var ErrKeyNotFound = errors.New("key not found")

// ErrKeyAlreadyExists is returned when a key already exists in a store and
// cannot be created.
var ErrKeyAlreadyExists = errors.New("key already exists")

// ErrRevisionMismatch is returned when a revision does not match the current
// revision of a key.
var ErrRevisionMismatch = errors.New("revision mismatch")
