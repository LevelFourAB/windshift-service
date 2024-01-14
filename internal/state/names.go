package state

import "windshift/service/internal/events"

// IsValidStoreName checks if the store name is valid.
func IsValidStoreName(name string) bool {
	return events.IsValidStreamName(name)
}

// IsValidKey checks if the key name is valid. The NATS documentation says
// that keys follow the same rules as subjects so we delegate to events.IsValidSubject.
func IsValidKey(name string) bool {
	return events.IsValidSubject(name, false)
}
