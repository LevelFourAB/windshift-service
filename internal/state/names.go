package state

import "github.com/levelfourab/windshift-server/internal/events"

// IsValidStoreName checks if the store name is valid.
func IsValidStoreName(name string) bool {
	return events.IsValidStreamName(name)
}

// IsValidKey checks if the key name is valid. The NATS documentation says
// that keys follow the same rules as subjects so we delegate to events.IsValidSubject.
func IsValidKey(name string) bool {
	return events.IsValidSubject(name, false)
}
