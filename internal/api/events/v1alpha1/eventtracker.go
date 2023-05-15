package v1alpha1

import (
	"time"
	"windshift/service/internal/events"
)

// eventTrackerEntry contains an event and the time it was added to the tracker.
type eventTrackerEntry struct {
	addedAt time.Time
	event   *events.Event
}

// eventTracker is a map of stream sequence numbers to events. It is used in
// Consume handler to track events sent to the client that have not yet been
// acknowledged or rejected.
type eventTracker map[uint64]*eventTrackerEntry

func newEventTracker() eventTracker {
	return make(map[uint64]*eventTrackerEntry)
}

// Add an event to be tracked.
func (e eventTracker) Add(event *events.Event) {
	e[event.StreamSeq] = &eventTrackerEntry{
		addedAt: time.Now(),
		event:   event,
	}
}

func (e eventTracker) MarkPinged(streamSeq uint64) {
	if entry, ok := e[streamSeq]; ok {
		entry.addedAt = time.Now()
	}
}

// Get an event that is being tracked.
func (e eventTracker) Get(streamSeq uint64) *events.Event {
	if entry, ok := e[streamSeq]; ok {
		return entry.event
	}

	return nil
}

// Remove an event from being tracked.
func (e eventTracker) Remove(streamSeq uint64) {
	delete(e, streamSeq)
}

// RemoveOlderThan removes all events that were added to the tracker before the
// given time.
func (e eventTracker) RemoveOlderThan(t time.Duration) {
	threshold := time.Now().Add(-t)

	for streamSeq, entry := range e {
		if entry.addedAt.Before(threshold) {
			delete(e, streamSeq)
		}
	}
}
