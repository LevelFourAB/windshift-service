package flowcontrol

import (
	"math"
	"time"
)

// FlowControl helps with controlling the flow of events. We provide a channel
// based API for consumers, but pulling from NATS is done in batches. To
// maintain a steady flow of events, we need to control the batch size and
// how many events are sent to the consumer.
//
// There are two goals in this:
//
// 1. Make sure that the consumer does not have to wait too long for events.
// 2. Make sure that the consumer is not overwhelmed with events.
//
// As we use a channel we can assume that the consumer is able to process
// events when they are pulled from the channel, and sending to the channel
// will block if the consumer is not processing events.
//
// So we simplify the problem to just controlling the batch size. We use an
// AIMD style algorithm to control the batch size. The idea is that we want
// to increase the batch size if the consumer is processing events quickly,
// and decrease the batch size if the consumer is slow.
//
// In this implementation we set a target fetch interval, with the goal of
// fetching a batch of events at this interval. When a batch size is requested
// we increase the size if the time since the last request is shorter than the
// target interval, and decrease the size if the time since the last request
// is longer than the target interval.
type FlowControl struct {
	// TargetFetchInterval is the target time between fetches from NATS. This
	// is used to calculate the batch size.
	TargetFetchInterval time.Duration

	// MinBatchSize is the minimum batch size. We will never go below this value.
	MinBatchSize int

	// MaxBatchSize is the maximum batch size. We will never go above this value.
	MaxBatchSize int

	// currentBatchSize is the currentBatchSize batch size.
	currentBatchSize int

	// lastBatchSizeRequest is when the batch size was requested.
	lastBatchSizeRequest time.Time

	// eventsHandledInLastBatch is the number of events handled in the last
	// batch. Helps determining if NATS returned less events than requested.
	eventsHandledInLastBatch int
}

// NewFlowControl creates a new flow control.
func NewFlowControl(
	targetFetchInterval time.Duration,
	minBatchSize int,
	maxBatchSize int,
) *FlowControl {
	return &FlowControl{
		TargetFetchInterval: targetFetchInterval,
		MinBatchSize:        minBatchSize,
		MaxBatchSize:        maxBatchSize,

		currentBatchSize:     minBatchSize,
		lastBatchSizeRequest: time.Now(),
	}
}

// GetBatchSize returns the batch size to use for the next fetch.
func (fc *FlowControl) GetBatchSize() int {
	if fc.eventsHandledInLastBatch < fc.currentBatchSize {
		// Less events were handled in the last batch than requested to be
		// fetched, this means the server returned less events than requested,
		// it's likely that we are at the end of the queue, so we don't want
		// to increase the batch size.
		return fc.currentBatchSize
	}

	// If the time since the last batch size request is shorter than the fetch
	// target interval, we need to increase the batch size.
	timeSinceLastRequest := time.Since(fc.lastBatchSizeRequest)
	if timeSinceLastRequest < fc.TargetFetchInterval {
		fc.currentBatchSize += 10

		if fc.currentBatchSize > fc.MaxBatchSize {
			fc.currentBatchSize = fc.MaxBatchSize
		}
	} else {
		// If we didn't process all the events within the target interval, we
		// decrease the batch size.
		amountToDecrease := int(math.Max(float64(fc.currentBatchSize)*0.2, 1))
		fc.currentBatchSize -= amountToDecrease

		if fc.currentBatchSize < fc.MinBatchSize {
			fc.currentBatchSize = fc.MinBatchSize
		}
	}

	fc.lastBatchSizeRequest = time.Now()
	fc.eventsHandledInLastBatch = 0
	return fc.currentBatchSize
}

// SentEvent is called when the consumer has sent an event to the consumer.
func (fc *FlowControl) SentEvent() {
	fc.eventsHandledInLastBatch++
}
