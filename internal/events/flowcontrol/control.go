package flowcontrol

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type ProcessType int

const (
	ProcessTypeAck ProcessType = iota
	ProcessTypeReject
	ProcessTypePermanentReject
	ProcessTypePing
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
// As we serve things over gRPC streams we don't have full control over if the
// consumer buffers events. So we control how many pending events a consumer
// can process at a time.
//
// We use a MIMD style algorithm to control the maximum amount of events being
// processed. The idea is that we want to increase the number of events if the
// consumer is processing events normally, and decrease the events if the
// consumer is slow or rejecting events.
type FlowControl struct {
	// maxPendingEvents is the maximum number of events that can be processed
	// at a time.
	maxPendingEvents int

	// currentProcessingLimit is the number of events that can currently be
	// processed at a time.
	currentProcessingLimit int64

	eventsAckedInLastInterval    int64
	eventsRejectedInLastInterval int64

	tracker *tracker
}

// NewFlowControl creates a new flow control.
func NewFlowControl(
	ctx context.Context,
	logger *zap.Logger,
	eventTimeout time.Duration,
	maxPendingEvents int,
) *FlowControl {
	fc := &FlowControl{
		maxPendingEvents:       maxPendingEvents,
		currentProcessingLimit: 1,
		tracker:                newTracker(logger),
	}

	go func() {
		eventTimeoutTicker := time.NewTicker(eventTimeout / 4)
		reevaulationTicker := time.NewTicker(100 * time.Millisecond)
		defer eventTimeoutTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				logger.Debug("Context done, destroying flow control")
				fc.tracker.Destroy()
				return
			case <-eventTimeoutTicker.C:
				// Remove old events that have timed out and count them as
				// rejected.
				removalCount := fc.tracker.RemoveOlderThan(eventTimeout)
				atomic.AddInt64(&fc.eventsRejectedInLastInterval, int64(removalCount))
			case <-reevaulationTicker.C:
				// Reevaluate the processing limit.
				fc.updateProcessingLimit()
			}
		}
	}()

	return fc
}

// GetBatchSize returns the number of events to load from NATS. This will
// adjust the processing limit based on the flow of events.
//
// This operation will block until we are processing less events than the
// limit.
func (fc *FlowControl) GetBatchSize() int {
	//fc.updateProcessingLimit()
	currentLimit := int(atomic.LoadInt64(&fc.currentProcessingLimit))
	fc.tracker.WaitUntilLessThan(currentLimit)
	return currentLimit - fc.tracker.Count()
}

func (fc *FlowControl) updateProcessingLimit() {
	acked := atomic.SwapInt64(&fc.eventsAckedInLastInterval, 0)
	rejected := atomic.SwapInt64(&fc.eventsRejectedInLastInterval, 0)

	currentLimit := atomic.LoadInt64(&fc.currentProcessingLimit)
	if rejected > 0 {
		// If we have rejected events, we treat this as a sign that the consumer
		// is overwhelmed and we should decrease how many events it can process
		// at a time.
		amountToDecrease := int64(float32(fc.currentProcessingLimit) * 0.15)
		if amountToDecrease == 0 {
			amountToDecrease = 1
		}

		currentLimit -= amountToDecrease

		if currentLimit < 1 {
			currentLimit = 1
		}
	} else if acked > 0 {
		// If a consumer has acknowledged events, we try to increase the number
		// of events it can process at a time.
		amountToIncrease := int64(float32(fc.currentProcessingLimit) * 0.1)
		if amountToIncrease == 0 {
			amountToIncrease = 1
		}

		currentLimit += amountToIncrease

		if currentLimit > int64(fc.maxPendingEvents) {
			currentLimit = int64(fc.maxPendingEvents)
		}
	}

	atomic.StoreInt64(&fc.currentProcessingLimit, currentLimit)
}

// Received is called when an event is received from NATS. It returns a function
// that should be called when the event is processed, such as acknowledging,
// rejecting or pinging it.
func (fc *FlowControl) Received(id uint64) func(processType ProcessType) {
	fc.tracker.Add(id)
	released := uint64(0)
	return func(processType ProcessType) {
		if processType == ProcessTypePing {
			fc.tracker.Ping(id)
			return
		}

		if !atomic.CompareAndSwapUint64(&released, 0, 1) {
			// The event has already been processed, so don't track it
			return
		}

		fc.tracker.Remove(id)
		if processType == ProcessTypeReject {
			// If an event is rejected we want to have that information to
			// be able to adjust the processing limit.
			atomic.AddInt64(&fc.eventsRejectedInLastInterval, 1)
		} else if processType == ProcessTypeAck {
			// This event was processed successfully, track it so we can
			// adjust the processing limit.
			atomic.AddInt64(&fc.eventsAckedInLastInterval, 1)
		}
	}
}
