package flowcontrol

import (
	"math"
	"sync/atomic"
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
// As we serve things over gRPC streams we don't have full control over if the
// consumer buffers events. So we use the rate of processing events to
// determine if we need to increase or decrease the batch size.
//
// We use a MIMD style algorithm to control the batch size. The idea is that
// we want to increase the batch size if the consumer is processing events
// quickly, and decrease the batch size if the consumer is slow.
type FlowControl struct {
	// MinBatchSize is the minimum batch size. We will never go below this value.
	MinBatchSize int

	// MaxBatchSize is the maximum batch size. We will never go above this value.
	MaxBatchSize int

	// currentBatchSize is the currentBatchSize batch size.
	currentBatchSize int

	// lastBatchSizeRequest is when the batch size was requested.
	lastBatchSizeRequest time.Time

	// eventsProcessedInLastInterval is the number of events processed since
	// the last batch size request.
	eventsProcessedInLastInterval int

	// eventsBlockedInLastInterval is the number of events blocked since the
	// last batch size request.
	eventsBlockedInLastInterval int

	semaphore *DynamicSemaphore
}

// NewFlowControl creates a new flow control.
func NewFlowControl(
	minBatchSize int,
	maxBatchSize int,
) *FlowControl {
	return &FlowControl{
		MinBatchSize: minBatchSize,
		MaxBatchSize: maxBatchSize,

		currentBatchSize:     minBatchSize,
		lastBatchSizeRequest: time.Now(),

		semaphore: NewDynamicSemaphore(minBatchSize * 2),
	}
}

// GetBatchSize returns the batch size to use for the next fetch.
func (fc *FlowControl) GetBatchSize() int {
	if fc.eventsProcessedInLastInterval == 0 {
		// Didn't process any events in the last batch, so we don't know if we
		// need to increase or decrease the batch size. We just return the
		// current batch size.
		fc.lastBatchSizeRequest = time.Now()
		return fc.currentBatchSize
	}

	// If we had no blocking events and we processed at least 90% of the events
	// in the last interval, we increase the batch size.
	if fc.eventsBlockedInLastInterval == 0 && fc.eventsProcessedInLastInterval >= int(float64(fc.currentBatchSize)*0.9) {
		amountToIncrease := int(math.Max(float64(fc.currentBatchSize)*0.1, 1))
		fc.currentBatchSize += amountToIncrease

		if fc.currentBatchSize > fc.MaxBatchSize {
			fc.currentBatchSize = fc.MaxBatchSize
		}
	} else {
		// If we didn't process all the events within the target interval, we
		// decrease the batch size.
		amountToDecrease := int(math.Max(float64(fc.currentBatchSize)*0.15, 1))
		fc.currentBatchSize -= amountToDecrease

		if fc.currentBatchSize < fc.MinBatchSize {
			fc.currentBatchSize = fc.MinBatchSize
		}
	}

	fc.lastBatchSizeRequest = time.Now()
	fc.eventsProcessedInLastInterval = 0
	fc.eventsBlockedInLastInterval = 0
	fc.adjustSemaphoreLimit()
	return fc.currentBatchSize
}

func (fc *FlowControl) adjustSemaphoreLimit() {
	// We adjust the limit of the semaphore so smaller batch sizes have extra
	// permits. This is to make sure that we don't block the consumer if it
	// buffers events.
	if fc.currentBatchSize < 10 {
		fc.semaphore.SetLimit(fc.currentBatchSize * 2)
	} else {
		fc.semaphore.SetLimit(fc.currentBatchSize + 10)
	}
}

// SentEvent is called when the consumer has sent an event to the consumer.
func (fc *FlowControl) AcquireSendLock() func() {
	release := fc.semaphore.TryAcquire()
	if release == nil {
		// Couldn't acquire a permit, record this and then block until we can
		// acquire a permit.
		release = fc.semaphore.Acquire()
	}

	released := uint64(0)
	return func() {
		if atomic.CompareAndSwapUint64(&released, 0, 1) {
			fc.eventsProcessedInLastInterval++
			release()
		}
	}
}

// SendLock returns a channel that will return a function that will release the
// send lock.
func (fc *FlowControl) SendLock() <-chan func() {
	ch := make(chan func(), 1)
	go func() {
		release := fc.AcquireSendLock()
		ch <- release
	}()
	return ch
}
