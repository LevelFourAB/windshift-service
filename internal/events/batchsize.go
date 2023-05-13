package events

import "time"

// batchSizer is used to dynamically determine the number of events to pull
// from NATS at once. The idea is to create a continuous flow of events that
// are sent to the client, while pulling as few events from NATS as possible.
type batchSizer struct {
	// maxBatchSize is the maximum number of events that can be pulled from the
	// event tracker at once.
	maxBatchSize int

	// minBatchSize is the minimum number of events that can be pulled from the
	// event tracker at once.
	minBatchSize int

	// batchSize is the current batch size.
	batchSize int

	// lastBatchSizeTime is the time at which the last batch size was pulled
	// from the event tracker.
	lastBatchSizeTime time.Time

	// shouldCalculate is true if the batch size should be calculated again
	// before the next batch is pulled from the event tracker.
	shouldCalculate bool
}

// newBatchSizer creates a new batch sizer.
func newBatchSizer(minBatchSize uint, maxBatchSize uint) *batchSizer {
	return &batchSizer{
		minBatchSize: int(minBatchSize),
		maxBatchSize: int(maxBatchSize),
		batchSize:    int(minBatchSize),
	}
}

// Processed indicates that at least one event was processed in the last batch.
func (b *batchSizer) Processed() {
	b.shouldCalculate = true
}

// Get the next batch size.
func (b *batchSizer) Get(maxBatchSize int) int {
	if !b.shouldCalculate {
		return b.batchSize
	}

	// Reset the shouldCalculate flag.
	b.shouldCalculate = false

	if time.Since(b.lastBatchSizeTime) > time.Second {
		// If the last batch size was pulled more than 1 second ago, reduce the
		// batch size a little bit. If the batch size is already at the minimum,
		// do nothing.
		amountToReduce := int(float64(b.batchSize) * 0.1)
		if amountToReduce < 1 {
			amountToReduce = 1
		}

		if b.batchSize-amountToReduce > b.minBatchSize {
			// There is room to reduce the batch size.
			b.batchSize -= amountToReduce
		}
	} else if b.batchSize < b.maxBatchSize {
		// If the last batch size was pulled less than 1 second ago and we are
		// not at the maximum, increase the batch size a little bit.
		amountToIncrease := int(float64(b.batchSize) * 0.1)
		if amountToIncrease < 1 {
			amountToIncrease = 1
		}

		if b.batchSize+amountToIncrease < b.maxBatchSize {
			// There is room to increase the batch size.
			b.batchSize += amountToIncrease
		}
	}

	// Update the last batch size time.
	b.lastBatchSizeTime = time.Now()

	if b.batchSize <= maxBatchSize {
		return b.batchSize
	}
	return maxBatchSize
}
