package flowcontrol

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type tracker struct {
	logger *zap.Logger

	items map[uint64]time.Time
	lock  sync.Mutex
	cond  *sync.Cond

	count int
}

func newTracker(logger *zap.Logger) *tracker {
	t := &tracker{
		logger: logger,
		items:  make(map[uint64]time.Time),
		count:  0,
	}
	t.cond = sync.NewCond(&t.lock)
	return t
}

func (t *tracker) Add(event uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.items == nil {
		return
	}
	t.items[event] = time.Now()
	t.cond.Broadcast()
}

func (t *tracker) Ping(event uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t.items[event]; ok {
		t.items[event] = time.Now()
	}
}

func (t *tracker) Remove(event uint64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	delete(t.items, event)
	t.cond.Broadcast()
}

func (t *tracker) Destroy() {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.items = nil
	t.cond.Broadcast()
}

func (t *tracker) RemoveOlderThan(d time.Duration) int {
	t.lock.Lock()
	defer t.lock.Unlock()

	threshold := time.Now().Add(-d)
	removedItems := 0

	for event, timestamp := range t.items {
		if timestamp.Before(threshold) {
			delete(t.items, event)
			removedItems++
			t.logger.Debug("Event expired", zap.Uint64("consumerSeq", event))
		}
	}

	if removedItems > 0 {
		t.cond.Broadcast()
	}
	return removedItems
}

func (t *tracker) WaitUntilLessThan(limit int) {
	t.lock.Lock()
	defer t.lock.Unlock()
	for t.items != nil && len(t.items) >= limit {
		t.cond.Wait()
	}
}

func (t *tracker) Count() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.items)
}
