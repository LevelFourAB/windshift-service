package flowcontrol

import (
	"sync"
	"sync/atomic"
)

type DynamicSemaphore struct {
	lock  sync.Mutex
	limit int
	count int
	cond  *sync.Cond
}

func NewDynamicSemaphore(limit int) *DynamicSemaphore {
	ds := &DynamicSemaphore{
		limit: limit,
		count: 0,
	}
	ds.cond = sync.NewCond(&ds.lock)
	return ds
}

// Acquire acquires a permit in the semaphore, will block until the permit is
// available. Returns a function that releases the permit.
func (ds *DynamicSemaphore) Acquire() func() {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	for ds.count >= ds.limit {
		ds.cond.Wait()
	}
	ds.count++

	released := uint32(0)
	return func() {
		if atomic.CompareAndSwapUint32(&released, 0, 1) {
			ds.lock.Lock()
			defer ds.lock.Unlock()
			ds.count--
			ds.cond.Broadcast()
		}
	}
}

// TryAcquire acquires a permit in the semaphore if one is available. Returns a
// function that releases the permit or nil if no permit was available.
func (ds *DynamicSemaphore) TryAcquire() func() {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	if ds.count >= ds.limit {
		return nil
	}
	ds.count++

	released := uint32(0)
	return func() {
		if atomic.CompareAndSwapUint32(&released, 0, 1) {
			ds.lock.Lock()
			defer ds.lock.Unlock()
			ds.count--
			ds.cond.Broadcast()
		}
	}
}

// Available returns the number of permits currently available in the semaphore.
func (ds *DynamicSemaphore) Available() int {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	return ds.limit - ds.count
}

func (ds *DynamicSemaphore) SetLimit(limit int) {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	ds.limit = limit
	ds.cond.Broadcast()
}
