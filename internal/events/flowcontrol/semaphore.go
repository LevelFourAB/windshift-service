package flowcontrol

import "sync"

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

func (ds *DynamicSemaphore) Acquire() {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	for ds.count >= ds.limit {
		ds.cond.Wait()
	}
	ds.count++
}

func (ds *DynamicSemaphore) Release() {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	ds.count--
	ds.cond.Broadcast()
}

func (ds *DynamicSemaphore) SetLimit(limit int) {
	ds.lock.Lock()
	defer ds.lock.Unlock()
	ds.limit = limit
	ds.cond.Broadcast()
}
