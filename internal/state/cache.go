package state

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// keyValueStoreCache is used to keep track of active nats.KeyValue buckets. Will expire
// buckets after a certain amount of time of them not being used.
type keyValueStoreCache struct {
	mu         sync.Mutex
	items      map[string]*cacheItem
	maxAge     time.Duration
	createFunc func(ctx context.Context, name string) (nats.KeyValue, error)

	stopCh chan struct{}
}

type cacheItem struct {
	value      nats.KeyValue
	lastAccess time.Time
}

func newKeyValueStoreCache(maxAge time.Duration, createFunc func(ctx context.Context, name string) (nats.KeyValue, error)) *keyValueStoreCache {
	cache := &keyValueStoreCache{
		items:      make(map[string]*cacheItem),
		maxAge:     maxAge,
		createFunc: createFunc,

		stopCh: make(chan struct{}),
	}

	go func() {
		ticker := time.NewTicker(maxAge / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cache.evict()
			case <-cache.stopCh:
				return
			}
		}
	}()

	return cache
}

func (c *keyValueStoreCache) Destroy() {
	close(c.stopCh)
}

func (c *keyValueStoreCache) Get(ctx context.Context, key string) (nats.KeyValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, ok := c.items[key]
	if !ok {
		value, err := c.createFunc(ctx, key)
		if err != nil {
			return nil, err
		}

		item = &cacheItem{
			value:      value,
			lastAccess: time.Now(),
		}

		c.items[key] = item
	}

	item.lastAccess = time.Now()
	return item.value, nil
}

func (c *keyValueStoreCache) evict() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if time.Since(item.lastAccess) > c.maxAge {
			delete(c.items, key)
		}
	}
}
