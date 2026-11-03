package identity

import "sync"

// lruCache is a bounded, concurrency-safe LRU map keyed by K and storing V.
//
// It replaces the previous "drop the entire cache on overflow" behavior: a
// total drop meant a spike of distinct keys above the cap caused a thundering
// re-verification of everything, even entries that had just been accessed. The
// LRU evicts only the least-recently-used entry on overflow, preserving the
// hit rate of warm entries while keeping memory bounded.
//
// The cap is the max number of entries. Lookups refresh recency; writes evict
// the LRU entry when full. A mutex guards all access.
type lruCache[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	order []K
	items map[K]V
}

func newLRUCache[K comparable, V any](cap int) *lruCache[K, V] {
	if cap < 1 {
		cap = 1
	}
	return &lruCache[K, V]{cap: cap, items: make(map[K]V)}
}

// get returns the value for key and marks it most-recently-used.
func (c *lruCache[K, V]) get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.moveToEnd(key)
	return v, true
}

// put inserts or updates key, marking it most-recently-used and evicting the
// LRU entry if the cache is at capacity.
func (c *lruCache[K, V]) put(key K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; ok {
		c.items[key] = v
		c.moveToEnd(key)
		return
	}
	if len(c.items) >= c.cap {
		c.evictLRU()
	}
	c.items[key] = v
	c.order = append(c.order, key)
}

// moveToEnd marks key as most-recently-used in the order slice.
func (c *lruCache[K, V]) moveToEnd(key K) {
	for i := len(c.order) - 1; i >= 0; i-- {
		if c.order[i] == key {
			if i != len(c.order)-1 {
				c.order = append(c.order[:i], c.order[i+1:]...)
				c.order = append(c.order, key)
			}
			return
		}
	}
}

// evictLRU removes the least-recently-used entry.
func (c *lruCache[K, V]) evictLRU() {
	if len(c.order) == 0 {
		return
	}
	oldest := c.order[0]
	c.order = c.order[1:]
	delete(c.items, oldest)
}

// len returns the number of entries (for tests).
func (c *lruCache[K, V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
