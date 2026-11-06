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
// All operations are O(1): a doubly-linked list keeps recency order and the
// map holds direct pointers to the nodes, so get/moveToEnd/evict never scan
// the cache. A mutex guards all access.
type lruCache[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	items map[K]*lruNode[K, V]
	head  *lruNode[K, V] // sentinel: item after head is MRU
	tail  *lruNode[K, V] // sentinel: item before tail is LRU
}

type lruNode[K comparable, V any] struct {
	key K
	val V
	prev *lruNode[K, V]
	next *lruNode[K, V]
}

func newLRUCache[K comparable, V any](cap int) *lruCache[K, V] {
	if cap < 1 {
		cap = 1
	}
	c := &lruCache[K, V]{cap: cap, items: make(map[K]*lruNode[K, V])}
	// Wire the two sentinels together so the list is empty.
	c.head = &lruNode[K, V]{}
	c.tail = &lruNode[K, V]{}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

// insertMRU places node right after the head sentinel (most-recently-used).
func (c *lruCache[K, V]) insertMRU(n *lruNode[K, V]) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

// remove detaches a node from the list regardless of its position.
func (c *lruCache[K, V]) remove(n *lruNode[K, V]) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

// get returns the value for key and marks it most-recently-used.
func (c *lruCache[K, V]) get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.remove(n)
	c.insertMRU(n)
	return n.val, true
}

// put inserts or updates key, marking it most-recently-used and evicting the
// LRU entry (the node just before tail) if the cache is at capacity.
func (c *lruCache[K, V]) put(key K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n, ok := c.items[key]; ok {
		n.val = v
		c.remove(n)
		c.insertMRU(n)
		return
	}
	if len(c.items) >= c.cap {
		lru := c.tail.prev
		if lru == c.head {
			c.mu.Unlock()
			return
		}
		c.remove(lru)
		delete(c.items, lru.key)
	}
	n := &lruNode[K, V]{key: key, val: v}
	c.items[key] = n
	c.insertMRU(n)
}

// len returns the number of entries (for tests).
func (c *lruCache[K, V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}
