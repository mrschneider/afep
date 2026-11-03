package identity

import "testing"

func TestLRUCache(t *testing.T) {
	c := newLRUCache[string, int](2)
	if n := c.len(); n != 0 {
		t.Fatalf("expected len 0, got %d", n)
	}

	c.put("a", 1)
	c.put("b", 2)
	if n := c.len(); n != 2 {
		t.Fatalf("expected len 2, got %d", n)
	}

	// Access "a" so it becomes MRU; "b" is now LRU
	val, ok := c.get("a")
	if !ok || val != 1 {
		t.Fatalf("expected get('a') = 1, got %v, %v", val, ok)
	}

	// Insert "c", should evict "b"
	c.put("c", 3)
	if n := c.len(); n != 2 {
		t.Fatalf("expected len 2, got %d", n)
	}

	if _, ok := c.get("b"); ok {
		t.Fatal("expected 'b' to be evicted")
	}

	val, ok = c.get("a")
	if !ok || val != 1 {
		t.Fatalf("expected 'a' to remain, got %v, %v", val, ok)
	}

	val, ok = c.get("c")
	if !ok || val != 3 {
		t.Fatalf("expected 'c' to remain, got %v, %v", val, ok)
	}
}
