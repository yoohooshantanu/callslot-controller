package cache

import (
	"sync"
	"time"
)

type TokenBucketEntry struct {
	Tokens     float64
	LastRefill time.Time
}

// L1LocalCache provides zero-network sub-microsecond rate limit evaluation
type L1LocalCache struct {
	mu      sync.RWMutex
	buckets map[string]*TokenBucketEntry
	ttl     time.Duration
}

func NewL1LocalCache(ttl time.Duration) *L1LocalCache {
	c := &L1LocalCache{
		buckets: make(map[string]*TokenBucketEntry),
		ttl:     ttl,
	}
	go c.cleanupLoop()
	return c
}

// AllowTokenBucket checks and consumes tokens in local memory
func (c *L1LocalCache) AllowTokenBucket(key string, capacity int64, refillRatePerSec float64, cost int64) (allowed bool, remaining int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	entry, ok := c.buckets[key]
	if !ok {
		entry = &TokenBucketEntry{
			Tokens:     float64(capacity),
			LastRefill: now,
		}
		c.buckets[key] = entry
	}

	// Refill tokens based on elapsed duration
	elapsed := now.Sub(entry.LastRefill).Seconds()
	entry.Tokens += elapsed * refillRatePerSec
	if entry.Tokens > float64(capacity) {
		entry.Tokens = float64(capacity)
	}
	entry.LastRefill = now

	// Check if sufficient tokens exist
	if entry.Tokens >= float64(cost) {
		entry.Tokens -= float64(cost)
		return true, int64(entry.Tokens)
	}

	return false, int64(entry.Tokens)
}

func (c *L1LocalCache) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for k, v := range c.buckets {
			if now.Sub(v.LastRefill) > c.ttl {
				delete(c.buckets, k)
			}
		}
		c.mu.Unlock()
	}
}
