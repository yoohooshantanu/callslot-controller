package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/tenantgate/internal/cache"
	"github.com/yoohooshantanu/tenantgate/internal/metrics"
	"github.com/yoohooshantanu/tenantgate/internal/model"
)

// SlidingWindowScript is an atomic Redis Lua script enforcing sliding window rate limits
const SlidingWindowScript = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])

-- Clean up expired entries outside the sliding window
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)

-- Count current requests in window
local current_count = redis.call('ZCARD', key)

if current_count + cost <= limit then
    -- Record request entries with guaranteed unique monotonic sequence
    local seq = redis.call('INCR', key .. ':seq')
    redis.call('PEXPIRE', key .. ':seq', window)
    for i = 1, cost do
        redis.call('ZADD', key, now, now .. '-' .. seq .. '-' .. i)
    end
    redis.call('PEXPIRE', key, window)
    return {1, limit - (current_count + cost), current_count + cost}
else
    return {0, 0, current_count}
end
`

// Engine orchestrates multi-tiered distributed rate limiting & concurrent call caps
type Engine struct {
	l1Cache       *cache.L1LocalCache
	rdb           *redis.Client
	slidingSHA    string
	policiesMu    sync.RWMutex
	policies      map[string]*model.QuotaPolicy
	activeCallsMu sync.RWMutex
	activeCalls   map[string]int64
}

func NewEngine(rdb *redis.Client) *Engine {
	e := &Engine{
		l1Cache:     cache.NewL1LocalCache(10 * time.Minute),
		rdb:         rdb,
		policies:    make(map[string]*model.QuotaPolicy),
		activeCalls: make(map[string]int64),
	}

	e.registerDefaultPolicies()

	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		sha, err := rdb.ScriptLoad(ctx, SlidingWindowScript).Result()
		if err == nil {
			e.slidingSHA = sha
		}
	}

	return e
}

func (e *Engine) registerDefaultPolicies() {
	e.policies[string(model.TierFree)] = &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  60,
		MaxConcurrentCalls: 5,
		BurstCapacity:      10,
		RefillRatePerSec:   1.0,
	}
	e.policies[string(model.TierGrowth)] = &model.QuotaPolicy{
		Tier:               model.TierGrowth,
		RequestsPerMinute:  600,
		MaxConcurrentCalls: 30,
		BurstCapacity:      50,
		RefillRatePerSec:   10.0,
	}
	e.policies[string(model.TierEnterprise)] = &model.QuotaPolicy{
		Tier:               model.TierEnterprise,
		RequestsPerMinute:  6000,
		MaxConcurrentCalls: 300,
		BurstCapacity:      200,
		RefillRatePerSec:   100.0,
	}
}

func (e *Engine) SetTenantPolicy(tenantID string, policy *model.QuotaPolicy) {
	e.policiesMu.Lock()
	defer e.policiesMu.Unlock()
	e.policies[tenantID] = policy
}

func (e *Engine) getPolicy(tenantID string) *model.QuotaPolicy {
	e.policiesMu.RLock()
	defer e.policiesMu.RUnlock()
	if p, ok := e.policies[tenantID]; ok {
		return p
	}
	// Default to Growth tier
	return e.policies[string(model.TierGrowth)]
}

// Close releases resources and stops background tasks
func (e *Engine) Close() {
	if e.l1Cache != nil {
		e.l1Cache.Close()
	}
}

// Check evaluates rate limits across concurrency caps and rate quotas
func (e *Engine) Check(ctx context.Context, req *model.RateLimitRequest) (*model.RateLimitResponse, error) {
	if req.TenantID == "" {
		return nil, model.ErrInvalidRequest
	}

	cost := req.Cost
	if cost <= 0 {
		cost = 1
	}

	policy := e.getPolicy(req.TenantID)

	var callSlotReserved bool
	rollbackCallSlot := func() {
		if callSlotReserved {
			e.activeCallsMu.Lock()
			if e.activeCalls[req.TenantID] >= cost {
				e.activeCalls[req.TenantID] -= cost
			} else {
				e.activeCalls[req.TenantID] = 0
			}
			e.activeCallsMu.Unlock()
			metrics.DefaultMetrics.UpdateActiveCalls(-cost)
			callSlotReserved = false
		}
	}

	// 1. Check Concurrent Calls Cap for call initiation requests
	if req.Resource == "call.initiate" {
		e.activeCallsMu.Lock()
		current := e.activeCalls[req.TenantID]
		if current+cost > policy.MaxConcurrentCalls {
			e.activeCallsMu.Unlock()
			metrics.DefaultMetrics.IncCheck(false, true)
			return &model.RateLimitResponse{
				Allowed:            false,
				Limit:              policy.RequestsPerMinute,
				ActiveCalls:        current,
				MaxConcurrentCalls: policy.MaxConcurrentCalls,
				ViolationReason:    fmt.Sprintf("maximum concurrent calls cap reached (%d/%d)", current, policy.MaxConcurrentCalls),
				CheckedAt:          time.Now().UTC(),
			}, nil
		}
		e.activeCalls[req.TenantID] = current + cost
		e.activeCallsMu.Unlock()
		metrics.DefaultMetrics.UpdateActiveCalls(cost)
		callSlotReserved = true
	}

	// 2. High-speed Fast Path: L1 Token Bucket Cache
	bucketKey := fmt.Sprintf("tb:%s:%s", req.TenantID, req.Resource)
	l1Allowed, l1Rem := e.l1Cache.AllowTokenBucket(bucketKey, policy.BurstCapacity, policy.RefillRatePerSec, cost)

	if !l1Allowed {
		// Strict rejection from local burst governor; release reserved call slot if any
		rollbackCallSlot()
		metrics.DefaultMetrics.IncCheck(false, true)
		return &model.RateLimitResponse{
			Allowed:           false,
			Limit:             policy.RequestsPerMinute,
			RemainingQuota:    0,
			ResetAfterSeconds: 1,
			ViolationReason:   "burst capacity token bucket depleted",
			CheckedAt:         time.Now().UTC(),
		}, nil
	}

	// 3. Sliding Window Check (Redis or In-Memory fallback)
	if e.rdb != nil && e.slidingSHA != "" {
		windowKey := fmt.Sprintf("rl:%s:%s", req.TenantID, req.Resource)
		nowMs := time.Now().UnixMilli()
		windowMs := int64(60 * 1000) // 1 minute sliding window

		res, err := e.rdb.EvalSha(ctx, e.slidingSHA, []string{windowKey}, nowMs, windowMs, policy.RequestsPerMinute, cost).Result()
		if err == nil {
			if vals, ok := res.([]interface{}); ok && len(vals) == 3 {
				allowed := vals[0].(int64) == 1
				remaining := vals[1].(int64)
				currentUsage := vals[2].(int64)

				metrics.DefaultMetrics.IncCheck(allowed, false)
				var reason string
				if !allowed {
					rollbackCallSlot()
					reason = fmt.Sprintf("sliding window limit exceeded (%d/%d req/min)", currentUsage, policy.RequestsPerMinute)
				}
				return &model.RateLimitResponse{
					Allowed:           allowed,
					Limit:             policy.RequestsPerMinute,
					CurrentUsage:      currentUsage,
					RemainingQuota:    remaining,
					ResetAfterSeconds: 60,
					ViolationReason:   reason,
					CheckedAt:         time.Now().UTC(),
				}, nil
			}
		}
	}

	// Fallback to local L1 approval
	metrics.DefaultMetrics.IncCheck(true, true)
	return &model.RateLimitResponse{
		Allowed:           true,
		Limit:             policy.RequestsPerMinute,
		CurrentUsage:      policy.BurstCapacity - l1Rem,
		RemainingQuota:    l1Rem,
		ResetAfterSeconds: 60,
		CheckedAt:         time.Now().UTC(),
	}, nil
}

// ReleaseCall drops the concurrent call counter when a call terminates
func (e *Engine) ReleaseCall(tenantID string) {
	e.activeCallsMu.Lock()
	defer e.activeCallsMu.Unlock()

	if e.activeCalls[tenantID] > 0 {
		e.activeCalls[tenantID]--
		metrics.DefaultMetrics.UpdateActiveCalls(-1)
	}
}
