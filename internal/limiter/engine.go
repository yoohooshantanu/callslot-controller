package limiter

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strings"
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

// ReserveCallScript atomically checks capacity and registers an exact Reservation ID in Redis
const ReserveCallScript = `
local res_key = KEYS[1]
local res_id = ARGV[1]
local max_cap = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local current = redis.call('HLEN', res_key)
if current < max_cap then
    redis.call('HSET', res_key, res_id, now)
    redis.call('EXPIRE', res_key, 7200)
    return {1, current + 1}
else
    return {0, current}
end
`

// ReleaseCallScript atomically removes an exact Reservation ID without affecting unrelated calls
const ReleaseCallScript = `
local res_key = KEYS[1]
local res_id = ARGV[1]

local deleted = redis.call('HDEL', res_key, res_id)
local remaining = redis.call('HLEN', res_key)
return {deleted, remaining}
`

// Engine orchestrates multi-tiered distributed rate limiting & concurrent call caps
type Engine struct {
	l1Cache            *cache.L1LocalCache
	rdb                *redis.Client
	slidingSHA         string
	reserveCallSHA     string
	releaseCallSHA     string
	policiesMu         sync.RWMutex
	policies           map[string]*model.QuotaPolicy
	activeResMu        sync.RWMutex
	activeReservations map[string]map[string]time.Time
}

func NewEngine(rdb *redis.Client) *Engine {
	e := &Engine{
		l1Cache:            cache.NewL1LocalCache(10 * time.Minute),
		rdb:                rdb,
		policies:           make(map[string]*model.QuotaPolicy),
		activeReservations: make(map[string]map[string]time.Time),
	}

	e.registerDefaultPolicies()

	if rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if sha, err := rdb.ScriptLoad(ctx, SlidingWindowScript).Result(); err == nil {
			e.slidingSHA = sha
		}
		if sha, err := rdb.ScriptLoad(ctx, ReserveCallScript).Result(); err == nil {
			e.reserveCallSHA = sha
		}
		if sha, err := rdb.ScriptLoad(ctx, ReleaseCallScript).Result(); err == nil {
			e.releaseCallSHA = sha
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

func generateReservationID(tenantID string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("res_%s_%d_%s", tenantID, time.Now().UnixNano(), hex.EncodeToString(b))
}

// Check evaluates rate limits across concurrency caps and rate quotas
func (e *Engine) Check(ctx context.Context, req *model.RateLimitRequest) (*model.RateLimitResponse, error) {
	if req.TenantID == "" {
		return nil, model.ErrInvalidRequest
	}

	if req.Resource == "" {
		req.Resource = "default"
	}

	cost := req.Cost
	if cost <= 0 {
		cost = 1
	}

	policy := e.getPolicy(req.TenantID)

	var reservedID string
	rollbackCallSlot := func() {
		if reservedID != "" {
			e.ReleaseCall(req.TenantID, reservedID)
			reservedID = ""
		}
	}

	// 1. Check Concurrent Calls Cap for call initiation requests
	if req.Resource == "call.initiate" {
		resID := generateReservationID(req.TenantID)

		if e.rdb != nil {
			// Distributed Atomic Concurrency Reservation in Redis via HSET
			callKey := fmt.Sprintf("cc:res:%s", req.TenantID)
			nowMs := time.Now().UnixMilli()

			res, err := e.evalScript(ctx, e.reserveCallSHA, ReserveCallScript, []string{callKey}, resID, policy.MaxConcurrentCalls, nowMs)
			if err != nil {
				// FAIL-CLOSED: Redis cluster failure must not bypass global quota limits
				metrics.DefaultMetrics.IncCheck(false, true)
				return &model.RateLimitResponse{
					Allowed:         false,
					ViolationReason: "redis cluster unavailable (fail-closed quota policy)",
					CheckedAt:       time.Now().UTC(),
				}, fmt.Errorf("%w: %v", model.ErrRedisFailure, err)
			}

			if vals, ok := res.([]interface{}); ok && len(vals) == 2 {
				allowed := vals[0].(int64) == 1
				currentCalls := vals[1].(int64)
				if !allowed {
					metrics.DefaultMetrics.IncCheck(false, true)
					return &model.RateLimitResponse{
						Allowed:            false,
						Limit:              policy.RequestsPerMinute,
						ActiveCalls:        currentCalls,
						MaxConcurrentCalls: policy.MaxConcurrentCalls,
						ViolationReason:    fmt.Sprintf("maximum concurrent calls cap reached (%d/%d)", currentCalls, policy.MaxConcurrentCalls),
						CheckedAt:          time.Now().UTC(),
					}, nil
				}
				reservedID = resID
				metrics.DefaultMetrics.UpdateActiveCalls(1)
			}
		} else {
			// In-memory fallback concurrency cap with reservation ID tracking
			e.activeResMu.Lock()
			resMap, ok := e.activeReservations[req.TenantID]
			if !ok {
				resMap = make(map[string]time.Time)
				e.activeReservations[req.TenantID] = resMap
			}
			current := int64(len(resMap))
			if current+1 > policy.MaxConcurrentCalls {
				e.activeResMu.Unlock()
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
			resMap[resID] = time.Now()
			e.activeResMu.Unlock()
			metrics.DefaultMetrics.UpdateActiveCalls(1)
			reservedID = resID
		}
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
	if e.rdb != nil {
		windowKey := fmt.Sprintf("rl:%s:%s", req.TenantID, req.Resource)
		nowMs := time.Now().UnixMilli()
		windowMs := int64(60 * 1000) // 1 minute sliding window

		res, err := e.evalScript(ctx, e.slidingSHA, SlidingWindowScript, []string{windowKey}, nowMs, windowMs, policy.RequestsPerMinute, cost)
		if err != nil {
			// FAIL-CLOSED: Redis cluster failure must not bypass sliding-window limits
			rollbackCallSlot()
			metrics.DefaultMetrics.IncCheck(false, true)
			return &model.RateLimitResponse{
				Allowed:         false,
				ViolationReason: "redis cluster unavailable (fail-closed quota policy)",
				CheckedAt:       time.Now().UTC(),
			}, fmt.Errorf("%w: %v", model.ErrRedisFailure, err)
		}

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
				ReservationID:     reservedID,
				ViolationReason:   reason,
				CheckedAt:         time.Now().UTC(),
			}, nil
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
		ReservationID:     reservedID,
		CheckedAt:         time.Now().UTC(),
	}, nil
}

func (e *Engine) evalScript(ctx context.Context, sha, script string, keys []string, args ...interface{}) (interface{}, error) {
	if sha != "" {
		res, err := e.rdb.EvalSha(ctx, sha, keys, args...).Result()
		if err == nil {
			return res, nil
		}
		if !strings.HasPrefix(err.Error(), "NOSCRIPT") {
			return nil, err
		}
	}
	return e.rdb.Eval(ctx, script, keys, args...).Result()
}

// ReleaseCall drops the concurrent call counter when calls terminate using the unique Reservation ID
func (e *Engine) ReleaseCall(tenantID, reservationID string) bool {
	if tenantID == "" || reservationID == "" {
		return false
	}

	released := false

	if e.rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		callKey := fmt.Sprintf("cc:res:%s", tenantID)
		res, err := e.evalScript(ctx, e.releaseCallSHA, ReleaseCallScript, []string{callKey}, reservationID)
		if err == nil {
			if vals, ok := res.([]interface{}); ok && len(vals) == 2 {
				deleted := vals[0].(int64)
				if deleted == 1 {
					metrics.DefaultMetrics.UpdateActiveCalls(-1)
					released = true
				}
			}
		}
	} else {
		e.activeResMu.Lock()
		if resMap, ok := e.activeReservations[tenantID]; ok {
			if _, exists := resMap[reservationID]; exists {
				delete(resMap, reservationID)
				metrics.DefaultMetrics.UpdateActiveCalls(-1)
				released = true
				if len(resMap) == 0 {
					delete(e.activeReservations, tenantID)
				}
			}
		}
		e.activeResMu.Unlock()
	}

	return released
}

// GetActiveCalls returns current concurrent calls for a tenant
func (e *Engine) GetActiveCalls(ctx context.Context, tenantID string) (int64, error) {
	if e.rdb != nil {
		callKey := fmt.Sprintf("cc:res:%s", tenantID)
		val, err := e.rdb.HLen(ctx, callKey).Result()
		return val, err
	}

	e.activeResMu.RLock()
	defer e.activeResMu.RUnlock()
	if resMap, ok := e.activeReservations[tenantID]; ok {
		return int64(len(resMap)), nil
	}
	return 0, nil
}
