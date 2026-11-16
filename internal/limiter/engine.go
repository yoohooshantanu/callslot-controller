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

// DefaultReservationTTL defines the maximum safety ceiling before an abandoned reservation is purged (4 hours)
const DefaultReservationTTL = 14400 // 4 hours in seconds

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

// ReserveCallScript atomically cleans up expired individual reservations and reserves an exact Reservation ID
// Uses a Redis Sorted Set (ZSET) where score = per-reservation expiration timestamp.
// This guarantees that no blanket key expiration can ever wipe out or reset active calls!
const ReserveCallScript = `
local key = KEYS[1]
local res_id = ARGV[1]
local max_cap = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

-- Clean up only expired individual reservations whose expiry timestamp has passed
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Count current active reservations
local current = redis.call('ZCARD', key)
if current < max_cap then
    -- Record individual reservation with its own exact expiry timestamp
    redis.call('ZADD', key, now + ttl, res_id)
    return {1, current + 1}
else
    return {0, current}
end
`

// ReleaseCallScript atomically removes an exact Reservation ID without affecting unrelated calls
const ReleaseCallScript = `
local key = KEYS[1]
local res_id = ARGV[1]

local deleted = redis.call('ZREM', key, res_id)
local remaining = redis.call('ZCARD', key)
return {deleted, remaining}
`

// RenewCallScript atomically extends the lease of an existing active Reservation ID.
// Uses ZADD with XX to guarantee elements are only updated if they already exist, never inserted.
const RenewCallScript = `
local key = KEYS[1]
local res_id = ARGV[1]
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

-- Clean up expired individual reservations
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- Check if reservation exists and is active
local score = redis.call('ZSCORE', key, res_id)
if score then
    -- Extend lease with new expiry timestamp (now + ttl)
    redis.call('ZADD', key, 'XX', now + ttl, res_id)
    return 1
else
    return 0
end
`

// Engine orchestrates multi-tiered distributed rate limiting & concurrent call caps
type Engine struct {
	l1Cache            *cache.L1LocalCache
	rdb                *redis.Client
	slidingSHA         string
	reserveCallSHA     string
	releaseCallSHA     string
	renewCallSHA       string
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
		if sha, err := rdb.ScriptLoad(ctx, RenewCallScript).Result(); err == nil {
			e.renewCallSHA = sha
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
			// Distributed Atomic Concurrency Reservation in Redis via Sorted Set
			callKey := fmt.Sprintf("cc:res:%s", req.TenantID)
			nowSec := time.Now().Unix()

			res, err := e.evalScript(ctx, e.reserveCallSHA, ReserveCallScript, []string{callKey}, resID, policy.MaxConcurrentCalls, nowSec, DefaultReservationTTL)
			if err != nil {
				// FAIL-CLOSED: Redis failure must reject requests rather than bypassing limits
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
			// In-memory fallback concurrency cap with individual per-reservation expiration
			e.activeResMu.Lock()
			resMap, ok := e.activeReservations[req.TenantID]
			if !ok {
				resMap = make(map[string]time.Time)
				e.activeReservations[req.TenantID] = resMap
			}

			// Prune expired individual reservations
			now := time.Now()
			for rID, exp := range resMap {
				if now.After(exp) {
					delete(resMap, rID)
				}
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
			resMap[resID] = now.Add(time.Duration(DefaultReservationTTL) * time.Second)
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

// RenewReservation extends the lease of an active concurrent call reservation.
// Returns true if renewed, false if reservation is missing or already expired.
func (e *Engine) RenewReservation(tenantID, reservationID string, ttlSec ...int64) bool {
	if tenantID == "" || reservationID == "" {
		return false
	}

	ttl := int64(DefaultReservationTTL)
	if len(ttlSec) > 0 && ttlSec[0] > 0 {
		ttl = ttlSec[0]
	}

	if e.rdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		callKey := fmt.Sprintf("cc:res:%s", tenantID)
		nowSec := time.Now().Unix()
		res, err := e.evalScript(ctx, e.renewCallSHA, RenewCallScript, []string{callKey}, reservationID, nowSec, ttl)
		if err == nil {
			if updated, ok := res.(int64); ok && updated == 1 {
				return true
			}
		}
		return false
	}

	// In-memory fallback
	e.activeResMu.Lock()
	defer e.activeResMu.Unlock()
	if resMap, ok := e.activeReservations[tenantID]; ok {
		if exp, exists := resMap[reservationID]; exists {
			now := time.Now()
			if !now.After(exp) {
				resMap[reservationID] = now.Add(time.Duration(ttl) * time.Second)
				return true
			}
			delete(resMap, reservationID)
		}
	}
	return false
}

// GetActiveCalls returns current concurrent calls for a tenant (excluding expired ones)
func (e *Engine) GetActiveCalls(ctx context.Context, tenantID string) (int64, error) {
	if e.rdb != nil {
		callKey := fmt.Sprintf("cc:res:%s", tenantID)
		// Prune expired before counting
		nowSec := time.Now().Unix()
		_ = e.rdb.ZRemRangeByScore(ctx, callKey, "-inf", fmt.Sprintf("%d", nowSec)).Err()
		val, err := e.rdb.ZCard(ctx, callKey).Result()
		return val, err
	}

	e.activeResMu.RLock()
	defer e.activeResMu.RUnlock()
	if resMap, ok := e.activeReservations[tenantID]; ok {
		now := time.Now()
		var count int64
		for _, exp := range resMap {
			if !now.After(exp) {
				count++
			}
		}
		return count, nil
	}
	return 0, nil
}
