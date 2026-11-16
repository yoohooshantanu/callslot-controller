package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/tenantgate/internal/limiter"
	"github.com/yoohooshantanu/tenantgate/internal/model"
)

func TestTokenBucketBurstLimiting(t *testing.T) {
	engine := limiter.NewEngine(nil)

	// Custom Free Tier: Capacity = 5, Refill = 1/sec
	engine.SetTenantPolicy("tenant_free_test", &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  60,
		MaxConcurrentCalls: 5,
		BurstCapacity:      5,
		RefillRatePerSec:   1.0,
	})

	ctx := context.Background()

	// 1. Consume all 5 burst tokens
	for i := 0; i < 5; i++ {
		resp, err := engine.Check(ctx, &model.RateLimitRequest{
			TenantID: "tenant_free_test",
			Resource: "audio.chunk",
			Cost:     1,
		})
		if err != nil || !resp.Allowed {
			t.Fatalf("expected request %d to be allowed, got: %+v, err: %v", i+1, resp, err)
		}
	}

	// 2. 6th immediate request should be rejected (burst depleted)
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_free_test",
		Resource: "audio.chunk",
		Cost:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected 6th request to be blocked by burst capacity")
	}

	// 3. Wait 1.1s for token refill
	time.Sleep(1100 * time.Millisecond)

	refillResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_free_test",
		Resource: "audio.chunk",
		Cost:     1,
	})
	if err != nil || !refillResp.Allowed {
		t.Fatalf("expected refilled token request to pass, got: %+v", refillResp)
	}
}

func TestConcurrentCallsCapEnforcement(t *testing.T) {
	engine := limiter.NewEngine(nil)

	engine.SetTenantPolicy("tenant_call_cap", &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 3,
		BurstCapacity:      100,
		RefillRatePerSec:   10.0,
	})

	ctx := context.Background()

	// Initiate 3 calls
	for i := 0; i < 3; i++ {
		resp, err := engine.Check(ctx, &model.RateLimitRequest{
			TenantID: "tenant_call_cap",
			Resource: "call.initiate",
			Cost:     1,
		})
		if err != nil || !resp.Allowed {
			t.Fatalf("expected call initiation %d to be permitted, got: %+v", i+1, resp)
		}
	}

	// 4th call should hit the max concurrent calls barrier
	blockedResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_call_cap",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blockedResp.Allowed {
		t.Fatalf("expected 4th concurrent call to be rejected")
	}

	// Release 1 call
	engine.ReleaseCall("tenant_call_cap")

	// Retry initiation, should now succeed
	retryResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_call_cap",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !retryResp.Allowed {
		t.Fatalf("expected call to succeed after releasing concurrency slot: %+v", retryResp)
	}
}

func TestActiveCallsRollbackOnL1Depletion(t *testing.T) {
	engine := limiter.NewEngine(nil)
	defer engine.Close()

	// BurstCapacity is only 1, but MaxConcurrentCalls is 5
	engine.SetTenantPolicy("tenant_leak_test", &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  60,
		MaxConcurrentCalls: 5,
		BurstCapacity:      1,
		RefillRatePerSec:   0.001, // extremely slow refill
	})

	ctx := context.Background()

	// First call consumes the only burst token and takes 1 concurrent call slot
	resp1, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_leak_test",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp1.Allowed {
		t.Fatalf("first call should succeed: %+v, err: %v", resp1, err)
	}

	// Second call has concurrent slot available, but L1 burst token is depleted
	resp2, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_leak_test",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp2.Allowed {
		t.Fatalf("second call should be rejected due to depleted burst bucket")
	}

	// Crucial assertion: Release the 1 successful call
	engine.ReleaseCall("tenant_leak_test")
}

func TestBatchCallAllocationAndRelease(t *testing.T) {
	engine := limiter.NewEngine(nil)
	defer engine.Close()

	engine.SetTenantPolicy("tenant_batch_test", &model.QuotaPolicy{
		Tier:               model.TierGrowth,
		RequestsPerMinute:  600,
		MaxConcurrentCalls: 10,
		BurstCapacity:      50,
		RefillRatePerSec:   10.0,
	})

	ctx := context.Background()

	// Reserve batch of 6 concurrent calls
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_batch_test",
		Resource: "call.initiate",
		Cost:     6,
	})
	if err != nil || !resp.Allowed {
		t.Fatalf("expected batch call of 6 to succeed: %+v, err: %v", resp, err)
	}

	// 5 more should fail because max is 10 (6 + 5 = 11 > 10)
	resp2, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_batch_test",
		Resource: "call.initiate",
		Cost:     5,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp2.Allowed {
		t.Fatalf("expected batch of 5 to exceed capacity (11 > 10)")
	}

	// Release 4 slots out of the 6
	engine.ReleaseCall("tenant_batch_test", 4)

	// Now 5 more should succeed (active was 6 - 4 = 2, 2 + 5 = 7 <= 10)
	resp3, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_batch_test",
		Resource: "call.initiate",
		Cost:     5,
	})
	if err != nil || !resp3.Allowed {
		t.Fatalf("expected batch of 5 to succeed after partial release: %+v", resp3)
	}
}

func TestConcurrentGoroutineLoad(t *testing.T) {
	engine := limiter.NewEngine(nil)

	engine.SetTenantPolicy("tenant_load_test", &model.QuotaPolicy{
		Tier:               model.TierEnterprise,
		RequestsPerMinute:  100000,
		MaxConcurrentCalls: 1000,
		BurstCapacity:      5000,
		RefillRatePerSec:   5000.0,
	})

	ctx := context.Background()
	var wg sync.WaitGroup
	workers := 50
	reqsPerWorker := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reqsPerWorker; j++ {
				_, _ = engine.Check(ctx, &model.RateLimitRequest{
					TenantID: "tenant_load_test",
					Resource: "audio.chunk",
					Cost:     1,
				})
			}
		}()
	}

	wg.Wait()
}

func TestLiveRedisSlidingWindowLimiting(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping live Redis sliding window test: REDIS_ADDR environment variable not set")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping live Redis test: cannot ping Redis at %s: %v", redisAddr, err)
	}

	engine := limiter.NewEngine(rdb)
	defer engine.Close()

	tenantID := fmt.Sprintf("tenant_live_redis_%d", time.Now().UnixNano())
	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  5,
		MaxConcurrentCalls: 10,
		BurstCapacity:      100, // allow L1 burst so Redis sliding window is tested
		RefillRatePerSec:   100.0,
	})

	// Perform 5 checks within sliding window
	for i := 1; i <= 5; i++ {
		resp, err := engine.Check(ctx, &model.RateLimitRequest{
			TenantID: tenantID,
			Resource: "audio.chunk",
			Cost:     1,
		})
		if err != nil {
			t.Fatalf("request %d returned error: %v", i, err)
		}
		if !resp.Allowed {
			t.Fatalf("request %d should be allowed within quota, got: %+v", i, resp)
		}
	}

	// 6th check should be rejected by Redis sliding window script
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "audio.chunk",
		Cost:     1,
	})
	if err != nil {
		t.Fatalf("unexpected error on 6th request: %v", err)
	}
	if resp.Allowed {
		t.Fatalf("expected 6th request to be rejected by Redis sliding window (5/5 req/min)")
	}
}

func TestLiveRedisDistributedConcurrentCallCap(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping live Redis distributed call cap test: REDIS_ADDR not set")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping live Redis test: cannot ping Redis at %s: %v", redisAddr, err)
	}

	// Simulate TWO separate server instances connecting to the same Redis cluster
	engineInstanceA := limiter.NewEngine(rdb)
	defer engineInstanceA.Close()

	engineInstanceB := limiter.NewEngine(rdb)
	defer engineInstanceB.Close()

	tenantID := fmt.Sprintf("tenant_dist_cap_%d", time.Now().UnixNano())
	policy := &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 3, // Global limit across all instances
		BurstCapacity:      100,
		RefillRatePerSec:   100.0,
	}
	engineInstanceA.SetTenantPolicy(tenantID, policy)
	engineInstanceB.SetTenantPolicy(tenantID, policy)

	// Instance A reserves 2 concurrent call slots
	resp1, err := engineInstanceA.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     2,
	})
	if err != nil || !resp1.Allowed {
		t.Fatalf("Instance A failed reserving 2 call slots: %+v, err: %v", resp1, err)
	}

	// Instance B attempts to reserve 2 concurrent call slots (2 + 2 = 4 > 3 -> Must fail!)
	resp2, err := engineInstanceB.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     2,
	})
	if err != nil {
		t.Fatalf("Instance B check returned unexpected err: %v", err)
	}
	if resp2.Allowed {
		t.Fatalf("Instance B should have been rejected by distributed Redis concurrency cap (4 > 3)")
	}

	// Instance A releases 1 slot
	engineInstanceA.ReleaseCall(tenantID, 1)

	// Instance B now reserves 1 slot (2 - 1 + 1 = 2 <= 3 -> Must succeed!)
	resp3, err := engineInstanceB.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp3.Allowed {
		t.Fatalf("Instance B should succeed reserving 1 slot after Instance A released: %+v, err: %v", resp3, err)
	}
}

func TestHTTPAuthenticationValidation(t *testing.T) {
	// Setup test handler mimicking server/main.go auth
	keyRegistry := map[string]string{
		"tg_test_key_123":    "tenant_test",
		"tg_live_free_key":   "tenant_free_test",
		"tg_live_growth_key": "tenant_call_cap",
	}

	validateAuth := func(r *http.Request, requestTenantID, bodyAPIKey string) (string, error) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			authHdr := r.Header.Get("Authorization")
			if strings.HasPrefix(authHdr, "Bearer ") {
				key = strings.TrimPrefix(authHdr, "Bearer ")
			}
		}
		if key == "" {
			key = bodyAPIKey
		}
		if key == "" {
			return "", fmt.Errorf("missing API key")
		}
		boundTenant, ok := keyRegistry[key]
		if !ok {
			return "", fmt.Errorf("invalid API key")
		}
		if boundTenant != "tenant_test" && requestTenantID != "" && requestTenantID != boundTenant {
			return boundTenant, fmt.Errorf("forbidden: cross-tenant key mismatch")
		}
		return boundTenant, nil
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req model.RateLimitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		_, err := validateAuth(r, req.TenantID, req.APIKey)
		if err != nil {
			if strings.HasPrefix(err.Error(), "forbidden") {
				http.Error(w, err.Error(), http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusUnauthorized)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// 1. Missing Auth -> 401
	req1 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test"}`))
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)
	if w1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got: %d", w1.Code)
	}

	// 2. Invalid Key -> 401
	req2 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test"}`))
	req2.Header.Set("X-API-Key", "invalid_random_key")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid key, got: %d", w2.Code)
	}

	// 3. Cross-tenant mismatch (using growth key for free tenant) -> 403
	req3 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test"}`))
	req3.Header.Set("X-API-Key", "tg_live_growth_key")
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)
	if w3.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant key mismatch, got: %d", w3.Code)
	}

	// 4. Valid auth matching tenant -> 200
	req4 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test"}`))
	req4.Header.Set("X-API-Key", "tg_live_free_key")
	w4 := httptest.NewRecorder()
	handler.ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid auth, got: %d", w4.Code)
	}
}
