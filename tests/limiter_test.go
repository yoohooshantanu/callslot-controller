package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/tenantgate/internal/limiter"
	"github.com/yoohooshantanu/tenantgate/internal/model"
	"github.com/yoohooshantanu/tenantgate/internal/server"
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

func TestConcurrentCallsCapEnforcementWithReservationID(t *testing.T) {
	engine := limiter.NewEngine(nil)

	engine.SetTenantPolicy("tenant_call_cap", &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 3,
		BurstCapacity:      100,
		RefillRatePerSec:   10.0,
	})

	ctx := context.Background()

	var reservationIDs []string

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
		if resp.ReservationID == "" {
			t.Fatalf("expected non-empty reservation ID for call %d", i+1)
		}
		reservationIDs = append(reservationIDs, resp.ReservationID)
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

	// Attempt release with non-existent/fake reservation ID -> must fail and NOT decrement
	fakeReleased := engine.ReleaseCall("tenant_call_cap", "res_invalid_fake_id")
	if fakeReleased {
		t.Fatalf("expected release with invalid reservation ID to return false")
	}

	// Active calls must still be 3
	active, _ := engine.GetActiveCalls(ctx, "tenant_call_cap")
	if active != 3 {
		t.Fatalf("active calls should remain 3 after fake release, got %d", active)
	}

	// Release 1 valid call using exact Reservation ID
	validReleased := engine.ReleaseCall("tenant_call_cap", reservationIDs[0])
	if !validReleased {
		t.Fatalf("expected release of valid reservation ID to return true")
	}

	// Duplicate release of same reservation ID -> idempotent, must return false
	dupReleased := engine.ReleaseCall("tenant_call_cap", reservationIDs[0])
	if dupReleased {
		t.Fatalf("duplicate release of same reservation ID must return false")
	}

	// Active calls must now be 2
	active, _ = engine.GetActiveCalls(ctx, "tenant_call_cap")
	if active != 2 {
		t.Fatalf("active calls should be 2, got %d", active)
	}

	// Retry initiation, should now succeed and return a new reservation ID
	retryResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_call_cap",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !retryResp.Allowed {
		t.Fatalf("expected call to succeed after releasing concurrency slot: %+v", retryResp)
	}
	if retryResp.ReservationID == "" {
		t.Fatalf("expected valid new reservation ID on retry initiation")
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

	// Active calls must still be 1 (second call rolled back cleanly)
	active, _ := engine.GetActiveCalls(ctx, "tenant_leak_test")
	if active != 1 {
		t.Fatalf("expected 1 active call after rollback, got %d", active)
	}

	// Release the 1 successful call
	engine.ReleaseCall("tenant_leak_test", resp1.ReservationID)

	active, _ = engine.GetActiveCalls(ctx, "tenant_leak_test")
	if active != 0 {
		t.Fatalf("expected 0 active calls after release, got %d", active)
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

func TestLiveRedisDistributedReservationIDCallCap(t *testing.T) {
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
		MaxConcurrentCalls: 2, // Global limit across all cluster instances
		BurstCapacity:      100,
		RefillRatePerSec:   100.0,
	}
	engineInstanceA.SetTenantPolicy(tenantID, policy)
	engineInstanceB.SetTenantPolicy(tenantID, policy)

	// Instance A reserves call 1
	resp1, err := engineInstanceA.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp1.Allowed || resp1.ReservationID == "" {
		t.Fatalf("Instance A failed reserving call 1: %+v, err: %v", resp1, err)
	}

	// Instance B reserves call 2
	resp2, err := engineInstanceB.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp2.Allowed || resp2.ReservationID == "" {
		t.Fatalf("Instance B failed reserving call 2: %+v, err: %v", resp2, err)
	}

	// Instance A attempts call 3 (1 + 1 + 1 = 3 > 2 -> Must be rejected!)
	resp3, err := engineInstanceA.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil {
		t.Fatalf("Instance A check returned error: %v", err)
	}
	if resp3.Allowed {
		t.Fatalf("expected call 3 to be rejected by global Redis concurrency ceiling (3 > 2)")
	}

	// Instance A releases call 1 using exact reservation ID
	released := engineInstanceA.ReleaseCall(tenantID, resp1.ReservationID)
	if !released {
		t.Fatalf("expected successful release of call 1 in Redis")
	}

	// Now Instance A attempts call 3 again (Active is 1, 1 + 1 = 2 <= 2 -> Must succeed!)
	resp4, err := engineInstanceA.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp4.Allowed || resp4.ReservationID == "" {
		t.Fatalf("expected call 3 to succeed after call 1 released: %+v, err: %v", resp4, err)
	}
}

func TestFailClosedRedisPolicy(t *testing.T) {
	// Connect to a deliberately non-existent Redis port to test fail-closed semantics
	deadRdb := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:54321", // dead port
		DialTimeout: 50 * time.Millisecond,
	})
	defer deadRdb.Close()

	engine := limiter.NewEngine(deadRdb)
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Check on dead Redis must FAIL CLOSED and return an error
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: "tenant_fail_closed_test",
		Resource: "call.initiate",
		Cost:     1,
	})
	if err == nil {
		t.Fatalf("expected error from fail-closed Redis policy when Redis is unreachable")
	}
	if resp != nil && resp.Allowed {
		t.Fatalf("fail-closed policy must NOT allow requests when Redis is unavailable")
	}
}

func TestProductionRegistryStartsEmpty(t *testing.T) {
	// Verify that server.LoadProductionKeyRegistry() starts completely empty
	reg := server.LoadProductionKeyRegistry()

	hardcodedKeys := []string{
		"tg_test_key_123",
		"tg_live_free_key",
		"tg_live_growth_key",
	}

	for _, k := range hardcodedKeys {
		if tenant, found := reg[k]; found {
			t.Fatalf("production registry must not contain hardcoded default credential %q (mapped to %q)", k, tenant)
		}
	}
}

func TestHTTPProductionRouterStrictAuthentication(t *testing.T) {
	engine := limiter.NewEngine(nil)
	defer engine.Close()

	engine.SetTenantPolicy("tenant_free_test", &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  60,
		MaxConcurrentCalls: 5,
		BurstCapacity:      10,
		RefillRatePerSec:   1.0,
	})

	testKeyRegistry := map[string]string{
		"tg_test_key_123":    "tenant_test",
		"tg_live_free_key":   "tenant_free_test",
		"tg_live_growth_key": "tenant_call_cap",
	}

	// Directly exercise the actual production router
	mux := server.SetupRouter(engine, testKeyRegistry)

	// 1. Missing Auth -> 401
	req1 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test","resource":"audio.chunk"}`))
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)
	if w1.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got: %d", w1.Code)
	}

	// 2. Invalid Key -> 401
	req2 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test","resource":"audio.chunk"}`))
	req2.Header.Set("X-API-Key", "invalid_random_key")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid key, got: %d", w2.Code)
	}

	// 3. Proves test-key bypass is eliminated: tg_test_key_123 cannot request tenant_free_test -> 403 Forbidden!
	req3 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test","resource":"audio.chunk"}`))
	req3.Header.Set("X-API-Key", "tg_test_key_123")
	w3 := httptest.NewRecorder()
	mux.ServeHTTP(w3, req3)
	if w3.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden when tg_test_key_123 requests tenant_free_test, got: %d", w3.Code)
	}

	// 4. Cross-tenant mismatch (growth key for free tenant) -> 403 Forbidden
	req4 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test","resource":"audio.chunk"}`))
	req4.Header.Set("X-API-Key", "tg_live_growth_key")
	w4 := httptest.NewRecorder()
	mux.ServeHTTP(w4, req4)
	if w4.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant key mismatch, got: %d", w4.Code)
	}

	// 5. Valid auth matching tenant -> 200 OK with Reservation ID on call.initiate
	req5 := httptest.NewRequest(http.MethodPost, "/v1/check", bytes.NewBufferString(`{"tenant_id":"tenant_free_test","resource":"call.initiate"}`))
	req5.Header.Set("X-API-Key", "tg_live_free_key")
	w5 := httptest.NewRecorder()
	mux.ServeHTTP(w5, req5)
	if w5.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid auth, got: %d, body: %s", w5.Code, w5.Body.String())
	}

	var checkResp model.RateLimitResponse
	_ = json.NewDecoder(w5.Body).Decode(&checkResp)
	if checkResp.ReservationID == "" {
		t.Fatalf("expected non-empty reservation ID in response")
	}

	// 6. Renew endpoint with valid reservation ID -> 200 OK with renewed: true
	renewPayload := fmt.Sprintf(`{"tenant_id":"tenant_free_test","reservation_id":"%s","ttl_seconds":7200}`, checkResp.ReservationID)
	reqRenew := httptest.NewRequest(http.MethodPost, "/v1/renew", bytes.NewBufferString(renewPayload))
	reqRenew.Header.Set("X-API-Key", "tg_live_free_key")
	wRenew := httptest.NewRecorder()
	mux.ServeHTTP(wRenew, reqRenew)
	if wRenew.Code != http.StatusOK {
		t.Fatalf("expected 200 on renew, got: %d, body: %s", wRenew.Code, wRenew.Body.String())
	}

	var renewResp model.RenewResponse
	_ = json.NewDecoder(wRenew.Body).Decode(&renewResp)
	if !renewResp.Renewed || renewResp.ReservationID != checkResp.ReservationID {
		t.Fatalf("expected renewed: true for valid reservation, got: %+v", renewResp)
	}

	// 7. Renew endpoint with cross-tenant key mismatch -> 403 Forbidden
	reqRenewCross := httptest.NewRequest(http.MethodPost, "/v1/renew", bytes.NewBufferString(renewPayload))
	reqRenewCross.Header.Set("X-API-Key", "tg_live_growth_key")
	wRenewCross := httptest.NewRecorder()
	mux.ServeHTTP(wRenewCross, reqRenewCross)
	if wRenewCross.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on cross-tenant renew, got: %d", wRenewCross.Code)
	}

	// 8. Release endpoint with valid reservation ID -> 200 OK
	relPayload := fmt.Sprintf(`{"tenant_id":"tenant_free_test","reservation_id":"%s"}`, checkResp.ReservationID)
	req6 := httptest.NewRequest(http.MethodPost, "/v1/release", bytes.NewBufferString(relPayload))
	req6.Header.Set("X-API-Key", "tg_live_free_key")
	w6 := httptest.NewRecorder()
	mux.ServeHTTP(w6, req6)
	if w6.Code != http.StatusOK {
		t.Fatalf("expected 200 on release, got: %d, body: %s", w6.Code, w6.Body.String())
	}

	// 9. Renew endpoint for released/expired reservation ID -> 404 Not Found with renewed: false
	reqRenewAfterRel := httptest.NewRequest(http.MethodPost, "/v1/renew", bytes.NewBufferString(renewPayload))
	reqRenewAfterRel.Header.Set("X-API-Key", "tg_live_free_key")
	wRenewAfterRel := httptest.NewRecorder()
	mux.ServeHTTP(wRenewAfterRel, reqRenewAfterRel)
	if wRenewAfterRel.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on renewing released reservation, got: %d", wRenewAfterRel.Code)
	}
}

func TestFailClosedRedisStartup(t *testing.T) {
	// If REDIS_ADDR is configured but unreachable, InitRedis MUST return an error rather than silently falling back
	_, err := server.InitRedis("127.0.0.1:54321") // dead port
	if err == nil {
		t.Fatalf("expected InitRedis to fail closed with error when Redis is unreachable")
	}

	// If REDIS_ADDR is empty, standalone in-memory mode is accepted
	client, err := server.InitRedis("")
	if err != nil || client != nil {
		t.Fatalf("expected nil client and nil error for empty redisAddr")
	}
}

func TestPerReservationExpiryNoReset(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping live Redis per-reservation test: REDIS_ADDR not set")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping live Redis test: cannot ping Redis at %s: %v", redisAddr, err)
	}

	engine := limiter.NewEngine(rdb)
	defer engine.Close()

	tenantID := fmt.Sprintf("tenant_per_res_%d", time.Now().UnixNano())
	callKey := fmt.Sprintf("cc:res:%s", tenantID)

	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 3,
		BurstCapacity:      100,
		RefillRatePerSec:   100.0,
	})

	// 1. Reserve Active Call 1
	resp1, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp1.Allowed || resp1.ReservationID == "" {
		t.Fatalf("failed reserving Call 1: %+v, err: %v", resp1, err)
	}

	// 2. Inject an already-expired reservation into the ZSET (score in past)
	pastSec := float64(time.Now().Unix() - 100)
	err = rdb.ZAdd(ctx, callKey, redis.Z{Score: pastSec, Member: "res_abandoned_past_id"}).Err()
	if err != nil {
		t.Fatalf("failed injecting past reservation: %v", err)
	}

	// 3. Reserve Active Call 2: Engine must purge the expired past reservation while keeping Call 1 intact!
	resp2, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp2.Allowed || resp2.ReservationID == "" {
		t.Fatalf("failed reserving Call 2: %+v, err: %v", resp2, err)
	}

	// 4. Verify Call 1 is STILL valid in Redis and active calls count is 2 (Call 1 + Call 2)
	active, err := engine.GetActiveCalls(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetActiveCalls failed: %v", err)
	}
	if active != 2 {
		t.Fatalf("expected exactly 2 active calls (expired reservation pruned, Call 1 preserved), got: %d", active)
	}

	// 5. Ensure Call 1 can be successfully released by its Reservation ID
	released := engine.ReleaseCall(tenantID, resp1.ReservationID)
	if !released {
		t.Fatalf("expected successful release of Call 1, proving it was not wiped out by expiry")
	}

	active, _ = engine.GetActiveCalls(ctx, tenantID)
	if active != 1 {
		t.Fatalf("expected 1 active call remaining after Call 1 released, got: %d", active)
	}
}

func TestLiveRedisReservationRenewal(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping live Redis reservation renewal test: REDIS_ADDR not set")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping live Redis test: cannot ping Redis at %s: %v", redisAddr, err)
	}

	engine := limiter.NewEngine(rdb)
	defer engine.Close()

	tenantID := fmt.Sprintf("tenant_renew_live_%d", time.Now().UnixNano())
	callKey := fmt.Sprintf("cc:res:%s", tenantID)

	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierGrowth,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 5,
		BurstCapacity:      100,
		RefillRatePerSec:   100.0,
	})

	// 1. Reserve Call slot
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp.Allowed || resp.ReservationID == "" {
		t.Fatalf("failed reserving initial call: %+v, err: %v", resp, err)
	}

	// 2. Renew reservation with custom TTL (e.g. 7200s)
	nowBefore := time.Now().Unix()
	renewed := engine.RenewReservation(tenantID, resp.ReservationID, 7200)
	if !renewed {
		t.Fatalf("expected successful renewal for active reservation %s", resp.ReservationID)
	}

	// 3. Inspect Redis ZSET score
	score, err := rdb.ZScore(ctx, callKey, resp.ReservationID).Result()
	if err != nil {
		t.Fatalf("failed getting score from Redis: %v", err)
	}
	expectedMinScore := float64(nowBefore + 7195)
	if score < expectedMinScore {
		t.Fatalf("expected renewed score >= %f, got %f", expectedMinScore, score)
	}

	// 4. Renew non-existent reservation ID -> must return false (no phantom reservation created!)
	fakeRenewed := engine.RenewReservation(tenantID, "res_phantom_fake_id", 3600)
	if fakeRenewed {
		t.Fatalf("expected renew to return false for non-existent reservation")
	}
	phantomScore, err := rdb.ZScore(ctx, callKey, "res_phantom_fake_id").Result()
	if err == nil || phantomScore > 0 {
		t.Fatalf("ZADD XX must never insert non-existent reservation into Redis ZSET!")
	}

	// 5. Release reservation
	released := engine.ReleaseCall(tenantID, resp.ReservationID)
	if !released {
		t.Fatalf("expected successful release")
	}

	// 6. Renew after release -> must return false
	afterReleaseRenewed := engine.RenewReservation(tenantID, resp.ReservationID, 3600)
	if afterReleaseRenewed {
		t.Fatalf("expected renew to fail after reservation was released")
	}
}

func TestFailClosedRedisMidRun(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping mid-run Redis failure test: REDIS_ADDR not set")
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed starting proxy listener: %v", err)
	}
	proxyAddr := lis.Addr().String()

	var activeConnsMu sync.Mutex
	var activeConns []net.Conn

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			clientConn, aErr := lis.Accept()
			if aErr != nil {
				return
			}
			targetConn, dErr := net.DialTimeout("tcp", redisAddr, time.Second)
			if dErr != nil {
				_ = clientConn.Close()
				return
			}

			activeConnsMu.Lock()
			activeConns = append(activeConns, clientConn, targetConn)
			activeConnsMu.Unlock()

			go func() {
				_, _ = io.Copy(targetConn, clientConn)
			}()
			go func() {
				_, _ = io.Copy(clientConn, targetConn)
			}()
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr:        proxyAddr,
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
		MaxRetries:  0,
	})
	engine := limiter.NewEngine(rdb)
	defer engine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	tenantID := fmt.Sprintf("tenant_midrun_%d", time.Now().UnixNano())

	// 1. Initial request while Redis is running succeeds
	resp1, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp1.Allowed {
		t.Fatalf("expected initial check to succeed, got: %+v, err: %v", resp1, err)
	}

	// 2. Abruptly kill Redis mid-run: close proxy listener and sever all active sockets
	_ = lis.Close()
	activeConnsMu.Lock()
	for _, c := range activeConns {
		_ = c.Close()
	}
	activeConnsMu.Unlock()
	<-serverDone

	// 3. Subsequent request mid-run MUST fail closed and return ErrRedisFailure
	resp2, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err == nil {
		t.Fatalf("expected error from fail-closed policy when Redis drops mid-run")
	}
	if resp2 != nil && resp2.Allowed {
		t.Fatalf("fail-closed policy must NOT allow requests when Redis dies mid-run")
	}
	if !errors.Is(err, model.ErrRedisFailure) {
		t.Fatalf("expected ErrRedisFailure, got: %v", err)
	}
}

func TestRenewalExpiredOrUnknownNeverRecreates(t *testing.T) {
	// A. In-Memory Mode
	engineMem := limiter.NewEngine(nil)
	defer engineMem.Close()

	tenantID := "tenant_renew_test"

	// 1. Unknown reservation returns false and does not create an active reservation
	if renewed := engineMem.RenewReservation(tenantID, "res_unknown_ghost_id", 3600); renewed {
		t.Fatalf("expected in-memory renew of unknown ID to return false")
	}
	active, _ := engineMem.GetActiveCalls(context.Background(), tenantID)
	if active != 0 {
		t.Fatalf("expected 0 active calls after failed renewal of unknown ID, got %d", active)
	}

	// B. Redis Mode (if REDIS_ADDR available)
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		return
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return
	}

	engineRedis := limiter.NewEngine(rdb)
	defer engineRedis.Close()

	redisTenant := fmt.Sprintf("tenant_renew_unit_%d", time.Now().UnixNano())
	callKey := fmt.Sprintf("cc:res:%s", redisTenant)

	// 2. Unknown reservation on Redis ZSET returns false and creates no member
	if renewed := engineRedis.RenewReservation(redisTenant, "res_phantom_zset_id", 3600); renewed {
		t.Fatalf("expected Redis renew of unknown ID to return false")
	}
	score, err := rdb.ZScore(ctx, callKey, "res_phantom_zset_id").Result()
	if err == nil || score > 0 {
		t.Fatalf("phantom member must not be inserted into Redis ZSET")
	}

	// 3. Expired reservation (score in past) returns false and is pruned from ZSET
	pastScore := float64(time.Now().Unix() - 60)
	_ = rdb.ZAdd(ctx, callKey, redis.Z{Score: pastScore, Member: "res_expired_zset_id"}).Err()

	if renewed := engineRedis.RenewReservation(redisTenant, "res_expired_zset_id", 3600); renewed {
		t.Fatalf("expected renew of expired reservation to return false")
	}
	// Assert member is pruned
	_, err = rdb.ZScore(ctx, callKey, "res_expired_zset_id").Result()
	if err == nil {
		t.Fatalf("expired reservation must be pruned upon renewal attempt")
	}
}

func TestConcurrentGoroutinesReservingAtCap(t *testing.T) {
	// Tests N goroutines concurrently attempting to reserve at capacity
	engine := limiter.NewEngine(nil)
	defer engine.Close()

	tenantID := "tenant_race_cap"
	maxCap := int64(5)

	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierGrowth,
		RequestsPerMinute:  100000,
		MaxConcurrentCalls: maxCap,
		BurstCapacity:      200,
		RefillRatePerSec:   1000.0,
	})

	ctx := context.Background()
	totalGoroutines := 50

	startCh := make(chan struct{})
	var wg sync.WaitGroup

	var mu sync.Mutex
	var successfulIDs []string
	var blockedCount int

	for i := 0; i < totalGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh // Align all goroutines to hit Check simultaneously

			resp, err := engine.Check(ctx, &model.RateLimitRequest{
				TenantID: tenantID,
				Resource: "call.initiate",
				Cost:     1,
			})
			if err != nil {
				return
			}

			mu.Lock()
			if resp.Allowed && resp.ReservationID != "" {
				successfulIDs = append(successfulIDs, resp.ReservationID)
			} else {
				blockedCount++
			}
			mu.Unlock()
		}()
	}

	// Release all goroutines simultaneously
	close(startCh)
	wg.Wait()

	// Assert exactly maxCap succeeded, no more
	if int64(len(successfulIDs)) != maxCap {
		t.Fatalf("expected exactly %d successful reservations under race, got %d", maxCap, len(successfulIDs))
	}
	if blockedCount != totalGoroutines-int(maxCap) {
		t.Fatalf("expected %d blocked requests, got %d", totalGoroutines-int(maxCap), blockedCount)
	}

	active, _ := engine.GetActiveCalls(ctx, tenantID)
	if active != maxCap {
		t.Fatalf("expected active calls to be %d, got %d", maxCap, active)
	}

	// Releasing 1 slot allows exactly 1 more reservation
	engine.ReleaseCall(tenantID, successfulIDs[0])
	nextResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !nextResp.Allowed {
		t.Fatalf("expected reservation to succeed after releasing 1 slot: %+v", nextResp)
	}
}

func TestReleaseUnknownReservationNoOp(t *testing.T) {
	engine := limiter.NewEngine(nil)
	defer engine.Close()

	tenantID := "tenant_noop_release"
	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  60,
		MaxConcurrentCalls: 3,
		BurstCapacity:      10,
		RefillRatePerSec:   1.0,
	})

	ctx := context.Background()

	// 1. Reserve legitimate call slot
	resp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp.Allowed || resp.ReservationID == "" {
		t.Fatalf("failed reserving initial call: %+v", resp)
	}

	// Active calls must be 1
	active, _ := engine.GetActiveCalls(ctx, tenantID)
	if active != 1 {
		t.Fatalf("expected 1 active call, got %d", active)
	}

	// 2. Release an unknown reservation ID -> safe no-op
	released := engine.ReleaseCall(tenantID, "res_completely_fake_id_999")
	if released {
		t.Fatalf("releasing unknown reservation ID must return false")
	}

	// Active calls must remain exactly 1 (not decremented)
	activeAfter, _ := engine.GetActiveCalls(ctx, tenantID)
	if activeAfter != 1 {
		t.Fatalf("active calls must remain 1 after no-op release, got %d", activeAfter)
	}

	// 3. Legitimate reservation can still be released cleanly
	validReleased := engine.ReleaseCall(tenantID, resp.ReservationID)
	if !validReleased {
		t.Fatalf("expected legitimate reservation to release cleanly")
	}

	activeFinal, _ := engine.GetActiveCalls(ctx, tenantID)
	if activeFinal != 0 {
		t.Fatalf("expected 0 active calls after legitimate release, got %d", activeFinal)
	}
}

func TestReservationExpiryFreesCapacity(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("Skipping live Redis reservation expiry frees capacity test: REDIS_ADDR not set")
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("cannot ping Redis: %v", err)
	}

	engine := limiter.NewEngine(rdb)
	defer engine.Close()

	tenantID := fmt.Sprintf("tenant_expiry_frees_%d", time.Now().UnixNano())
	callKey := fmt.Sprintf("cc:res:%s", tenantID)

	engine.SetTenantPolicy(tenantID, &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 1, // Cap is strictly 1
		BurstCapacity:      100,
		RefillRatePerSec:   100.0,
	})

	// 1. Reserve Call 1 -> Cap full (1/1)
	resp1, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp1.Allowed || resp1.ReservationID == "" {
		t.Fatalf("failed reserving Call 1: %+v", resp1)
	}

	// 2. Call 2 immediately rejected (1/1 full)
	blockedResp, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || blockedResp.Allowed {
		t.Fatalf("expected Call 2 to be blocked by capacity 1/1")
	}

	// 3. Expire Call 1 by setting score to the past
	pastSec := float64(time.Now().Unix() - 10)
	err = rdb.ZAdd(ctx, callKey, redis.Z{Score: pastSec, Member: resp1.ReservationID}).Err()
	if err != nil {
		t.Fatalf("failed setting past score: %v", err)
	}

	// 4. Reserve Call 2 again: engine prunes Call 1, sees active == 0 < 1, and grants slot!
	resp2, err := engine.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "call.initiate",
		Cost:     1,
	})
	if err != nil || !resp2.Allowed || resp2.ReservationID == "" {
		t.Fatalf("expected Call 2 to succeed after Call 1 expired: %+v, err: %v", resp2, err)
	}

	// 5. Active calls count is exactly 1 (Call 2 only)
	active, err := engine.GetActiveCalls(ctx, tenantID)
	if err != nil || active != 1 {
		t.Fatalf("expected exactly 1 active call, got %d", active)
	}

	// Call 1 cannot be released (already expired/pruned); Call 2 releases cleanly
	if engine.ReleaseCall(tenantID, resp1.ReservationID) {
		t.Fatalf("releasing already-pruned Call 1 should return false")
	}
	if !engine.ReleaseCall(tenantID, resp2.ReservationID) {
		t.Fatalf("releasing active Call 2 should return true")
	}
}

func TestL1BurstScalesPerInstanceVsSharedL2(t *testing.T) {
	// Demonstrates the architectural trade-off documented in README:
	// L1 Token Bucket is process-local (<869 ns/op).
	// Two separate Engine instances each have their own L1 cache.
	// Therefore, aggregate instantaneous burst capacity across N instances = N * BurstCapacity.

	engineInstance1 := limiter.NewEngine(nil)
	defer engineInstance1.Close()

	engineInstance2 := limiter.NewEngine(nil)
	defer engineInstance2.Close()

	tenantID := "tenant_multi_instance_burst"
	burstCap := int64(3)

	policy := &model.QuotaPolicy{
		Tier:               model.TierFree,
		RequestsPerMinute:  1000,
		MaxConcurrentCalls: 10,
		BurstCapacity:      burstCap,
		RefillRatePerSec:   0.0001, // Negligible refill so burst depletion is deterministic
	}

	engineInstance1.SetTenantPolicy(tenantID, policy)
	engineInstance2.SetTenantPolicy(tenantID, policy)

	ctx := context.Background()

	// 1. Instance 1 exhausts its 3 burst tokens
	for i := 0; i < int(burstCap); i++ {
		resp, err := engineInstance1.Check(ctx, &model.RateLimitRequest{
			TenantID: tenantID,
			Resource: "audio.chunk",
			Cost:     1,
		})
		if err != nil || !resp.Allowed {
			t.Fatalf("Instance 1 request %d should be allowed: %+v", i+1, resp)
		}
	}

	// 4th request on Instance 1 is rejected (L1 bucket depleted)
	blocked1, err := engineInstance1.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "audio.chunk",
		Cost:     1,
	})
	if err != nil || blocked1.Allowed {
		t.Fatalf("expected 4th request on Instance 1 to be blocked by depleted L1 burst")
	}

	// 2. Instance 2 immediately receives requests: its own local L1 cache has 3 fresh burst tokens!
	for i := 0; i < int(burstCap); i++ {
		resp, err := engineInstance2.Check(ctx, &model.RateLimitRequest{
			TenantID: tenantID,
			Resource: "audio.chunk",
			Cost:     1,
		})
		if err != nil || !resp.Allowed {
			t.Fatalf("Instance 2 request %d should be allowed independently: %+v", i+1, resp)
		}
	}

	// 4th request on Instance 2 is also rejected
	blocked2, err := engineInstance2.Check(ctx, &model.RateLimitRequest{
		TenantID: tenantID,
		Resource: "audio.chunk",
		Cost:     1,
	})
	if err != nil || blocked2.Allowed {
		t.Fatalf("expected 4th request on Instance 2 to be blocked by depleted L1 burst")
	}

	// Combined instantaneous burst absorbed = 3 (Instance 1) + 3 (Instance 2) = 6 = 2 * BurstCapacity
}
