package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
