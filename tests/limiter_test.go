package tests

import (
	"context"
	"sync"
	"testing"
	"time"

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

func BenchmarkTenantGateL1Check(b *testing.B) {
	engine := limiter.NewEngine(nil)
	ctx := context.Background()
	req := &model.RateLimitRequest{
		TenantID: "tenant_bench",
		Resource: "audio.chunk",
		Cost:     1,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = engine.Check(ctx, req)
		}
	})
}
