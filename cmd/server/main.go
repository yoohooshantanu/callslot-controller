package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/tenantgate/internal/limiter"
	"github.com/yoohooshantanu/tenantgate/internal/logger"
	"github.com/yoohooshantanu/tenantgate/internal/metrics"
	"github.com/yoohooshantanu/tenantgate/internal/model"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

var keyRegistry = map[string]string{
	"tg_test_key_123":    "tenant_test",
	"tg_live_free_key":   "tenant_free_test",
	"tg_live_growth_key": "tenant_call_cap",
}

func init() {
	if envKey := os.Getenv("TENANTGATE_API_KEY"); envKey != "" {
		keyRegistry[envKey] = "tenant_production"
	}
}

// validateAuth extracts and verifies the API key, checking tenant binding
func validateAuth(r *http.Request, requestTenantID, bodyAPIKey string) (string, error) {
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

	// If key is bound to a specific tenant, ensure request tenant matches
	if boundTenant != "tenant_test" && requestTenantID != "" && requestTenantID != boundTenant {
		return boundTenant, fmt.Errorf("forbidden: cross-tenant key mismatch")
	}

	return boundTenant, nil
}

func main() {
	port := getEnv("PORT", "8082")
	redisAddr := os.Getenv("REDIS_ADDR")

	logger.L.Info("starting_tenantgate", "port", port)

	var rdb *redis.Client
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.L.Warn("redis_ping_failed_falling_back_to_l1", "error", err)
			rdb = nil
		} else {
			logger.L.Info("redis_connected", "addr", redisAddr)
		}
		cancel()
	}

	engine := limiter.NewEngine(rdb)

	mux := http.NewServeMux()

	// High-speed Check API (RateLimit policy decision)
	mux.HandleFunc("/v1/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB payload ceiling
		var req model.RateLimitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
			return
		}

		// Authenticate request
		_, authErr := validateAuth(r, req.TenantID, req.APIKey)
		if authErr != nil {
			status := http.StatusUnauthorized
			if strings.HasPrefix(authErr.Error(), "forbidden") {
				status = http.StatusForbidden
			}
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), status)
			return
		}

		resp, err := engine.Check(r.Context(), &req)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusBadRequest)
			return
		}

		// Set IETF standard rate limiting response headers
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("RateLimit-Limit", strconv.FormatInt(resp.Limit, 10))
		w.Header().Set("RateLimit-Remaining", strconv.FormatInt(resp.RemainingQuota, 10))
		w.Header().Set("RateLimit-Reset", strconv.FormatInt(resp.ResetAfterSeconds, 10))

		if !resp.Allowed {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		_ = json.NewEncoder(w).Encode(resp)
	})

	// Release concurrent call session: /v1/release
	mux.HandleFunc("/v1/release", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB payload ceiling
		var req struct {
			TenantID string `json:"tenant_id"`
			APIKey   string `json:"api_key,omitempty"`
			Count    int64  `json:"count,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" {
			http.Error(w, `{"error":"tenant_id required"}`, http.StatusBadRequest)
			return
		}

		// Authenticate request
		_, authErr := validateAuth(r, req.TenantID, req.APIKey)
		if authErr != nil {
			status := http.StatusUnauthorized
			if strings.HasPrefix(authErr.Error(), "forbidden") {
				status = http.StatusForbidden
			}
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), status)
			return
		}

		engine.ReleaseCall(req.TenantID, req.Count)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "released"})
	})

	// Telemetry & Health (Unauthenticated endpoints)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	// pprof
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.L.Info("tenantgate_listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_tenantgate")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	engine.Close()
	logger.L.Info("tenantgate_terminated_cleanly")
}
