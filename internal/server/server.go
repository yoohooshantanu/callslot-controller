package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yoohooshantanu/concurrency-governor/internal/limiter"
	"github.com/yoohooshantanu/concurrency-governor/internal/metrics"
	"github.com/yoohooshantanu/concurrency-governor/internal/model"
)

// InitRedis creates and validates a Redis client connection.
// If redisAddr is configured, failing the initial ping returns an error to enforce fail-closed startup.
func InitRedis(redisAddr string) (*redis.Client, error) {
	if redisAddr == "" {
		return nil, nil // Intentional standalone in-memory mode
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis startup ping failed for %s: %w", redisAddr, err)
	}

	return rdb, nil
}

// LoadProductionKeyRegistry initializes key registry strictly from environment configuration
func LoadProductionKeyRegistry() map[string]string {
	keys := make(map[string]string)
	if envKey := os.Getenv("TENANTGATE_API_KEY"); envKey != "" {
		keys[envKey] = "tenant_production"
	}
	if multiKeys := os.Getenv("TENANTGATE_API_KEYS"); multiKeys != "" {
		pairs := strings.Split(multiKeys, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
				keys[parts[0]] = parts[1]
			}
		}
	}
	return keys
}

// ValidateAuth extracts and verifies the API key, enforcing strict 1:1 tenant binding without exceptions
func ValidateAuth(r *http.Request, keyRegistry map[string]string, requestTenantID, bodyAPIKey string) (string, error) {
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

	// -------
	// Strictly enforce tenant ownership: no bypasses or test exceptions permitted
	// -------
	if requestTenantID != "" && requestTenantID != boundTenant {
		return boundTenant, fmt.Errorf("forbidden: cross-tenant key mismatch")
	}

	return boundTenant, nil
}

// SetupRouter creates the HTTP handler mux using the provided engine and key registry
func SetupRouter(engine *limiter.Engine, keyRegistry map[string]string) *http.ServeMux {
	mux := http.NewServeMux()

	// -------
	// High-speed Check API (RateLimit policy decision)
	// -------
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

		_, authErr := ValidateAuth(r, keyRegistry, req.TenantID, req.APIKey)
		if authErr != nil {
			statusCode := http.StatusUnauthorized
			if strings.HasPrefix(authErr.Error(), "forbidden") {
				statusCode = http.StatusForbidden
			}
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), statusCode)
			return
		}

		resp, err := engine.Check(r.Context(), &req)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusServiceUnavailable)
			return
		}

		// -------
		// Standard IETF rate limit response headers
		// -------
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

	// -------
	// Release concurrent call session (idempotent by reservation ID)
	// -------
	mux.HandleFunc("/v1/release", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB payload ceiling
		var req model.ReleaseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" {
			http.Error(w, `{"error":"tenant_id required"}`, http.StatusBadRequest)
			return
		}

		if req.ReservationID == "" {
			http.Error(w, `{"error":"reservation_id required for release"}`, http.StatusBadRequest)
			return
		}

		// Authenticate request
		_, authErr := ValidateAuth(r, keyRegistry, req.TenantID, req.APIKey)
		if authErr != nil {
			statusCode := http.StatusUnauthorized
			if strings.HasPrefix(authErr.Error(), "forbidden") {
				statusCode = http.StatusForbidden
			}
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), statusCode)
			return
		}

		released := engine.ReleaseCall(req.TenantID, req.ReservationID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "released",
			"released": released,
		})
	})

	// -------
	// Renew concurrent call session lease
	// -------
	mux.HandleFunc("/v1/renew", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB payload ceiling
		var req model.RenewRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" {
			http.Error(w, `{"error":"tenant_id required"}`, http.StatusBadRequest)
			return
		}

		if req.ReservationID == "" {
			http.Error(w, `{"error":"reservation_id required for renewal"}`, http.StatusBadRequest)
			return
		}

		_, authErr := ValidateAuth(r, keyRegistry, req.TenantID, req.APIKey)
		if authErr != nil {
			statusCode := http.StatusUnauthorized
			if strings.HasPrefix(authErr.Error(), "forbidden") {
				statusCode = http.StatusForbidden
			}
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, authErr.Error()), statusCode)
			return
		}

		ttl := req.TTLSeconds
		if ttl <= 0 {
			ttl = limiter.DefaultReservationTTL
		}

		renewed := engine.RenewReservation(req.TenantID, req.ReservationID, ttl)
		w.Header().Set("Content-Type", "application/json")
		if !renewed {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(model.RenewResponse{
				TenantID:      req.TenantID,
				ReservationID: req.ReservationID,
				Renewed:       false,
				TTLSeconds:    0,
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(model.RenewResponse{
			TenantID:      req.TenantID,
			ReservationID: req.ReservationID,
			Renewed:       true,
			TTLSeconds:    ttl,
		})
	})

	// -------
	// Telemetry and observability endpoints
	// -------
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

	return mux
}
