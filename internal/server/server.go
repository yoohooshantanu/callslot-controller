package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yoohooshantanu/tenantgate/internal/limiter"
	"github.com/yoohooshantanu/tenantgate/internal/metrics"
	"github.com/yoohooshantanu/tenantgate/internal/model"
)

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

	// Strictly enforce tenant ownership: No bypasses or test exceptions permitted
	if requestTenantID != "" && requestTenantID != boundTenant {
		return boundTenant, fmt.Errorf("forbidden: cross-tenant key mismatch")
	}

	return boundTenant, nil
}

// SetupRouter creates the HTTP handler mux using the provided engine and key registry
func SetupRouter(engine *limiter.Engine, keyRegistry map[string]string) *http.ServeMux {
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

	// Release concurrent call session: /v1/release (Idempotent by Reservation ID)
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

	return mux
}
