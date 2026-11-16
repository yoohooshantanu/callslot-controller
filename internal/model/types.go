package model

import (
	"errors"
	"time"
)

var (
	ErrTenantNotFound = errors.New("tenant tier configuration not found")
	ErrQuotaExceeded  = errors.New("rate limit quota exceeded")
	ErrInvalidRequest = errors.New("invalid rate limit check request")
	ErrRedisFailure   = errors.New("redis cluster unavailable: fail-closed quota policy")
)

type TierLevel string

const (
	TierFree       TierLevel = "FREE"       // e.g. 5 concurrent calls, 30 req/min
	TierGrowth     TierLevel = "GROWTH"     // e.g. 25 concurrent calls, 300 req/min
	TierEnterprise TierLevel = "ENTERPRISE" // e.g. 250 concurrent calls, 5,000 req/min
)

// QuotaPolicy defines hierarchical multi-tenant limits
type QuotaPolicy struct {
	Tier               TierLevel `json:"tier"`
	RequestsPerMinute  int64     `json:"requests_per_minute"`
	MaxConcurrentCalls int64     `json:"max_concurrent_calls"`
	BurstCapacity      int64     `json:"burst_capacity"`
	RefillRatePerSec   float64   `json:"refill_rate_per_sec"`
}

// RateLimitRequest is sent by gateways or internal services
type RateLimitRequest struct {
	TenantID string `json:"tenant_id"`
	APIKey   string `json:"api_key,omitempty"`
	Resource string `json:"resource"` // e.g., "call.initiate", "audio.chunk", "ai.completion"
	Cost     int64  `json:"cost"`     // token or request weight (default 1)
	ClientIP string `json:"client_ip,omitempty"`
}

// RateLimitResponse is returned with standard IETF RateLimit headers
type RateLimitResponse struct {
	Allowed            bool      `json:"allowed"`
	Limit              int64     `json:"limit"`
	CurrentUsage       int64     `json:"current_usage"`
	RemainingQuota     int64     `json:"remaining_quota"`
	ResetAfterSeconds  int64     `json:"reset_after_seconds"`
	ActiveCalls        int64     `json:"active_calls"`
	MaxConcurrentCalls int64     `json:"max_concurrent_calls"`
	ReservationID      string    `json:"reservation_id,omitempty"`
	ViolationReason    string    `json:"violation_reason,omitempty"`
	CheckedAt          time.Time `json:"checked_at"`
}

// ReleaseRequest releases an active call reservation idempotently
type ReleaseRequest struct {
	TenantID      string `json:"tenant_id"`
	ReservationID string `json:"reservation_id"`
	APIKey        string `json:"api_key,omitempty"`
}

// RenewRequest extends the lease of an active call reservation
type RenewRequest struct {
	TenantID      string `json:"tenant_id"`
	ReservationID string `json:"reservation_id"`
	TTLSeconds    int64  `json:"ttl_seconds,omitempty"`
	APIKey        string `json:"api_key,omitempty"`
}

// RenewResponse returns the result of the reservation lease renewal
type RenewResponse struct {
	TenantID      string `json:"tenant_id"`
	ReservationID string `json:"reservation_id"`
	Renewed       bool   `json:"renewed"`
	TTLSeconds    int64  `json:"ttl_seconds"`
}
