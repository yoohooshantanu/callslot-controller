# ConcurrencyGovernor: Low-Latency Distributed Multi-Tenant Rate Limiter & Concurrency Governor

[![CI Pipeline](https://github.com/yoohooshantanu/concurrency-governor/actions/workflows/ci.yml/badge.svg)](https://github.com/yoohooshantanu/concurrency-governor/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**ConcurrencyGovernor** is a low-latency, distributed multi-tenant rate limiting and concurrent call capacity governor written in Go. It is designed to sit at the edge or ingress of AI voice and telephony infrastructure to prevent API abuse, enforce tier-based quotas, eliminate noisy neighbors, and protect against LLM credit exhaustion.

---

## 1. Architecture Overview

```
Client / Telephony Gateway (CallRelay / External Carrier)
                   │
                   │ POST /v1/check
                   │ Headers: X-API-Key: tg_live_...
                   │ Body: {"tenant_id": "cust_123", "resource": "call.initiate", "cost": 1}
                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 0. HTTP Authentication & Tenant Binding Middleware                     │
│    ├── Validates API Key via registry; blocks wildcard bypasses        │
│    └── Prevents cross-tenant spoofing (401/403)                        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Authorized
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 1. Distributed Concurrent Call Governor (Atomic Redis Lua Script)      │
│    ├── Atomic INCRBY / DECRBY with capacity ceiling checks             │
│    ├── Preserves global limits across multiple cluster instances       │
│    └── Automatic rollback if downstream token bucket fails             │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Slot Reserved
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ 2. Dual-Layer Rate Limiting Engine                                     │
│    ├── L1 Fast Path: In-Memory Token Bucket (< 1 µs check)             │
│    │    └── Instant burst capacity absorption                          │
│    └── L2 Distributed Sync: Redis Atomic Lua Script (EVALSHA)          │
│         └── Sliding Window Log across distributed cluster nodes        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  ▼                                   ▼
          200 OK (Allowed)                429 Too Many Requests
    RateLimit-Limit: 6000                 RateLimit-Limit: 6000
    RateLimit-Remaining: 5990             RateLimit-Remaining: 0
    RateLimit-Reset: 60                   RateLimit-Reset: 1
```

---

## 2. Key Technical Capabilities

### A. Dual-Algorithm Rate Limiting & Architectural Trade-offs
ConcurrencyGovernor utilizes a multi-tiered rate limiting design balancing sub-microsecond latency with cluster-wide consistency:
- **L1 In-Memory Token Bucket (Process-Local)**: Employs an atomic in-process token bucket to absorb burstable telephony audio chunks in sub-microsecond time (**~869 ns/op**) without incurring network round-trips. Because L1 state is process-local, maximum instantaneous cluster burst capacity scales with the number of running gateway instances ($N \times \text{BurstCapacity}$).
- **L2 Distributed Sliding Window (Cluster-Wide Redis Lua)**: Enforces precise per-minute request quotas globally across all distributed gateway nodes using atomic sorted-set (`ZREMRANGEBYSCORE`, `ZCARD`, `ZADD`) transactions.

### B. Distributed Concurrent Voice Session Governor & Lease Renewal
Unlike generic HTTP rate limiters that only count requests per second, ConcurrencyGovernor actively tracks **concurrent active voice sessions**:
- `call.initiate` assigns an exact `ReservationID` and reserves a slot in Redis via an atomic Lua script (`ReserveCallScript`), ensuring cluster-wide concurrency ceilings (e.g., Free: 5, Growth: 30, Enterprise: 300) are enforced across all server instances.
- **Per-Reservation Expiry Safety Ceiling**: Each reservation is stored with its individual expiration timestamp in a Redis Sorted Set (`cc:res:<tenant>`). The `DefaultReservationTTL` is 4 hours (14,400s), serving as a fail-safe garbage collection ceiling for abandoned or dropped calls without ever resetting active calls.
- **Reservation Lease Renewal (`/v1/renew`)**: For long-running calls, telephony workers can periodically renew their active lease via `POST /v1/renew` (`RenewReservation`). This uses atomic `ZADD XX` to extend the expiry timestamp without allowing phantom reservation creation.
- **Idempotent Slot Release (`/v1/release`)**: When a call completes, `/v1/release` decrements the session counter and removes the specific reservation via `ReleaseCallScript`.
- **Automatic Rollback**: If an L1 burst limit or downstream filter fails, reserved call slots are automatically rolled back without leaking capacity.

### C. HTTP Authentication & Tenant Isolation
- Requests to `/v1/check` and `/v1/release` require valid API keys passed via `X-API-Key`, `Authorization: Bearer <key>`, or `api_key` payload.
- Keys are strictly bound to authorized tenants. Cross-tenant tampering is blocked with `403 Forbidden`.

### D. Standard IETF Compliance
Every evaluation returns standard HTTP rate-limiting headers:
- `RateLimit-Limit`: Maximum quota for the window.
- `RateLimit-Remaining`: Remaining credits.
- `RateLimit-Reset`: Seconds until quota reset.

---

## 3. Microbenchmarks & Latency

Evaluated under high concurrency using Go's native benchmark framework:

```bash
$ go test -bench="BenchmarkConcurrencyGovernorL1Check" -benchmem ./tests/...
BenchmarkConcurrencyGovernorL1Check-8    1455925    869.1 ns/op    160 B/op    4 allocs/op
PASS
ok      github.com/yoohooshantanu/concurrency-governor/tests    6.718s
```

- **Evaluation Speed**: **~869 nanoseconds per decision** on the hot path.
- **Heap Efficiency**: 160 B/op and 4 allocations per check.

---

## 4. Quickstart

### Running Standalone
```bash
# Clone the repository
git clone https://github.com/yoohooshantanu/concurrency-governor.git
cd concurrency-governor

# Run tests
go test -v ./tests/...

# Start service
go run ./cmd/server
```

### Checking Rate Limits
```bash
curl -X POST http://localhost:8082/v1/check \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tg_live_free_key" \
  -d '{
    "tenant_id": "tenant_free_test",
    "resource": "call.initiate",
    "cost": 1
  }'
```

### Renewing an Active Call Slot Lease
```bash
curl -X POST http://localhost:8082/v1/renew \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tg_live_free_key" \
  -d '{
    "tenant_id": "tenant_free_test",
    "reservation_id": "res_a1b2c3d4",
    "ttl_seconds": 14400
  }'
```

### Releasing a Call Slot
```bash
curl -X POST http://localhost:8082/v1/release \
  -H "Content-Type: application/json" \
  -H "X-API-Key: tg_live_free_key" \
  -d '{
    "tenant_id": "tenant_free_test",
    "reservation_id": "res_a1b2c3d4"
  }'
```

---

## 5. Repository Layout

```
concurrency-governor/
├── cmd/server/main.go            # Entrypoint, route handlers, auth middleware, IETF headers
├── internal/
│   ├── model/types.go            # Domain models: QuotaPolicy, TierLevel, RateLimitRequest/Response
│   ├── cache/l1_cache.go         # In-memory Token Bucket L1 cache (< 1 µs evaluation)
│   ├── limiter/engine.go         # Atomic Redis Lua sliding window script & distributed concurrency governor
│   ├── metrics/metrics.go        # Prometheus telemetry exposition (/metrics)
│   └── logger/logger.go          # Structured slog JSON logger
├── tests/limiter_test.go         # Live Redis sliding window, distributed call cap, auth, & load tests
├── docker-compose.yml            # Multi-container setup with Redis 7
├── Dockerfile                    # Multi-stage production container build
└── .github/workflows/ci.yml      # GitHub Actions CI with live Redis 7 service container & -race detector
```

---

## 6. License

MIT License. Authored by Shantanu Juvekar (`santanujuvekar@gmail.com`).
