package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	checksTotal   uint64
	allowedTotal  uint64
	rejectedTotal uint64
	l1Hits        uint64
	l2Hits        uint64
	activeCalls   int64
}

var DefaultMetrics = &Metrics{}

func (m *Metrics) IncCheck(allowed bool, l1Hit bool) {
	atomic.AddUint64(&m.checksTotal, 1)
	if allowed {
		atomic.AddUint64(&m.allowedTotal, 1)
	} else {
		atomic.AddUint64(&m.rejectedTotal, 1)
	}
	if l1Hit {
		atomic.AddUint64(&m.l1Hits, 1)
	} else {
		atomic.AddUint64(&m.l2Hits, 1)
	}
}

func (m *Metrics) UpdateActiveCalls(delta int64) {
	atomic.AddInt64(&m.activeCalls, delta)
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		fmt.Fprintf(w, "# HELP tenantgate_checks_total Total rate limit check evaluations\n")
		fmt.Fprintf(w, "# TYPE tenantgate_checks_total counter\n")
		fmt.Fprintf(w, "tenantgate_checks_total %d\n\n", atomic.LoadUint64(&m.checksTotal))

		fmt.Fprintf(w, "# HELP tenantgate_decisions_total Allowed vs rejected traffic counters\n")
		fmt.Fprintf(w, "# TYPE tenantgate_decisions_total counter\n")
		fmt.Fprintf(w, "tenantgate_decisions_total{decision=\"allowed\"} %d\n", atomic.LoadUint64(&m.allowedTotal))
		fmt.Fprintf(w, "tenantgate_decisions_total{decision=\"rejected\"} %d\n\n", atomic.LoadUint64(&m.rejectedTotal))

		fmt.Fprintf(w, "# HELP tenantgate_cache_hits_total L1 local in-memory vs L2 Redis lookups\n")
		fmt.Fprintf(w, "# TYPE tenantgate_cache_hits_total counter\n")
		fmt.Fprintf(w, "tenantgate_cache_hits_total{layer=\"l1_memory\"} %d\n", atomic.LoadUint64(&m.l1Hits))
		fmt.Fprintf(w, "tenantgate_cache_hits_total{layer=\"l2_redis\"} %d\n\n", atomic.LoadUint64(&m.l2Hits))

		fmt.Fprintf(w, "# HELP tenantgate_active_calls Current concurrent voice sessions across tenants\n")
		fmt.Fprintf(w, "# TYPE tenantgate_active_calls gauge\n")
		fmt.Fprintf(w, "tenantgate_active_calls %d\n", atomic.LoadInt64(&m.activeCalls))
	}
}
