// Package metrics exposes AFEP counters and gauges in Prometheus text
// exposition format without pulling a client library into the reference
// implementation. Histograms remain out of scope: the latency samples are
// rendered as avg + max (sufficient for a reference daemon).
package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Counter is a monotonically increasing named counter.
type Counter struct {
	name string
	help string
	v    atomic.Uint64
}

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge is a named instantaneous value (set, not accumulated).
type Gauge struct {
	name string
	help string
	v    atomic.Int64
}

// Set replaces the value.
func (g *Gauge) Set(v int64) { g.v.Store(v) }

// Value returns the current value.
func (g *Gauge) Value() int64 { return g.v.Load() }

// Latency is a named latency sample set: count, cumulative sum and max,
// rendered as <name>_total (count), <name>_avg and <name>_max in seconds.
type Latency struct {
	name  string
	help  string
	count atomic.Uint64
	sumNs atomic.Uint64
	maxNs atomic.Uint64
}

// Observe records one duration in nanoseconds.
func (l *Latency) Observe(ns int64) {
	l.count.Add(1)
	l.sumNs.Add(uint64(ns))
	for {
		cur := l.maxNs.Load()
		if ns <= int64(cur) || l.maxNs.CompareAndSwap(cur, uint64(ns)) {
			return
		}
	}
}

// Count returns the number of observed samples.
func (l *Latency) Count() uint64 { return l.count.Load() }

// AvgSeconds returns the average sample duration (0 with no samples).
func (l *Latency) AvgSeconds() float64 {
	n := l.count.Load()
	if n == 0 {
		return 0
	}
	return float64(l.sumNs.Load()) / float64(n) / 1e9
}

// MaxSeconds returns the largest sample duration (0 with no samples).
func (l *Latency) MaxSeconds() float64 {
	return float64(l.maxNs.Load()) / 1e9
}

var (
	// EnvelopeAck counts envelopes whose handler completed (acked).
	EnvelopeAck = &Counter{name: "afep_envelopes_acked_total", help: "Envelopes acked by the handler"}
	// EnvelopeTerm counts poison envelopes terminated.
	EnvelopeTerm = &Counter{name: "afep_envelopes_terminated_total", help: "Poison envelopes terminated (no redelivery)"}
	// EnvelopeNak counts transient failures scheduled for redelivery.
	EnvelopeNak = &Counter{name: "afep_envelopes_nacked_total", help: "Envelopes nacked for redelivery"}
	// EnvelopePanic counts handler panics contained by recovery.
	EnvelopePanic = &Counter{name: "afep_handler_panics_total", help: "Handler panics contained by recovery"}
	// BidPublished counts bids published on the live core-NATS path.
	BidPublished = &Counter{name: "afep_bids_published_total", help: "Bids published on the live path"}
	// BidArchiveFailed counts best-effort AFEP_BIDS archive failures.
	BidArchiveFailed = &Counter{name: "afep_bid_archive_failures_total", help: "AFEP_BIDS archive publish failures"}
	// RevokedRejected counts envelopes rejected because the issuer card is revoked.
	RevokedRejected = &Counter{name: "afep_revoked_rejected_total", help: "Envelopes rejected over a revoked issuer card"}
	// RateLimited counts gRPC requests rejected by the ingress limiter.
	RateLimited = &Counter{name: "afep_grpc_rate_limited_total", help: "gRPC requests rejected by the ingress rate limiter"}
)

var (
	// TaskStoreSize is the number of envelopes currently in the taskstore.
	TaskStoreSize = &Gauge{name: "afep_taskstore_size", help: "Envelopes currently in the durable task store"}
	// TaskStoreFlushSeconds samples each taskstore flush (duration).
	TaskStoreFlushSeconds = &Latency{name: "afep_taskstore_flush_seconds", help: "Taskstore flush duration"}
	// TaskStoreDeltaEntries is the size of the pending delta (last flush).
	TaskStoreDeltaEntries = &Gauge{name: "afep_taskstore_delta_entries", help: "Pending taskstore delta entries (last flush)"}
	// TaskStoreCompactions counts base compactions (O(N) folds).
	TaskStoreCompactions = &Counter{name: "afep_taskstore_compactions_total", help: "Taskstore base compactions"}
	// EnvelopeHandleSeconds samples each mesh OnEnvelope handling (duration).
	EnvelopeHandleSeconds = &Latency{name: "afep_envelope_handle_seconds", help: "Envelope handling duration"}
)

var allCounters = []*Counter{
	EnvelopeAck, EnvelopeTerm, EnvelopeNak, EnvelopePanic,
	BidPublished, BidArchiveFailed, RevokedRejected, RateLimited,
	TaskStoreCompactions,
}

var allGauges = []*Gauge{
	TaskStoreSize,
	TaskStoreDeltaEntries,
}

var allLatencies = []*Latency{
	TaskStoreFlushSeconds, EnvelopeHandleSeconds,
}

// Render writes every metric in Prometheus text exposition format.
func Render(w io.Writer) {
	for _, c := range allCounters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			c.name, c.help, c.name, c.name, c.v.Load())
	}
	for _, g := range allGauges {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n",
			g.name, g.help, g.name, g.name, g.v.Load())
	}
	for _, l := range allLatencies {
		fmt.Fprintf(w, "# HELP %s_total %s (count)\n# TYPE %s_total counter\n%s_total %d\n",
			l.name, l.help, l.name, l.name, l.count.Load())
		fmt.Fprintf(w, "# HELP %s_avg %s (average, seconds)\n# TYPE %s_avg gauge\n%s_avg %.6f\n",
			l.name, l.help, l.name, l.name, l.AvgSeconds())
		fmt.Fprintf(w, "# HELP %s_max %s (max, seconds)\n# TYPE %s_max gauge\n%s_max %.6f\n",
			l.name, l.help, l.name, l.name, l.MaxSeconds())
	}
}
