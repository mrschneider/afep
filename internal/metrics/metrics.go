// Package metrics exposes AFEP counters in Prometheus text exposition
// format without pulling a client library into the reference
// implementation (counters only; histograms are out of scope).
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

var all = []*Counter{
	EnvelopeAck, EnvelopeTerm, EnvelopeNak, EnvelopePanic,
	BidPublished, BidArchiveFailed, RevokedRejected, RateLimited,
}

// Render writes every counter in Prometheus text exposition format.
func Render(w io.Writer) {
	for _, c := range all {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			c.name, c.help, c.name, c.name, c.v.Load())
	}
}
