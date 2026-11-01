package metrics

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestRenderCoversAllKinds checks the exposition output for counters,
// gauges and latency samples (count/avg/max). Globals accumulate across
// the process, so lines are matched by prefix, not exact count.
func TestRenderCoversAllKinds(t *testing.T) {
	EnvelopeAck.Inc()
	TaskStoreSize.Set(42)
	TaskStoreFlushSeconds.Observe(int64(2500 * time.Millisecond))
	EnvelopeHandleSeconds.Observe(int64(500 * time.Millisecond))
	EnvelopeHandleSeconds.Observe(int64(300 * time.Millisecond))

	var sb strings.Builder
	Render(&sb)
	out := sb.String()

	// (pattern, label) — each must appear exactly once as a metric line.
	cases := []struct {
		pat string
		lab string
	}{
		{`^afep_envelopes_acked_total \d+$`, "counter"},
		{`^afep_taskstore_size 42$`, "gauge"},
		{`^afep_taskstore_flush_seconds_total \d+$`, "latency count"},
		{`^afep_taskstore_flush_seconds_max 2\.500000$`, "latency max"},
		{`^afep_envelope_handle_seconds_total \d+$`, "latency count 2"},
		{`^afep_envelope_handle_seconds_avg \d\.\d+$`, "latency avg"},
		{`^afep_envelope_handle_seconds_max \d\.\d+$`, "latency max 2"},
	}
	for _, c := range cases {
		re := regexp.MustCompile("(?m)" + c.pat)
		if n := len(re.FindAllString(out, -1)); n != 1 {
			t.Errorf("%s: pattern %s matched %d times", c.lab, c.pat, n)
		}
	}
	if !strings.Contains(out, "# TYPE afep_taskstore_size gauge") {
		t.Errorf("gauge TYPE line missing\n%s", out)
	}
}
