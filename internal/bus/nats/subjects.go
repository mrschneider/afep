// Package natsbus bridges AFEP onto NATS JetStream: outbound-only
// dial-tone connections, durable streams and a KV card bucket.
package natsbus

import (
	"errors"
	"fmt"
	"strings"
)

// Well-known verbs on afep.<domain>.<agent>.<verb>.
const (
	VerbEnvelopes = "envelopes"
	VerbBids      = "bids"
	VerbTelemetry = "telemetry"
)

// Wildcard is the single-segment wildcard usable in subscriptions.
const Wildcard = "*"

// Subject builds afep.<domain>.<agent>.<verb>.
func Subject(domain, agent, verb string) (string, error) {
	for name, seg := range map[string]string{"domain": domain, "agent": agent, "verb": verb} {
		if seg == "" {
			return "", fmt.Errorf("afep/natsbus: empty %s segment", name)
		}
		if strings.ContainsAny(seg, " >.") {
			return "", fmt.Errorf("afep/natsbus: invalid %s segment %q", name, seg)
		}
	}
	return "afep." + domain + "." + agent + "." + verb, nil
}

// ParseSubject splits afep.<domain>.<agent>.<verb>.
func ParseSubject(subject string) (domain, agent, verb string, err error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != "afep" {
		return "", "", "", errors.New("afep/natsbus: bad subject " + subject)
	}
	return parts[1], parts[2], parts[3], nil
}
