// Package clearance decides whether an AgentCard may serve a tenant.
package clearance

import (
	"fmt"
	"strings"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// Holds reports whether card carries the exact grant, a global "*", or a
// prefix wildcard such as "tenant:*".
func Holds(card *afepv1.AgentCard, grant string) bool {
	for _, g := range card.GetSecurityClearances() {
		if g == grant {
			return true
		}
		if strings.HasSuffix(g, "*") && strings.HasPrefix(grant, strings.TrimSuffix(g, "*")) {
			return true
		}
	}
	return false
}

// CanServe checks the card against the envelope tenant. An empty tenant
// carries no requirement (schema-only calls); otherwise the card needs
// "tenant:<name>" (wildcards apply).
func CanServe(card *afepv1.AgentCard, tenant string) error {
	if tenant == "" {
		return nil
	}
	if !Holds(card, "tenant:"+tenant) {
		return fmt.Errorf("afep/clearance: card %q holds no grant for tenant %q", card.GetId(), tenant)
	}
	return nil
}
