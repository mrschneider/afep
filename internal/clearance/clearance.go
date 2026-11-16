// Package clearance decides whether an AgentCard may serve a tenant.
package clearance

import (
	"fmt"
	"strings"
	"sync"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// parsedClearances is the compiled form of a card's security clearances:
// the exact grants as a set plus the prefix of every wildcard grant
// ("tenant:*" -> "tenant:"). holds is then an O(1) set probe plus a short
// prefix scan, instead of re-running HasSuffix/TrimSuffix/HasPrefix over the
// raw grant list on every serve check.
type parsedClearances struct {
	exact    map[string]struct{}
	prefixes []string
}

func parseClearances(grants []string) *parsedClearances {
	p := &parsedClearances{exact: make(map[string]struct{}, len(grants))}
	for _, g := range grants {
		// Keep the raw grant in the exact set (so a literal "tenant:*"
		// still matches by equality, exactly as the old loop did) and,
		// when it is a wildcard, also record its prefix.
		p.exact[g] = struct{}{}
		if strings.HasSuffix(g, "*") {
			p.prefixes = append(p.prefixes, strings.TrimSuffix(g, "*"))
		}
	}
	return p
}

func (p *parsedClearances) holds(grant string) bool {
	if _, ok := p.exact[grant]; ok {
		return true
	}
	for _, pre := range p.prefixes {
		if strings.HasPrefix(grant, pre) {
			return true
		}
	}
	return false
}

// clearancesCache compiles a card's clearances once, keyed on the card
// POINTER. Cards are immutable self-signed artifacts shared across
// goroutines (the bus mirrors one pointer per issuer; the self card is a
// single pointer), so the compiled form is stable for the pointer's whole
// life — the same idiom the identity layer uses for its card digest cache.
// Keying on the pointer (not the id) stays correct when a test or a
// re-issued card reuses an id with different clearances.
var clearancesCache sync.Map // *afepv1.AgentCard -> *parsedClearances

func compiled(card *afepv1.AgentCard) *parsedClearances {
	if v, ok := clearancesCache.Load(card); ok {
		return v.(*parsedClearances)
	}
	p := parseClearances(card.GetSecurityClearances())
	clearancesCache.Store(card, p)
	return p
}

// Holds reports whether card carries the exact grant, a global "*", or a
// prefix wildcard such as "tenant:*".
func Holds(card *afepv1.AgentCard, grant string) bool {
	return compiled(card).holds(grant)
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
