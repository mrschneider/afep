// Package registry keeps signature-verified AgentCards for direct lookup.
package registry

import (
	"sort"
	"sync"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
	"github.com/mrschneider/afep/internal/identity"
)

// Registry is an in-memory card store that only accepts verified cards.
type Registry struct {
	mu    sync.RWMutex
	cards map[string]*afepv1.AgentCard
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{cards: make(map[string]*afepv1.AgentCard)}
}

// Put verifies the card signature before storing it.
func (r *Registry) Put(card *afepv1.AgentCard) error {
	if err := identity.VerifyCard(card); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cards[card.GetId()] = card
	return nil
}

// Get returns the card for id.
func (r *Registry) Get(id string) (*afepv1.AgentCard, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	card, ok := r.cards[id]
	return card, ok
}

// List returns all cards sorted by id.
func (r *Registry) List() []*afepv1.AgentCard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*afepv1.AgentCard, 0, len(r.cards))
	for _, c := range r.cards {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return out
}

// ByCapability returns cards advertising name, sorted by id.
func (r *Registry) ByCapability(name string) []*afepv1.AgentCard {
	var out []*afepv1.AgentCard
	for _, c := range r.List() {
		for _, capability := range c.GetCapabilities() {
			if capability.GetName() == name {
				out = append(out, c)
				break
			}
		}
	}
	return out
}
