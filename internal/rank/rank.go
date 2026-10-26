// Package rank orders candidate bids deterministically: score descending,
// agent id ascending as the tie-break.
package rank

import (
	"sort"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

// Sort orders bids in place.
func Sort(bids []*afepv1.Bid) {
	sort.SliceStable(bids, func(i, j int) bool {
		if bids[i].GetScore() != bids[j].GetScore() {
			return bids[i].GetScore() > bids[j].GetScore()
		}
		return bids[i].GetAgentId() < bids[j].GetAgentId()
	})
}
