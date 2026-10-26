package rank

import (
	"testing"

	afepv1 "github.com/mrschneider/afep/gen/go/afep/v1"
)

func TestSortScoreDescThenID(t *testing.T) {
	bids := []*afepv1.Bid{
		{AgentId: "b/low", Score: 10},
		{AgentId: "c/tie", Score: 50},
		{AgentId: "a/tie", Score: 50},
		{AgentId: "d/high", Score: 99},
	}
	Sort(bids)
	want := []string{"d/high", "a/tie", "c/tie", "b/low"}
	for i, id := range want {
		if bids[i].GetAgentId() != id {
			t.Fatalf("position %d = %q, want %q", i, bids[i].GetAgentId(), id)
		}
	}
}

func TestSortEmpty(t *testing.T) {
	Sort(nil) // must not panic
}
