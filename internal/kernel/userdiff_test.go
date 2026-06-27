package kernel

import (
	"sort"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

func ids(users []model.UserSpec) []int {
	out := make([]int, 0, len(users))
	for _, u := range users {
		out = append(out, u.ID)
	}
	sort.Ints(out)
	return out
}

func uuidByID(users []model.UserSpec, id int) string {
	for _, u := range users {
		if u.ID == id {
			return u.UUID
		}
	}
	return ""
}

func TestUserDiffAddAndRemove(t *testing.T) {
	old := []model.UserSpec{{ID: 1, UUID: "a"}, {ID: 2, UUID: "b"}}
	next := []model.UserSpec{{ID: 2, UUID: "b"}, {ID: 3, UUID: "c"}}

	toAdd, toRemove := UserDiff(old, next)
	if got := ids(toAdd); len(got) != 1 || got[0] != 3 {
		t.Fatalf("toAdd = %v, want [3]", got)
	}
	if got := ids(toRemove); len(got) != 1 || got[0] != 1 {
		t.Fatalf("toRemove = %v, want [1]", got)
	}
}

// Regression for bug #5: a UUID change on the same ID must produce BOTH a
// remove (old UUID) and an add (new UUID), so kernels purge the stale UUID
// before re-adding the ID instead of colliding and leaving the old one live.
func TestUserDiffUUIDChangeRemovesOldAndAddsNew(t *testing.T) {
	old := []model.UserSpec{{ID: 5, UUID: "old-uuid"}}
	next := []model.UserSpec{{ID: 5, UUID: "new-uuid"}}

	toAdd, toRemove := UserDiff(old, next)

	if len(toAdd) != 1 || toAdd[0].ID != 5 || toAdd[0].UUID != "new-uuid" {
		t.Fatalf("toAdd = %+v, want one user {5, new-uuid}", toAdd)
	}
	if len(toRemove) != 1 || toRemove[0].ID != 5 || toRemove[0].UUID != "old-uuid" {
		t.Fatalf("toRemove = %+v, want one user {5, old-uuid}", toRemove)
	}
}

// A pure property change (limits only, same UUID) must NOT be treated as an
// add or a remove — otherwise every limit tweak would churn the inbound.
func TestUserDiffLimitOnlyChangeIsNoDiff(t *testing.T) {
	old := []model.UserSpec{{ID: 7, UUID: "u7", SpeedLimit: 8}}
	next := []model.UserSpec{{ID: 7, UUID: "u7", SpeedLimit: 16}}

	toAdd, toRemove := UserDiff(old, next)
	if len(toAdd) != 0 || len(toRemove) != 0 {
		t.Fatalf("toAdd=%v toRemove=%v, want both empty for limit-only change", toAdd, toRemove)
	}
}

// Mixed batch: one new user, one removed user, one UUID-rotated user, one
// untouched user. The rotated user appears in both lists.
func TestUserDiffMixedBatch(t *testing.T) {
	old := []model.UserSpec{
		{ID: 1, UUID: "keep"},
		{ID: 2, UUID: "gone"},
		{ID: 3, UUID: "old3"},
	}
	next := []model.UserSpec{
		{ID: 1, UUID: "keep"},
		{ID: 3, UUID: "new3"},
		{ID: 4, UUID: "fresh"},
	}

	toAdd, toRemove := UserDiff(old, next)

	if got := ids(toAdd); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("toAdd ids = %v, want [3 4]", got)
	}
	if uuidByID(toAdd, 3) != "new3" {
		t.Fatalf("toAdd id 3 uuid = %q, want new3", uuidByID(toAdd, 3))
	}
	if got := ids(toRemove); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("toRemove ids = %v, want [2 3]", got)
	}
	if uuidByID(toRemove, 3) != "old3" {
		t.Fatalf("toRemove id 3 uuid = %q, want old3", uuidByID(toRemove, 3))
	}
}
