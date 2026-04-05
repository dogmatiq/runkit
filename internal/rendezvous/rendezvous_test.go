package rendezvous_test

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xrapid"
	. "github.com/dogmatiq/runkit/internal/rendezvous"
	"pgregory.net/rapid"
)

func TestRendezvous(t *testing.T) {
	t.Run("empty candidate set", func(t *testing.T) {
		work := uuidpb.Generate()
		if _, ok := Winner(work, nil); ok {
			t.Fatal("Winner() returned true with empty candidate set")
		}

		if got := Rank(work, nil); got != nil {
			t.Fatalf("Rank() returned non-nil for empty candidates: %v", got)
		}

		if Wins(work, uuidpb.Generate(), nil) {
			t.Fatal("Wins() returned true for empty candidates")
		}
	})

	rapid.Check(t, func(t *rapid.T) {
		var (
			workloads      uuidpb.Map[*uuidpb.UUID]
			candidateSet   uuidpb.Set
			candidateSlice = func() []*uuidpb.UUID {
				return slices.Collect(candidateSet.All())
			}
		)

		t.Repeat(map[string]func(*rapid.T){
			"": func(t *rapid.T) {
				// Invariant: known workload->winner mappings are stable, and
				// all three functions agree.
				for work, want := range workloads.All() {
					got, ok := Winner(work, candidateSlice())
					if !ok {
						t.Fatal("Winner() returned false for non-empty candidate set")
					}
					if !got.Equal(want) {
						t.Fatalf("unexpected winner for workload %q: got %q, want %q", work, got, want)
					}

					if !Wins(work, want, candidateSlice()) {
						t.Fatalf("Wins() returned false for known winner %q of workload %q", want, work)
					}

					ranked := Rank(work, candidateSlice())
					if !ranked[0].Equal(want) {
						t.Fatalf("unexpected Rank()[0] for workload %q: got %q, want %q", work, ranked[0], want)
					}
				}
			},

			"add a candidate": func(t *rapid.T) {
				id := uuidpb.Generate()
				candidateSet.Add(id)

				// Existing workloads may only move to the newly added candidate.
				for work, want := range workloads.All() {
					got, ok := Winner(work, candidateSlice())
					if !ok {
						t.Fatal("Winner() returned false for non-empty candidate set")
					}

					if got.Equal(id) {
						workloads.Set(work, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"workload %q unexpectedly moved from %q to %q after adding candidate %q",
							work, want, got, id,
						)
					}
				}
			},

			"remove a candidate": func(t *rapid.T) {
				if candidateSet.Len() == 0 {
					t.Skip("candidate set is empty")
				}

				id := xrapid.SampledFromSeq(candidateSet.All()).Draw(t, "candidate to remove")
				candidateSet.Delete(id)

				if candidateSet.Len() == 0 {
					workloads.Clear()
					return
				}

				// Workloads on the removed candidate are reassigned; all others
				// must remain on their current candidate.
				for work, want := range workloads.All() {
					got, ok := Winner(work, candidateSlice())
					if !ok {
						t.Fatal("Winner() returned false for non-empty candidate set")
					}

					if want.Equal(id) {
						workloads.Set(work, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"workload %q unexpectedly moved from %q to %q after removing %q",
							work, want, got, id,
						)
					}
				}
			},

			"select winner for a new workload": func(t *rapid.T) {
				if candidateSet.Len() == 0 {
					t.Skip("candidate set is empty")
				}

				work := uuidpb.Generate()
				got, ok := Winner(work, candidateSlice())
				if !ok {
					t.Fatal("Winner() returned false for non-empty candidate set")
				}

				if !candidateSet.Has(got) {
					t.Fatalf("Winner() returned %q which is not in the candidate set", got)
				}

				ranked := Rank(work, candidateSlice())
				if !ranked[0].Equal(got) {
					t.Fatalf("unexpected Rank()[0] for workload %q: got %q, want %q", work, ranked[0], got)
				}

				if !Wins(work, got, candidateSlice()) {
					t.Fatalf("Wins() returned false for winner %q of workload %q", got, work)
				}

				workloads.Set(work, got)
			},

			"self-affinity": func(t *rapid.T) {
				if candidateSet.Len() == 0 {
					t.Skip("candidate set is empty")
				}

				work := xrapid.SampledFromSeq(candidateSet.All()).Draw(t, "candidate as workload")

				got, ok := Winner(work, candidateSlice())
				if !ok {
					t.Fatal("Winner() returned false for non-empty candidate set")
				}
				if !got.Equal(work) {
					t.Fatalf("unexpected winner in self-affinity for workload %q: got %q, want %q", work, got, work)
				}
				ranked := Rank(work, candidateSlice())
				if !ranked[0].Equal(work) {
					t.Fatalf("unexpected Rank()[0] in self-affinity for workload %q: got %q, want %q", work, ranked[0], work)
				}
				if !Wins(work, work, candidateSlice()) {
					t.Fatalf("Wins() returned false for self-affinity workload %q", work)
				}

				workloads.Set(work, work)
			},

			"non-member candidate does not win": func(t *rapid.T) {
				if candidateSet.Len() == 0 {
					t.Skip("candidate set is empty")
				}

				work := uuidpb.Generate()
				nonMember := uuidpb.Generate()
				if Wins(work, nonMember, candidateSlice()) {
					t.Fatalf("Wins returned true for non-member candidate %q", nonMember)
				}
			},

			"result is independent of candidate slice order": func(t *rapid.T) {
				if candidateSet.Len() == 0 {
					t.Skip("candidate set is empty")
				}

				work := uuidpb.Generate()
				original := candidateSlice()

				shuffled := make([]*uuidpb.UUID, len(original))
				copy(shuffled, original)
				rand.Shuffle(len(shuffled), func(i, j int) {
					shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				})

				wantWinner, _ := Winner(work, original)
				gotWinner, _ := Winner(work, shuffled)
				if !gotWinner.Equal(wantWinner) {
					t.Fatalf("unexpected winner after shuffle for workload %q: got %q, want %q", work, gotWinner, wantWinner)
				}

				wantRank := Rank(work, original)
				gotRank := Rank(work, shuffled)
				for i := range wantRank {
					if !gotRank[i].Equal(wantRank[i]) {
						t.Fatalf("unexpected Rank()[%d] after shuffle for workload %q: got %q, want %q", i, work, gotRank[i], wantRank[i])
					}
				}
			},
		})
	})
}
