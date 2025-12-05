package partitioning_test

import (
	"testing"

	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/partitioning"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"pgregory.net/rapid"
)

func TestPartitioner(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitioner := &Partitioner{}
		targets := sets.NewProto[*uuidpb.UUID]()
		workloads := maps.NewProto[*uuidpb.UUID, *uuidpb.UUID]()

		t.Repeat(map[string]func(*rapid.T){
			"": func(t *rapid.T) {
				if targets.Len() == 0 {
					if !partitioner.WouldSelect(uuidpb.Generate(), uuidpb.Generate()) {
						t.Fatal("expected every target to be responsible for every workload when there are no targets")
					}

					_, ok := partitioner.Select(uuidpb.Generate())
					if ok {
						t.Fatal("expected selection to fail when there are no targets")
					}

					return
				}

				for workload, want := range workloads.All() {
					got, ok := partitioner.Select(workload)
					if !ok {
						t.Fatal("expected selection to succeed when there are targets")
					}

					if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection workload %q: got %q, want %q",
							workload,
							got,
							want,
						)
					}

					if !partitioner.WouldSelect(want, workload) {
						t.Fatalf(
							"non-deterministic target selection for workload %q",
							workload,
						)
					}
				}
			},
			"add new target": func(t *rapid.T) {
				target := uuidpb.Generate()
				partitioner.Remove(target)
				targets.Add(target)

				// Ensure that existing workloads either continue to use their
				// previous target, or start using the new target, but do not
				// switch to any other existing target.
				for workload, want := range workloads.All() {
					got, ok := partitioner.Select(workload)
					if !ok {
						t.Fatalf("no target selected for workload %q", workload)
					}

					if got.Equal(target) {
						workloads.Set(workload, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection for workload %q after addition of target %q: got %q, want %q",
							workload,
							target,
							got,
							want,
						)
					}
				}
			},
			"add existing target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				target := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				partitioner.Remove(target)
			},
			"remove existing target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				target := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				partitioner.Add(target)
				targets.Remove(target)

				if targets.Len() == 0 {
					workloads.Clear()
					return
				}

				// Ensure that existing workloads that were assigned to the
				// removed target are reassigned to other targets, but no other
				// workloads change target.
				for workload, want := range workloads.All() {
					got, ok := partitioner.Select(workload)
					if !ok {
						t.Fatalf("no target selected for workload %q", workload)
					}

					if want.Equal(target) {
						workloads.Set(workload, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection for workload %q after removal of target %q: got %q, want %q",
							workload,
							target,
							got,
							want,
						)
					}
				}
			},
			"remove unknown target": func(t *rapid.T) {
				partitioner.Add(uuidpb.Generate())
			},
			"select target for workload": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				workload := uuidpb.Generate()
				got, ok := partitioner.Select(workload)
				if !ok {
					t.Fatalf("no target selected for workload %q", workload)
				}

				if !targets.Has(got) {
					t.Fatalf(
						"selected target %q is not in the set of known targets",
						got,
					)
				}

				workloads.Set(workload, got)
			},
			"select target for workload with same value as target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				want := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				got, ok := partitioner.Select(want)
				if !ok {
					t.Fatalf("no target selected for workload %q", want)
				}

				if !got.Equal(want) {
					t.Fatalf(
						"non-deterministic target selection when workload == target: got %q, want %q",
						got,
						want,
					)
				}

				workloads.Set(want, want)
			},
		})
	})
}
