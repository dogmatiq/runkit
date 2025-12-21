package partition_test

import (
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/partition"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"pgregory.net/rapid"
)

func TestPartitioner(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var (
			partitioner Partitioner
			partitions  uuidpb.Set
			workloads   uuidpb.Map[*uuidpb.UUID]
		)

		t.Repeat(map[string]func(*rapid.T){
			"": func(t *rapid.T) {
				if partitions.Len() == 0 {
					if !partitioner.WouldSelectPartition(uuidpb.Generate(), uuidpb.Generate()) {
						t.Fatal("expected every partition to be responsible for every workload when there are no partitions")
					}

					_, ok := partitioner.SelectPartition(uuidpb.Generate())
					if ok {
						t.Fatal("expected selection to fail when there are no partitions")
					}

					return
				}

				for work, want := range workloads.All() {
					got, ok := partitioner.SelectPartition(work)
					if !ok {
						t.Fatal("expected selection to succeed when there are partitions")
					}

					if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic partition selection for workload %q: got %q, want %q",
							work,
							got,
							want,
						)
					}

					if !partitioner.WouldSelectPartition(want, work) {
						t.Fatalf(
							"non-deterministic partition selection for workload %q",
							work,
						)
					}
				}
			},
			"add a new partition": func(t *rapid.T) {
				partitionID := uuidpb.Generate()
				partitioner.AddPartition(partitionID)
				partitions.Add(partitionID)

				// Ensure that existing workloads either continue to use their
				// previous partition, or start using the new partition, but do
				// not switch to any other existing partition.
				for work, want := range workloads.All() {
					got, ok := partitioner.SelectPartition(work)
					if !ok {
						t.Fatalf("no partition selected for workload %q", work)
					}

					if got.Equal(partitionID) {
						workloads.Set(work, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic partition selection for workload %q after addition of partition %q: got %q, want %q",
							work,
							partitionID,
							got,
							want,
						)
					}
				}
			},
			"add an existing partition": func(t *rapid.T) {
				if partitions.Len() == 0 {
					t.Skip("no existing partitions")
				}

				partitionID := xrapid.SampledFromSeq(partitions.All()).Draw(t, "existing partition")
				partitioner.AddPartition(partitionID)
			},
			"remove an existing partition": func(t *rapid.T) {
				if partitions.Len() == 0 {
					t.Skip("no existing partitions")
				}

				partitionID := xrapid.SampledFromSeq(partitions.All()).Draw(t, "existing partition")
				partitioner.RemovePartition(partitionID)
				partitions.Delete(partitionID)

				if partitions.Len() == 0 {
					workloads.Clear()
					return
				}

				// Ensure that existing workloads that were assigned to the
				// removed paritition are reassigned to other partitions, but no
				// other workloads change partition.
				for work, want := range workloads.All() {
					got, ok := partitioner.SelectPartition(work)
					if !ok {
						t.Fatalf("no partition selected for workload %q", work)
					}

					if want.Equal(partitionID) {
						workloads.Set(work, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic partition selection for workload %q after removal of partition %q: got %q, want %q",
							work,
							partitionID,
							got,
							want,
						)
					}
				}
			},
			"remove an unknown partition": func(t *rapid.T) {
				partitioner.RemovePartition(uuidpb.Generate())
			},
			"select a partition for a workload": func(t *rapid.T) {
				if partitions.Len() == 0 {
					t.Skip("no existing partitions")
				}

				work := uuidpb.Generate()
				got, ok := partitioner.SelectPartition(work)
				if !ok {
					t.Fatalf("no partition selected for workload %q", work)
				}

				if !partitions.Has(got) {
					t.Fatalf(
						"selected partition %q is not in the set of known partitions",
						got,
					)
				}

				workloads.Set(work, got)
			},
			"select a partition for a workload with same value as a known partition": func(t *rapid.T) {
				if partitions.Len() == 0 {
					t.Skip("no existing partitions")
				}

				want := xrapid.SampledFromSeq(partitions.All()).Draw(t, "existing partition")
				got, ok := partitioner.SelectPartition(want)
				if !ok {
					t.Fatalf("no partition selected for workload %q", want)
				}

				if !got.Equal(want) {
					t.Fatalf(
						"non-deterministic partition selection when workload == partition: got %q, want %q",
						got,
						want,
					)
				}

				workloads.Set(want, want)
			},
		})
	})
}
