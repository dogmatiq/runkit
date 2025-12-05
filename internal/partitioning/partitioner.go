package partitioning

import (
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Partitioner distributes workloads across a set of targets using rendezvous
// hashing. Both targets and workloads are identified by UUIDs.
type Partitioner struct {
	targets atomic.Pointer[[]*uuidpb.UUID]
}

// AddTarget adds a target to the partitioner.
func (p *Partitioner) AddTarget(id *uuidpb.UUID) {
	for {
		before := p.targets.Load()
		after := cloneAndInsert(before, id)

		if before == after {
			return
		}

		if p.targets.CompareAndSwap(before, after) {
			return
		}
	}
}

// RemoveTarget removes a target from the partitioner.
func (p *Partitioner) RemoveTarget(id *uuidpb.UUID) {
	for {
		before := p.targets.Load()
		after := cloneAndRemove(before, id)

		if before == after {
			return
		}

		if p.targets.CompareAndSwap(before, after) {
			return
		}
	}
}

// SelectTarget returns the ID of the target that should handle the given
// workload.
func (p *Partitioner) SelectTarget(workload *uuidpb.UUID) (*uuidpb.UUID, bool) {
	targets := p.targets.Load()

	if targets == nil {
		return nil, false
	}

	var (
		hash xxhash.Digest
		wins *uuidpb.UUID
		best uint64
	)

	for _, target := range *targets {
		// Whenever a workload _has_ the same ID as a target, that target should
		// always be selected. This is a simple mechanism to have targets "own"
		// workloads that originate from them.
		if workload.Equal(target) {
			return target, true
		}

		hash.Reset()
		hash.Write(target.AsBytes())
		hash.Write(workload.AsBytes())

		if s := hash.Sum64(); s > best {
			wins = target
			best = s
		}
	}

	return wins, true
}

func cloneAndInsert(set *[]*uuidpb.UUID, id *uuidpb.UUID) *[]*uuidpb.UUID {
	if set == nil {
		return &[]*uuidpb.UUID{id}
	}

	n := len(*set)
	result := make([]*uuidpb.UUID, n+1)

	for i, x := range *set {
		if x.Equal(id) {
			return set
		}

		if x.Less(id) {
			result[i] = x
		} else {
			result[i] = id
			copy(result[i+1:], (*set)[i:])
			return &result
		}
	}

	result[n] = id

	return &result
}

func cloneAndRemove(set *[]*uuidpb.UUID, id *uuidpb.UUID) *[]*uuidpb.UUID {
	if set == nil {
		return nil
	}

	if len(*set) == 1 {
		if (*set)[0].Equal(id) {
			return nil
		}
		return set
	}

	result := make([]*uuidpb.UUID, 0, len(*set)-1)

	for i, x := range *set {
		if x.Equal(id) {
			result = append(result, (*set)[i+1:]...)
			return &result
		}

		result = append(result, x)
	}

	return set
}
