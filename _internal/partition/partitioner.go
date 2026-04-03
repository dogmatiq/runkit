package partition

import (
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Partitioner distributes workloads across a set of partitions using rendezvous
// hashing. Both partitions and workloads are identified by UUIDs.
type Partitioner struct {
	partitions atomic.Pointer[[]*uuidpb.UUID]
}

// AddPartition adds a partition to the partitioner.
func (p *Partitioner) AddPartition(id *uuidpb.UUID) {
	for {
		before := p.partitions.Load()
		after := cloneAndInsert(before, id)

		if before == after {
			return
		}

		if p.partitions.CompareAndSwap(before, after) {
			return
		}
	}
}

// RemovePartition removes a partition from the partitioner.
func (p *Partitioner) RemovePartition(id *uuidpb.UUID) {
	for {
		before := p.partitions.Load()
		after := cloneAndRemove(before, id)

		if before == after {
			return
		}

		if p.partitions.CompareAndSwap(before, after) {
			return
		}
	}
}

// SelectPartition returns the ID of the partition that owns the given workload.
func (p *Partitioner) SelectPartition(work *uuidpb.UUID) (*uuidpb.UUID, bool) {
	partitions := p.partitions.Load()

	if partitions == nil {
		return nil, false
	}

	var (
		hash xxhash.Digest
		wins *uuidpb.UUID
		best uint64
	)

	for _, id := range *partitions {
		// Whenever a workload _has_ the same ID as a partition, that partition
		// should always be selected. This is a simple mechanism to have
		// partitions "own" workloads that originate from them.
		if work.Equal(id) {
			return id, true
		}

		hash.Reset()
		hash.Write(id.AsBytes())
		hash.Write(work.AsBytes())

		if s := hash.Sum64(); s > best {
			wins = id
			best = s
		}
	}

	return wins, true
}

// WouldSelectPartition returns true if the given partition owns the given
// workload.
func (p *Partitioner) WouldSelectPartition(id, work *uuidpb.UUID) bool {
	if work.Equal(id) {
		return true
	}

	partitions := p.partitions.Load()

	if partitions == nil {
		// If, for whatever reason, there are no partitions, we consider every
		// partition to be responsible for every workload. This is not a normal
		// mode of operation, but it maintains progress in the case of
		// misconfiguration.
		return true
	}

	var (
		hash xxhash.Digest
		wins *uuidpb.UUID
		best uint64
	)

	for _, id := range *partitions {
		hash.Reset()
		hash.Write(id.AsBytes())
		hash.Write(work.AsBytes())

		if s := hash.Sum64(); s > best {
			wins = id
			best = s
		}
	}

	return wins.Equal(id)
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
