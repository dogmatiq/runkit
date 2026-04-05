// Package rendezvous implements rendezvous hashing for workload-to-candidate
// assignment.
package rendezvous

import (
	"cmp"
	"slices"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/zeebo/xxh3"
)

// Winner returns the candidate that wins the rendezvous for workload. It returns
// false if candidates is empty.
//
// If workload is equal to one of the candidates, that candidate always wins
// (self-affinity), regardless of other candidates' scores.
func Winner(workload *uuidpb.UUID, candidates []*uuidpb.UUID) (*uuidpb.UUID, bool) {
	switch len(candidates) {
	case 0:
		return nil, false
	case 1:
		return candidates[0], true
	}

	w := workload.AsByteArray()
	var winner scoredCandidate

	for _, c := range candidates {
		// Self-affinity: if workload is a candidate, it wins without scoring.
		if workload.Equal(c) {
			return c, true
		}

		sc := scoredCandidate{c, score(w, c.AsByteArray())}
		if winner.candidate == nil || sc.Compare(winner) < 0 {
			winner = sc
		}
	}

	return winner.candidate, true
}

// Wins returns true if candidate wins the rendezvous for workload among
// candidates. It returns false if candidates is empty or if candidate is not a
// member of candidates.
func Wins(workload, candidate *uuidpb.UUID, candidates []*uuidpb.UUID) bool {
	switch len(candidates) {
	case 0:
		return false
	case 1:
		return candidate.Equal(candidates[0])
	}

	if workload.Equal(candidate) {
		// Self-affinity: candidate wins iff it is a member of candidates.
		// No scoring required.
		for _, c := range candidates {
			if c.Equal(candidate) {
				return true
			}
		}
		return false
	}

	winner, ok := Winner(workload, candidates)
	return ok && winner.Equal(candidate)
}

// Rank returns candidates sorted in descending order of their rendezvous score
// for workload. The first element is the winner. Returns nil if candidates is
// empty.
func Rank(workload *uuidpb.UUID, candidates []*uuidpb.UUID) []*uuidpb.UUID {
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return []*uuidpb.UUID{candidates[0]}
	}

	w := workload.AsByteArray()

	// Score non-self candidates; the self-affinity candidate (if any) is
	// extracted and placed first without participating in scoring or sorting.
	var self *uuidpb.UUID
	scored := make([]scoredCandidate, 0, len(candidates))

	for _, c := range candidates {
		if workload.Equal(c) {
			self = c
		} else {
			scored = append(scored, scoredCandidate{c, score(w, c.AsByteArray())})
		}
	}

	slices.SortFunc(scored, scoredCandidate.Compare)

	result := make([]*uuidpb.UUID, 0, len(candidates))
	if self != nil {
		result = append(result, self)
	}

	for _, sc := range scored {
		result = append(result, sc.candidate)
	}

	return result
}

// score computes the rendezvous hash score for a workload/candidate pair using
// XXH3-64.
func score(workload, candidate [16]byte) uint64 {
	var buf [32]byte
	copy(buf[:16], workload[:])
	copy(buf[16:], candidate[:])
	return xxh3.Hash(buf[:])
}

// scoredCandidate pairs a candidate with its rendezvous score for sorting.
type scoredCandidate struct {
	candidate *uuidpb.UUID
	score     uint64
}

// Compare returns a negative value if s ranks before b, positive if after, and
// zero if equal.
//
// Higher score wins; ties are broken by lower UUID, making the result
// independent of the order of the candidates slice.
func (s scoredCandidate) Compare(b scoredCandidate) int {
	if s.score != b.score {
		return cmp.Compare(b.score, s.score) // descending: higher score ranks first
	}
	return s.candidate.Compare(b.candidate)
}
