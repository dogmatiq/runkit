// Package rendezvous implements rendezvous hashing for workload-to-candidate
// assignment.
package rendezvous

import (
	"cmp"
	"slices"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/zeebo/xxh3"
)

// Winner returns the candidate that wins the rendezvous for w. It returns
// false if candidates is empty.
//
// If w is equal to one of the candidates, that candidate always wins
// (self-affinity), regardless of other candidates' scores.
func Winner(w *uuidpb.UUID, candidates []*uuidpb.UUID) (*uuidpb.UUID, bool) {
	switch len(candidates) {
	case 0:
		return nil, false
	case 1:
		return candidates[0], true
	}

	wb := w.AsByteArray()
	var winner scoredCandidate

	for _, x := range candidates {
		// Self-affinity: if w is a candidate, it wins without scoring.
		if w.Equal(x) {
			return x, true
		}

		sc := scoredCandidate{x, score(wb, x.AsByteArray())}
		if winner.candidate == nil || sc.Compare(winner) < 0 {
			winner = sc
		}
	}

	return winner.candidate, true
}

// Wins returns true if c wins the rendezvous for w among candidates. It
// returns false if candidates is empty or if c is not a member of candidates.
func Wins(w, c *uuidpb.UUID, candidates []*uuidpb.UUID) bool {
	switch len(candidates) {
	case 0:
		return false
	case 1:
		return c.Equal(candidates[0])
	}

	if w.Equal(c) {
		// Self-affinity: c wins iff it is a member of candidates.
		// No scoring required.
		for _, x := range candidates {
			if x.Equal(c) {
				return true
			}
		}
		return false
	}

	winner, ok := Winner(w, candidates)
	return ok && winner.Equal(c)
}

// Rank returns candidates sorted in descending order of their rendezvous score
// for w. The first element is the winner. Returns nil if candidates is empty.
func Rank(w *uuidpb.UUID, candidates []*uuidpb.UUID) []*uuidpb.UUID {
	switch len(candidates) {
	case 0:
		return nil
	case 1:
		return []*uuidpb.UUID{candidates[0]}
	}

	wb := w.AsByteArray()

	// Score non-self candidates; the self-affinity candidate (if any) is
	// extracted and placed first without participating in scoring or sorting.
	var self *uuidpb.UUID
	scored := make([]scoredCandidate, 0, len(candidates))

	for _, x := range candidates {
		if w.Equal(x) {
			self = x
		} else {
			scored = append(scored, scoredCandidate{x, score(wb, x.AsByteArray())})
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

// RankAbove returns the candidates that rank above c for w, in descending order
// of their rendezvous score. c itself and any lower-ranked candidates are
// excluded.
//
// If c is not a member of candidates, all candidates rank above it, so the
// result is equal to Rank(w, candidates).
func RankAbove(w, c *uuidpb.UUID, candidates []*uuidpb.UUID) []*uuidpb.UUID {
	if w.Equal(c) {
		// Self-affinity: c wins unconditionally, so nothing ranks above it.
		return nil
	}

	all := Rank(w, candidates)
	for i, x := range all {
		if x.Equal(c) {
			return all[:i]
		}
	}
	return all
}

// score computes the rendezvous hash score for a workload/candidate pair using
// XXH3-64.
func score(w, c [16]byte) uint64 {
	var buf [32]byte
	copy(buf[:16], w[:])
	copy(buf[16:], c[:])
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
