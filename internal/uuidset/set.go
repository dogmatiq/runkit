// Package uuidset provides a set of [uuid.UUID] values, and [SetR], a read-only
// view of such a set.
package uuidset

import (
	"iter"
	"maps"
	"slices"
	"strings"
	"uuid"
)

// Set is an unordered set of unique [uuid.UUID] values.
//
// The zero value is an empty set that is ready to use. Reads of a nil *Set
// behave as reads of an empty set.
type Set struct {
	members map[uuid.UUID]struct{}
}

// New returns a set that contains the given members.
func New(members ...uuid.UUID) *Set {
	s := &Set{}

	for _, id := range members {
		s.Add(id)
	}

	return s
}

// Add adds id to the set.
func (s *Set) Add(id uuid.UUID) {
	if s.members == nil {
		s.members = map[uuid.UUID]struct{}{}
	}

	s.members[id] = struct{}{}
}

// Delete removes id from the set.
func (s *Set) Delete(id uuid.UUID) {
	delete(s.members, id)
}

// R returns a read-only view of the set. Reads through the view see members
// that are added to or deleted from the set after the view is made.
func (s *Set) R() SetR {
	return SetR{set: s}
}

// Has reports whether the set contains id.
func (s *Set) Has(id uuid.UUID) bool {
	if s == nil {
		return false
	}

	_, ok := s.members[id]
	return ok
}

// Len returns the number of members in the set.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}

	return len(s.members)
}

// IsEmpty reports whether the set has no members.
func (s *Set) IsEmpty() bool {
	return s.Len() == 0
}

// All returns an iterator over the members of the set, in no particular order.
func (s *Set) All() iter.Seq[uuid.UUID] {
	return func(yield func(uuid.UUID) bool) {
		if s == nil {
			return
		}

		for id := range s.members {
			if !yield(id) {
				return
			}
		}
	}
}

// IsEqual reports whether the set has the same members as x.
func (s *Set) IsEqual(x SetR) bool {
	if s.Len() != x.Len() {
		return false
	}

	for id := range x.All() {
		if !s.Has(id) {
			return false
		}
	}

	return true
}

// Diff returns a new set. It contains the members of the set that are missing
// from x.
func (s *Set) Diff(x SetR) *Set {
	diff := &Set{}

	for id := range s.All() {
		if !x.Has(id) {
			diff.Add(id)
		}
	}

	return diff
}

// Clone returns a new set with the same members as the set.
func (s *Set) Clone() *Set {
	if s == nil {
		return &Set{}
	}

	return &Set{members: maps.Clone(s.members)}
}

// String returns the members of the set in braces, in ascending order,
// separated by commas. For example, "{<id1>, <id2>}".
func (s *Set) String() string {
	var w strings.Builder

	w.WriteString("{")

	ids := slices.SortedFunc(
		s.All(),
		uuid.UUID.Compare,
	)

	for i, id := range ids {
		if i > 0 {
			w.WriteString(", ")
		}

		w.WriteString(id.String())
	}

	w.WriteString("}")

	return w.String()
}

// SetR is a read-only view of a [Set].
//
// The zero value is a view of an empty set. Reads through the view see members
// that are added to or deleted from the viewed set after the view is made.
type SetR struct {
	set *Set
}

// Has reports whether the set contains id.
func (s SetR) Has(id uuid.UUID) bool {
	return s.set.Has(id)
}

// Len returns the number of members in the set.
func (s SetR) Len() int {
	return s.set.Len()
}

// IsEmpty reports whether the set has no members.
func (s SetR) IsEmpty() bool {
	return s.set.IsEmpty()
}

// All returns an iterator over the members of the set, in no particular order.
func (s SetR) All() iter.Seq[uuid.UUID] {
	return s.set.All()
}

// IsEqual reports whether the set has the same members as x.
func (s SetR) IsEqual(x SetR) bool {
	return s.set.IsEqual(x)
}

// Diff returns a new set. It contains the members of the set that are missing
// from x.
func (s SetR) Diff(x SetR) *Set {
	return s.set.Diff(x)
}

// Clone returns a new set with the same members as the set.
func (s SetR) Clone() *Set {
	return s.set.Clone()
}

// String returns the members of the set in braces, in ascending order,
// separated by commas. For example, "{<id1>, <id2>}".
func (s SetR) String() string {
	return s.set.String()
}
