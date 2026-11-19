package uuidset_test

import (
	"iter"
	"slices"
	"testing"
	"uuid"

	. "github.com/dogmatiq/runkit/internal/uuidset"
)

var (
	id1 = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	id2 = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	id3 = uuid.MustParse("33333333-3333-4333-8333-333333333333")
)

// members returns the members of the set in ascending order.
func members(s interface{ All() iter.Seq[uuid.UUID] }) []uuid.UUID {
	return slices.SortedFunc(
		s.All(),
		uuid.UUID.Compare,
	)
}

// expectEmpty fails the test unless the set is empty.
func expectEmpty(t *testing.T, s interface {
	Has(uuid.UUID) bool
	Len() int
	IsEmpty() bool
	All() iter.Seq[uuid.UUID]
	String() string
}) {
	t.Helper()

	if s.Has(id1) {
		t.Fatal("expected the set not to have a member")
	}

	if s.Len() != 0 {
		t.Fatalf("unexpected length: got %d, want 0", s.Len())
	}

	if !s.IsEmpty() {
		t.Fatal("expected the set to be empty")
	}

	if got := members(s); len(got) != 0 {
		t.Fatalf("unexpected members: %v", got)
	}

	if got, want := s.String(), "{}"; got != want {
		t.Fatalf("unexpected string: got %q, want %q", got, want)
	}
}

func TestSet(t *testing.T) {
	t.Run("func Add()", func(t *testing.T) {
		t.Run("it adds the member to a zero-value set", func(t *testing.T) {
			var s Set
			s.Add(id1)

			if !s.Has(id1) {
				t.Fatal("expected the set to have the member")
			}
		})

		t.Run("it keeps one member when the same ID is added twice", func(t *testing.T) {
			s := New(id1)
			s.Add(id1)

			if got, want := s.Len(), 1; got != want {
				t.Fatalf("unexpected length: got %d, want %d", got, want)
			}
		})
	})

	t.Run("func Delete()", func(t *testing.T) {
		t.Run("it removes the member", func(t *testing.T) {
			s := New(id1, id2)
			s.Delete(id1)

			want := []uuid.UUID{id2}
			if got := members(s); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})

		t.Run("it leaves the set unchanged when the ID isn't a member", func(t *testing.T) {
			s := New(id1)
			s.Delete(id2)

			want := []uuid.UUID{id1}
			if got := members(s); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})

		t.Run("it leaves a zero-value set empty", func(t *testing.T) {
			var s Set
			s.Delete(id1)

			expectEmpty(t, &s)
		})
	})

	t.Run("func All()", func(t *testing.T) {
		t.Run("it yields each member once", func(t *testing.T) {
			s := New(id3, id1, id2, id1)

			want := []uuid.UUID{id1, id2, id3}
			if got := members(s); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})

		t.Run("it stops when the caller stops", func(t *testing.T) {
			var n int
			for range New(id1, id2, id3).All() {
				n++
				break
			}

			if n != 1 {
				t.Fatalf("unexpected number of members: got %d, want 1", n)
			}
		})
	})

	t.Run("func IsEqual()", func(t *testing.T) {
		tests := []struct {
			Name string
			A    *Set
			B    *Set
			Want bool
		}{
			{
				Name: "same members",
				A:    New(id1, id2),
				B:    New(id2, id1),
				Want: true,
			},
			{
				Name: "both empty",
				A:    New(),
				B:    &Set{},
				Want: true,
			},
			{
				Name: "different members",
				A:    New(id1, id2),
				B:    New(id1, id3),
				Want: false,
			},
			{
				Name: "subset",
				A:    New(id1),
				B:    New(id1, id2),
				Want: false,
			},
			{
				Name: "superset",
				A:    New(id1, id2),
				B:    New(id1),
				Want: false,
			},
		}

		for _, test := range tests {
			t.Run(test.Name, func(t *testing.T) {
				other := test.B.R()

				if got := test.A.IsEqual(other); got != test.Want {
					t.Fatalf("unexpected result: got %t, want %t", got, test.Want)
				}
			})
		}
	})

	t.Run("func Diff()", func(t *testing.T) {
		t.Run("it returns the members that are missing from the other set", func(t *testing.T) {
			other := New(id2, id3).R()
			diff := New(id1, id2).Diff(other)

			want := []uuid.UUID{id1}
			if got := members(diff); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})
	})

	t.Run("func Clone()", func(t *testing.T) {
		t.Run("it returns a set that changes independently", func(t *testing.T) {
			s := New(id1)
			clone := s.Clone()

			clone.Add(id2)
			s.Add(id3)

			if clone.Has(id3) {
				t.Fatalf("expected the clone not to see a change to the original: %s", clone)
			}

			if s.Has(id2) {
				t.Fatalf("expected the original not to see a change to the clone: %s", s)
			}
		})
	})

	t.Run("func String()", func(t *testing.T) {
		t.Run("it lists the members in ascending order", func(t *testing.T) {
			got := New(id3, id1, id2).String()
			want := "{" + id1.String() + ", " + id2.String() + ", " + id3.String() + "}"

			if got != want {
				t.Fatalf("unexpected string: got %q, want %q", got, want)
			}
		})
	})
}

func TestNilSet(t *testing.T) {
	var s *Set

	t.Run("it reads as an empty set", func(t *testing.T) {
		expectEmpty(t, s)

		other := New().R()
		if !s.IsEqual(other) {
			t.Fatal("expected a nil set to equal an empty set")
		}
	})

	t.Run("it can be cloned", func(t *testing.T) {
		expectEmpty(t, s.Clone())
	})

	t.Run("it can be diffed", func(t *testing.T) {
		other := New(id1).R()

		expectEmpty(t, s.Diff(other))
	})
}

func TestSetR(t *testing.T) {
	t.Run("when it's the zero value", func(t *testing.T) {
		var s SetR

		t.Run("it reads as an empty set", func(t *testing.T) {
			expectEmpty(t, s)

			other := New().R()
			if !s.IsEqual(other) {
				t.Fatal("expected the zero value to equal an empty set")
			}
		})

		t.Run("it can be cloned", func(t *testing.T) {
			expectEmpty(t, s.Clone())
		})

		t.Run("it can be diffed", func(t *testing.T) {
			other := New(id1).R()

			expectEmpty(t, s.Diff(other))
		})
	})

	t.Run("when it views a set", func(t *testing.T) {
		t.Run("it sees members that are added or deleted afterwards", func(t *testing.T) {
			s := New(id1)
			view := s.R()

			s.Add(id2)
			s.Delete(id1)

			want := []uuid.UUID{id2}
			if got := members(view); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})

		t.Run("it can be compared with another view", func(t *testing.T) {
			a := New(id1, id2).R()
			b := New(id2, id1).R()

			if !a.IsEqual(b) {
				t.Fatal("expected the views to be equal")
			}
		})

		t.Run("it can be diffed against another view", func(t *testing.T) {
			a := New(id1, id2).R()
			b := New(id1).R()

			want := []uuid.UUID{id2}
			if got := members(a.Diff(b)); !slices.Equal(got, want) {
				t.Fatalf("unexpected members: got %v, want %v", got, want)
			}
		})
	})
}
