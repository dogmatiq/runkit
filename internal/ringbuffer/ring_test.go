package ringbuffer_test

import (
	"testing"

	. "github.com/dogmatiq/runkit/internal/ringbuffer"
	"pgregory.net/rapid"
)

func TestRing(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		const capacity = 10
		var (
			elements []int
			subject  = New[int](capacity)
		)

		t.Repeat(map[string]func(*rapid.T){
			"push": func(t *rapid.T) {
				v := rapid.Int().Draw(t, "value")

				subject.Push(v)
				elements = append(elements, v)

				if len(elements) > capacity {
					copy(elements, elements[1:])
					elements = elements[:capacity]
				}
			},

			"": func(t *rapid.T) {
				// Cap()
				{
					if subject.Cap() != capacity {
						t.Fatalf("unexpected capacity: got %d, want %d", subject.Cap(), capacity)
					}
				}

				// Len()
				{
					if subject.Len() != len(elements) {
						t.Fatalf("unexpected length: got %d, want %d", subject.Len(), len(elements))
					}
				}

				// At()
				{
					for i, want := range elements {
						got := subject.At(i)

						if got != want {
							t.Fatalf("unexpected value at index %d: got %d, want %d", i, got, want)
						}
					}
				}

				// All()
				{
					count := 0

					for i, got := range subject.All() {
						count++
						want := elements[i]

						if got != want {
							t.Fatalf("unexpected value yielded at index %d: got %d, want %d", i, got, want)
						}
					}

					if count != len(elements) {
						t.Fatalf("unexpected number of values yielded: got %d, want %d", count, len(elements))
					}
				}

				// Slice()
				{
					var begin, end int
					if len(elements) > 0 {
						begin = rapid.IntRange(0, len(elements)-1).Draw(t, "begin")
						end = rapid.IntRange(begin, len(elements)).Draw(t, "end")
					}

					expect := elements[begin:end]
					count := 0

					for i, got := range subject.Slice(begin, end) {
						count++
						want := expect[i]

						if got != want {
							t.Fatalf("unexpected value yielded at index %d: got %d, want %d", i, got, want)
						}
					}

					if count != len(expect) {
						t.Fatalf("unexpected number of values yielded: got %d, want %d", count, len(expect))
					}
				}

				// full Slice()
				{
					count := 0

					for i, got := range subject.Slice(0, subject.Len()) {
						count++
						want := elements[i]

						if got != want {
							t.Fatalf("unexpected value yielded at index %d: got %d, want %d", i, got, want)
						}
					}

					if count != len(elements) {
						t.Fatalf("unexpected number of values yielded: got %d, want %d", count, len(elements))
					}
				}
			},
		})
	})
}
