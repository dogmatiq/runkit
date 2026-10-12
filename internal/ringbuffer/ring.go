package ringbuffer

import (
	"fmt"
	"iter"
)

// Ring is a fixed-capacity ring buffer.
type Ring[T any] struct {
	values []T
	head   int
}

// New returns a new ring buffer with the given capacity.
func New[T any](capacity int) *Ring[T] {
	return &Ring[T]{
		values: make([]T, 0, capacity),
	}
}

// Cap returns the maximum number of values that can be stored in the ring
// buffer.
func (r *Ring[T]) Cap() int {
	return cap(r.values)
}

// Len returns the number of values currently stored in the ring buffer.
func (r *Ring[T]) Len() int {
	return len(r.values)
}

// Push adds a value to the ring buffer, overwriting the oldest value if the
// buffer is full.
func (r *Ring[T]) Push(v T) {
	if len(r.values) < cap(r.values) {
		r.values = append(r.values, v)
		return
	}

	r.values[r.head] = v
	r.head++

	if r.head == cap(r.values) {
		r.head = 0
	}
}

// At returns the value at the given index, where 0 is the oldest value and
// Len()-1 is the newest value.
func (r *Ring[T]) At(idx int) T {
	if idx < 0 || idx >= len(r.values) {
		panic(fmt.Sprintf("index out of range [%d] with length %d", idx, len(r.values)))
	}

	idx = r.indexOf(idx)

	return r.values[idx]
}

// All returns an iterator that yields all of the values in the ring buffer, in
// the order they were written.
func (r *Ring[T]) All() iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		yieldSlices(
			yield,
			r.values[r.head:],
			r.values[:r.head],
		)
	}
}

// Slice returns an iterator that yields the values in the ring buffer between
// the given begin and end indices, where 0 is the oldest value and Len()-1 is
// the newest value.
func (r *Ring[T]) Slice(begin, end int) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		if end < begin {
			panic(fmt.Sprintf("invalid slice indices: %d < %d", end, begin))
		}

		if begin < 0 || end > len(r.values) {
			panic(fmt.Sprintf("slice bounds out of range [%d:%d] with length %d", begin, end, len(r.values)))
		}

		if begin == end {
			return
		}

		begin = r.indexOf(begin)
		end = r.indexOf(end)

		if begin < end {
			yieldSlices(
				yield,
				r.values[begin:end],
			)
		} else {
			yieldSlices(
				yield,
				r.values[begin:],
				r.values[:end],
			)
		}
	}
}

// indexOf returns the index of the value at the given logical index, where 0 is
// the oldest value and Len()-1 is the newest value.
func (r *Ring[T]) indexOf(idx int) int {
	if r.head == 0 {
		return idx
	}

	idx += r.head

	if idx >= cap(r.values) {
		idx -= cap(r.values)
	}

	return idx
}

// yieldSlices yields the values in the given slices.
func yieldSlices[T any](
	yield func(int, T) bool,
	slices ...[]T,
) {
	idx := 0

	for _, slice := range slices {
		for _, v := range slice {
			if !yield(idx, v) {
				return
			}
			idx++
		}
	}
}
