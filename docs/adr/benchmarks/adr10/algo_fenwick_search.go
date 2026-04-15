package adr10

// FenwickSearch performs a Fenwick tree search over the journal to find the
// record whose range covers the target offset, or nil if there is no such
// record.
//
// It descends the implicit Fenwick tree encoded in the FenwickEvents header
// field to locate the target in O(log n) time, without maintaining an explicit
// bracket. It falls back to binary search if the descent produces an invalid
// result.
//
// See https://en.wikipedia.org/wiki/Fenwick_tree
func FenwickSearch(j *Journal, t EventOffset) *Record {
	_, pEnd := j.Bounds()

	// idx is a 1-based count of transactions whose event ranges lie entirely
	// before the target offset. At the end of the descent, j.Get(idx) is the
	// candidate record.
	idx := JournalPosition(0)

	// oBegin accumulates the total number of events in the transactions
	// accounted for by idx, i.e. the event offset at which j.Get(idx) begins.
	var oBegin EventOffset

	// Start with the largest power of two that fits within the journal, then
	// halve it on each step. This is the standard Fenwick tree descent: we
	// consider each bit of the final index from most significant to least
	// significant.
	bit := JournalPosition(1)
	for bit<<1 <= pEnd {
		bit <<= 1
	}

	for bit > 0 {
		// next is the candidate index if we take the current bit: it points to
		// the last position of the power-of-two bucket [idx, idx+bit).
		next := idx + bit

		if next <= pEnd {
			// FenwickEvents at position (next-1) stores the total number of
			// events in the bucket [next - lowbit(next), next), where
			// lowbit(next) == bit. Adding it to oBegin gives the cumulative
			// event count up to and including that bucket.
			record := j.Get(next - 1)
			if oBegin+EventOffset(record.TransactionHeader.FenwickEvents) <= t {
				// The entire bucket lies before the target, so we can advance
				// past it and add its events to the running oBegin.
				idx = next
				oBegin += EventOffset(record.TransactionHeader.FenwickEvents)
			}
		}

		bit >>= 1
	}

	// idx now holds the number of transactions that end entirely before t,
	// so j.Get(idx) is the first transaction that could contain t.
	if idx >= pEnd {
		// The descent overshot the end of the journal. This should not happen
		// with a valid target, but fall back to binary search to be safe.
		return binarySearchInBracket(j, 0, pEnd, t)
	}

	record := j.Get(idx)
	if record.ContainsOffset(t) == LookNoFurther {
		return &record
	}

	// The Fenwick tree produced an incorrect result, likely because of
	// zero-event bookkeeping records disrupting the prefix sums. Fall back to
	// binary search over the full journal.
	return binarySearchInBracket(j, 0, pEnd, t)
}
