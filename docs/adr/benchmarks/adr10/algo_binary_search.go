package adr10

// BinarySearch performs a binary search over the journal to find the record
// whose range covers the target offset, or nil if there is no such record.
//
// It serves as a correctness and performance baseline.
func BinarySearch(j *Journal, t EventOffset) *Record {
	pBegin, pEnd := j.Bounds()
	return binarySearchInBracket(j, pBegin, pEnd, t)
}

// binarySearchInBracket performs a binary search over the bracket [pBegin, pEnd).
func binarySearchInBracket(
	j *Journal,
	pBegin, pEnd JournalPosition,
	t EventOffset,
) *Record {
	for pBegin < pEnd {
		pProbe := midpoint(pBegin, pEnd)
		record := j.Get(pProbe)

		switch record.ContainsOffset(t) {
		case LookNoFurther:
			return &record

		case LookToTheLeft:
			// Rule out the probe position and any records to the right of it.
			pEnd = pProbe

		case LookToTheRight:
			// Rule out the probe position and any records to the left of it.
			pBegin = pProbe + 1
		}
	}

	return nil
}
