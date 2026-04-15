package adr10

// ExponentialSearch probes backwards from the end of the journal in
// exponentially increasing steps until it finds a record that is to the left of
// the target offset, then performs a binary search between that record and the
// prior probe position.
func ExponentialSearch(j *Journal, target EventOffset) *Record {
	var (
		pBegin, pEnd = j.Bounds()
		stepSize     = JournalPosition(1)
		pProbe       = pEnd - stepSize
	)

	for {
		record := j.Get(pProbe)

		switch record.ContainsOffset(target) {
		case LookNoFurther:
			return &record

		case LookToTheLeft:
			// Rule out the probe position and any records to the right of it.
			pEnd = pProbe

			// Increase the step size exponentially.
			stepSize *= 2

			// Choose the next probe, but make sure we don't underflow pBegin.
			if stepSize >= (pEnd - pBegin) {
				pProbe = pBegin
			} else {
				pProbe = pEnd - stepSize
			}

		case LookToTheRight:
			// Rule out the probe position and any records to the left of it.
			// Then perform a binary search over the remaining bracket.
			pBegin = pProbe + 1
			return binarySearchInBracket(j, pBegin, pEnd, target)
		}
	}
}
