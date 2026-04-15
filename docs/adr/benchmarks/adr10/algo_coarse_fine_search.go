package adr10

// CoarseFineSearch searches the journal for the record whose range
// covers the target offset, or nil if there is no such record.
//
// It makes at most two informed probes — one using whole-journal average
// event density (events per transaction), one using the local event density
// near the first probe — before falling back to binary search over the
// remaining bracket.
//
// The first (coarse) probe uses the average event density across the whole
// journal to estimate a position near the target. The second (fine) probe
// uses the per-record exponentially weighted moving average (EWMA) density
// stored on the coarse probe's record to refine the estimate. If either probe
// hits, the search terminates early. Otherwise, binary search resolves the
// remaining bracket.
func CoarseFineSearch(j *Journal, t EventOffset) *Record {
	var (
		pBegin, pEnd = j.Bounds()
		oBegin       EventOffset
		oEnd         = j.Get(pEnd - 1).TransactionHeader.End
	)

	pCoarse := interpolateByMeanDensity(pBegin, pEnd, oBegin, oEnd, t)
	coarse := j.Get(pCoarse)

	switch coarse.ContainsOffset(t) {
	case LookNoFurther:
		return &coarse
	case LookToTheLeft:
		pEnd = coarse.Pos
	case LookToTheRight:
		pBegin = coarse.Pos + 1
	}

	if pBegin >= pEnd {
		return nil
	}

	pFine, ok := interpolateByEWMADensity(coarse, t, pBegin, pEnd)
	if !ok {
		// Without local-density metadata there is no second informed probe, so
		// fall back to binary search over the bracket.
		return binarySearchInBracket(j, pBegin, pEnd, t)
	}

	fine := j.Get(pFine)

	switch fine.ContainsOffset(t) {
	case LookNoFurther:
		return &fine
	case LookToTheLeft:
		pEnd = fine.Pos
	case LookToTheRight:
		pBegin = fine.Pos + 1
	}

	if pBegin >= pEnd {
		return nil
	}

	return binarySearchInBracket(j, pBegin, pEnd, t)
}
