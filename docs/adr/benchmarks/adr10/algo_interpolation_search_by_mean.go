package adr10

// InterpolationSearchByMean performs an interpolation search over the journal
// to find the record whose range covers the target offset, or nil if there is
// no such record.
//
// It estimates each probe position from the mean event density (events per
// transaction) across the current bracket.
//
// This is the algorithm that was ultimately chosen in ADR-10.
func InterpolationSearchByMean(j *Journal, t EventOffset) *Record {
	var (
		pBegin, pEnd = j.Bounds()
		oBegin       = EventOffset(0)
		oEnd         = j.Get(pEnd - 1).TransactionHeader.End
	)

	for pBegin < pEnd {
		pProbe := interpolateByMeanDensity(pBegin, pEnd, oBegin, oEnd, t)
		record := j.Get(pProbe)

		switch record.ContainsOffset(t) {
		case LookNoFurther:
			return &record

		case LookToTheLeft:
			// Rule out the probe position and any records to the right of it.
			pEnd = pProbe
			oEnd = record.TransactionHeader.Begin

		case LookToTheRight:
			// Rule out the probe position and any records to the left of it.
			pBegin = pProbe + 1
			oBegin = record.TransactionHeader.End
		}
	}

	return nil
}
