package adr10

// InterpolationSearchByEWMA performs an interpolation search over the journal
// to find the record whose range covers the target offset, or nil if there is
// no such record.
//
// After each probe it estimates the next probe position using the EWMA event
// density (events per transaction) stored on the probed record. If the record
// does not carry EWMA data, it falls back to the bracket's mean event density.
func InterpolationSearchByEWMA(j *Journal, t EventOffset) *Record {
	var (
		pBegin, pEnd = j.Bounds()
		oBegin       EventOffset
		oEnd         = j.Get(pEnd - 1).TransactionHeader.End
		pProbe       = interpolateByMeanDensity(pBegin, pEnd, oBegin, oEnd, t)
	)

	for {
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

		if pBegin >= pEnd {
			return nil
		}

		var ok bool
		pProbe, ok = interpolateByEWMADensity(record, t, pBegin, pEnd)
		if !ok {
			// When the probe does not carry usable EWMA data, fall back to the
			// bracket's mean density.
			pProbe = interpolateByMeanDensity(pBegin, pEnd, oBegin, oEnd, t)
			continue
		}

		// If the EWMA projection lands on the bracket edge, use the midpoint
		// instead. This avoids repeatedly hugging one side when the EWMA lags a
		// sharp regime change.
		if pProbe == pBegin || pProbe == pEnd-1 {
			pProbe = midpoint(pBegin, pEnd)
		}
	}
}
