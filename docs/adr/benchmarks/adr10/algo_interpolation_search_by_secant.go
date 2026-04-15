package adr10

// InterpolationSearchBySecant performs an interpolation search over the journal
// to find the record whose range covers the target offset, or nil if there is
// no such record.
//
// It estimates each probe position from the local event density
// (events per transaction), measured from the slope between two previously
// observed records — one on each side of the target. Until both records are
// available, it uses the bracket's mean event density.
//
// Unlike [InterpolationSearchByEWMA], the density signal comes from probes made
// during the search itself, not from write-time metadata.
//
// See https://en.wikipedia.org/wiki/Secant_method
func InterpolationSearchBySecant(j *Journal, t EventOffset) *Record {
	var (
		pBegin, pEnd = j.Bounds()
		oBegin       EventOffset
		oEnd         = j.Get(pEnd - 1).TransactionHeader.End

		// haveLeftSample and haveRightSample track whether we have observed a
		// probe to the left and right of the target, respectively. Until we have
		// both, we fall back to the bracket-average interpolation.
		haveLeftSample  bool
		haveRightSample bool

		// leftSamplePos/rightSamplePos are the journal positions of the two most
		// recent probes that fell to the left and right of the target. Together
		// they define the secant line used to project the next probe.
		leftSamplePos  JournalPosition
		rightSamplePos JournalPosition

		// leftSampleOffset/rightSampleOffset are the representative event offsets
		// of those probes. We use the midpoint of each record's event range
		// rather than its Begin or End, so that the slope estimate is not biased
		// toward either edge of the record.
		leftSampleOffset  EventOffset
		rightSampleOffset EventOffset

		// lastProbe detects when consecutive probes land on the same record,
		// which signals that the slope estimate is no longer reliable.
		lastProbe = -1
	)

	for pBegin < pEnd {
		// Start with the bracket-average interpolation as the default probe.
		pProbe := interpolateByMeanDensity(pBegin, pEnd, oBegin, oEnd, t)

		if haveLeftSample && haveRightSample &&
			leftSamplePos < rightSamplePos &&
			leftSampleOffset < rightSampleOffset {
			// We have two valid sample points. Apply the secant projection:
			// estimate the probe position by interpolating between the two
			// samples using the observed slope between them, rather than the
			// bracket's average slope.
			spanPos := rightSamplePos - leftSamplePos
			spanOffset := rightSampleOffset - leftSampleOffset
			targetOffsetDistance := int64(t) - int64(leftSampleOffset)

			pProbe = clamp(
				leftSamplePos+JournalPosition(targetOffsetDistance*int64(spanPos)/int64(spanOffset)),
				pBegin,
				pEnd-1,
			)
		}

		// If the probe has landed on a bracket edge or on the same record as
		// last time, the slope estimate is no longer reliable (e.g. the two
		// samples have collapsed onto the same record, or the estimate is
		// oscillating). Fall back to the bracket midpoint to guarantee progress.
		if pBegin+1 < pEnd && (pProbe == pBegin || pProbe == pEnd-1 || int(pProbe) == lastProbe) {
			pProbe = midpoint(pBegin, pEnd)
		}

		record := j.Get(pProbe)
		lastProbe = int(pProbe)

		switch record.ContainsOffset(t) {
		case LookNoFurther:
			return &record

		case LookToTheLeft:
			// Rule out the probe position and any records to the right of it.
			// Record this probe as the new right-side sample for the secant slope.
			haveRightSample = true
			rightSamplePos = record.Pos
			rightSampleOffset = midpoint(record.TransactionHeader.Begin, record.TransactionHeader.End)

			pEnd = record.Pos
			oEnd = record.TransactionHeader.Begin

		case LookToTheRight:
			// Rule out the probe position and any records to the left of it.
			// Record this probe as the new left-side sample for the secant slope.
			haveLeftSample = true
			leftSamplePos = record.Pos
			leftSampleOffset = midpoint(record.TransactionHeader.Begin, record.TransactionHeader.End)

			pBegin = record.Pos + 1
			oBegin = record.TransactionHeader.End
		}
	}

	return nil
}
