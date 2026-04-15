package adr10

// clamp returns x if it is between min and max, or the nearest bound if it is
// outside that range.
func clamp[T JournalPosition | EventOffset](x, min, max T) T {
	if x < min {
		return min
	}

	if x > max {
		return max
	}

	return x
}

// midpoint returns the value halfway between begin and end. It is the same as
// (begin + end) / 2, but without the risk of overflow.
func midpoint[T JournalPosition | EventOffset](begin, end T) T {
	return T(uint64(begin+end) >> 1)
}

// interpolateByMeanDensity estimates the journal position of the record
// containing t by assuming uniform event density (events per transaction)
// across the bracket [pBegin, pEnd). It returns the position that is the same
// relative distance between pBegin and pEnd as t is between oBegin and oEnd.
//
// The caller must ensure oBegin <= t < oEnd.
func interpolateByMeanDensity(
	pBegin, pEnd JournalPosition,
	oBegin, oEnd EventOffset,
	t EventOffset,
) JournalPosition {
	var (
		numberOfEvents       = oEnd - oBegin
		numberOfTransactions = pEnd - pBegin
		relativeOffset       = t - oBegin
		relativePosition     = JournalPosition(relativeOffset) * numberOfTransactions / JournalPosition(numberOfEvents)
	)

	return pBegin + relativePosition
}

// interpolateByEWMADensity estimates the journal position of the record
// containing t using the local event density (events per transaction) stored in
// r.
//
// Each record stores this density when the transaction is committed, computed
// as an exponentially weighted moving average (EWMA) that gives more weight to
// recent records than older ones. This function uses that per-record rate to
// estimate how far from r's position the target transaction is likely to sit.
//
// Note: the EWMA field exists only in the simulated journal used by this
// benchmark package. Real journal records do not include it — the algorithm
// chosen in the ADR does not require it.
//
// Unlike [interpolateByMeanDensity], which assumes uniform density across the
// whole bracket, this function uses the density local to r. Projecting from
// r.Pos (rather than pBegin) is correct because the EWMA reflects conditions
// near r's position, not conditions across the whole bracket.
//
// It returns false if r's EWMA rate is zero or negative. This can happen if the
// journal contains a long run of transactions that recorded no events — such as
// bookkeeping records — which would pull the EWMA toward zero. A zero rate
// would cause a division by zero, so the caller must fall back to another
// estimate.
//
// The result is clamped to [pBegin, pEnd-1] to ensure it remains within the
// bracket.
func interpolateByEWMADensity(
	r Record,
	t EventOffset,
	pBegin, pEnd JournalPosition,
) (JournalPosition, bool) {
	if r.TransactionHeader.EventsPerTransactionEWMA <= 0 {
		return 0, false
	}

	var (
		targetOffsetDistance    = float64(t) - float64(r.TransactionHeader.Begin)
		estimatedRecordDistance = int(targetOffsetDistance / r.TransactionHeader.EventsPerTransactionEWMA)
	)

	return clamp(
		r.Pos+JournalPosition(estimatedRecordDistance),
		pBegin,
		pEnd-1,
	), true
}
