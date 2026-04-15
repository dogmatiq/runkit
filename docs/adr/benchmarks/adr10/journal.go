package adr10

// JournalSize is the number of transactions in the benchmark journal.
const JournalSize = 1_000_000

type (
	// JournalPosition is the position of a transaction in the journal, which is
	// a zero-based index.
	JournalPosition uint64

	// EventOffset is the offset of an event in the stream, which is a
	// zero-based index.
	EventOffset uint64
)

// Journal is an in-memory simulation of the read-only portion of
// persistencekit's Journal interface, used as a benchmark fixture.
//
// Unlike the real interface, all operations are synchronous, infallible, and
// require no context. The metrics methods (EnableMetrics, DisableMetrics, and
// CaptureMetrics) are benchmark infrastructure with no equivalent in the real
// interface.
type Journal struct {
	transactions   []Record
	reads          int
	metricsEnabled bool
}

// EnableMetrics enables read tracking.
func (j *Journal) EnableMetrics() {
	j.metricsEnabled = true
}

// DisableMetrics disables read tracking.
func (j *Journal) DisableMetrics() {
	j.metricsEnabled = false
}

// CaptureMetrics resets the read counter to zero and returns the previous count.
func (j *Journal) CaptureMetrics() int {
	n := j.reads
	j.reads = 0
	return n
}

// Get returns the record at the given position.
func (j *Journal) Get(pos JournalPosition) Record {
	if j.metricsEnabled {
		j.reads++
	}
	return j.transactions[pos]
}

// Bounds returns the half-open interval [begin, end) describing the positions
// of the first and last transactions in the journal.
//
// Since this journal is never truncated, begin is always 0.
func (j *Journal) Bounds() (begin, end JournalPosition) {
	if j.metricsEnabled {
		j.reads++
	}
	return 0, JournalPosition(len(j.transactions))
}

// Record models a journal record that contains an event stream transaction
// header. The "operations" portion of the transaction is omitted since it is
// not relevant to the search algorithms.
type Record struct {
	// Pos is the location of this record within the journal.
	Pos JournalPosition

	// TransactionHeader is the transaction header, as described by ADR-10 with
	// additional fields used for candidate search algorithms that were
	// ultimately rejected.
	TransactionHeader struct {
		Begin, End EventOffset

		// EventsPerTransactionEWMA is an exponentially weighted moving average
		// (EWMA) of events-per-transaction. It tracks "local" density preceding
		// this transaction.
		//
		// See
		// https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
		//
		// This field is not present in real transaction headers, it is present
		// only to benchmark the [InterpolationSearchByEWMA] and
		// [CoarseFineSearch] algorithms.
		EventsPerTransactionEWMA float64

		// FenwickEvents is the number of events covered by the Fenwick-tree
		// bucket rooted at this transaction. It represents a power-of-two-sized
		// suffix ending at this transaction's journal position.
		FenwickEvents uint64
	}
}

// ComparisonResult is the result of comparing a transaction's [begin, end)
// header to a target offset.
type ComparisonResult int

// These values are returned by [Record.ContainsOffset] when comparing a
// transaction's header to a target offset.
const (
	LookToTheLeft  ComparisonResult = -1
	LookNoFurther  ComparisonResult = 0
	LookToTheRight ComparisonResult = 1
)

// ContainsOffset compares the transaction's header to the given offset.
func (t Record) ContainsOffset(offset EventOffset) ComparisonResult {
	if t.TransactionHeader.Begin > offset {
		return LookToTheLeft
	}

	if t.TransactionHeader.End <= offset {
		return LookToTheRight
	}

	return LookNoFurther
}
