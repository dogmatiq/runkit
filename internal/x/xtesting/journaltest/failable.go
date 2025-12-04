package journaltest

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

// FailurePoint is an enumeration of journal operations that can be
// forced to fail using a [FailableJournalStore].
type FailurePoint string

// Errors that can be triggered by [Failer.Induce].
var (
	BeforeJournalOpen   FailurePoint = "before journal open"
	BeforeJournalAppend FailurePoint = "before journal append"
	AfterJournalAppend  FailurePoint = "after journal append"
)

// FailableBinaryStore is a [journal.Store] that can be configured to induce errors
// on specific operations.
type FailableBinaryStore struct {
	m      sync.Mutex
	store  memoryjournal.BinaryStore
	counts map[FailurePoint]uint64
}

// Open returns the journal with the given name.
func (s *FailableBinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	s.m.Lock()
	if s.store.BeforeOpen == nil {
		s.store.BeforeOpen = func(string) error { return s.fail(BeforeJournalOpen) }
		s.store.BeforeAppend = func(string, []byte) error { return s.fail(BeforeJournalAppend) }
		s.store.AfterAppend = func(string, []byte) error { return s.fail(AfterJournalAppend) }
	}
	s.m.Unlock()

	return s.store.Open(ctx, name)
}

func (s *FailableBinaryStore) init() {
	if s.counts == nil {
		s.counts = map[FailurePoint]uint64{}
	}
}

// ScheduleFailure schedules an error to be returned on the next occurrence of
// the given operation.
//
// Each time it's called, it schedules one additional failure for that
// operation.
func (s *FailableBinaryStore) ScheduleFailure(fp FailurePoint) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.counts == nil {
		s.counts = map[FailurePoint]uint64{}
	}

	s.counts[fp]++
}

// WillFail reports whether the given operation is scheduled to fail.
func (s *FailableBinaryStore) WillFail(fp FailurePoint) bool {
	s.m.Lock()
	defer s.m.Unlock()

	return s.counts[fp] > 0
}

// fail returns an induced error if the given operation is scheduled to fail and
// decrements the counter for that operation. Otherwise, it returns nil.
func (s *FailableBinaryStore) fail(fp FailurePoint) error {
	s.m.Lock()
	defer s.m.Unlock()

	n := s.counts[fp]
	if n == 0 {
		return nil
	}

	s.counts[fp]--

	return inducedError{fp}
}

type inducedError struct {
	fp FailurePoint
}

func (e inducedError) Error() string {
	return "induced journal error: " + string(e.fp)
}

// IsInducedError returns true if the given error was induced by an
// [Failer].
func IsInducedError(err error) bool {
	var x inducedError
	return errors.As(err, &x)
}
