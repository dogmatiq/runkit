package journaltest

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/journal"
)

// FailurePoint is an enumeration of journal operations that can be
// forced to fail using a [FailableBinaryStore].
type FailurePoint string

// Errors that can be induced.
var (
	BeforeOpen   FailurePoint = "before open"
	BeforeAppend FailurePoint = "before append"
	AfterAppend  FailurePoint = "after append"
)

// IsInducedError returns true if the given error was induced by an
// [Failer].
func IsInducedError(err error) bool {
	var x inducedError
	return errors.As(err, &x)
}

// FailableBinaryStore is a [journal.Store] that can be configured to induce errors
// on specific operations.
type FailableBinaryStore struct {
	NonFailing memoryjournal.BinaryStore

	in       journal.BinaryInterceptor
	m        sync.Mutex
	failable journal.BinaryStore
	counts   map[FailurePoint]uint64
}

// Open returns the journal with the given name.
func (s *FailableBinaryStore) Open(ctx context.Context, name string) (journal.BinaryJournal, error) {
	s.m.Lock()
	s.init()
	s.m.Unlock()

	return s.failable.Open(ctx, name)
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

func (s *FailableBinaryStore) init() {
	if s.failable != nil {
		return
	}

	s.failable = journal.WithInterceptor(&s.NonFailing, &s.in)

	s.in.BeforeOpen(func(string) error {
		return s.fail(BeforeOpen)
	})

	s.in.BeforeAppend(func(string, []byte) error {
		return s.fail(BeforeAppend)
	})

	s.in.AfterAppend(func(string, []byte) error {
		return s.fail(AfterAppend)
	})
}

type inducedError struct {
	fp FailurePoint
}

func (e inducedError) Error() string {
	return "induced journal error: " + string(e.fp)
}
