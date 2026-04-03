package xerrors

import (
	"errors"
	"fmt"
)

// FatalError is an error that causes the engine to exit completely.
//
// Note that it does not implement an Unwrap() method because we don't want
// fatal errors to be identified by [errors.Is] and [errors.As].
type FatalError struct {
	Cause error
}

// Fatal returns an error that causes the engine to exit completely.
//
// Deprecated: panic
func Fatal(format string, args ...any) error {
	return FatalError{fmt.Errorf(format, args...)}
}

// Bug returns an [FatalError] error that indicates a bug in the engine.
//
// Deprecated: panic
func Bug(format string, args ...any) error {
	return Fatal("[DOGMA BUG] "+format, args...)
}

// IsFatal returns true if err is a [FatalError].
//
// Deprecated: Bugs should panic instead.
func IsFatal(err error) bool {
	var f FatalError
	return errors.As(err, &f)
}

func (e FatalError) Error() string {
	return e.Cause.Error()
}
