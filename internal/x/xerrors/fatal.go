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
func Fatal(format string, args ...any) error {
	return FatalError{fmt.Errorf(format, args...)}
}

// Bug returns an [FatalError] error that indicates a bug in the engine.
func Bug(format string, args ...any) error {
	return fmt.Errorf("[BUG] "+format, args...)
}

// IsFatal returns true if err is a [FatalError].
func IsFatal(err error) bool {
	var f FatalError
	return errors.As(err, &f)
}

func (e FatalError) Error() string {
	return e.Cause.Error()
}
