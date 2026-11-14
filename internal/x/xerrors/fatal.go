package xerrors

import "errors"

// Fatal returns a fatal error that wraps err.
//
// It returns err unchanged if err is nil or is already a fatal error.
//
// Use it for a failure after which the work must stop instead of being
// retried, such as a server that can't listen on its configured address.
//
// See [IsFatal].
func Fatal(err error) error {
	if err == nil || IsFatal(err) {
		return err
	}

	return fatalError{err}
}

// IsFatal returns true if err is a fatal error.
//
// A fatal error is, or wraps, an error created by [Fatal].
func IsFatal(err error) bool {
	_, ok := errors.AsType[fatalError](err)
	return ok
}

type fatalError struct {
	err error
}

func (e fatalError) Error() string {
	return e.err.Error()
}

func (e fatalError) Unwrap() error {
	return e.err
}
