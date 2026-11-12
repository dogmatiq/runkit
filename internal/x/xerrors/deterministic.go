package xerrors

import "errors"

// Deterministic returns a deterministic error that wraps err.
//
// It returns err unchanged if err is nil or is already a deterministic error.
//
// Use it for a failure that happens again each time the same work is retried,
// such as data that can't be unmarshaled.
//
// See [IsDeterministic].
func Deterministic(err error) error {
	if err == nil || IsDeterministic(err) {
		return err
	}

	return deterministicError{err}
}

// IsDeterministic returns true if err is a deterministic error.
//
// A deterministic error is, or wraps, an error created by [Deterministic], or a
// [PanicError].
func IsDeterministic(err error) bool {
	if _, ok := errors.AsType[deterministicError](err); ok {
		return true
	}

	_, ok := errors.AsType[PanicError](err)
	return ok
}

type deterministicError struct {
	err error
}

func (e deterministicError) Error() string {
	return e.err.Error()
}

func (e deterministicError) Unwrap() error {
	return e.err
}
