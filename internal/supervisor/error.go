package supervisor

import "fmt"

// Fatal returns an error that causes [Run] to stop retrying and return err.
//
// It is used for failures that retrying can not recover from.
func Fatal(err error) error {
	return fatalError{err}
}

// Fatalf returns an error formatted according to format, as per [fmt.Errorf],
// that causes [Run] to stop retrying and return it.
//
// It is used for failures that retrying can not recover from.
func Fatalf(format string, args ...any) error {
	return Fatal(fmt.Errorf(format, args...))
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

// Deterministic returns an error that causes [Run] to retry fn as after a
// deterministic failure, which retrying alone can't clear.
//
// [Run] logs such a failure at ERROR at once, and waits longer between retries
// after it than after others.
func Deterministic(err error) error {
	return deterministicError{err}
}

// Deterministicf returns an error formatted according to format, as per
// [fmt.Errorf], that causes [Run] to retry fn as described by [Deterministic].
func Deterministicf(format string, args ...any) error {
	return Deterministic(fmt.Errorf(format, args...))
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
