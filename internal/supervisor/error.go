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

// Persistent returns an error that causes [Run] to retry fn with a backoff
// suited to failures that persist until something outside the engine changes,
// such as a handler's code or data.
//
// [Run] waits longer between retries after such a failure than after others.
func Persistent(err error) error {
	return persistentError{err}
}

// Persistentf returns an error formatted according to format, as per
// [fmt.Errorf], that causes [Run] to retry fn with the backoff described by
// [Persistent].
func Persistentf(format string, args ...any) error {
	return Persistent(fmt.Errorf(format, args...))
}

type persistentError struct {
	err error
}

func (e persistentError) Error() string {
	return e.err.Error()
}

func (e persistentError) Unwrap() error {
	return e.err
}
