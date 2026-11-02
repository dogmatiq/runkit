package supervisor

// Fatal returns an error that causes [Run] to stop retrying and return err.
//
// It is used for failures that retrying can not recover from.
func Fatal(err error) error {
	return fatalError{err}
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

type persistentError struct {
	err error
}

func (e persistentError) Error() string {
	return e.err.Error()
}

func (e persistentError) Unwrap() error {
	return e.err
}
