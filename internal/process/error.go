package process

import "errors"

// errApplication is an error that indicates a failure in application code or
// message or process state decoding. Deadlines that fail with this error are
// postponed after their delivery transaction is rolled back.
var errApplication = errors.New("process application failure")
