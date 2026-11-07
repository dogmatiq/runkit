package process

import "errors"

// errApplication identifies failures in application code or message and state
// decoding, so deadline delivery can postpone the failed message after rollback.
var errApplication = errors.New("process application failure")
