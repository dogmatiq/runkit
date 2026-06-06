package commandqueue

// BackoffBase is the base interval of the exponential backoff applied to
// Nack'd commands.
//
// It is exported for testing purposes, but is not intended to be used by
// application code.
const BackoffBase = baseRetryInterval

// BackoffCap is the maximum interval between retries.
//
// It is exported for testing purposes, but is not intended to be used by
// application code.
const BackoffCap = maximumRetryInterval
