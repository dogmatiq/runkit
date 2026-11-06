package xtesting

import (
	"runtime"
	"testing"
)

// LogGoroutines logs the stack of every goroutine, so that a test that times
// out shows where the goroutines it was waiting on are blocked.
func LogGoroutines(t testing.TB) {
	t.Helper()

	buf := make([]byte, 1<<20)

	// [runtime.Stack] truncates the dump to the buffer, so the buffer grows
	// until the dump fits.
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			t.Logf("goroutines:\n%s", buf[:n])
			return
		}

		buf = make([]byte, 2*len(buf))
	}
}
