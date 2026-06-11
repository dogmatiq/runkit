package xerrors

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// PanicError is an error that wraps a value recovered from a panic, along with
// the stack trace at the point of the panic.
type PanicError struct {
	Value      any
	StackTrace string
}

func (e PanicError) Error() string {
	return fmt.Sprintf("recovered from panic: %v", e.Value)
}

func (e PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// Recover calls fn and returns a [PanicError] if fn panics.
//
// If the panic originates directly within fn itself, it is not caught and
// propagates normally. This ensures that bugs in the engine's own closure logic
// are not silently swallowed.
func Recover(fn func()) (err error) {
	// Capture the stack depth from [Recover] down to the goroutine root. This
	// is used to trim engine frames from the bottom of the panic stack so that
	// the stack trace only includes user code.
	var pcs [128]uintptr
	depth := runtime.Callers(1, pcs[:])

	defer func() {
		if r := recover(); r != nil {
			stack := captureStack(depth)
			if stack == "" {
				// The panic originated directly in fn (no user frames between
				// the panic site and the closure). Propagate it.
				panic(r)
			}
			err = PanicError{
				Value:      r,
				StackTrace: stack,
			}
		}
	}()

	fn()
	return nil
}

// captureStack returns a formatted stack trace of the panicking code,
// excluding recovery machinery at the top and engine frames (including the
// closure passed to [Recover]) at the bottom.
func captureStack(depth int) string {
	var pcs [128]uintptr

	// Skip: runtime.Callers, captureStack, deferred func, runtime.gopanic.
	n := runtime.Callers(4, pcs[:])

	// Trim the closure frame and engine frames from the bottom.
	n -= depth + 1
	if n <= 0 {
		return ""
	}

	var buf strings.Builder
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		buf.WriteString(frame.File)
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(frame.Line))
		buf.WriteByte(' ')
		buf.WriteString(frame.Function)
		buf.WriteByte('\n')
		if !more {
			break
		}
	}

	return buf.String()
}
