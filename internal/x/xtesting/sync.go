package xtesting

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/x/xsync"
)

// WaitTimeout is the maximum amount of time that the helpers in this package
// wait for a condition to be met before failing the test.
//
// It is longer under CI, where tests run under heavier load.
var WaitTimeout = 10 * time.Second

func init() {
	if os.Getenv("CI") != "" {
		WaitTimeout = 30 * time.Second
	}
}

// WaitForLatches waits for all of the given latches to be set. It fails the
// test if all latches are not set within [WaitTimeout].
func WaitForLatches(
	t testing.TB,
	latches ...*xsync.Latch,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var (
		group sync.WaitGroup
		count atomic.Uint32
	)

	for _, latch := range latches {
		group.Go(func() {
			select {
			case <-ctx.Done():
			case <-latch.Chan():
				count.Add(1)
			}
		})
	}

	group.Wait()

	if got, want := count.Load(), uint32(len(latches)); got != want {
		t.Fatalf("timeout waiting for latches: got %d of %d latches set", got, want)
	}
}

// WaitFor polls cond until it returns true. It fails the test if that does not
// occur within [WaitTimeout].
func WaitFor(
	t testing.TB,
	description string,
	cond func() bool,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	for !cond() {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s", description)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// WaitForObservable waits until the value of o satisfies ok. It fails the test
// if that does not occur within [WaitTimeout], and reports the most recent
// value.
func WaitForObservable[T any](
	t testing.TB,
	description string,
	o xsync.ObservableR[T],
	ok func(T) bool,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	for {
		v, changed := o.Load()

		if ok(v) {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for %s: got %v", description, v)
		case <-changed:
		}
	}
}

// Go runs the given function in a goroutine until the test ends.
//
// It returns a function that can be called to stop the goroutine early, which
// blocks until fn returns.
func Go(
	t testing.TB,
	fn func(ctx context.Context),
) func() {
	t.Helper()

	return GoE(
		t,
		func(ctx context.Context) error {
			fn(ctx)
			return nil
		},
	)
}

// GoE runs the given function in a goroutine until the test ends.
//
// If fn returns an error other than a context cancellation error, the test
// fails.
//
// It returns a function that can be called to stop the goroutine early, which
// blocks until fn returns.
func GoE(
	t testing.TB,
	fn func(ctx context.Context) error,
) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})

	stop := func() {
		cancel()
		<-stopped
	}

	t.Cleanup(stop)

	go func() {
		defer close(stopped)

		if err := fn(ctx); err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}
		}
	}()

	return stop
}
