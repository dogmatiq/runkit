package cluster_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestObservable(t *testing.T) {
	// setup returns an observable of the set that read returns, on a running
	// node, and a function that notifies the given cluster-wide signal.
	setup := func(t *testing.T, read func() *uuidcollection.Set) (*Observable[*uuidcollection.Set], func(string)) {
		db := xtesting.NewDatabase(t)

		signals := &signal.Bus{
			DB:           db,
			EngineLogger: spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, signals.Run)

		n := &Node{
			ID:                uuidpb.Generate(),
			DB:                db,
			Signals:           signals,
			HeartbeatInterval: 20 * time.Millisecond,
			HeartbeatTimeout:  100 * time.Millisecond,
			EngineLogger:      spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, n.Run)

		o := &Observable[*uuidcollection.Set]{
			Node:   n,
			Signal: "<signal>",
			Read: func(context.Context) (*uuidcollection.Set, error) {
				return read(), nil
			},
		}

		send := func(signal string) {
			t.Helper()
			sendSignal(t, db, signal)
		}

		return o, send
	}

	// waitForValue waits until the observable's value is the given set.
	waitForValue := func(t *testing.T, o *Observable[*uuidcollection.Set], want *uuidcollection.Set) {
		t.Helper()

		xtesting.WaitForObservable(
			t,
			"the observable's value",
			o,
			func(got *uuidcollection.Set) bool {
				return got.IsEqual(want)
			},
		)
	}

	t.Run("it reads the value when it starts", func(t *testing.T) {
		t.Parallel()

		id := uuidpb.Generate()
		o, _ := setup(t, func() *uuidcollection.Set {
			return uuidcollection.NewSet(id)
		})

		xtesting.GoE(t, o.Run)

		waitForValue(t, o, uuidcollection.NewSet(id))
	})

	t.Run("it reads the value again when the cluster-wide signal is notified", func(t *testing.T) {
		t.Parallel()

		var current atomic.Pointer[uuidcollection.Set]
		current.Store(uuidcollection.NewSet(uuidpb.Generate()))

		o, send := setup(t, current.Load)
		xtesting.GoE(t, o.Run)
		waitForValue(t, o, current.Load())

		next := uuidcollection.NewSet(uuidpb.Generate())
		current.Store(next)
		send("<signal>")

		waitForValue(t, o, next)
	})

	t.Run("it publishes the value only when it changes", func(t *testing.T) {
		t.Parallel()

		id := uuidpb.Generate()

		var reads atomic.Int32
		o, send := setup(t, func() *uuidcollection.Set {
			reads.Add(1)

			// Each read returns a new set with the same contents.
			return uuidcollection.NewSet(id)
		})

		xtesting.GoE(t, o.Run)
		waitForValue(t, o, uuidcollection.NewSet(id))

		_, changed := o.Load()
		before := reads.Load()

		send("<signal>")

		xtesting.WaitFor(
			t,
			"the observable to read the value again",
			func() bool {
				return reads.Load() > before
			},
		)

		select {
		case <-changed:
			t.Fatal("observable published a value that had not changed")
		default:
		}
	})

	t.Run("it returns an error if the value can't be read", func(t *testing.T) {
		t.Parallel()

		o, _ := setup(t, nil)
		o.Read = func(context.Context) (*uuidcollection.Set, error) {
			return nil, errors.New("<error>")
		}

		err := o.Run(t.Context())

		if err == nil || err.Error() != "<error>" {
			t.Fatalf("unexpected error: got %v, want <error>", err)
		}
	})
}
