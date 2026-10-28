package eventstream_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCatalog(t *testing.T) {
	t.Run("it sends existing streams to a new subscriber", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		want := xtesting.CreateEventStreams(t, db, 3)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		streamIDs := make(chan *uuidpb.UUID, 10)
		unsubscribe := catalog.Subscribe(streamIDs)
		defer unsubscribe()

		expectToReceiveStreamIDs(t, streamIDs, want...)
		expectNoStreamIDs(t, streamIDs)
	})

	t.Run("it sends newly created streams to existing subscribers", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		streamIDs := make(chan *uuidpb.UUID, 10)
		unsubscribe := catalog.Subscribe(streamIDs)
		defer unsubscribe()

		want := xtesting.CreateEventStream(t, db)
		notifyStreamCreated(t, db, want)

		expectToReceiveStreamIDs(t, streamIDs, want)
	})

	t.Run("it sends each stream once", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		streamIDs := make(chan *uuidpb.UUID, 10)
		unsubscribe := catalog.Subscribe(streamIDs)
		defer unsubscribe()

		want := xtesting.CreateEventStream(t, db)

		// Each signal causes the complete set of streams to be read again, but
		// subscribers must only be told about each stream once.
		notifyStreamCreated(t, db, want)
		expectToReceiveStreamIDs(t, streamIDs, want)

		notifyStreamCreated(t, db, want)
		expectNoStreamIDs(t, streamIDs)
	})

	t.Run("it delivers to multiple subscribers", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		streamIDs1 := make(chan *uuidpb.UUID, 10)
		unsubscribe1 := catalog.Subscribe(streamIDs1)
		defer unsubscribe1()

		streamIDs2 := make(chan *uuidpb.UUID, 10)
		unsubscribe2 := catalog.Subscribe(streamIDs2)
		defer unsubscribe2()

		want := xtesting.CreateEventStream(t, db)
		notifyStreamCreated(t, db, want)

		expectToReceiveStreamIDs(t, streamIDs1, want)
		expectToReceiveStreamIDs(t, streamIDs2, want)
	})

	t.Run("it stops delivering after unsubscribe", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		streamIDs := make(chan *uuidpb.UUID, 10)
		unsubscribe := catalog.Subscribe(streamIDs)

		first := xtesting.CreateEventStream(t, db)
		notifyStreamCreated(t, db, first)
		expectToReceiveStreamIDs(t, streamIDs, first)

		unsubscribe()

		second := xtesting.CreateEventStream(t, db)
		notifyStreamCreated(t, db, second)
		expectNoStreamIDs(t, streamIDs)
	})

	t.Run("it accepts subscriptions before the first query completes", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		xtesting.Go(t, catalog.Run)

		// Subscribe immediately, without waiting for the catalog to have
		// discovered anything. The subscription must be accepted, and the
		// subscriber must be told about streams created afterwards.
		streamIDs := make(chan *uuidpb.UUID, 10)
		unsubscribe := catalog.Subscribe(streamIDs)
		defer unsubscribe()

		want := xtesting.CreateEventStream(t, db)
		notifyStreamCreated(t, db, want)

		expectToReceiveStreamIDs(t, streamIDs, want)
	})

	t.Run("it does not block subscribers after it has stopped", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.Go(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		stop := xtesting.Go(t, catalog.Run)
		stop()

		unsubscribe := catalog.Subscribe(make(chan *uuidpb.UUID))
		unsubscribe()
	})
}

// notifyStreamCreated sends the signal that is normally sent when a stream is
// created by the engine. The test helpers create streams directly, without
// sending it.
func notifyStreamCreated(t testing.TB, db *sql.DB, streamID *uuidpb.UUID) {
	t.Helper()

	if err := signal.Notify(
		t.Context(),
		db,
		"eventstream.create."+streamID.AsString(),
	); err != nil {
		t.Fatal(err)
	}
}

// expectToReceiveStreamIDs asserts that exactly the given stream IDs are
// received, in any order.
func expectToReceiveStreamIDs(
	t testing.TB,
	streamIDs <-chan *uuidpb.UUID,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidpb.NewSet(want...)

	for range want {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for stream ID")
		case got := <-streamIDs:
			if !expected.Has(got) {
				t.Fatalf("unexpected or duplicate stream ID: %s", got)
			}
			expected.Delete(got)
		}
	}
}

// expectNoStreamIDs asserts that no stream ID is received within a short
// period.
func expectNoStreamIDs(
	t testing.TB,
	streamIDs <-chan *uuidpb.UUID,
) {
	t.Helper()

	select {
	case got := <-streamIDs:
		t.Fatalf("unexpected stream ID: %s", got)
	case <-time.After(100 * time.Millisecond):
	}
}
