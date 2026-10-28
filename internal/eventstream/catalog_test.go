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
	setup := func(t *testing.T) (*sql.DB, *Catalog) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		signals := &signal.Listener{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, signals.Run)

		catalog := &Catalog{
			DB:      db,
			Signals: signals,
			Logger:  logger,
		}

		return db, catalog
	}

	t.Run("it reports existing streams", func(t *testing.T) {
		db, catalog := setup(t)

		want := xtesting.CreateEventStreams(t, db, 3)

		xtesting.GoE(t, catalog.Run)

		expectStreamIDs(t, catalog, want...)
	})

	t.Run("it reports newly created streams", func(t *testing.T) {
		db, catalog := setup(t)

		xtesting.GoE(t, catalog.Run)

		first := xtesting.CreateEventStream(t, db)
		expectStreamIDs(t, catalog, first)

		second := xtesting.CreateEventStream(t, db)
		expectStreamIDs(t, catalog, first, second)
	})

	t.Run("it does not report a change when no streams are created", func(t *testing.T) {
		db, catalog := setup(t)

		xtesting.GoE(t, catalog.Run)

		want := xtesting.CreateEventStream(t, db)
		expectStreamIDs(t, catalog, want)

		_, changed := catalog.StreamIDs()

		// Each signal causes the complete set of streams to be read again, but
		// the set is only replaced when it has grown.
		if err := signal.Notify(
			t.Context(),
			db,
			"eventstream.create."+want.AsString(),
		); err != nil {
			t.Fatal(err)
		}

		select {
		case <-changed:
			got, _ := catalog.StreamIDs()
			t.Fatalf("unexpected change to stream IDs: %s", got)
		case <-time.After(100 * time.Millisecond):
		}
	})
}

// expectStreamIDs waits for the catalog to report exactly the given stream
// IDs.
func expectStreamIDs(
	t testing.TB,
	catalog *Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidpb.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := catalog.StreamIDs()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected stream IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
