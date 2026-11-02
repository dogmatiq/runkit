package eventstream_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/runkit"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestRemoteCatalog_streamIDs(t *testing.T) {
	t.Run("it reports streams that exist when it starts", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				streamIDs := xtesting.CreateEventStreams(t, db, 3)
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				expectStreamIDs(t, catalog, streamIDs...)
			},
			consumerAPIApp(),
		)
	})

	t.Run("it reports streams that are created after it starts", func(t *testing.T) {
		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *runkit.Engine, db *sql.DB) {
				client, _ := newConsumerClient(t, engine)

				catalog := &RemoteCatalog{
					Client: client,
					Logger: spruce.NewTestLogger(t),
				}
				xtesting.GoE(t, catalog.Run)

				first := xtesting.CreateEventStream(t, db)
				expectStreamIDs(t, catalog, first)

				second := xtesting.CreateEventStream(t, db)
				expectStreamIDs(t, catalog, first, second)
			},
			consumerAPIApp(),
		)
	})
}
