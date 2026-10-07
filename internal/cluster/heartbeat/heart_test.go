package heartbeat_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster/heartbeat"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestWriter(t *testing.T) {
	t.Run("it writes a heartbeat record", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		h := &Heart{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.Run(ctx)
		}()

		xtesting.WaitForQueryResult(
			t,
			"heartbeat record exists",
			1,
			db,
			`SELECT COUNT(*)
			FROM cluster.heartbeats
			WHERE node_id = $1`,
			xsql.UUID(h.NodeID),
		)

		cancel()
		<-done
	})

	t.Run("it deletes the record on graceful shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		w := &Heart{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		done := make(chan struct{})
		go func() {
			w.Run(ctx)
			close(done)
		}()

		xtesting.WaitForQueryResult(
			t,
			"heartbeat record exists",
			1,
			db,
			`SELECT COUNT(*)
			FROM cluster.heartbeats
			WHERE node_id = $1`,
			xsql.UUID(w.NodeID),
		)

		cancel()

		xtesting.WaitForQueryResult(
			t,
			"heartbeat record deleted",
			0,
			db,
			`SELECT COUNT(*)
			FROM cluster.heartbeats
			WHERE node_id = $1`,
			xsql.UUID(w.NodeID),
		)

		<-done
	})
}
