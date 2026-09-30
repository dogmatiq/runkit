package heartbeat_test

import (
	"context"
	"errors"
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

		w := &Writer{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		errCh := make(chan error, 1)
		go func() {
			errCh <- w.Run(ctx)
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

		if err := <-errCh; !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("it returns an error when the initial write conflicts with an existing row", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		nodeID := uuidpb.Generate()

		w1 := &Writer{
			NodeID: nodeID,
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		done := make(chan struct{})
		go func() {
			w1.Run(ctx)
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
			xsql.UUID(w1.NodeID),
		)

		w2 := &Writer{
			NodeID: nodeID,
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		err := w2.Run(ctx)

		if err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: got %v", err)
		}

		// Ensure the first writer has finished.
		cancel()
		<-done
	})

	t.Run("it deletes the record on graceful shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		w := &Writer{
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
