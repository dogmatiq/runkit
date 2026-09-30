package heartbeat_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster/heartbeat"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestReader(t *testing.T) {
	t.Run("it returns the live nodes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		w1 := &Writer{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}
		w2 := &Writer{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		go w1.Run(ctx)
		go w2.Run(ctx)

		xtesting.WaitForQueryResult(
			t,
			"both heartbeat records exist",
			2,
			db,
			`SELECT COUNT(*) FROM cluster.heartbeats`,
		)

		r := &Reader{DB: db}

		got, err := r.LiveNodes(ctx)
		if err != nil {
			t.Fatal(err)
		}

		gotSet := uuidpb.Set{}
		for _, id := range got {
			gotSet.Add(id)
		}

		if gotSet.Len() != 2 || !gotSet.Has(w1.NodeID) || !gotSet.Has(w2.NodeID) {
			t.Fatalf("unexpected live nodes: got %v, want [%s %s]", got, w1.NodeID, w2.NodeID)
		}
	})

	t.Run("it excludes expired nodes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := xtesting.NewDatabase(t)

		live := uuidpb.Generate()
		expired := uuidpb.Generate()

		if _, err := db.ExecContext(
			ctx,
			`INSERT INTO cluster.heartbeats (node_id, expires_at)
			VALUES ($1, clock_timestamp() + interval '1 hour'),
			       ($2, clock_timestamp() - interval '1 hour')`,
			xsql.UUID(live),
			xsql.UUID(expired),
		); err != nil {
			t.Fatal(err)
		}

		r := &Reader{DB: db}

		got, err := r.LiveNodes(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if len(got) != 1 || !got[0].Equal(live) {
			t.Fatalf("unexpected live nodes: got %v, want [%s]", got, live)
		}
	})

	t.Run("it reflects graceful node departure", func(t *testing.T) {
		ctx := context.Background()

		db := xtesting.NewDatabase(t)

		staying := &Writer{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}
		leavingCtx, leave := context.WithCancel(ctx)

		leaving := &Writer{
			NodeID: uuidpb.Generate(),
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		stayingCtx, stopStaying := context.WithCancel(ctx)
		defer stopStaying()

		go staying.Run(stayingCtx)

		leavingDone := make(chan struct{})
		go func() {
			leaving.Run(leavingCtx)
			close(leavingDone)
		}()

		xtesting.WaitForQueryResult(
			t,
			"both heartbeat records exist",
			2,
			db,
			`SELECT COUNT(*) FROM cluster.heartbeats`,
		)

		// The leaving node deletes its record on graceful shutdown.
		leave()
		<-leavingDone

		r := &Reader{DB: db}

		deadline := time.Now().Add(xtesting.WaitTimeout)
		for {
			got, err := r.LiveNodes(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if len(got) == 1 && got[0].Equal(staying.NodeID) {
				return
			}

			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for leaving node to depart: got %v", got)
			}

			time.Sleep(5 * time.Millisecond)
		}
	})
}
