package schema_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCreate_upgradesDeadlineIndexesWithoutChangingPendingWork(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	if _, err := db.ExecContext(t.Context(), `DROP INDEX process.pending_deadlines_by_handler_type_time;
	DROP INDEX process.pending_deadlines_by_instance;
	DROP TRIGGER signal_deadlines ON process.deadlines;
	DROP FUNCTION process.signal_deadlines();
	CREATE INDEX pending_deadlines_by_handler ON process.deadlines (handler_key, deliver_at);
	INSERT INTO process.deadlines (message_id, message_type_id, handler_key, instance_id, envelope, failures, deliver_at)
	VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
	 '00000000-0000-0000-0000-000000000003', 'instance', decode('010203', 'hex'), 3, '2100-01-01T00:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := schema.Create(t.Context(), db); err != nil {
			t.Fatal(err)
		}
	}
	xtesting.ExpectQueryResult(t, "existing deadline preserved", true, db,
		`SELECT failures = 3 AND instance_id = 'instance' AND envelope = decode('010203', 'hex') AND deliver_at = '2100-01-01T00:00:00Z' FROM process.deadlines`)
	xtesting.ExpectQueryResult(t, "new indexes installed", 2, db,
		`SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'process' AND indexname IN ('pending_deadlines_by_handler_type_time', 'pending_deadlines_by_instance')`)
	xtesting.ExpectQueryResult(t, "old index replaced", 0, db,
		`SELECT COUNT(*) FROM pg_indexes WHERE schemaname = 'process' AND indexname = 'pending_deadlines_by_handler'`)
}

func TestDeadlineNotifications_areHandlerSpecificAndTransactional(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	ctx, cancel := context.WithCancel(t.Context())
	bus := &signal.Bus{DB: db, Logger: spruce.NewTestLogger(t)}
	signals, stop := bus.Watch(ctx, "process.deadlines.00000000-0000-0000-0000-000000000003", 1)
	defer stop()
	<-signals // Initial query notification; drain before starting LISTEN.
	done := make(chan error, 1)
	go func() { done <- bus.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	select {
	case <-signals:
	case <-time.After(5 * time.Second):
		t.Fatal("bus did not begin listening")
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO process.deadlines (message_id, message_type_id, handler_key, instance_id, envelope, deliver_at)
	VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002',
	 '00000000-0000-0000-0000-000000000003', 'instance', decode('01', 'hex'), now())`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signals:
		t.Fatal("deadline signaled before transaction committed")
	case <-time.After(20 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case topic := <-signals:
		if topic != "" {
			t.Fatalf("unexpected topic: %s", topic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("committed deadline did not signal its handler")
	}
	// Another handler must not wake this watcher.
	if _, err := db.ExecContext(t.Context(), `INSERT INTO process.deadlines (message_id, message_type_id, handler_key, instance_id, envelope, deliver_at)
	VALUES ('00000000-0000-0000-0000-000000000004', '00000000-0000-0000-0000-000000000002',
	 '00000000-0000-0000-0000-000000000005', 'instance', decode('01', 'hex'), now())`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-signals:
		t.Fatal("another handler signaled this watcher")
	case <-time.After(20 * time.Millisecond):
	}
}
