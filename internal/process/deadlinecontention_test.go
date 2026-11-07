package process_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// A handoff can leave both locks held by the old owner. Claiming must skip
// that row and allow another instance to use the only execution slot.
func TestDeadlineConsumer_contendedInstanceLeavesOtherWorkAvailable(t *testing.T) {
	for _, holdDeadline := range []bool{false, true} {
		name := "instance_only"
		if holdDeadline {
			name = "instance_and_deadline"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := xtesting.NewDatabase(t)
			sink := newSink(t, db)
			handled := make(chan string, 2)
			sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).HandleDeadlineFunc = func(
				ctx context.Context, _ dogma.ProcessRoot, scope dogma.ProcessDeadlineScope[dogma.ProcessRoot], _ dogma.Deadline,
			) error {
				select {
				case handled <- scope.InstanceID():
				case <-ctx.Done():
				}
				return nil
			}
			var first *uuidpb.UUID
			for i, instance := range []string{"blocked", "available"} {
				if _, err := db.ExecContext(t.Context(), `INSERT INTO process.instances (handler_key, instance_id) VALUES ($1, $2)`, xsql.UUID(sink.Identity.GetKey()), instance); err != nil {
					t.Fatal(err)
				}
				packer := sink.Packer.PackEffects(sink.Packer.PackCommand(stubs.CommandA1), sink.Identity, envelope.WithInstanceID(instance))
				env := packer.PackDeadline(stubs.DeadlineA1, envelope.WithScheduledFor(time.Now().Add(time.Duration(i-2)*time.Second)))
				packer.Seal()
				if _, err := db.ExecContext(t.Context(), `INSERT INTO process.deadlines (message_id, message_type_id, handler_key, instance_id, envelope, deliver_at)
				VALUES ($1, $2, $3, $4, $5, $6)`, xsql.UUID(env.GetBody().GetMessageId()), xsql.UUID(env.GetBody().GetMessage().GetTypeId()), xsql.UUID(sink.Identity.GetKey()), instance, xsql.Envelope(env), env.GetBody().GetScheduledFor().AsTime()); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					first = env.GetBody().GetMessageId()
				}
			}
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(t.Context(), `SELECT 1 FROM process.instances WHERE handler_key = $1 AND instance_id = 'blocked' FOR UPDATE`, xsql.UUID(sink.Identity.GetKey())); err != nil {
				t.Fatal(err)
			}
			if holdDeadline {
				if _, err := tx.ExecContext(t.Context(), `SELECT 1 FROM process.deadlines WHERE message_id = $1 FOR UPDATE`, xsql.UUID(first)); err != nil {
					t.Fatal(err)
				}
			}
			startDeadlineTestConsumer(t, db, sink, 1)
			wait, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			select {
			case instance := <-handled:
				if instance != "available" {
					t.Fatalf("unexpected delivery: %s", instance)
				}
			case <-wait.Done():
				t.Fatal("available instance was starved by a locked deadline")
			}
		})
	}
}

type consumerNodeKey struct{}

func startDeadlineTestConsumer(t *testing.T, db *sql.DB, sink *Sink, slots int, additionalTypes ...*uuidpb.UUID) (*DeadlineConsumer, func()) {
	t.Helper()
	logger := spruce.NewTestLogger(t)
	bus := &signal.Bus{DB: db, Logger: logger}
	node := &cluster.Node{ID: uuidpb.Generate(), Signals: bus, Interval: 10 * time.Millisecond, Timeout: 100 * time.Millisecond, Logger: logger}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), consumerNodeKey{}, node.ID.AsString()))
	group, ctx := errgroup.WithContext(ctx)
	nodeCtx, cancelNode := context.WithCancel(ctx)
	nodeStopped := make(chan struct{})
	typ, _ := dogma.RegisteredMessageTypeOf(stubs.DeadlineA1)
	types := uuidcollection.NewSet(uuidpb.MustParse(typ.ID()))
	for _, id := range additionalTypes {
		types.Add(id)
	}
	consumer := &DeadlineConsumer{Node: node, Signals: bus, Sink: sink,
		DeadlineTypeIDs: types, Capacity: make(chan struct{}, slots),
		LockTimeout: 20 * time.Millisecond, ContentionDelay: 200 * time.Millisecond,
		BackoffBase: time.Millisecond, BackoffCap: time.Second, Logger: logger}
	group.Go(func() error { return bus.Run(ctx) })
	group.Go(func() error {
		defer close(nodeStopped)
		err := node.Run(nodeCtx)
		if nodeCtx.Err() != nil && errors.Is(err, nodeCtx.Err()) {
			return nil
		}
		return err
	})
	group.Go(func() error { return consumer.Run(ctx) })
	stopNode := sync.OnceFunc(func() { cancelNode(); <-nodeStopped })
	t.Cleanup(func() {
		cancel()
		stopNode()
		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	wait, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	for {
		membership, changed := node.Membership().Load()
		if membership.LocalNodeIsResponsibleFor(sink.Identity.GetKey()) {
			break
		}
		select {
		case <-changed:
		case <-wait.Done():
			t.Fatal("node never became responsible")
		}
	}
	return consumer, stopNode
}

func TestDeadlineConsumer_endSkipsClaimedRowAndRecoveryDiscardsIt(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	sink := newSink(t, db)
	handler := sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
	handler.RouteEventToInstanceFunc = func(context.Context, dogma.Event) (string, bool, error) { return "instance", true, nil }
	handler.HandleEventFunc = func(_ context.Context, _ dogma.ProcessRoot, scope dogma.ProcessEventScope[dogma.ProcessRoot], _ dogma.Event) error {
		scope.End()
		return nil
	}
	var calls atomic.Int64
	handler.HandleDeadlineFunc = func(context.Context, dogma.ProcessRoot, dogma.ProcessDeadlineScope[dogma.ProcessRoot], dogma.Deadline) error {
		calls.Add(1)
		return nil
	}
	id := uuidpb.Generate()
	typ, _ := dogma.RegisteredMessageTypeOf(stubs.DeadlineA1)
	if _, err := db.ExecContext(t.Context(), `INSERT INTO process.instances (handler_key, instance_id) VALUES ($1, 'instance')`, xsql.UUID(sink.Identity.GetKey())); err != nil {
		t.Fatal(err)
	}
	// Corrupt bytes demonstrate that the ended-instance path discards the row
	// before decoding it or invoking application code.
	if _, err := db.ExecContext(t.Context(), `INSERT INTO process.deadlines (message_id, message_type_id, handler_key, instance_id, envelope, deliver_at)
	VALUES ($1, $2, $3, 'instance', decode('ff', 'hex'), now())`, xsql.UUID(id), typ.ID(), xsql.UUID(sink.Identity.GetKey())); err != nil {
		t.Fatal(err)
	}
	claim, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Rollback()
	if _, err := claim.ExecContext(t.Context(), `SELECT 1 FROM process.deadlines WHERE message_id = $1 FOR UPDATE`, xsql.UUID(id)); err != nil {
		t.Fatal(err)
	}
	stream, envs := xtesting.AppendToNewEventStream(t, db, stubs.EventA1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, ok, err := sink.ConsumeSegment(ctx, 0, eventstream.Segment{StreamID: stream, End: 1, Envelopes: envs}); err != nil || !ok {
		t.Fatalf("ending waited on claimed deadline: %v, %t", err, ok)
	}
	xtesting.ExpectQueryResult(t, "instance ended", true, db, `SELECT ended AND state IS NULL FROM process.instances`)
	xtesting.ExpectQueryResult(t, "claimed deadline was skipped", 1, db, `SELECT COUNT(*) FROM process.deadlines`)
	// The old attempt crashes instead of cleaning up its skipped row.
	if err := claim.Rollback(); err != nil {
		t.Fatal(err)
	}
	startDeadlineTestConsumer(t, db, sink, 1)
	xtesting.WaitForNoPendingDeadlines(t, db)
	if calls.Load() != 0 {
		t.Fatal("ended instance invoked deadline handler")
	}
}

func TestDeadlineConsumer_ownershipLossLetsAttemptFinish(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	sink := newSink(t, db)
	fixture := &queueFixture{db: db, packer: sink.Packer}
	key := sink.Identity.GetKey().AsString()
	entered := make(chan string, 4)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var oldCalls atomic.Int64
	handler := sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot])
	handler.HandleDeadlineFunc = func(ctx context.Context, _ dogma.ProcessRoot, scope dogma.ProcessDeadlineScope[dogma.ProcessRoot], _ dogma.Deadline) error {
		entered <- ctx.Value(consumerNodeKey{}).(string)
		if scope.InstanceID() == "old" {
			oldCalls.Add(1)
			select {
			case <-release:
				scope.Mutate(func(root dogma.ProcessRoot) { root.(*stubs.ProcessRootStub).Value = "finished" })
			case <-ctx.Done():
				t.Error("ownership loss cancelled an in-flight attempt")
				return ctx.Err()
			}
		}
		return nil
	}
	fixture.seed(t, key, "old", stubs.DeadlineA1, time.Now().Add(-time.Second))
	old, stopMembership := startDeadlineTestConsumer(t, db, sink, 1)
	if got := queueReceive(t, entered); got != old.Node.ID.AsString() {
		t.Fatalf("unexpected initial owner: %s", got)
	}
	stopMembership() // Keep the consumer and its attempt context alive.
	next, _ := startDeadlineTestConsumer(t, db, sink, 1)
	fixture.seed(t, key, "new", stubs.DeadlineA1, time.Now())
	if got := queueReceive(t, entered); got != next.Node.ID.AsString() {
		t.Fatalf("new work delivered by wrong node: %s", got)
	}
	unblock()
	xtesting.WaitForNoPendingDeadlines(t, db)
	xtesting.ExpectQueryResult(t, "old attempt committed after ownership loss", "finished", db,
		`SELECT convert_from(state, 'UTF8')::jsonb ->> 'value' FROM process.instances WHERE instance_id = 'old'`)
	if oldCalls.Load() != 1 {
		t.Fatalf("old deadline invoked %d times", oldCalls.Load())
	}
}

func TestDeadlineConsumer_claimProbeDoesNotRetainUnselectedLocks(t *testing.T) {
	t.Parallel()
	db := xtesting.NewDatabase(t)
	sink := newSink(t, db)
	f := &queueFixture{db: db, packer: sink.Packer}
	key := sink.Identity.GetKey().AsString()
	blocked := f.seed(t, key, "blocked", stubs.DeadlineA1, time.Now().Add(-3*time.Second))
	f.seed(t, key, "chosen", stubs.DeadlineA1, time.Now().Add(-2*time.Second))
	other := f.seed(t, key, "other-type", stubs.DeadlineB1, time.Now().Add(-time.Second))
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SELECT 1 FROM process.deadlines WHERE message_id = $1 FOR UPDATE`, xsql.UUID(blocked.GetBody().GetMessageId())); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).HandleDeadlineFunc = func(ctx context.Context, _ dogma.ProcessRoot, scope dogma.ProcessDeadlineScope[dogma.ProcessRoot], _ dogma.Deadline) error {
		if scope.InstanceID() == "chosen" {
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	typ, _ := dogma.RegisteredMessageTypeOf(stubs.DeadlineB1)
	startDeadlineTestConsumer(t, db, sink, 1, uuidpb.MustParse(typ.ID()))
	queueReceive(t, started)
	var id uuidpb.UUID
	if err := db.QueryRowContext(t.Context(), `SELECT message_id FROM process.deadlines WHERE message_id = $1 FOR UPDATE SKIP LOCKED`, xsql.UUID(other.GetBody().GetMessageId())).Scan(xsql.UUID(&id)); err != nil {
		t.Fatalf("fallback probe retained the unselected type's lock: %v", err)
	}
}

func TestDeadlineConsumer_allDueRowsLockedDoesNotSpin(t *testing.T) {
	t.Parallel()
	var queries, calls atomic.Int64
	db := xtesting.NewDatabaseWithFailingQueries(t, func(query string, _ []any) bool {
		if strings.Contains(query, "process.deadlines") {
			queries.Add(1)
		}
		return false
	})
	sink := newSink(t, db)
	sink.Handler.(*stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]).HandleDeadlineFunc = func(context.Context, dogma.ProcessRoot, dogma.ProcessDeadlineScope[dogma.ProcessRoot], dogma.Deadline) error {
		calls.Add(1)
		return nil
	}
	f := &queueFixture{db: db, packer: sink.Packer}
	message := f.seed(t, sink.Identity.GetKey().AsString(), "locked", stubs.DeadlineA1, time.Now())
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SELECT 1 FROM process.deadlines WHERE message_id = $1 FOR UPDATE`, xsql.UUID(message.GetBody().GetMessageId())); err != nil {
		t.Fatal(err)
	}
	startDeadlineTestConsumer(t, db, sink, 1)
	before := queries.Load()
	<-time.After(250 * time.Millisecond)
	if got := queries.Load() - before; got > 20 {
		t.Fatalf("locked queue spun through %d deadline queries", got)
	}
	if calls.Load() != 0 {
		t.Fatal("locked deadline was invoked")
	}
}
