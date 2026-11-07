package process_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

type queueNodeKey struct{}

type queueFixture struct {
	db     *sql.DB
	app    *stubs.ApplicationStub
	packer *envelope.Packer
}

type queueEngine struct {
	index int
	node  *cluster.Node
	stop  func()
}

func newQueueFixture(t *testing.T, handlers ...*stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]) *queueFixture {
	t.Helper()
	id := uuidpb.Generate()
	var routes []dogma.HandlerRoute
	for _, handler := range handlers {
		routes = append(routes, dogma.ViaProcess(handler))
	}
	return &queueFixture{db: xtesting.NewDatabase(t), app: &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) { c.Identity("queue-test", id.AsString()); c.Routes(routes...) },
	}, packer: &envelope.Packer{Application: identitypb.New("queue-test", id)}}
}

func queueHandler(key string, handle func(context.Context, *stubs.ProcessRootStub, dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], dogma.Deadline) error) *stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub] {
	return &stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
		ConfigureFunc: func(c dogma.ProcessConfigurer) {
			c.Identity("handler-"+key[:8], key)
			c.Routes(dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](), dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](), dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeA]]())
		}, HandleDeadlineFunc: handle,
	}
}

func (f *queueFixture) start(t *testing.T, index int, options ...runkit.EngineOption) *queueEngine {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), queueNodeKey{}, index))
	node := make(chan *cluster.Node, 1)
	ctx = contexthook.With(ctx, func(h contexthook.Node) { node <- h.Node })
	engine := runkit.New(f.app, f.db, append([]runkit.EngineOption{runkit.WithLogger(spruce.NewTestLogger(t))}, options...)...)
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = engine.Run(ctx) }()
	result := &queueEngine{index: index}
	result.stop = sync.OnceFunc(func() {
		cancel()
		<-done
		if !errors.Is(runErr, context.Canceled) {
			t.Errorf("engine stopped unexpectedly: %v", runErr)
		}
	})
	t.Cleanup(result.stop)
	wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	select {
	case result.node = <-node:
	case <-done:
		t.Fatalf("engine failed during initialization: %v", runErr)
	case <-wait.Done():
		t.Fatal("engine did not initialize its node")
	}
	select {
	case <-engine.Ready():
	case <-done:
		t.Fatalf("engine failed before readiness: %v", runErr)
	case <-wait.Done():
		t.Fatal("engine did not become ready")
	}
	return result
}

func waitQueueMembership(t *testing.T, engines ...*queueEngine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for _, engine := range engines {
		for {
			m, changed := engine.node.Membership().Load()
			if m != nil && m.Peers().Len() == len(engines)-1 {
				break
			}
			select {
			case <-changed:
			case <-ctx.Done():
				t.Fatal("queue membership did not converge")
			}
		}
	}
}

func queueOwner(t *testing.T, key string, engines ...*queueEngine) *queueEngine {
	t.Helper()
	for _, engine := range engines {
		m, _ := engine.node.Membership().Load()
		if m.LocalNodeIsResponsibleFor(uuidpb.MustParse(key)) {
			return engine
		}
	}
	t.Fatal("no queue owner")
	return nil
}

func (f *queueFixture) seed(t *testing.T, key, instance string, message dogma.Deadline, when time.Time) *envelopepb.Envelope {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO process.instances (handler_key, instance_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, key, instance); err != nil {
		t.Fatal(err)
	}
	packer := f.packer.PackEffects(f.packer.PackCommand(stubs.CommandX1), identitypb.New("handler-"+key[:8], uuidpb.MustParse(key)), envelope.WithInstanceID(instance))
	env := packer.PackDeadline(message, envelope.WithScheduledFor(when))
	packer.Seal()
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO process.deadlines
		(message_id, message_type_id, handler_key, instance_id, envelope, deliver_at)
		VALUES ($1, $2, $3, $4, $5, $6)`, xsql.UUID(env.GetBody().GetMessageId()),
		xsql.UUID(env.GetBody().GetMessage().GetTypeId()), key, instance, xsql.Envelope(env), when); err != nil {
		t.Fatal(err)
	}
	return env
}

func queueReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for queue delivery")
	}
	var zero T
	return zero
}

func TestQueueDelivery_assignmentAndFailover(t *testing.T) {
	t.Parallel()
	const key = "d74a2347-e371-4bf0-866d-f36b4f74eb3e"
	type delivery struct {
		node     int
		instance string
	}
	deliveries := make(chan delivery, 8)
	f := newQueueFixture(t, queueHandler(key, func(ctx context.Context, _ *stubs.ProcessRootStub, scope dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], _ dogma.Deadline) error {
		deliveries <- delivery{ctx.Value(queueNodeKey{}).(int), scope.InstanceID()}
		return nil
	}))
	engines := []*queueEngine{f.start(t, 0), f.start(t, 1), f.start(t, 2)}
	waitQueueMembership(t, engines...)
	owner := queueOwner(t, key, engines...)
	for i := range 3 {
		f.seed(t, key, fmt.Sprint(i), stubs.DeadlineA1, time.Now())
	}
	for range 3 {
		if got := queueReceive(t, deliveries); got.node != owner.index {
			t.Fatalf("deadline delivered by node %d, owner is %d", got.node, owner.index)
		}
	}
	xtesting.WaitForNoPendingDeadlines(t, f.db)
	// No notification is sent after departure: the new owner must discover
	// this durable future deadline when membership changes.
	f.seed(t, key, "after-departure", stubs.DeadlineA1, time.Now().Add(3*time.Second))
	owner.stop()
	var remaining []*queueEngine
	for _, engine := range engines {
		if engine != owner {
			remaining = append(remaining, engine)
		}
	}
	waitQueueMembership(t, remaining...)
	next := queueOwner(t, key, remaining...)
	if got := queueReceive(t, deliveries); got.node != next.index || got.instance != "after-departure" {
		t.Fatalf("unexpected failover delivery: %+v, expected node %d", got, next.index)
	}
	xtesting.WaitForNoPendingDeadlines(t, f.db)
}

func TestQueueDelivery_capacityIsSharedAcrossHandlers(t *testing.T) {
	t.Parallel()
	limit := 10 * runtime.GOMAXPROCS(0)
	entered := make(chan struct{}, 2*limit+4)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var active, peak atomic.Int64
	handle := func(ctx context.Context, _ *stubs.ProcessRootStub, _ dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], _ dogma.Deadline) error {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	keys := []string{"cdd25762-d411-4bb3-92a0-55c0d233e539", "a50c9f0f-3d4a-4374-9137-1fab6fcfbad1"}
	f := newQueueFixture(t, queueHandler(keys[0], handle), queueHandler(keys[1], handle))
	node := f.start(t, 0)
	waitQueueMembership(t, node)
	for _, key := range keys {
		for i := range limit + 2 {
			f.seed(t, key, fmt.Sprint(i), stubs.DeadlineA1, time.Now())
		}
	}
	for range limit {
		queueReceive(t, entered)
	}
	select {
	case <-entered:
		t.Fatal("another handler exceeded the shared execution limit")
	case <-time.After(200 * time.Millisecond):
	}
	unblock()
	xtesting.WaitForNoPendingDeadlines(t, f.db)
	if got := peak.Load(); got > int64(limit) || got < 2 {
		t.Fatalf("unexpected parallelism: %d, limit %d", got, limit)
	}
}

func TestQueueDelivery_timeoutsRestoreAndEarlierInsertWakesDiscovery(t *testing.T) {
	t.Parallel()
	const key = "9b9a0a65-c68e-4de3-b5c4-b452acb24c54"
	delivered := make(chan string, 4)
	f := newQueueFixture(t, queueHandler(key, func(_ context.Context, _ *stubs.ProcessRootStub, _ dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], message dogma.Deadline) error {
		delivered <- string(message.(*stubs.DeadlineStub[stubs.TypeA]).Content)
		return nil
	}))
	// Assert restoration inside the same database transaction, on both the
	// contention UPDATE and the successful delivery's DELETE.
	if _, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION process.assert_queue_timeout() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
	 IF current_setting('lock_timeout') <> '0' THEN RAISE EXCEPTION 'lock timeout was not restored'; END IF;
	 IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
	 RETURN NEW;
	END $$;
	CREATE TRIGGER assert_queue_timeout BEFORE UPDATE OR DELETE ON process.deadlines FOR EACH ROW EXECUTE FUNCTION process.assert_queue_timeout()`); err != nil {
		t.Fatal(err)
	}
	original := f.seed(t, key, "instance", stubs.DeadlineA1, time.Now().Add(-time.Second))
	tx, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SELECT 1 FROM process.instances WHERE handler_key = $1 AND instance_id = 'instance' FOR UPDATE`, key); err != nil {
		t.Fatal(err)
	}
	node := f.start(t, 0, runkit.WithQueueLockTimeout(20*time.Millisecond), runkit.WithQueueContentionDelay(time.Minute))
	waitQueueMembership(t, node)
	xtesting.WaitForQueryResult(t, "deadline deferred without application failure", true, f.db,
		`SELECT deliver_at > clock_timestamp() + interval '30 seconds' AND failures = 0 FROM process.deadlines`)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := f.db.QueryRowContext(t.Context(), `SELECT envelope FROM process.deadlines`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	env := &envelopepb.Envelope{}
	if err := env.UnmarshalBinary(stored); err != nil {
		t.Fatal(err)
	}
	if !env.GetBody().GetScheduledFor().AsTime().Equal(original.GetBody().GetScheduledFor().AsTime()) {
		t.Fatal("contention changed original scheduled time")
	}
	select {
	case <-delivered:
		t.Fatal("contended deadline invoked handler")
	default:
	}
	message := &stubs.DeadlineStub[stubs.TypeA]{Content: "earlier"}
	newDeadline := f.seed(t, key, "instance", message, time.Now())
	if got := queueReceive(t, delivered); got != "earlier" {
		t.Fatalf("unexpected delivery: %s", got)
	}
	xtesting.WaitForQueryResult(t, "earlier deadline committed", 0, f.db, `SELECT COUNT(*) FROM process.deadlines WHERE message_id = $1`, xsql.UUID(newDeadline.GetBody().GetMessageId()))
}

func TestQueueDelivery_retriesDoNotBlockAnotherDeadlineForTheInstance(t *testing.T) {
	t.Parallel()
	const key = "4f2bf974-0fa5-4bf0-9d75-2c94243022e6"
	good := make(chan struct{}, 4)
	f := newQueueFixture(t, queueHandler(key, func(_ context.Context, _ *stubs.ProcessRootStub, _ dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], message dogma.Deadline) error {
		if string(message.(*stubs.DeadlineStub[stubs.TypeA]).Content) == "bad" {
			return errors.New("application failure")
		}
		good <- struct{}{}
		return nil
	}))
	bad := f.seed(t, key, "instance", &stubs.DeadlineStub[stubs.TypeA]{Content: "bad"}, time.Now().Add(-2*time.Second))
	second := f.seed(t, key, "instance", stubs.DeadlineA1, time.Now().Add(-time.Second))
	node := f.start(t, 0)
	waitQueueMembership(t, node)
	queueReceive(t, good)
	xtesting.WaitForQueryResult(t, "other deadline consumed", 0, f.db, `SELECT COUNT(*) FROM process.deadlines WHERE message_id = $1`, xsql.UUID(second.GetBody().GetMessageId()))
	xtesting.ExpectQueryResult(t, "failed deadline remains independently retryable", true, f.db, `SELECT failures > 0 FROM process.deadlines WHERE message_id = $1`, xsql.UUID(bad.GetBody().GetMessageId()))
}

func TestQueueDelivery_writeFailureRollsBackWithoutCancellingOtherAttempts(t *testing.T) {
	t.Parallel()
	const key = "d951d017-1403-46e9-adbd-9035d48e11bd"
	survivorStarted := make(chan struct{})
	markStarted := sync.OnceFunc(func() { close(survivorStarted) })
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	attempted := make(chan struct{}, 16)
	f := newQueueFixture(t, queueHandler(key, func(ctx context.Context, _ *stubs.ProcessRootStub, scope dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], _ dogma.Deadline) error {
		if scope.InstanceID() == "survivor" {
			markStarted()
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				t.Error("another attempt was cancelled by component backoff")
				return ctx.Err()
			}
		}
		select {
		case <-survivorStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		scope.Mutate(func(root *stubs.ProcessRootStub) { root.Value = "changed" })
		scope.ExecuteCommand(stubs.CommandA1)
		scope.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))
		select {
		case attempted <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}))
	if _, err := f.db.ExecContext(t.Context(), `CREATE TABLE process.write_fault (enabled boolean NOT NULL);
	INSERT INTO process.write_fault VALUES (true);
	CREATE FUNCTION process.fail_deadline_delete() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
	 IF OLD.instance_id = 'faulty' AND (SELECT enabled FROM process.write_fault) THEN
	  RAISE EXCEPTION 'injected database write failure';
	 END IF;
	 RETURN OLD;
	END $$;
	CREATE TRIGGER fail_deadline_delete BEFORE DELETE ON process.deadlines FOR EACH ROW EXECUTE FUNCTION process.fail_deadline_delete()`); err != nil {
		t.Fatal(err)
	}
	original := f.seed(t, key, "faulty", stubs.DeadlineA1, time.Now().Add(-2*time.Second))
	survivor := f.seed(t, key, "survivor", stubs.DeadlineA1, time.Now().Add(-time.Second))
	node := f.start(t, 0)
	waitQueueMembership(t, node)
	// A second invocation proves the first transaction has rolled back and
	// supervised retry is running. Failure is injected after result writes.
	queueReceive(t, attempted)
	queueReceive(t, attempted)
	unblock()
	xtesting.WaitForQueryResult(t, "other in-flight attempt committed", 0, f.db, `SELECT COUNT(*) FROM process.deadlines WHERE message_id = $1`, xsql.UUID(survivor.GetBody().GetMessageId()))
	xtesting.ExpectQueryResult(t, "commands rolled back", 0, f.db, `SELECT COUNT(*) FROM commandqueue.commands`)
	xtesting.ExpectQueryResult(t, "successor rolled back", 1, f.db, `SELECT COUNT(*) FROM process.deadlines`)
	xtesting.ExpectQueryResult(t, "state rolled back", true, f.db, `SELECT state IS NULL FROM process.instances WHERE instance_id = 'faulty'`)
	xtesting.ExpectQueryResult(t, "database failure did not increment application failures", uint64(0), f.db, `SELECT failures FROM process.deadlines WHERE message_id = $1`, xsql.UUID(original.GetBody().GetMessageId()))
	if _, err := f.db.ExecContext(t.Context(), `UPDATE process.write_fault SET enabled = false`); err != nil {
		t.Fatal(err)
	}
	xtesting.WaitForQueryResult(t, "retry eventually committed once", 1, f.db, `SELECT COUNT(*) FROM commandqueue.commands`)
	xtesting.ExpectQueryResult(t, "original deadline consumed", 0, f.db, `SELECT COUNT(*) FROM process.deadlines WHERE message_id = $1`, xsql.UUID(original.GetBody().GetMessageId()))
	xtesting.ExpectQueryResult(t, "one successor committed", 1, f.db, `SELECT COUNT(*) FROM process.deadlines`)
}

func TestQueueDelivery_shutdownCancelsAndRollsBackAttempt(t *testing.T) {
	t.Parallel()
	const key = "aa7fdf52-1d28-4cde-85f2-ce096bc937c1"
	entered := make(chan struct{}, 1)
	f := newQueueFixture(t, queueHandler(key, func(ctx context.Context, _ *stubs.ProcessRootStub, scope dogma.ProcessDeadlineScope[*stubs.ProcessRootStub], _ dogma.Deadline) error {
		scope.Mutate(func(root *stubs.ProcessRootStub) { root.Value = "uncommitted" })
		scope.ExecuteCommand(stubs.CommandA1)
		scope.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(time.Hour))
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	f.seed(t, key, "instance", stubs.DeadlineA1, time.Now())
	node := f.start(t, 0)
	waitQueueMembership(t, node)
	queueReceive(t, entered)
	node.stop()
	xtesting.ExpectQueryResult(t, "shutdown kept original deadline", 1, f.db, `SELECT COUNT(*) FROM process.deadlines`)
	xtesting.ExpectQueryResult(t, "shutdown did not record an application failure", uint64(0), f.db, `SELECT failures FROM process.deadlines`)
	xtesting.ExpectQueryResult(t, "shutdown did not commit commands", 0, f.db, `SELECT COUNT(*) FROM commandqueue.commands`)
	xtesting.ExpectQueryResult(t, "shutdown rolled back state", true, f.db, `SELECT state IS NULL FROM process.instances`)
}
