package process_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// A handoff can leave both locks held by the old owner. SKIP LOCKED in the
// deferral update must not cause repeated discovery to starve other instances.
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
			logger := spruce.NewTestLogger(t)
			bus := &signal.Bus{DB: db, Logger: logger}
			node := &cluster.Node{ID: uuidpb.Generate(), Signals: bus, Interval: 10 * time.Millisecond, Timeout: 100 * time.Millisecond, Logger: logger}
			typ, _ := dogma.RegisteredMessageTypeOf(stubs.DeadlineA1)
			consumer := &DeadlineConsumer{Node: node, Signals: bus, Sink: sink,
				DeadlineTypeIDs: uuidcollection.NewSet(uuidpb.MustParse(typ.ID())), Capacity: make(chan struct{}, 1),
				LockTimeout: 20 * time.Millisecond, ContentionDelay: 200 * time.Millisecond,
				BackoffBase: time.Millisecond, BackoffCap: time.Second, Logger: logger}
			ctx, cancel := context.WithCancel(t.Context())
			group, ctx := errgroup.WithContext(ctx)
			group.Go(func() error { return bus.Run(ctx) })
			group.Go(func() error { return node.Run(ctx) })
			group.Go(func() error { return consumer.Run(ctx) })
			defer func() {
				cancel()
				if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
			wait, stop := context.WithTimeout(t.Context(), 2*time.Second)
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
