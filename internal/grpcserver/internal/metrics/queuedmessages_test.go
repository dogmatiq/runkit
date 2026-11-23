package metrics_test

import (
	"database/sql"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	. "github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics"
)

func TestDueAndFailingQueuedMessages(t *testing.T) {
	t.Run("it counts the messages that are due and the messages that are failing, for each queue and handler", func(t *testing.T) {
		t.Parallel()

		f := newDueAndFailingFixture(t)
		f.Expect(t, f.Counted)
	})

	t.Run("it sends a zero when a count falls to nothing", func(t *testing.T) {
		t.Parallel()

		f := newDueAndFailingFixture(t)
		f.Expect(t, f.Counted)

		if _, err := f.DB.ExecContext(
			t.Context(),
			`DELETE FROM runkit.queued_messages`,
		); err != nil {
			t.Fatal(err)
		}

		zeros := map[string]map[string]uint64{}
		for name, values := range f.Counted {
			zeros[name] = map[string]uint64{}
			for subject := range values {
				zeros[name][subject] = 0
			}
		}

		f.Expect(t, zeros)
	})
}

// dueAndFailingFixture is the data of the tests of
// [DueAndFailingQueuedMessages].
type dueAndFailingFixture struct {
	DB *sql.DB

	// Counted is the expected value of each subject of each metric, keyed by
	// the metric's full name and the subject's UUID, once the messages are
	// queued.
	Counted map[string]map[string]uint64

	// Expect fails the test unless the reporter's samples become the given
	// values.
	Expect func(t *testing.T, want map[string]map[string]uint64)
}

// newDueAndFailingFixture queues messages in a new database, and returns a
// fixture with a reporter of [DueAndFailingQueuedMessages] that watches them.
//
// Queue 1 has a command that is due, a command that is failing and waits for
// its next attempt, and a deadline that is due and failing. Queue 2 has a
// command that is due and whose type is missing from the handler keys.
func newDueAndFailingFixture(t *testing.T) dueAndFailingFixture {
	t.Helper()

	var (
		queue1             = uuid.New()
		queue2             = uuid.New()
		routedType         = uuid.New()
		unroutedType       = uuid.New()
		deadlineType       = uuid.New()
		commandHandlerKey  = uuid.New()
		deadlineHandlerKey = uuid.New()
	)

	subject := func(id uuid.UUID) string { return id.String() }

	db := xtesting.NewDatabase(t)

	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)

	queueMessage(t, db, queue1, routedType, uuid.Nil(), past, 0)
	queueMessage(t, db, queue1, routedType, uuid.Nil(), future, 1)
	queueMessage(t, db, queue1, deadlineType, deadlineHandlerKey, past, 2)
	queueMessage(t, db, queue2, unroutedType, uuid.Nil(), past, 0)

	reporter := &Reporter{
		DB:           db,
		EngineLogger: spruce.NewTestLogger(t),
		MinInterval:  time.Millisecond,
		MaxDutyCycle: 1,
		Groups: []Group{
			DueAndFailingQueuedMessages(
				map[uuid.UUID]uuid.UUID{routedType: commandHandlerKey},
			),
		},
	}

	observable, release := reporter.Acquire()
	t.Cleanup(release)

	return dueAndFailingFixture{
		DB: db,
		Counted: map[string]map[string]uint64{
			nameOf[metricsgrpc.QueueCommandsDue]():     {subject(queue1): 1, subject(queue2): 1},
			nameOf[metricsgrpc.QueueCommandsFailing](): {subject(queue1): 1},
			nameOf[metricsgrpc.QueueDeadlinesDue]():    {subject(queue1): 1},

			nameOf[metricsgrpc.QueueDeadlinesFailing](): {subject(queue1): 1},

			nameOf[metricsgrpc.HandlerCommandsDue]():      {subject(commandHandlerKey): 1},
			nameOf[metricsgrpc.HandlerCommandsFailing]():  {subject(commandHandlerKey): 1},
			nameOf[metricsgrpc.HandlerDeadlinesDue]():     {subject(deadlineHandlerKey): 1},
			nameOf[metricsgrpc.HandlerDeadlinesFailing](): {subject(deadlineHandlerKey): 1},
		},
		Expect: func(t *testing.T, want map[string]map[string]uint64) {
			t.Helper()

			expectSamples(t, reporter, observable, want)
		},
	}
}

// nameOf returns the full name of the message of type M.
func nameOf[M any, P interface {
	*M
	proto.Message
}]() string {
	return string(proto.MessageName(P(new(M))))
}

// queueMessage adds a message to a queue. The message is a deadline if
// handlerKey is a UUID other than the nil UUID. Its next attempt is at
// nextAttemptAt, and it has failed the given number of times.
func queueMessage(
	t testing.TB,
	db *sql.DB,
	queueID uuid.UUID,
	typeID uuid.UUID,
	handlerKey uuid.UUID,
	nextAttemptAt time.Time,
	failures int,
) {
	t.Helper()

	var firstFailedAt sql.NullTime
	if failures > 0 {
		firstFailedAt = sql.NullTime{Time: nextAttemptAt, Valid: true}
	}

	if _, err := db.ExecContext(
		t.Context(),
		`INSERT INTO runkit.queued_messages (
			message_id,
			message_type_id,
			queue_id,
			handler_key,
			instance_id,
			envelope,
			failures,
			first_failed_at,
			scheduled_for,
			next_attempt_at
		) VALUES (
			$1, $2, $3, $4, $5, '\x01', $6, $7, $8, $8
		)`,
		uuid.New().String(),
		typeID.String(),
		queueID.String(),
		xsql.UUID(handlerKey),
		sql.NullString{String: "instance", Valid: handlerKey != uuid.Nil()},
		failures,
		firstFailedAt,
		nextAttemptAt,
	); err != nil {
		t.Fatal(err)
	}
}

// valueOf returns the value that the message of the named metric holds.
func valueOf(t testing.TB, name string, data []byte) uint64 {
	t.Helper()

	messageType, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName(name),
	)
	if err != nil {
		t.Fatal(err)
	}

	message := messageType.New()
	if err := proto.Unmarshal(data, message.Interface()); err != nil {
		t.Fatal(err)
	}

	field := message.Descriptor().Fields().ByName("value")

	return message.Get(field).Uint()
}
