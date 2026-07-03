package xtesting

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WaitForEmptyCommandQueue waits until the command queue is empty. If this does
// not occur within [WaitTimeout], the test fails.
func WaitForEmptyCommandQueue(
	t testing.TB,
	q xsql.Querier,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		"wait for empty command queue",
		0,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
}

// ExpectCommandIDToBeQueued asserts that the command with the given ID is present
// in the queue.
func ExpectCommandIDToBeQueued(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		fmt.Sprintf("expect command %q to be queued", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// ExpectCommandToBeQueued asserts that the command queue contains the given
// command.
func ExpectCommandToBeQueued(
	t testing.TB,
	q xsql.Querier,
	want dogma.Command,
) {
	t.Helper()

	rows, err := q.QueryContext(
		t.Context(),
		`SELECT
			c.envelope
		FROM commandqueue.commands AS c`,
	)
	if err != nil {
		t.Fatalf("unable to query events: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		env := &envelopepb.Envelope{}
		if err := rows.Scan(xsql.Envelope(env)); err != nil {
			t.Fatalf("unable to scan event: %v", err)
		}

		got, err := envelopepb.Unpack[dogma.Command](env)
		if err != nil {
			t.Fatalf("unable to unpack command: %v", err)
		}

		if reflect.DeepEqual(got, want) {
			return
		}

		t.Logf("command does not match:")
		t.Logf("+++ got:\n%s", dapper.Format(got))
		t.Logf("--- want:\n%s", dapper.Format(want))

	}

	t.Fatal("command is not on the queue")
}

// ExpectCommandToBeUnattempted asserts that the command with the given ID is
// present in the queue and has never been attempted (failures = 0).
func ExpectCommandToBeUnattempted(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		fmt.Sprintf("expect command %q to be queued with no attempts", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1
		AND failures = 0`,
		xsql.UUID(messageID),
	)
}

// WaitForCommandToBeRemovedFromQueue waits until the command with the given ID
// is removed from the queue. If this does not occur within [WaitTimeout], the
// test fails.
func WaitForCommandToBeRemovedFromQueue(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		fmt.Sprintf("wait for command %q to be removed from queue", messageID),
		0,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// WaitForCommandToBePostponed waits until the command with the given ID has
// been postponed. If this does not occur within [WaitTimeout], the test fails.
func WaitForCommandToBePostponed(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		fmt.Sprintf("wait for command %q to be postponed", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1
		AND deliver_at > enqueued_at`,
		xsql.UUID(messageID),
	)
}

// EnqueuePostponedCommand inserts a command directly into the command queue
// with deliver_at set far in the future, so it will not be picked up by the
// dequeue loop. It returns the envelope of the enqueued command.
func EnqueuePostponedCommand(
	t testing.TB,
	x xsql.Executor,
	command dogma.Command,
) *envelopepb.Envelope {
	t.Helper()

	env := packTestCommand(t, command)

	if _, err := x.ExecContext(
		t.Context(),
		`INSERT INTO commandqueue.commands (
			message_id,
			message_type_id,
			envelope,
			deliver_at
		) VALUES (
			$1, $2, $3,
			clock_timestamp() + INTERVAL '24 hours'
		)`,
		xsql.UUID(env.GetBody().GetMessageId()),
		xsql.UUID(env.GetBody().GetMessage().GetTypeId()),
		xsql.Envelope(env),
	); err != nil {
		t.Fatal(err)
	}

	return env
}

func packTestCommand(t testing.TB, command dogma.Command) *envelopepb.Envelope {
	t.Helper()

	mt, ok := dogma.RegisteredMessageTypeOf(command)
	if !ok {
		t.Fatalf("%T is not a registered message type", command)
	}

	data, err := command.MarshalBinary()
	if err != nil {
		t.Fatalf("unable to marshal %T: %v", command, err)
	}

	id := uuidpb.Generate()

	env := envelopepb.NewEnvelopeBuilder().
		WithHeader(
			envelopepb.NewHeaderBuilder().
				WithCausationId(id).
				WithCorrelationId(id).
				WithSource(envelopepb.NewSourceBuilder().
					WithApplication(identitypb.New("test", uuidpb.MustParse(appKey))).
					Build()).
				Build(),
		).
		WithBody(
			envelopepb.NewBodyBuilder().
				WithMessageId(id).
				WithCreatedAt(timestamppb.Now()).
				WithMessage(
					envelopepb.NewMessageBuilder().
						WithTypeId(uuidpb.MustParse(mt.ID())).
						WithDescription(command.MessageDescription()).
						WithData(data).
						Build(),
				).
				Build(),
		).
		Build()

	if err := env.Validate(); err != nil {
		t.Fatalf("invalid envelope: %v", err)
	}

	return env
}
