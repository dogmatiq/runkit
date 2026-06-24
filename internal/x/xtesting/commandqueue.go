package xtesting

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
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

// ExpectCommandToBeQueued asserts that the command with the given ID is present
// in the queue.
func ExpectCommandToBeQueued(
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
			correlation_id,
			message_type_id,
			envelope,
			deliver_at
		) VALUES (
			$1, $2, $3, $4,
			clock_timestamp() + INTERVAL '24 hours'
		)`,
		xsql.UUID(env.GetBody().GetMessageId()),
		xsql.UUID(env.GetHeader().GetCorrelationId()),
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
