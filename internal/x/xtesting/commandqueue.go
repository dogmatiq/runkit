package xtesting

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// AddCommandDirectlyToQueue adds the given directly command to the engine's command
// queue, bypassing the engine's [dogma.CommandExecutor] implementation.
func AddCommandDirectlyToQueue(
	t testing.TB,
	engine *dogmaengine.Engine,
	command dogma.Command,
) *envelopepb.Envelope {
	t.Helper()

	app := runtimeconfig.FromApplication(engine.App)

	packer := &envelopepb.Packer{
		Application: app.Identity(),
	}

	commandEnvelope := packer.PackCommand(command)

	if err := commandqueue.Add(
		t.Context(),
		engine.DB,
		commandEnvelope,
	); err != nil {
		t.Fatalf("unable to add command to queue: %v", err)
	}

	return commandEnvelope
}

// ExpectEmptyCommandQueueEventually asserts that all pending commands are
// eventually removed from the queue.
func ExpectEmptyCommandQueueEventually(
	t testing.TB,
	q xsql.Querier,
) {
	ExpectQueryResultEventually(
		t,
		"command queue drained",
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands`,
	)
}

// ExpectCommandToBeQueued asserts that the command queue contains all
// commands with the given IDs.
func ExpectCommandToBeQueued(
	t testing.TB,
	q xsql.Querier,
	messageIDs ...*uuidpb.UUID,
) {
	ExpectQueryResult(
		t,
		fmt.Sprintf(
			"command queue contains %d specific messages",
			len(messageIDs),
		),
		len(messageIDs),
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = ANY($1)`,
		xsql.UUIDs(messageIDs),
	)
}

// ExpectCommandToBeRemovedFromQueueEventually asserts that the command queue
// eventually does not contain the commands with the given IDs.
func ExpectCommandToBeRemovedFromQueueEventually(
	t testing.TB,
	q xsql.Querier,
	messageIDs ...*uuidpb.UUID,
) {
	ExpectQueryResultEventually(
		t,
		fmt.Sprintf(
			"command queue does not contain %d specific messages",
			len(messageIDs),
		),
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = ANY($1)`,
		xsql.UUIDs(messageIDs),
	)
}
