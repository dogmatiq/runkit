package commandqueue_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"google.golang.org/protobuf/proto"
)

func TestEnqueue(t *testing.T) {
	db, _ := database.NewTestDB(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	t.Run("it stores the command", func(t *testing.T) {
		wantEnv := packer.PackCommand(stubs.CommandA1)
		messageID := wantEnv.GetBody().GetMessageId()

		if err := Enqueue(t.Context(), tx, wantEnv); err != nil {
			t.Fatal(err)
		}

		got, ok := getCommand(t, tx, messageID)
		if !ok {
			t.Fatalf("expected command %s to be on the queue", messageID.AsString())
		}
		if !proto.Equal(got.Envelope, wantEnv) {
			t.Fatalf("unexpected envelope: got %v, want %v", got.Envelope, wantEnv)
		}
	})

	t.Run("it deduplicates by idempotency key", func(t *testing.T) {
		// The key dedupes a subsequent Enqueue while the first command is
		// still on the queue.
		{
			const key = "<idempotency-key>"

			env1 := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(key))
			env2 := packer.PackCommand(stubs.CommandA2, envelopepb.WithIdempotencyKey(key))

			id1 := env1.GetBody().GetMessageId()
			id2 := env2.GetBody().GetMessageId()

			if err := Enqueue(t.Context(), tx, env1); err != nil {
				t.Fatal(err)
			}
			if err := Enqueue(t.Context(), tx, env2); err != nil {
				t.Fatal(err)
			}

			if _, ok := getCommand(t, tx, id1); !ok {
				t.Fatalf("expected command %s to be on the queue", id1.AsString())
			}
			if _, ok := getCommand(t, tx, id2); ok {
				t.Fatalf("did not expect command %s to be on the queue", id2.AsString())
			}
		}

		// The idempotency key remains in-effect after the first command has
		// been nacked, reset, and acked.
		{
			const key = "<idempotency-key-after-handling>"

			env1 := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(key))
			id1 := env1.GetBody().GetMessageId()

			if err := Enqueue(t.Context(), tx, env1); err != nil {
				t.Fatal(err)
			}
			if err := Nack(t.Context(), tx, id1); err != nil {
				t.Fatal(err)
			}
			if err := Reset(t.Context(), tx, id1); err != nil {
				t.Fatal(err)
			}
			if err := Ack(t.Context(), tx, id1); err != nil {
				t.Fatal(err)
			}

			env2 := packer.PackCommand(stubs.CommandA2, envelopepb.WithIdempotencyKey(key))
			id2 := env2.GetBody().GetMessageId()

			if err := Enqueue(t.Context(), tx, env2); err != nil {
				t.Fatal(err)
			}
			if _, ok := getCommand(t, tx, id2); ok {
				t.Fatalf("did not expect command %s to be on the queue", id2.AsString())
			}
		}
	})
}

func TestAck(t *testing.T) {
	db, _ := database.NewTestDB(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	env := packer.PackCommand(stubs.CommandA1)
	messageID := env.GetBody().GetMessageId()

	if err := Enqueue(t.Context(), tx, env); err != nil {
		t.Fatal(err)
	}

	if err := Ack(t.Context(), tx, messageID); err != nil {
		t.Fatal(err)
	}

	if _, ok := getCommand(t, tx, messageID); ok {
		t.Fatalf("did not expect command %s to be on the queue", messageID.AsString())
	}
}

func TestNack(t *testing.T) {
	db, _ := database.NewTestDB(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	env := packer.PackCommand(stubs.CommandA1)
	messageID := env.GetBody().GetMessageId()

	if err := Enqueue(t.Context(), tx, env); err != nil {
		t.Fatal(err)
	}

	// now() is stable for the duration of the tx; capture it once.
	nowRow := tx.QueryRowContext(t.Context(), `SELECT now()`)

	var txNow time.Time
	if err := nowRow.Scan(&txNow); err != nil {
		t.Fatal(err)
	}

	// First Nack schedules the next attempt one base interval into the future.
	{
		if err := Nack(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}
		got, _ := getCommand(t, tx, messageID)
		wantNext := txNow.Add(BackoffBase)
		if !got.NextAttemptAt.Equal(wantNext) {
			t.Fatalf("unexpected next_attempt_at: got %v, want %v", got.NextAttemptAt, wantNext)
		}
	}

	// Second Nack doubles the interval.
	{
		if err := Nack(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}
		got, _ := getCommand(t, tx, messageID)
		wantNext := txNow.Add(2 * BackoffBase)
		if !got.NextAttemptAt.Equal(wantNext) {
			t.Fatalf("unexpected next_attempt_at: got %v, want %v", got.NextAttemptAt, wantNext)
		}
	}

	// Enough further Nacks that the exponential growth must overflow the cap.
	{
		// Smallest N such that 2^(N-1) * BackoffBase > BackoffCap.
		overflowAttempts := 1
		for d := BackoffBase; d <= BackoffCap; d *= 2 {
			overflowAttempts++
		}
		for range overflowAttempts {
			if err := Nack(t.Context(), tx, messageID); err != nil {
				t.Fatal(err)
			}
		}
		got, _ := getCommand(t, tx, messageID)
		wantNext := txNow.Add(BackoffCap)
		if !got.NextAttemptAt.Equal(wantNext) {
			t.Fatalf("unexpected next_attempt_at: got %v, want %v", got.NextAttemptAt, wantNext)
		}
	}
}

func TestReset(t *testing.T) {
	db, _ := database.NewTestDB(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	t.Run("it restarts the backoff progression", func(t *testing.T) {
		env := packer.PackCommand(stubs.CommandA1)
		messageID := env.GetBody().GetMessageId()

		if err := Enqueue(t.Context(), tx, env); err != nil {
			t.Fatal(err)
		}
		if err := Nack(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}
		if err := Nack(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}

		if err := Reset(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}

		nowRow := tx.QueryRowContext(t.Context(), `SELECT now()`)

		var txNow time.Time
		if err := nowRow.Scan(&txNow); err != nil {
			t.Fatal(err)
		}

		if err := Nack(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}

		got, _ := getCommand(t, tx, messageID)
		wantNext := txNow.Add(BackoffBase)
		if !got.NextAttemptAt.Equal(wantNext) {
			t.Fatalf("unexpected next_attempt_at: got %v, want %v", got.NextAttemptAt, wantNext)
		}
	})

	t.Run("it clears the route", func(t *testing.T) {
		env := packer.PackCommand(stubs.CommandA2)
		messageID := env.GetBody().GetMessageId()

		if err := Enqueue(t.Context(), tx, env); err != nil {
			t.Fatal(err)
		}

		const (
			handlerKey = "ef5ea913-f312-422f-9c40-32280c17ac04"
			instanceID = "<instance-1>"
		)
		if _, err := tx.ExecContext(
			t.Context(),
			`UPDATE command_queue SET
				routed_to_handler_key = $2,
				routed_to_aggregate_instance_id = $3
			WHERE message_id = $1`,
			database.MarshalUUID(messageID),
			handlerKey,
			instanceID,
		); err != nil {
			t.Fatal(err)
		}

		if err := Reset(t.Context(), tx, messageID); err != nil {
			t.Fatal(err)
		}

		row := tx.QueryRowContext(
			t.Context(),
			`SELECT routed_to_handler_key IS NULL
			FROM command_queue
			WHERE message_id = $1`,
			database.MarshalUUID(messageID),
		)

		var cleared bool
		if err := row.Scan(&cleared); err != nil {
			t.Fatal(err)
		}
		if !cleared {
			t.Fatalf("did not expect a route for command %s to remain", messageID.AsString())
		}
	})
}

// enqueuedCommand is a pending command on the queue.
type enqueuedCommand struct {
	Envelope      *envelopepb.Envelope
	NextAttemptAt time.Time
}

// getCommand reads the command with the given message ID from the queue.
func getCommand(t *testing.T, tx *sql.Tx, messageID *uuidpb.UUID) (enqueuedCommand, bool) {
	t.Helper()

	row := tx.QueryRowContext(
		t.Context(),
		`SELECT envelope, next_attempt_at
		FROM command_queue
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	)

	command := enqueuedCommand{
		Envelope: &envelopepb.Envelope{},
	}
	err := row.Scan(
		database.UnmarshalEnvelope(command.Envelope),
		&command.NextAttemptAt,
	)
	if err == sql.ErrNoRows {
		return enqueuedCommand{}, false
	}
	if err != nil {
		t.Fatal(err)
	}

	return command, true
}
