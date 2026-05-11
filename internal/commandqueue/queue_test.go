package commandqueue_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	. "github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/pgtest"
	"google.golang.org/protobuf/proto"
)

func TestEnqueue(t *testing.T) {
	db, _ := pgtest.Setup(t)
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
		messageID := wantEnv.GetBody().GetMessageId().AsString()

		if err := Enqueue(t.Context(), tx, wantEnv); err != nil {
			t.Fatal(err)
		}

		got, ok := getCommand(t, tx, messageID)
		if !ok {
			t.Fatalf("expected command %s to be on the queue", messageID)
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

			id1 := env1.GetBody().GetMessageId().AsString()
			id2 := env2.GetBody().GetMessageId().AsString()

			if err := Enqueue(t.Context(), tx, env1); err != nil {
				t.Fatal(err)
			}
			if err := Enqueue(t.Context(), tx, env2); err != nil {
				t.Fatal(err)
			}

			if _, ok := getCommand(t, tx, id1); !ok {
				t.Fatalf("expected command %s to be on the queue", id1)
			}
			if _, ok := getCommand(t, tx, id2); ok {
				t.Fatalf("did not expect command %s to be on the queue", id2)
			}
		}

		// The idempotency key remains in-effect after the first command has
		// been nacked, reset, and acked.
		{
			const key = "<idempotency-key-after-handling>"

			env1 := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(key))
			id1 := env1.GetBody().GetMessageId().AsString()

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
			id2 := env2.GetBody().GetMessageId().AsString()

			if err := Enqueue(t.Context(), tx, env2); err != nil {
				t.Fatal(err)
			}
			if _, ok := getCommand(t, tx, id2); ok {
				t.Fatalf("did not expect command %s to be on the queue", id2)
			}
		}
	})
}

func TestAck(t *testing.T) {
	db, _ := pgtest.Setup(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	env := packer.PackCommand(stubs.CommandA1)
	messageID := env.GetBody().GetMessageId().AsString()

	if err := Enqueue(t.Context(), tx, env); err != nil {
		t.Fatal(err)
	}

	if err := Ack(t.Context(), tx, messageID); err != nil {
		t.Fatal(err)
	}

	if _, ok := getCommand(t, tx, messageID); ok {
		t.Fatalf("did not expect command %s to be on the queue", messageID)
	}
}

func TestNack(t *testing.T) {
	db, _ := pgtest.Setup(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	env := packer.PackCommand(stubs.CommandA1)
	messageID := env.GetBody().GetMessageId().AsString()

	if err := Enqueue(t.Context(), tx, env); err != nil {
		t.Fatal(err)
	}

	// now() is stable for the duration of the tx; capture it once.
	var txNow time.Time
	if err := tx.QueryRowContext(t.Context(), `SELECT now()`).Scan(&txNow); err != nil {
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
	db, _ := pgtest.Setup(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	env := packer.PackCommand(stubs.CommandA1)
	messageID := env.GetBody().GetMessageId().AsString()

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

	var txNow time.Time
	if err := tx.QueryRowContext(t.Context(), `SELECT now()`).Scan(&txNow); err != nil {
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
}

// enqueuedCommand is a pending command on the queue.
type enqueuedCommand struct {
	Envelope      *envelopepb.Envelope
	NextAttemptAt time.Time
}

// getCommand reads the command with the given message ID from the queue.
func getCommand(t *testing.T, tx *sql.Tx, messageID string) (enqueuedCommand, bool) {
	t.Helper()

	var (
		command enqueuedCommand
		envData []byte
	)
	err := tx.QueryRowContext(
		t.Context(),
		`SELECT
			envelope,
			next_attempt_at
		FROM commandqueue.commands
		WHERE message_id = $1`,
		messageID,
	).Scan(&envData, &command.NextAttemptAt)
	if err == sql.ErrNoRows {
		return enqueuedCommand{}, false
	}
	if err != nil {
		t.Fatal(err)
	}

	command.Envelope = &envelopepb.Envelope{}
	if err := command.Envelope.UnmarshalBinary(envData); err != nil {
		t.Fatal(err)
	}

	return command, true
}
