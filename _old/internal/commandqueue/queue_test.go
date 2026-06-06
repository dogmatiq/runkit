package commandqueue_test

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEnqueue(t *testing.T) {
	db, _ := databasetest.New(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	t.Run("it stores the command", func(t *testing.T) {
		cases := []struct {
			Name                string
			Envelope            *envelopepb.Envelope
			HandlerKey          *uuidpb.UUID
			AggregateInstanceID *string
		}{
			{
				Name:                "with an aggregate instance ID",
				Envelope:            packer.PackCommand(stubs.CommandA1),
				HandlerKey:          uuidpb.Generate(),
				AggregateInstanceID: new("<instance>"),
			},
			{
				Name:                "without an aggregate instance ID",
				Envelope:            packer.PackCommand(stubs.CommandA1),
				HandlerKey:          uuidpb.Generate(),
				AggregateInstanceID: nil,
			},
		}

		for _, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				databasetest.Transact(t, db, func(tx *sql.Tx) {
					if err := Enqueue(
						t.Context(),
						tx,
						c.Envelope,
						c.HandlerKey,
						c.AggregateInstanceID,
					); err != nil {
						t.Fatal(err)
					}

					got, ok := getPendingCommand(
						t,
						tx,
						c.Envelope.GetBody().GetMessageId(),
					)
					if !ok {
						t.Fatalf(
							"expected command %s to be on the queue",
							c.Envelope.GetBody().GetMessageId().AsString(),
						)
					}

					xtesting.ExpectEnvelope(t, got.Envelope, c.Envelope)

					if !got.HandlerKey.Equal(c.HandlerKey) {
						t.Fatalf("unexpected handler key: got %q, want %q", got.HandlerKey, c.HandlerKey)
					}

					if c.AggregateInstanceID == nil && got.AggregateInstanceID != nil {
						t.Fatalf(
							"unexpected aggregate instance ID: got <nil>, want %q",
							*c.AggregateInstanceID,
						)
					}

					if c.AggregateInstanceID != nil && got.AggregateInstanceID == nil {
						t.Fatalf(
							"unexpected aggregate instance ID: got %q, want <nil>",
							*c.AggregateInstanceID,
						)
					}

					if c.AggregateInstanceID != nil && *got.AggregateInstanceID != *c.AggregateInstanceID {
						t.Fatalf(
							"unexpected aggregate instance ID: got %q, want %q",
							*got.AggregateInstanceID,
							*c.AggregateInstanceID,
						)
					}
				})
			})
		}
	})

	t.Run("it deduplicates by idempotency key", func(t *testing.T) {
		t.Run("when the original command is still pending", func(t *testing.T) {
			databasetest.Transact(t, db, func(tx *sql.Tx) {
				idempotencyKey := uuidpb.Generate().AsString()

				originalCommand := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(idempotencyKey))
				duplicateCommand := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(idempotencyKey))

				if err := Enqueue(
					t.Context(),
					tx,
					originalCommand,
					uuidpb.Generate(),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				if err := Enqueue(
					t.Context(),
					tx,
					duplicateCommand,
					uuidpb.Generate(),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				originalID := originalCommand.GetBody().GetMessageId()
				duplicateID := duplicateCommand.GetBody().GetMessageId()

				if _, ok := getPendingCommand(t, tx, originalID); !ok {
					t.Fatalf("expected initial command %s to be on the queue", originalID.AsString())
				}

				if _, ok := getPendingCommand(t, tx, duplicateID); ok {
					t.Fatalf("did not expect duplicate command %s to be on the queue", duplicateID.AsString())
				}
			})
		})

		t.Run("when the original command has been dequeued", func(t *testing.T) {
			databasetest.Transact(t, db, func(tx *sql.Tx) {
				idempotencyKey := uuidpb.Generate().AsString()

				originalCommand := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(idempotencyKey))
				duplicateCommand := packer.PackCommand(stubs.CommandA1, envelopepb.WithIdempotencyKey(idempotencyKey))

				if err := Enqueue(
					t.Context(),
					tx,
					originalCommand,
					uuidpb.Generate(),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				if err := Dequeue(
					t.Context(),
					tx,
					originalCommand.GetBody().GetMessageId(),
				); err != nil {
					t.Fatal(err)
				}

				if err := Enqueue(
					t.Context(),
					tx,
					duplicateCommand,
					uuidpb.Generate(),
					nil,
				); err != nil {
					t.Fatal(err)
				}

				duplicateID := duplicateCommand.GetBody().GetMessageId()

				if _, ok := getPendingCommand(t, tx, duplicateID); ok {
					t.Fatalf("did not expect duplicate command %s to be on the queue", duplicateID.AsString())
				}
			})
		})
	})
}

func TestDequeue(t *testing.T) {
	db, _ := databasetest.New(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	databasetest.Transact(
		t,
		db,
		func(tx *sql.Tx) {
			envelope := packer.PackCommand(stubs.CommandA1)
			messageID := envelope.GetBody().GetMessageId()

			if err := Enqueue(
				t.Context(),
				tx,
				envelope,
				uuidpb.Generate(),
				nil,
			); err != nil {
				t.Fatal(err)
			}

			if err := Dequeue(t.Context(), tx, messageID); err != nil {
				t.Fatal(err)
			}

			if _, ok := getPendingCommand(t, tx, messageID); ok {
				t.Fatalf("did not expect command %s to be on the queue", messageID.AsString())
			}
		},
	)
}

func TestBackoff(t *testing.T) {
	db, _ := databasetest.New(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	databasetest.Transact(t, db, func(tx *sql.Tx) {
		envelope := packer.PackCommand(stubs.CommandA1)
		messageID := envelope.GetBody().GetMessageId()

		if err := Enqueue(
			t.Context(),
			tx,
			envelope,
			uuidpb.Generate(),
			nil,
		); err != nil {
			t.Fatal(err)
		}

		// Capture the transaction's stable "now" timestamp.
		row := tx.QueryRowContext(
			t.Context(),
			`SELECT now()`,
		)

		var now time.Time
		if err := row.Scan(&now); err != nil {
			t.Fatal(err)
		}

		// First call schedules the next attempt one base interval into the future.
		{
			if err := Backoff(t.Context(), tx, messageID); err != nil {
				t.Fatal(err)
			}

			got, _ := getPendingCommand(t, tx, messageID)
			want := now.Add(BackoffBase)
			if !got.NextAttemptAt.Equal(want) {
				t.Fatalf("unexpected next attempt time: got %v, want %v", got.NextAttemptAt, want)
			}
		}

		// Second call schedules the next attempt two base intervals into the future.
		{
			if err := Backoff(t.Context(), tx, messageID); err != nil {
				t.Fatal(err)
			}

			got, _ := getPendingCommand(t, tx, messageID)
			want := now.Add(2 * BackoffBase)
			if !got.NextAttemptAt.Equal(want) {
				t.Fatalf("unexpected next attempt time: got %v, want %v", got.NextAttemptAt, want)
			}
		}

		// Make enough calls that the next attempt time hits the limit.
		{
			// Smallest N such that 2^(N-1) * BackoffBase > BackoffCap.
			for d := BackoffBase; d <= BackoffCap; d *= 2 {
				if err := Backoff(t.Context(), tx, messageID); err != nil {
					t.Fatal(err)
				}
			}

			got, _ := getPendingCommand(t, tx, messageID)
			want := now.Add(BackoffCap)
			if !got.NextAttemptAt.Equal(want) {
				t.Fatalf("unexpected next attempt time: got %v, want %v", got.NextAttemptAt, want)
			}
		}
	})
}

// pendingCommand is a pending command on the queue.
type pendingCommand struct {
	Envelope            *envelopepb.Envelope
	HandlerKey          *uuidpb.UUID
	AggregateInstanceID *string
	NextAttemptAt       time.Time
}

// getPendingCommand reads the command with the given message ID from the queue.
func getPendingCommand(
	t *testing.T,
	q database.Executor,
	messageID *uuidpb.UUID,
) (pendingCommand, bool) {
	t.Helper()

	row := q.QueryRowContext(
		t.Context(),
		`SELECT
			envelope,
			handler_key,
			aggregate_instance_id,
			next_attempt_at
		FROM pending_commands
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	)

	command := pendingCommand{
		Envelope:   &envelopepb.Envelope{},
		HandlerKey: &uuidpb.UUID{},
	}
	if err := row.Scan(
		database.UnmarshalEnvelope(command.Envelope),
		database.UnmarshalUUID(command.HandlerKey),
		&command.AggregateInstanceID,
		&command.NextAttemptAt,
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		return pendingCommand{}, false
	}

	return command, true
}
