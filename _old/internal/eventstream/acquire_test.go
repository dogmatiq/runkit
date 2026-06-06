package eventstream_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
)

func TestAcquire(t *testing.T) {
	db, _ := databasetest.New(t)

	var firstStreamID *uuidpb.UUID

	// Create a new stream on first call.
	databasetest.Transact(
		t,
		db,
		func(tx *sql.Tx) {
			streamID, err := Acquire(t.Context(), tx)
			if err != nil {
				t.Fatal(err)
			}

			if err := streamID.Validate(); err != nil {
				t.Fatal(err)
			}

			firstStreamID = streamID
		},
	)

	// Return the same ID on subsequent calls.
	databasetest.Transact(
		t,
		db,
		func(tx *sql.Tx) {
			streamID, err := Acquire(t.Context(), tx)
			if err != nil {
				t.Fatal(err)
			}

			if !streamID.Equal(firstStreamID) {
				t.Fatalf(
					"unexpected stream ID: got %q, want %q",
					streamID,
					firstStreamID,
				)
			}
		},
	)

	// Create a new stream if existing streams are at capacity.
	databasetest.ExecOne(
		t,
		db,
		`UPDATE event_streams SET
			next_offset = $1
		WHERE event_stream_id = $2`,
		Capacity,
		database.MarshalUUID(firstStreamID),
	)
	databasetest.ExecOne(
		t,
		db,
		`INSERT INTO events (
			event_stream_id,
			event_offset,
			correlation_id,
			message_type_id,
			envelope
		) VALUES ($1, $2, $3, $4, $5)`,
		database.MarshalUUID(firstStreamID),
		0,
		database.MarshalUUID(uuidpb.Generate()),
		database.MarshalUUID(uuidpb.Generate()),
		[]byte{},
	)

	databasetest.Transact(
		t,
		db,
		func(tx *sql.Tx) {
			streamID, err := Acquire(t.Context(), tx)
			if err != nil {
				t.Fatal(err)
			}

			if err := streamID.Validate(); err != nil {
				t.Fatal(err)
			}

			if streamID.Equal(firstStreamID) {
				t.Fatalf(
					"unexpected stream ID: got %q, want a different ID",
					streamID,
				)
			}
		},
	)
}
