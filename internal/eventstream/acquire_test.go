package eventstream_test

import (
	"database/sql"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestAcquire(t *testing.T) {
	db := xtesting.NewDatabase(t)

	var firstStreamID *uuidpb.UUID

	// Create a new stream on first call.
	xtesting.Transact(
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

	xtesting.ExpectEventStreamCount(
		t,
		db,
		1,
	)

	// Return the same ID on subsequent calls.
	xtesting.Transact(
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

	xtesting.ExpectEventStreamCount(
		t,
		db,
		1,
	)

	// Create a new stream if existing streams are at capacity.
	xtesting.ExecOne(
		t,
		db,
		`UPDATE dogma.event_streams SET
			next_offset = $1
		WHERE id = $2`,
		Capacity,
		xsql.UUID(firstStreamID),
	)
	xtesting.ExecOne(
		t,
		db,
		`INSERT INTO dogma.events (
			message_id,
			event_stream_id,
			event_stream_offset,
			envelope
		) VALUES ($1, $2, $3, $4)`,
		xsql.UUID(uuidpb.Generate()),
		xsql.UUID(firstStreamID),
		0,
		[]byte{},
	)

	xtesting.Transact(
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

	xtesting.ExpectEventStreamCount(
		t,
		db,
		2,
	)

}
