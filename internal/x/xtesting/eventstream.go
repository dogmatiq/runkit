package xtesting

import (
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// ExpectEventStreamCount asserts that the number of event streams in the database
// matches the given value.
func ExpectEventStreamCount(
	t testing.TB,
	q xsql.Querier,
	want int,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		"event stream count",
		want,
		q,
		`SELECT COUNT(*)
		FROM dogma.event_streams`,
	)
}

// ExpectEventEnvelopesAtOffset asserts that an event stream contains the given
// event envelopes, starting at the given offset.
func ExpectEventEnvelopesAtOffset(
	t testing.TB,
	q xsql.Querier,
	eventStreamID *uuidpb.UUID,
	offset eventstream.Offset,
	want ...*envelopepb.Envelope,
) {
	t.Helper()

	rows, err := q.QueryContext(
		t.Context(),
		`SELECT
			event_stream_offset,
			envelope
		FROM dogma.events
		WHERE event_stream_id = $1
		AND event_stream_offset >= $2
		ORDER BY event_stream_offset
		LIMIT $3`,
		xsql.UUID(eventStreamID),
		offset,
		len(want),
	)
	if err != nil {
		t.Fatalf("unable to query events: %v", err)
	}
	defer rows.Close()

	wantOffset := offset

	for rows.Next() {
		var (
			gotOffset   eventstream.Offset
			gotEnvelope = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&gotOffset,
			xsql.Envelope(gotEnvelope),
		); err != nil {
			t.Fatalf("unable to scan event envelope: %v", err)
		}

		if gotOffset != wantOffset {
			t.Fatalf(
				"unexpected event stream offset: got %d, want %d",
				gotOffset,
				wantOffset,
			)
		}

		wantEnvelope := want[0]
		want = want[1:]

		if !proto.Equal(gotEnvelope, wantEnvelope) {
			t.Logf("unexpected event envelope at offset %d:", gotOffset)
			t.Logf("+++ got:\n%s", prototext.Format(gotEnvelope))
			t.Logf("--- want:\n%s", prototext.Format(wantEnvelope))
			t.FailNow()
		}

		wantOffset++
	}

	if len(want) != 0 {
		t.Fatalf(
			"missing event at offset: %d",
			wantOffset,
		)
	}
}
