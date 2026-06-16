package xtesting

import (
	"reflect"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// CreateEventStreams creates the given number of event streams.
func CreateEventStreams(
	t testing.TB,
	x xsql.Executor,
	count int,
) []*uuidpb.UUID {
	t.Helper()

	streamIDs := make([]*uuidpb.UUID, count)

	for i := range count {
		streamIDs[i] = uuidpb.Generate()

		if _, err := x.ExecContext(
			t.Context(),
			`INSERT INTO eventstream.streams (id) VALUES ($1)`,
			xsql.UUID(streamIDs[i]),
		); err != nil {
			t.Fatal(err)
		}
	}

	return streamIDs
}

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
		FROM eventstream.streams`,
	)
}

// ExpectEventCount asserts that the total number of events in the database
// matches the given value.
func ExpectEventCount(
	t testing.TB,
	q xsql.Querier,
	want int,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		"event count",
		want,
		q,
		`SELECT COUNT(*)
		FROM eventstream.events`,
	)
}

// ExpectContiguousEvents asserts that an event stream contains the given
// events, starting at the given offset, with no gaps.
func ExpectContiguousEvents(
	t testing.TB,
	q xsql.Querier,
	streamID *uuidpb.UUID,
	offset uint64,
	want ...dogma.Event,
) {
	t.Helper()

	rows, err := q.QueryContext(
		t.Context(),
		`SELECT
			e.stream_offset,
			e.envelope
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
		AND e.stream_offset >= $2
		ORDER BY e.stream_offset
		LIMIT $3`,
		xsql.UUID(streamID),
		offset,
		len(want),
	)
	if err != nil {
		t.Fatalf("unable to query events: %v", err)
	}
	defer rows.Close()

	wantOffset := offset

	for rows.Next() {
		var gotOffset uint64
		env := &envelopepb.Envelope{}
		if err := rows.Scan(&gotOffset, xsql.Envelope(env)); err != nil {
			t.Fatalf("unable to scan event: %v", err)
		}

		if gotOffset != wantOffset {
			t.Fatalf(
				"unexpected event stream offset: got %d, want %d",
				gotOffset,
				wantOffset,
			)
		}

		got, err := envelopepb.Unpack[dogma.Event](env)
		if err != nil {
			t.Fatalf("unable to unpack event: %v", err)
		}

		wantEvent := want[0]
		want = want[1:]

		if !reflect.DeepEqual(got, wantEvent) {
			t.Logf("unexpected event at offset %d:", gotOffset)
			t.Logf("+++ got:\n%#v", got)
			t.Logf("--- want:\n%#v", wantEvent)
			t.FailNow()
		}

		wantOffset++
	}

	if len(want) != 0 {
		t.Fatalf("missing %d event(s)", len(want))
	}
}

// ExpectContiguousEventEnvelopes asserts that an event stream contains the
// given event envelopes, starting at the given offset.
func ExpectContiguousEventEnvelopes(
	t testing.TB,
	q xsql.Querier,
	streamID *uuidpb.UUID,
	offset uint64,
	want ...*envelopepb.Envelope,
) {
	t.Helper()

	rows, err := q.QueryContext(
		t.Context(),
		`SELECT
			e.stream_offset,
			e.envelope
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
		AND e.stream_offset >= $2
		ORDER BY e.stream_offset
		LIMIT $3`,
		xsql.UUID(streamID),
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
			gotOffset   uint64
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

// ExpectNoUnconsumedEventsEventually asserts that the handler with the given
// key eventually catches up to the tail of all non-empty event streams.
//
// It assumes that all events on all streams are of types that the handler
// consumes. If that is not the case this assertion will hang until it times out.
func ExpectNoUnconsumedEventsEventually(
	t testing.TB,
	q xsql.Querier,
	handlerKey string,
) {
	t.Helper()

	ExpectQueryResultEventually(
		t,
		"no unconsumed events",
		0,
		q,
		`SELECT COUNT(*)
		FROM eventstream.streams AS s
		LEFT JOIN eventstream.handler_checkpoints AS h
			ON h.handler_key = $1
			AND h.stream_id = s.id
		WHERE s.next_offset > 0
		AND (
			h.stream_id IS NULL
			OR h.checkpoint_offset IS NULL
			OR h.checkpoint_offset < s.next_offset
		)`,
		handlerKey,
	)
}
