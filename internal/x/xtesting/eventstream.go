package xtesting

import (
	"iter"
	"reflect"
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
)

// ExpectEvents reads all events from the event stream and verifies that they
// match the expected events in order.
func ExpectEvents(
	t testing.TB,
	got iter.Seq2[*envelopepb.Envelope, error],
	want ...dogma.Event,
) {
	t.Helper()

	for gotEnvelope, err := range got {
		if err != nil {
			t.Fatal(err)
		}

		if len(want) == 0 {
			t.Fatalf(
				"unexpected event on stream at offset %d",
				eventstream.OffsetOf(gotEnvelope),
			)
		}

		var wantEvent dogma.Event
		wantEvent, want = want[0], want[1:]

		gotEvent, err := envelopepb.Unpack[dogma.Event](gotEnvelope)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(wantEvent, gotEvent) {
			t.Fatalf(
				"unexpected event at offset %d:\n\tgot:  %s\n\twant: %s",
				eventstream.OffsetOf(gotEnvelope),
				dapper.Format(gotEvent),
				dapper.Format(wantEvent),
			)
		}
	}

	if len(want) != 0 {
		t.Fatalf("expected %d more events", len(want))
	}
}

// ExpectEventEnvelopes reads all envelopes from the event stream and verifies
// that they match the expected envelopes in order.
func ExpectEventEnvelopes(
	t testing.TB,
	got iter.Seq2[*envelopepb.Envelope, error],
	want ...*envelopepb.Envelope,
) {
	t.Helper()

	for gotEnvelope, err := range got {
		if err != nil {
			t.Fatal(err)
		}

		if len(want) == 0 {
			t.Fatalf(
				"unexpected event on stream at offset %d",
				eventstream.OffsetOf(gotEnvelope),
			)
		}

		ExpectEnvelope(t, gotEnvelope, want[0])
		want = want[1:]
	}

	if len(want) != 0 {
		t.Fatalf("expected %d more events", len(want))
	}
}
