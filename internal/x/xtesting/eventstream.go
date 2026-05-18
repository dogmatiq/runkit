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

	for eventEnvelope, err := range got {
		if err != nil {
			t.Fatal(err)
		}

		if len(want) == 0 {
			t.Fatalf("unexpected event on stream: %s", dapper.Format(eventEnvelope))
		}

		var wantEvent dogma.Event
		wantEvent, want = want[0], want[1:]

		gotEvent, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(wantEvent, gotEvent) {
			t.Fatalf(
				"unexpected event at offset %d:\n\tgot:  %s\n\twant: %s",
				eventstream.OffsetOf(eventEnvelope),
				dapper.Format(gotEvent),
				dapper.Format(wantEvent),
			)
		}
	}

	if len(want) != 0 {
		t.Fatalf("expected %d more events on stream", len(want))
	}
}
