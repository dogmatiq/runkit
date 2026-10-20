package eventstream_test

import (
	"testing"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestReader(t *testing.T) {
	t.Run("it reads historical events", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		streamID, want := xtesting.AppendToNewEventStream(
			t,
			db,
			EventE1,
			EventE2,
			EventE3,
		)

		for idx, env := range want {
			envelopepb.SetExtension(
				env.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(streamID).
					WithOffset(uint64(idx)).
					Build(),
			)
		}

		reader := &Reader{DB: db}
		xtesting.Go(t, reader.Run)

		envelopes := make(chan *envelopepb.Envelope)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
				MessageTypeUUID[*EventStub[TypeF]](),
			),
			envelopes,
		)
		defer unsubscribe()

		xtesting.ExpectToReceiveN(
			t,
			"historical events",
			envelopes,
			want...,
		)
	})
}
