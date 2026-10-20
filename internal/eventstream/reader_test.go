package eventstream_test

import (
	"testing"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestReader(t *testing.T) {
	t.Run("it reads historical events", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		notifications := &notification.Listener{
			DB:     db,
			Logger: logger,
		}
		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:            db,
			Notifications: notifications,
			Logger:        logger,
		}
		xtesting.Go(t, reader.Run)

		streamID, want := xtesting.AppendToNewEventStream(
			t,
			db,
			EventE1,
			EventE2,
			EventE3,
		)

		envelopes := make(chan *envelopepb.Envelope)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)
		defer unsubscribe()

		// We expect events obtained from the stream to have the
		// [envelopepb.EventStreamPosition] extension.
		for idx, env := range want {
			envelopepb.SetExtension(
				env.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(streamID).
					WithOffset(uint64(idx)).
					Build(),
			)
		}

		xtesting.ExpectToReceiveN(
			t,
			"historical events",
			envelopes,
			want...,
		)
	})

	t.Run("it reads contemporary events", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		notifications := &notification.Listener{
			DB:     db,
			Logger: logger,
		}
		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:            db,
			Notifications: notifications,
			Logger:        logger,
		}
		xtesting.Go(t, reader.Run)

		// Note that the channel has a large enough buffer to receive all
		// contemporary events without lagging and returning to historical
		// consumption.
		envelopes := make(chan *envelopepb.Envelope, 3)
		streamID := xtesting.CreateEventStream(t, db)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)
		defer unsubscribe()

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		// We expect events obtained from the stream to have the
		// [envelopepb.EventStreamPosition] extension.
		for idx, env := range want {
			envelopepb.SetExtension(
				env.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(streamID).
					WithOffset(uint64(idx)).
					Build(),
			)
		}

		xtesting.ExpectToReceiveN(
			t,
			"contemporary events",
			envelopes,
			want...,
		)
	})

	t.Run("it reads historical events, then contemporary events", func(t *testing.T) {
		t.Skip()

		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		notifications := &notification.Listener{
			DB:     db,
			Logger: logger,
		}
		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:            db,
			Notifications: notifications,
			Logger:        logger,
		}
		xtesting.Go(t, reader.Run)

		streamID, wantHistorical := xtesting.AppendToNewEventStream(
			t,
			db,
			EventE1,
			EventE2,
		)

		// Note that the channel has a buffer of 1 to receive the final
		// contemporary event without lagging and returning to historical
		// consumption.
		envelopes := make(chan *envelopepb.Envelope, 1)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)
		defer unsubscribe()

		// We expect events obtained from the stream to have the
		// [envelopepb.EventStreamPosition] extension.
		for idx, env := range wantHistorical {
			envelopepb.SetExtension(
				env.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(streamID).
					WithOffset(uint64(idx)).
					Build(),
			)
		}

		xtesting.ExpectToReceiveN(
			t,
			"historical events",
			envelopes,
			wantHistorical...,
		)

		wantContemporary := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE3,
		)

		// We expect events obtained from the stream to have the
		// [envelopepb.EventStreamPosition] extension.
		for idx, env := range wantContemporary {
			envelopepb.SetExtension(
				env.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(streamID).
					WithOffset(uint64(len(wantHistorical)+idx)).
					Build(),
			)
		}

		xtesting.ExpectToReceiveN(
			t,
			"contemporary events",
			envelopes,
			wantContemporary...,
		)
	})
}
