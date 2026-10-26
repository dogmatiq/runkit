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
	t.Run("it delivers historical then contemporary events in order", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		historical := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)
		defer unsubscribe()

		xtesting.ExpectToReceiveN(
			t,
			"historical events",
			envelopes,
			historical...,
		)

		contemporary := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		xtesting.ExpectToReceiveN(
			t,
			"contemporary events",
			envelopes,
			contemporary...,
		)
	})

	t.Run("it delivers to multiple consumers on the same stream", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		types := uuidpb.NewSet(
			MessageTypeUUID[*EventStub[TypeE]](),
		)

		envelopes1 := make(chan *envelopepb.Envelope, 10)
		unsubscribe1 := reader.Subscribe(streamID, 0, types, envelopes1)
		defer unsubscribe1()

		envelopes2 := make(chan *envelopepb.Envelope, 10)
		unsubscribe2 := reader.Subscribe(streamID, 0, types, envelopes2)
		defer unsubscribe2()

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		xtesting.ExpectToReceiveN(t, "events for first consumer", envelopes1, want...)
		xtesting.ExpectToReceiveN(t, "events for second consumer", envelopes2, want...)
	})

	t.Run("it delivers to consumers on different streams independently", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID1 := xtesting.CreateEventStream(t, db)
		streamID2 := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		types := uuidpb.NewSet(
			MessageTypeUUID[*EventStub[TypeE]](),
		)

		envelopes1 := make(chan *envelopepb.Envelope, 10)
		unsubscribe1 := reader.Subscribe(streamID1, 0, types, envelopes1)
		defer unsubscribe1()

		envelopes2 := make(chan *envelopepb.Envelope, 10)
		unsubscribe2 := reader.Subscribe(streamID2, 0, types, envelopes2)
		defer unsubscribe2()

		want1 := xtesting.AppendToEventStream(t, db, streamID1, EventE1, EventE2)
		want2 := xtesting.AppendToEventStream(t, db, streamID2, EventE3)

		xtesting.ExpectToReceiveN(t, "events on first stream", envelopes1, want1...)
		xtesting.ExpectToReceiveN(t, "events on second stream", envelopes2, want2...)

		expectNoEnvelopes(t, envelopes1)
		expectNoEnvelopes(t, envelopes2)
	})

	t.Run("it filters by message type across the historical and contemporary phases", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		historical := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventX1,
		)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)
		defer unsubscribe()

		xtesting.ExpectToReceiveN(
			t,
			"historical events",
			envelopes,
			historical[0],
		)

		contemporary := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventX2,
			EventE2,
		)

		xtesting.ExpectToReceiveN(
			t,
			"contemporary events",
			envelopes,
			contemporary[1],
		)

		expectNoEnvelopes(t, envelopes)
	})

	t.Run("it stops delivering after unsubscribing while reading historical events", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		// The channel is unbuffered so that the consumer remains in the
		// historical phase, blocked on delivery, until the test receives.
		envelopes := make(chan *envelopepb.Envelope)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)

		xtesting.ExpectToReceiveN(
			t,
			"first event",
			envelopes,
			want[0],
		)

		unsubscribe()

		expectNoEnvelopes(t, envelopes)
	})

	t.Run("it stops delivering after unsubscribing while reading contemporary events", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			envelopes,
		)

		want := xtesting.AppendToEventStream(t, db, streamID, EventE1)

		xtesting.ExpectToReceiveN(
			t,
			"event",
			envelopes,
			want...,
		)

		unsubscribe()

		xtesting.AppendToEventStream(t, db, streamID, EventE2)

		expectNoEnvelopes(t, envelopes)
	})

	t.Run("it returns a slow consumer to historical reading and catches it up", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		// A tiny cache and a consumer channel that holds a single event
		// guarantee that the consumer falls behind and is bounced between the
		// contemporary and historical scanners.
		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 2,
			HistoricalBatchSize:   2,
		}

		xtesting.Go(t, reader.Run)

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

		var want []*envelopepb.Envelope
		for range 10 {
			want = append(
				want,
				xtesting.AppendToEventStream(t, db, streamID, EventE1)...,
			)
		}

		// Every event must arrive, in order, with no duplicates, regardless of
		// how many times the consumer changes phase along the way.
		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		expectNoEnvelopes(t, envelopes)
	})

	t.Run("it serves many slow consumers on one stream concurrently", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 2,
			HistoricalBatchSize:   2,
		}

		xtesting.Go(t, reader.Run)

		types := uuidpb.NewSet(
			MessageTypeUUID[*EventStub[TypeE]](),
		)

		// Many consumers that each hold a single event cause the contemporary
		// scanner to desynchronize consumers at the same time as the Reader is
		// handing other consumers to it. This would deadlock if the Reader did
		// not keep accepting desynchronized consumers while doing so.
		const consumers = 5
		channels := make([]chan *envelopepb.Envelope, consumers)

		for i := range channels {
			channels[i] = make(chan *envelopepb.Envelope, 1)
			unsubscribe := reader.Subscribe(streamID, 0, types, channels[i])
			defer unsubscribe()
		}

		var want []*envelopepb.Envelope
		for range 20 {
			want = append(
				want,
				xtesting.AppendToEventStream(t, db, streamID, EventE1)...,
			)
		}

		for i, ch := range channels {
			xtesting.ExpectToReceiveN(
				t,
				"events for consumer "+string(rune('0'+i)),
				ch,
				want...,
			)
		}
	})

	t.Run("it continues serving other consumers when one unsubscribes", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		types := uuidpb.NewSet(
			MessageTypeUUID[*EventStub[TypeE]](),
		)

		envelopes1 := make(chan *envelopepb.Envelope, 10)
		unsubscribe1 := reader.Subscribe(streamID, 0, types, envelopes1)

		envelopes2 := make(chan *envelopepb.Envelope, 10)
		unsubscribe2 := reader.Subscribe(streamID, 0, types, envelopes2)
		defer unsubscribe2()

		first := xtesting.AppendToEventStream(t, db, streamID, EventE1)

		xtesting.ExpectToReceiveN(t, "first event for first consumer", envelopes1, first...)
		xtesting.ExpectToReceiveN(t, "first event for second consumer", envelopes2, first...)

		unsubscribe1()

		second := xtesting.AppendToEventStream(t, db, streamID, EventE2)

		xtesting.ExpectToReceiveN(t, "second event for second consumer", envelopes2, second...)
		expectNoEnvelopes(t, envelopes1)
	})

	t.Run("it stops cleanly with consumers in both phases", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 10,
			HistoricalBatchSize:   10,
		}

		stop := xtesting.Go(t, reader.Run)

		types := uuidpb.NewSet(
			MessageTypeUUID[*EventStub[TypeE]](),
		)

		// This consumer catches up immediately and moves to the contemporary
		// phase.
		contemporary := make(chan *envelopepb.Envelope, 10)
		reader.Subscribe(streamID, 0, types, contemporary)

		xtesting.ExpectToReceiveN(t, "events", contemporary, want...)

		// This consumer never receives, so it remains blocked in the
		// historical phase.
		historical := make(chan *envelopepb.Envelope)
		reader.Subscribe(streamID, 0, types, historical)

		// Stopping must return promptly; stop() blocks until Run has returned.
		stop()

		// Subscribing after the reader has stopped must not block.
		unsubscribe := reader.Subscribe(streamID, 0, types, make(chan *envelopepb.Envelope))
		unsubscribe()
	})
}
