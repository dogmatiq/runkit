package eventstream_test

import (
	"testing"
	"time"

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

		// A cache size of one means the consumer, which starts at the beginning
		// of the stream, is behind the cache and so reads historical events
		// before catching up.
		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 1,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		batches := make(chan Batch, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			batches,
		)
		defer unsubscribe()

		expectToReceiveEnvelopes(t, batches, historical...)

		contemporary := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		checkpoint := expectToReceiveEnvelopes(t, batches, contemporary...)

		if checkpoint != 5 {
			t.Fatalf("unexpected checkpoint: got %d, want 5", checkpoint)
		}
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

		batches1 := make(chan Batch, 10)
		unsubscribe1 := reader.Subscribe(streamID, 0, types, batches1)
		defer unsubscribe1()

		batches2 := make(chan Batch, 10)
		unsubscribe2 := reader.Subscribe(streamID, 0, types, batches2)
		defer unsubscribe2()

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		expectToReceiveEnvelopes(t, batches1, want...)
		expectToReceiveEnvelopes(t, batches2, want...)
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

		batches1 := make(chan Batch, 10)
		unsubscribe1 := reader.Subscribe(streamID1, 0, types, batches1)
		defer unsubscribe1()

		batches2 := make(chan Batch, 10)
		unsubscribe2 := reader.Subscribe(streamID2, 0, types, batches2)
		defer unsubscribe2()

		want1 := xtesting.AppendToEventStream(t, db, streamID1, EventE1, EventE2)
		want2 := xtesting.AppendToEventStream(t, db, streamID2, EventE3)

		expectToReceiveEnvelopes(t, batches1, want1...)
		expectToReceiveEnvelopes(t, batches2, want2...)

		expectNoBatches(t, batches1)
		expectNoBatches(t, batches2)
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

		// A cache size of one means the consumer, which starts at the beginning
		// of the stream, is behind the cache and so reads historical events
		// before catching up.
		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 1,
			HistoricalBatchSize:   10,
		}

		xtesting.Go(t, reader.Run)

		batches := make(chan Batch, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			batches,
		)
		defer unsubscribe()

		checkpoint := expectToReceiveEnvelopes(t, batches, historical[0])

		// The checkpoint reported with the historical events must already be
		// past the irrelevant event at offset 1.
		if checkpoint != 2 {
			t.Fatalf("unexpected checkpoint: got %d, want 2", checkpoint)
		}

		contemporary := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventX2,
			EventE2,
		)

		checkpoint = expectToReceiveEnvelopes(t, batches, contemporary[1])

		if checkpoint != 4 {
			t.Fatalf("unexpected checkpoint: got %d, want 4", checkpoint)
		}
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

		// A cache size of one means the consumer, which starts at the beginning
		// of the stream, is behind the cache and so reads historical events. A
		// historical batch size of one means each event is delivered in its
		// own batch.
		reader := &Reader{
			DB:                    db,
			Notifications:         notifications,
			Logger:                logger,
			ContemporaryCacheSize: 1,
			HistoricalBatchSize:   1,
		}

		xtesting.Go(t, reader.Run)

		// The channel is unbuffered so that the consumer remains in the
		// historical phase, blocked on delivery, until the test receives.
		batches := make(chan Batch)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			batches,
		)

		expectToReceiveEnvelopes(t, batches, want[0])

		unsubscribe()

		expectNoBatches(t, batches)
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

		batches := make(chan Batch, 10)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			batches,
		)

		want := xtesting.AppendToEventStream(t, db, streamID, EventE1)

		expectToReceiveEnvelopes(t, batches, want...)

		unsubscribe()

		xtesting.AppendToEventStream(t, db, streamID, EventE2)

		expectNoBatches(t, batches)
	})

	t.Run("it returns a slow consumer to historical reading and catches it up", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		// A tiny cache and a consumer channel that holds a single batch
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

		batches := make(chan Batch, 1)
		unsubscribe := reader.Subscribe(
			streamID,
			0,
			uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			batches,
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
		checkpoint := expectToReceiveEnvelopes(t, batches, want...)

		if checkpoint != 10 {
			t.Fatalf("unexpected checkpoint: got %d, want 10", checkpoint)
		}

		expectNoBatches(t, batches)
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

		// Many consumers that each hold a single batch cause the contemporary
		// scanner to desynchronize consumers at the same time as the Reader is
		// handing other consumers to it. This would deadlock if the Reader did
		// not keep accepting desynchronized consumers while doing so.
		const consumers = 5
		channels := make([]chan Batch, consumers)

		for i := range channels {
			channels[i] = make(chan Batch, 1)
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

		for _, ch := range channels {
			expectToReceiveEnvelopes(t, ch, want...)
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

		batches1 := make(chan Batch, 10)
		unsubscribe1 := reader.Subscribe(streamID, 0, types, batches1)

		batches2 := make(chan Batch, 10)
		unsubscribe2 := reader.Subscribe(streamID, 0, types, batches2)
		defer unsubscribe2()

		first := xtesting.AppendToEventStream(t, db, streamID, EventE1)

		expectToReceiveEnvelopes(t, batches1, first...)
		expectToReceiveEnvelopes(t, batches2, first...)

		unsubscribe1()

		second := xtesting.AppendToEventStream(t, db, streamID, EventE2)

		expectToReceiveEnvelopes(t, batches2, second...)
		expectNoBatches(t, batches1)
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

		// This consumer is served from the cache.
		contemporary := make(chan Batch, 10)
		reader.Subscribe(streamID, 0, types, contemporary)

		expectToReceiveEnvelopes(t, contemporary, want...)

		// This consumer never receives, so it is returned to reading
		// historical events, where it remains blocked on delivery.
		historical := make(chan Batch)
		reader.Subscribe(streamID, 0, types, historical)

		// Stopping must return promptly; stop() blocks until Run has returned.
		stop()

		// Subscribing after the reader has stopped must not block.
		unsubscribe := reader.Subscribe(streamID, 0, types, make(chan Batch))
		unsubscribe()
	})
}

// expectToReceiveEnvelopes asserts that the given envelopes are received, in
// order, across one or more batches. It returns the checkpoint of the last
// batch received.
//
// It fails if any batch's checkpoint is lower than that of the batch before it.
func expectToReceiveEnvelopes(
	t testing.TB,
	batches <-chan Batch,
	want ...*envelopepb.Envelope,
) uint64 {
	t.Helper()

	var (
		got        []*envelopepb.Envelope
		checkpoint uint64
	)

	for len(got) < len(want) {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for envelopes: got %d of %d", len(got), len(want))
		case batch := <-batches:
			if batch.Checkpoint < checkpoint {
				t.Fatalf("checkpoint moved backwards: got %d, previously %d", batch.Checkpoint, checkpoint)
			}

			checkpoint = batch.Checkpoint
			got = append(got, batch.Envelopes...)
		}
	}

	xtesting.Expect(t, "envelopes", got, want)

	return checkpoint
}

// expectNoBatches asserts that no batch is delivered within a short period.
func expectNoBatches(
	t testing.TB,
	batches <-chan Batch,
) {
	t.Helper()

	select {
	case batch := <-batches:
		t.Fatalf("unexpected batch with checkpoint %d and %d envelopes", batch.Checkpoint, len(batch.Envelopes))
	case <-time.After(100 * time.Millisecond):
	}
}
