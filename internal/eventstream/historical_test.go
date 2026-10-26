package eventstream_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestHistoricalReader(t *testing.T) {
	t.Run("it delivers all matching events and reports the consumer as synced at the stream head", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		expectSynced(t, synced, consumer)

		if got, want := consumer.Checkpoint.Load(), uint64(3); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it reads in multiple batches when events exceed the batch size", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
			EventE1,
			EventE2,
		)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 2,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		expectSynced(t, synced, consumer)

		if got, want := consumer.Checkpoint.Load(), uint64(5); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it begins at the consumer's checkpoint", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(1)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want[1:]...,
		)

		expectSynced(t, synced, consumer)
	})

	t.Run("it skips events of irrelevant types and still advances the checkpoint to the end of the stream", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		// End with an irrelevant event so that the checkpoint must skip past
		// it to reach the end of the stream.
		all := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventX1,
			EventE2,
			EventX2,
		)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			all[0],
			all[2],
		)

		expectSynced(t, synced, consumer)

		if got, want := consumer.Checkpoint.Load(), uint64(4); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it reports the consumer as synced without delivering anything when only irrelevant events exist", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventX1,
			EventX2,
		)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		expectSynced(t, synced, consumer)
		expectNoEnvelopes(t, envelopes)

		if got, want := consumer.Checkpoint.Load(), uint64(2); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it reports the consumer as synced immediately on an empty stream", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		expectSynced(t, synced, consumer)
		expectNoEnvelopes(t, envelopes)

		if got, want := consumer.Checkpoint.Load(), uint64(0); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it does not move the checkpoint backwards", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		// The consumer is ahead of the stream's next offset, which can happen
		// if it saw events via another route before being handed to this
		// reader.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(5)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		expectSynced(t, synced, consumer)
		expectNoEnvelopes(t, envelopes)

		if got, want := consumer.Checkpoint.Load(), uint64(5); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it blocks until the consumer receives", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		// The channel is unbuffered, so the reader can only make progress as
		// fast as the test receives.
		envelopes := make(chan *envelopepb.Envelope)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		xtesting.GoE(t, reader.Run)

		expectNoSynced(t, synced)

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		expectSynced(t, synced, consumer)
	})

	t.Run("it stops without reporting synced when the consumer is gone", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		// Nothing ever receives from the channel, so the reader would block
		// forever were it not for the consumer being gone.
		envelopes := make(chan *envelopepb.Envelope)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		consumer.Gone.Set()

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		if err := reader.Run(t.Context()); err != nil {
			t.Fatal(err)
		}

		expectNoSynced(t, synced)

		if got, want := consumer.Checkpoint.Load(), uint64(0); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it resumes from the consumer's checkpoint after a restart", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)
		synced := make(chan *Consumer, 1)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		envelopes := make(chan *envelopepb.Envelope)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		stop := xtesting.GoE(t, reader.Run)

		// Receive the first event, then stop the reader while it is blocked
		// delivering the second.
		xtesting.ExpectToReceiveN(
			t,
			"first event",
			envelopes,
			want[0],
		)

		stop()

		if got, want := consumer.Checkpoint.Load(), uint64(1); got != want {
			t.Fatalf("unexpected checkpoint after stop: got %d, want %d", got, want)
		}

		// Running the same reader again must deliver only the remaining
		// events, without duplicating the first.
		xtesting.GoE(t, reader.Run)

		xtesting.ExpectToReceiveN(
			t,
			"remaining events",
			envelopes,
			want[1:]...,
		)

		expectSynced(t, synced, consumer)

		if got, want := consumer.Checkpoint.Load(), uint64(3); got != want {
			t.Fatalf("unexpected checkpoint: got %d, want %d", got, want)
		}
	})

	t.Run("it returns an error when the stream does not exist", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		synced := make(chan *Consumer, 1)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: uuidpb.Generate(),
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    synced,
			BatchSize: 10,
		}

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err := reader.Run(ctx)

		if ctx.Err() != nil {
			t.Fatal("timeout waiting for reader to return")
		}

		if err == nil {
			t.Fatal("expected an error, got nil")
		}

		expectNoSynced(t, synced)
	})

	t.Run("it panics when the consumer's message types are empty", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		streamID := xtesting.CreateEventStream(t, db)

		consumer := &Consumer{
			StreamID:     streamID,
			MessageTypes: uuidpb.NewSet(),
			Envelopes:    make(chan *envelopepb.Envelope, 10),
		}
		consumer.Checkpoint.Store(0)

		reader := &HistoricalReader{
			DB:        db,
			Consumer:  consumer,
			Synced:    make(chan *Consumer, 1),
			BatchSize: 10,
		}

		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		reader.Run(t.Context())
	})
}

// expectSynced asserts that consumer is the next consumer to be reported as
// synced.
func expectSynced(
	t testing.TB,
	synced <-chan *Consumer,
	consumer *Consumer,
) {
	t.Helper()

	select {
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting for consumer to be synced")
	case got := <-synced:
		if got != consumer {
			t.Fatal("unexpected consumer was synced")
		}
	}
}

// expectNoSynced asserts that no consumer is reported as synced within a short
// period.
func expectNoSynced(
	t testing.TB,
	synced <-chan *Consumer,
) {
	t.Helper()

	select {
	case <-synced:
		t.Fatal("unexpected consumer sync")
	case <-time.After(100 * time.Millisecond):
	}
}
