package scanner_test

import (
	"context"
	"testing"
	"time"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestContemporary(t *testing.T) {
	t.Run("it reads the most recent events when started", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

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

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     2,
		}

		xtesting.GoE(t, scanner.Run)

		// Reading begins two events before the end of the stream.
		expectToReceiveBatches(t, batches, streamID, 3, want[3:]...)
		expectNoBatches(t, batches)
	})

	t.Run("it reads from the beginning when the stream is shorter than the batch size", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		expectToReceiveBatches(t, batches, streamID, 0, want...)
		expectNoBatches(t, batches)
	})

	t.Run("it reads events appended after it starts", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		// The stream is empty when the scanner starts, so the empty batch it
		// sends confirms that it is running before the events under test are
		// appended; otherwise it might begin reading relative to an end of the
		// stream that already includes them.
		expectToReceiveEmptyBatch(t, batches, streamID, 0)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		expectToReceiveBatches(t, batches, streamID, 0, want...)
	})

	t.Run("it reads in multiple batches when events exceed the batch size", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     2,
		}

		xtesting.GoE(t, scanner.Run)

		// See "it reads events appended after it starts" for why the stream
		// is empty when the scanner starts.
		expectToReceiveEmptyBatch(t, batches, streamID, 0)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
			EventE1,
		)

		// The events arrive across several messages, but their offsets must
		// be contiguous.
		expectToReceiveBatches(t, batches, streamID, 0, want...)
	})

	t.Run("it begins reading relative to the end of the stream after a restart", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

		first := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     2,
		}

		stop := xtesting.GoE(t, scanner.Run)

		expectToReceiveBatches(t, batches, streamID, 1, first[1:]...)

		stop()

		second := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		// The scanner holds no state, so it begins two events before the new
		// end of the stream rather than where it left off.
		xtesting.GoE(t, scanner.Run)

		expectToReceiveBatches(t, batches, streamID, 3, second...)
	})

	t.Run("it sends an empty batch when the stream is empty", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		batches := make(chan Batch, 10)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Batches:       batches,
			BatchSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		expectToReceiveEmptyBatch(t, batches, streamID, 0)
		expectNoBatches(t, batches)
	})

	t.Run("it returns an error when the stream does not exist", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		notifications := &notification.Listener{DB: db, Logger: logger}

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      uuidpb.Generate(),
			Batches:       make(chan Batch, 10),
			BatchSize:     10,
		}

		ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
		defer cancel()

		err := scanner.Run(ctx)

		if ctx.Err() != nil {
			t.Fatal("timeout waiting for scanner to return")
		}

		if err == nil {
			t.Fatal("expected an error, got nil")
		}
	})
}

// expectToReceiveEmptyBatch asserts that an empty batch with the given
// checkpoint is received, as sent by the scanner when it finds the stream
// empty.
func expectToReceiveEmptyBatch(
	t testing.TB,
	batches <-chan Batch,
	streamID *uuidpb.UUID,
	checkpoint uint64,
) {
	t.Helper()

	select {
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting for empty batch")
	case b := <-batches:
		if len(b.Envelopes) != 0 {
			t.Fatalf("unexpected non-empty batch: got %d envelopes", len(b.Envelopes))
		}

		if !b.StreamID.Equal(streamID) {
			t.Fatalf("unexpected stream ID: got %s, want %s", b.StreamID, streamID)
		}

		if b.Checkpoint != checkpoint {
			t.Fatalf("unexpected checkpoint: got %d, want %d", b.Checkpoint, checkpoint)
		}
	}
}

// expectToReceiveBatches asserts that the given envelopes are received, in
// order, across one or more batches, the first of which begins at the given
// offset. It fails if the offsets are not contiguous across batches, or if any
// batch is empty.
func expectToReceiveBatches(
	t testing.TB,
	batches <-chan Batch,
	streamID *uuidpb.UUID,
	offset uint64,
	want ...*envelopepb.Envelope,
) {
	t.Helper()

	var got []*envelopepb.Envelope

	for len(got) < len(want) {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for batches: got %d of %d envelopes", len(got), len(want))
		case b := <-batches:
			if len(b.Envelopes) == 0 {
				t.Fatal("unexpected empty batch")
			}

			if !b.StreamID.Equal(streamID) {
				t.Fatalf("unexpected stream ID: got %s, want %s", b.StreamID, streamID)
			}

			if got := b.Checkpoint - uint64(len(b.Envelopes)); got != offset {
				t.Fatalf("unexpected offset: got %d, want %d", got, offset)
			}

			offset = b.Checkpoint
			got = append(got, b.Envelopes...)
		}
	}

	xtesting.Expect(t, "envelopes", got, want)
}
