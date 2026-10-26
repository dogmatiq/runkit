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
	t.Run("it delivers events appended after a consumer joins at the head", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)
	})

	t.Run("it serves a consumer from the cache", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

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
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		// The consumer may be registered before or after the scanner has
		// loaded its cache. Either way it must be served from the cache
		// without any further events being appended.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(1)
		scanner.Subscribe <- consumer

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want[1:]...,
		)
	})

	t.Run("it caches the last events on the stream when started", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
			EventE1,
		)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     2,
		}

		xtesting.GoE(t, scanner.Run)

		// The cache holds the last two events, so a consumer at the start of
		// that window is served from the cache.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(2)
		scanner.Subscribe <- consumer

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want[2:]...,
		)
	})

	t.Run("it desyncs a consumer that is behind the cache", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
			EventE1,
		)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     2,
		}

		xtesting.GoE(t, scanner.Run)

		// The cache holds offsets 2 and 3, so a consumer at offset 0 can not
		// be served and must be sent back to historical reading. This must
		// hold whether the consumer is registered before or after the
		// scanner's first batch arrives.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		expectDesynced(t, desynced, consumer)
		xtesting.ExpectNoReceive(t, "envelope", envelopes)
	})

	t.Run("it ignores events of irrelevant message types", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		all := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventX1,
			EventE1,
			EventX2,
		)

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			all[1],
		)

		// Appending another relevant event confirms the consumer's checkpoint
		// was advanced past the irrelevant events, otherwise it would be
		// re-delivered the earlier event or desynced.
		more := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE2,
		)

		xtesting.ExpectToReceiveN(
			t,
			"subsequent events",
			envelopes,
			more...,
		)

		xtesting.ExpectNoReceive(t, "consumer desync", desynced)
	})

	t.Run("it desyncs a consumer exactly once when its channel is full", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 10)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		// The consumer's channel can hold a single event, so the second
		// event can not be delivered and the consumer must be desynced.
		envelopes := make(chan *envelopepb.Envelope, 1)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		expectDesynced(t, desynced, consumer)

		// Appending further events must not cause the desynced consumer to be
		// desynced again, nor to receive anything more.
		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
		)

		xtesting.ExpectNoReceive(t, "consumer desync", desynced)

		xtesting.ExpectToReceiveN(
			t,
			"the single delivered event",
			envelopes,
			want[0],
		)

		xtesting.ExpectNoReceive(t, "envelope", envelopes)
	})

	t.Run("it delivers the correct events after the cache wraps", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

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
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     3,
		}

		xtesting.GoE(t, scanner.Run)

		// A probe consumer at the head lets the test know when the scanner
		// has processed the event that causes the cache to wrap.
		probeEnvelopes := make(chan *envelopepb.Envelope, 10)
		probe := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: probeEnvelopes,
		}
		probe.Checkpoint.Store(3)
		scanner.Subscribe <- probe

		second := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
		)

		xtesting.ExpectToReceiveN(
			t,
			"probe events",
			probeEnvelopes,
			second...,
		)

		// The cache now holds offsets 1, 2 and 3, having discarded offset 0.
		// A consumer at offset 2 must receive exactly offsets 2 and 3.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(2)
		scanner.Subscribe <- consumer

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			first[2],
			second[0],
		)

		xtesting.ExpectNoReceive(t, "consumer desync", desynced)
	})

	t.Run("it stops delivering to a consumer that is gone", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
		)

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		consumer.Gone.Set()
		scanner.Unsubscribe <- consumer

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE2,
		)

		xtesting.ExpectNoReceive(t, "envelope", envelopes)
		xtesting.ExpectNoReceive(t, "consumer desync", desynced)
	})

	t.Run("it resumes serving existing consumers after a restart", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		stop := xtesting.GoE(t, scanner.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		stop()

		// Events appended while the scanner is stopped must be delivered to the
		// consumer once the same scanner is run again.
		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		xtesting.GoE(t, scanner.Run)

		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want...,
		)

		xtesting.ExpectNoReceive(t, "consumer desync", desynced)
	})

	t.Run("it desyncs existing consumers that fall behind the cache during a restart", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     2,
		}

		stop := xtesting.GoE(t, scanner.Run)

		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(0)
		scanner.Subscribe <- consumer

		stop()

		// More events are appended than the cache can hold, so the consumer
		// can not be caught up from the cache when the scanner is run again.
		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		xtesting.GoE(t, scanner.Run)

		expectDesynced(t, desynced, consumer)
		xtesting.ExpectNoReceive(t, "envelope", envelopes)
	})

	t.Run("it waits for a consumer that is ahead of the cache", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)
		streamID := xtesting.CreateEventStream(t, db)
		notifications := &notification.Listener{DB: db, Logger: logger}
		desynced := make(chan *Consumer, 1)

		xtesting.Go(t, notifications.Run)

		xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
		)

		scanner := &Contemporary{
			DB:            db,
			Notifications: notifications,
			StreamID:      streamID,
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      desynced,
			CacheSize:     10,
		}

		xtesting.GoE(t, scanner.Run)

		// The consumer has already seen events beyond the end of the cache,
		// which holds offsets 0 and 1. It must not be desynced, and must not
		// receive anything until the stream reaches its checkpoint.
		envelopes := make(chan *envelopepb.Envelope, 10)
		consumer := &Consumer{
			StreamID: streamID,
			MessageTypes: uuidpb.NewSet(
				MessageTypeUUID[*EventStub[TypeE]](),
			),
			Envelopes: envelopes,
		}
		consumer.Checkpoint.Store(4)
		scanner.Subscribe <- consumer

		want := xtesting.AppendToEventStream(
			t,
			db,
			streamID,
			EventE1,
			EventE2,
			EventE3,
		)

		// Only the event at offset 4 is at or after the consumer's checkpoint.
		xtesting.ExpectToReceiveN(
			t,
			"events",
			envelopes,
			want[2],
		)

		xtesting.ExpectNoReceive(t, "consumer desync", desynced)
		xtesting.ExpectNoReceive(t, "envelope", envelopes)
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
			Subscribe:     make(chan *Consumer),
			Unsubscribe:   make(chan *Consumer),
			Desynced:      make(chan *Consumer, 1),
			CacheSize:     10,
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

// expectDesynced asserts that consumer is the next consumer to be reported as
// desynced.
func expectDesynced(
	t testing.TB,
	desynced <-chan *Consumer,
	consumer *Consumer,
) {
	t.Helper()

	select {
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting for consumer to be desynced")
	case got := <-desynced:
		if got != consumer {
			t.Fatal("unexpected consumer was desynced")
		}
	}
}
