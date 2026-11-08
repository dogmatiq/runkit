package signal_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestBus(t *testing.T) {
	setup := func(t *testing.T) *Bus {
		bus := &Bus{
			DB:           xtesting.NewDatabase(t),
			EngineLogger: spruce.NewTestLogger(t),
		}

		xtesting.GoE(t, bus.Run)
		waitForBus(t, bus)

		return bus
	}

	t.Run("it delivers an empty signal as soon as watching begins", func(t *testing.T) {
		bus := setup(t)

		ch, stop := bus.Watch(t.Context(), "topic", 10)
		defer stop()

		xtesting.ExpectToReceiveN(t, "signals", ch, "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it delivers signals to watchers of the topic", func(t *testing.T) {
		bus := setup(t)

		a := watch(t, bus, "topic-a", 10)
		b := watch(t, bus, "topic-b", 10)

		send(t, bus, "topic-a")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectNoReceive(t, "signal on b", b)

		send(t, bus, "topic-b")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
		xtesting.ExpectNoReceive(t, "signal on a", a)
	})

	t.Run("it delivers signals to every watcher of the topic", func(t *testing.T) {
		bus := setup(t)

		a := watch(t, bus, "topic", 10)
		b := watch(t, bus, "topic", 10)

		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})

	t.Run("it delivers signals on topics beneath the watched topic", func(t *testing.T) {
		bus := setup(t)

		ch := watch(t, bus, "a.b", 10)

		send(t, bus, "a.b.c")
		send(t, bus, "a.bc")
		send(t, bus, "a")
		send(t, bus, "x.a.b")
		send(t, bus, "a.b")
		send(t, bus, "a.b.c.d")

		// The value received is the part of the topic beneath the watched
		// topic.
		xtesting.ExpectToReceiveN(t, "signals", ch, "c", "", "c.d")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it closes the channel when the context is canceled, and delivers nothing more", func(t *testing.T) {
		bus := setup(t)

		ctx, cancel := context.WithCancel(t.Context())
		ch, stop := bus.Watch(ctx, "topic", 10)
		defer stop()

		witness := watch(t, bus, "topic", 10)

		xtesting.ExpectToReceiveN(t, "signals", ch, "")

		cancel()
		expectClosed(t, ch)

		// The witness receiving the signal proves that it was dispatched after
		// the channel was closed, without a send on the closed channel.
		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "")
	})

	t.Run("it closes the channel when the watch is stopped, and delivers nothing more", func(t *testing.T) {
		bus := setup(t)

		ch, stop := bus.Watch(t.Context(), "topic", 10)
		witness := watch(t, bus, "topic", 10)

		xtesting.ExpectToReceiveN(t, "signals", ch, "")

		stop()
		expectClosed(t, ch)

		// The witness receiving the signal proves that it was dispatched after
		// the channel was closed, without a send on the closed channel.
		send(t, bus, "topic")
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "")
	})

	t.Run("it allows a watch to be stopped more than once", func(t *testing.T) {
		bus := setup(t)

		ch, stop := bus.Watch(t.Context(), "topic", 10)

		stop()
		stop()
		expectClosed(t, ch)
	})

	t.Run("it allows a watch to be stopped after the context is canceled", func(t *testing.T) {
		bus := setup(t)

		ctx, cancel := context.WithCancel(t.Context())
		ch, stop := bus.Watch(ctx, "topic", 10)

		cancel()
		expectClosed(t, ch)
		stop()
	})

	t.Run("it drops signals when the watcher's channel is full", func(t *testing.T) {
		bus := setup(t)

		ch := watch(t, bus, "topic", 1)
		witness := watch(t, bus, "topic", 10)

		send(t, bus, "topic")
		send(t, bus, "topic")
		send(t, bus, "topic")

		// The witness receiving every signal proves that the full channel did
		// not hold up dispatching.
		xtesting.ExpectToReceiveN(t, "signals on witness", witness, "", "", "")

		// Only the first signal fit in the watcher's channel.
		xtesting.ExpectToReceiveN(t, "signals", ch, "")
		xtesting.ExpectNoReceive(t, "signal", ch)
	})

	t.Run("it sends signals to every bus", func(t *testing.T) {
		bus := setup(t)
		other := &Bus{
			DB:           bus.DB,
			EngineLogger: bus.EngineLogger,
		}

		xtesting.GoE(t, other.Run)
		waitForBus(t, other)

		own := watch(t, bus, "topic", 10)
		received := watch(t, other, "topic", 10)

		send(t, bus, "topic.a")
		send(t, bus, "topic.b")

		xtesting.ExpectToReceiveN(t, "signals on the sending bus", own, "a", "b")
		xtesting.ExpectToReceiveN(t, "signals on the other bus", received, "a", "b")
	})

	t.Run("it signals every watcher when it starts listening", func(t *testing.T) {
		bus := &Bus{
			DB:           xtesting.NewDatabase(t),
			EngineLogger: spruce.NewTestLogger(t),
		}

		a := watch(t, bus, "a", 10)
		b := watch(t, bus, "a.b", 10)

		xtesting.GoE(t, bus.Run)

		// Nothing is sent; the signal comes from the bus itself, as though it
		// were on the watched topic, so that a watcher that read state before
		// the bus was listening can read it again.
		xtesting.ExpectToReceiveN(t, "signals on a", a, "")
		xtesting.ExpectToReceiveN(t, "signals on b", b, "")
	})

	t.Run("it panics if the capacity is not positive", func(t *testing.T) {
		bus := &Bus{}

		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		bus.Watch(t.Context(), "topic", 0)
	})
}

// watch watches the given topic until the test ends, and returns the channel
// after receiving the empty signal that is delivered as soon as watching
// begins.
func watch(t testing.TB, bus *Bus, topic string, capacity int) <-chan string {
	t.Helper()

	ch, stop := bus.Watch(t.Context(), topic, capacity)
	t.Cleanup(stop)

	xtesting.ExpectToReceiveN(t, "initial signal", ch, "")

	return ch
}

// expectClosed fails the test unless the given channel is closed, after
// receiving anything already in it.
func expectClosed(t testing.TB, ch <-chan string) {
	t.Helper()

	timeout := time.After(xtesting.WaitTimeout)

	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for the channel to be closed")
		case _, ok := <-ch:
			if !ok {
				return
			}
		}
	}
}

// send sends a signal on the given topic.
func send(t testing.TB, bus *Bus, topic string) {
	t.Helper()

	if err := bus.Send(t.Context(), topic); err != nil {
		t.Fatal(err)
	}
}

// waitForBus blocks until the bus is receiving signals.
//
// Signals sent before the bus has started listening are lost, so a probe
// signal is sent on a unique topic until it is received.
func waitForBus(t testing.TB, bus *Bus) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	topic := "probe." + uuidpb.Generate().AsString()
	probes, stop := bus.Watch(ctx, topic, 1)
	defer stop()

	<-probes

	deadline := time.After(xtesting.WaitTimeout)

	for {
		send(t, bus, topic)

		select {
		case <-deadline:
			t.Fatal("timeout waiting for bus to start listening")
		case <-probes:
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}
