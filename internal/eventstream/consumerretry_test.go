package eventstream_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

// TestConsumer_partialSuccessReloadsDurableCheckpoint verifies that a failed
// segment is retried from the sink's stored checkpoint rather than the beginning
// of the segment or the offset returned with the error.
func TestConsumer_partialSuccessReloadsDurableCheckpoint(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{"handling error", errors.New("later event failed")},
		{"checkpoint conflict", nil},
	} {
		t.Run(failure.name, func(t *testing.T) {
			node := newConsumerNode(t)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			logger := spruce.NewTestLogger(t)
			catalog := &Catalog{
				CacheSize: 10,
				Logger:    logger,
			}
			src := &source.Memory{}
			id := uuidpb.Generate()
			src.Append(Segment{
				StreamID: id,
				Begin:    0,
				End:      10,
			})

			watchDone := make(chan error, 1)
			go func() {
				watchDone <- catalog.Watch(ctx, src, nil)
			}()
			defer func() {
				cancel()
				<-watchDone
			}()

			for {
				ids, changed := catalog.StreamIDs().Load()
				if ids.Has(id) {
					break
				}

				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}

			var (
				mu      sync.Mutex
				durable uint64
				loads   []uint64
				calls   int
			)
			done := make(chan struct{})
			sink := &sinkStub{
				CheckpointOffsetFunc: func(context.Context, *uuidpb.UUID) (uint64, error) {
					mu.Lock()
					defer mu.Unlock()

					loads = append(loads, durable)

					return durable, nil
				},
				ConsumeSegmentFunc: func(_ context.Context, cp uint64, seg Segment) (uint64, bool, error) {
					mu.Lock()
					defer mu.Unlock()

					calls++
					if calls == 1 {
						if cp != 0 || seg.Begin != 0 {
							t.Errorf("initial checkpoint/segment = %d/%d", cp, seg.Begin)
						}

						// Earlier event transactions committed before the later failure.
						durable = 4

						return 0, false, failure.err
					}

					if cp != 4 || seg.Begin != 4 {
						t.Errorf("resumed checkpoint/segment = %d/%d, want 4/4", cp, seg.Begin)
					}

					close(done)

					// Empty/filtered segments need not advance a sink's durable checkpoint.
					return cp, true, nil
				},
			}

			c := &Consumer{
				EventStreams: catalog,
				Sink:         sink,
				Logger:       logger,
			}

			c.Node = node

			stopped := make(chan error, 1)
			go func() {
				stopped <- c.Run(ctx)
			}()

			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}

			cancel()
			if err := <-stopped; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}

			mu.Lock()
			defer mu.Unlock()

			if len(loads) != 2 || loads[0] != 0 || loads[1] != 4 {
				t.Fatalf("checkpoint loads = %v, want [0 4]", loads)
			}
		})
	}
}

// newConsumerNode starts a single-node cluster for the consumer tests.
func newConsumerNode(t *testing.T) *cluster.Node {
	t.Helper()

	logger := spruce.NewTestLogger(t)
	signals := &signal.Bus{
		DB:     xtesting.NewDatabase(t),
		Logger: logger,
	}
	xtesting.GoE(t, signals.Run)
	node := &cluster.Node{
		ID:       uuidpb.Generate(),
		Signals:  signals,
		Interval: 20 * time.Millisecond,
		Timeout:  100 * time.Millisecond,
		Logger:   logger,
	}
	xtesting.GoE(t, node.Run)

	return node
}
