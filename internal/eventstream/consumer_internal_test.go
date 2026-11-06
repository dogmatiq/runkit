package eventstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// TestConsumer_partialSuccessReloadsDurableCheckpoint verifies that a failed
// segment is retried from the sink's stored checkpoint rather than the beginning
// of the segment or the offset returned with the error.
func TestConsumer_partialSuccessReloadsDurableCheckpoint(t *testing.T) {
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
	sink := &recordingSink{
		checkpoint: func(context.Context, *uuidpb.UUID) (uint64, error) {
			mu.Lock()
			defer mu.Unlock()

			loads = append(loads, durable)

			return durable, nil
		},
		consume: func(_ context.Context, cp uint64, seg Segment) (uint64, error) {
			mu.Lock()
			defer mu.Unlock()

			calls++
			if calls == 1 {
				if cp != 0 || seg.Begin != 0 {
					t.Errorf("initial checkpoint/segment = %d/%d", cp, seg.Begin)
				}

				// Earlier event transactions committed before the later failure.
				durable = 4

				return 4, errors.New("later event failed")
			}

			if cp != 4 || seg.Begin != 4 {
				t.Errorf("resumed checkpoint/segment = %d/%d, want 4/4", cp, seg.Begin)
			}

			close(done)

			// Empty/filtered segments need not advance a sink's durable checkpoint.
			return cp, nil
		},
	}

	c := &Consumer{
		EventStreams: catalog,
		Sink:         sink,
		EventTypeIDs: uuidcollection.NewSet(uuidpb.Generate()),
		Logger:       logger,
	}

	c.group, ctx = errgroup.WithContext(ctx)
	c.startStreamConsumer(ctx, id)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	cancel()
	if err := c.group.Wait(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(loads) != 2 || loads[0] != 0 || loads[1] != 4 {
		t.Fatalf("checkpoint loads = %v, want [0 4]", loads)
	}
}

// TestConsumer_fatalSinkErrorPropagates verifies that a fatal sink error stops
// consumption instead of being retried.
func TestConsumer_fatalSinkErrorPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cause := errors.New("stop")
	fatal := supervisor.Fatal(cause)

	c := &Consumer{
		Logger: spruce.NewTestLogger(t),
		Sink: &recordingSink{
			checkpoint: func(context.Context, *uuidpb.UUID) (uint64, error) {
				return 0, fatal
			},
		},
	}

	c.group, ctx = errgroup.WithContext(ctx)
	c.startStreamConsumer(ctx, uuidpb.Generate())

	if err := c.group.Wait(); !errors.Is(err, cause) {
		t.Fatalf("got %v, want %v", err, fatal)
	}
}

// recordingSink implements [Sink] by calling the supplied functions.
type recordingSink struct {
	checkpoint func(context.Context, *uuidpb.UUID) (uint64, error)
	consume    func(context.Context, uint64, Segment) (uint64, error)
}

func (s *recordingSink) CheckpointOffset(
	ctx context.Context,
	id *uuidpb.UUID,
) (uint64, error) {
	return s.checkpoint(ctx, id)
}

func (s *recordingSink) ConsumeSegment(
	ctx context.Context,
	cp uint64,
	seg Segment,
) (uint64, error) {
	return s.consume(ctx, cp, seg)
}
