package projection_test

import (
	"context"
	"sync"

	"github.com/dogmatiq/dogma"
)

// checkpointStore is a test helper that stores a projection's checkpoint
// offsets in memory, so that a stub projection message handler can implement
// the optimistic concurrency control that Dogma requires of projections.
//
// Tests that run several engines use it so that a duplicate delivery, which
// can occur while the engines join or leave the cluster, is rejected as a real
// projection would reject it, rather than being reported as a test failure.
//
// The zero value is ready to use.
type checkpointStore struct {
	m       sync.Mutex
	offsets map[string]uint64
}

// HandleEvent handles an event as a projection that implements optimistic
// concurrency control does. Its return values are suitable for returning from
// [dogma.ProjectionMessageHandler].HandleEvent.
//
// If s.CheckpointOffset() matches the stored checkpoint offset for the stream,
// it calls fn, then stores the offset after the event unless fn returns an
// error. Otherwise, it returns the stored offset without calling fn.
//
// fn may be nil.
func (c *checkpointStore) HandleEvent(
	s dogma.ProjectionEventScope,
	fn func() error,
) (uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	stored := c.offsets[s.StreamID()]

	if s.CheckpointOffset() != stored {
		return stored, nil
	}

	if fn != nil {
		if err := fn(); err != nil {
			return 0, err
		}
	}

	if c.offsets == nil {
		c.offsets = map[string]uint64{}
	}

	next := s.Offset() + 1
	c.offsets[s.StreamID()] = next

	return next, nil
}

// CheckpointOffset returns the stored checkpoint offset for the stream with the
// given ID. It is suitable for use as
// [stubs.ProjectionMessageHandlerStub].CheckpointOffsetFunc.
func (c *checkpointStore) CheckpointOffset(_ context.Context, id string) (uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.offsets[id], nil
}
