package xtesting

import (
	"context"
	"sync"
	"time"
)

// ContextWithManualDeadline returns a context that has a deadline, and a
// function that makes the context behave as though that deadline had just
// passed. The deadline is never reached otherwise.
//
// It allows a test to verify the behavior of an operation when its deadline
// passes, without depending on the wall clock.
func ContextWithManualDeadline(parent context.Context) (context.Context, func()) {
	ctx := &manualDeadlineContext{
		Context:  parent,
		deadline: time.Now().Add(24 * time.Hour),
		done:     make(chan struct{}),
	}

	expire := func() {
		ctx.once.Do(func() {
			close(ctx.done)
		})
	}

	// Cancellation of the parent must still propagate.
	context.AfterFunc(parent, expire)

	return ctx, expire
}

// manualDeadlineContext is a context with a deadline that is reached on demand.
type manualDeadlineContext struct {
	context.Context

	deadline time.Time
	done     chan struct{}
	once     sync.Once
}

func (c *manualDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func (c *manualDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *manualDeadlineContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}

	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
