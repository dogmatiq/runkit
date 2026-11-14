package supervisor

import (
	"context"
	"errors"
	"log/slog"

	"github.com/dogmatiq/runkit/internal/failure"
	"golang.org/x/sync/errgroup"
)

// Group runs supervised functions in their own goroutines, and stops them all
// if any of them fails fatally.
type Group struct {
	ctx   context.Context
	group *errgroup.Group
}

// WithContext returns a new group, and a context derived from ctx that is
// canceled when a function started in the group fails fatally, or when
// [Group.Wait] returns.
func WithContext(ctx context.Context) (*Group, context.Context) {
	group, ctx := errgroup.WithContext(ctx)

	return &Group{
		ctx:   ctx,
		group: group,
	}, ctx
}

// Go calls [Run] in a new goroutine, with the group's context.
func (g *Group) Go(
	description string,
	policy failure.Policy,
	engineLogger *slog.Logger,
	fn func(context.Context) error,
) {
	g.group.Go(func() error {
		err := Run(g.ctx, description, policy, engineLogger, fn)

		// Run returns the context's error once the context is canceled, which
		// is not a failure of fn.
		if g.ctx.Err() != nil && errors.Is(err, g.ctx.Err()) {
			return nil
		}

		return err
	})
}

// Wait blocks until every function started with [Group.Go] has returned.
//
// It returns the error from the first function to fail fatally, or nil if none
// did.
func (g *Group) Wait() error {
	return g.group.Wait()
}
