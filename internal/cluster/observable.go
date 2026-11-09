package cluster

import (
	"context"

	"github.com/dogmatiq/cosyne"
)

// Observable is a value stored in the database that every node keeps a copy
// of, and reads again each time a particular cluster-wide signal is notified.
//
// It publishes the value only when it changes, so a signal that changes
// nothing wakes no observers.
type Observable[T interface{ IsEqual(T) bool }] struct {
	// Node is the node that receives the cluster-wide signal.
	Node *Node

	// Signal is the cluster-wide signal that triggers a read.
	Signal string

	// Read reads the value from the database.
	Read func(ctx context.Context) (T, error)

	value cosyne.Observable[T]
}

// Load returns the current value, and a channel that is closed when that value
// is replaced. It has the same signature as [cosyne.Observable.Load].
//
// The value is the zero value of T until the first read.
func (o *Observable[T]) Load() (T, <-chan struct{}) {
	return o.value.Load()
}

// Run reads the value straight away, and again each time the cluster-wide
// signal is notified, until ctx is canceled or a read fails.
//
// It must be called at most once.
func (o *Observable[T]) Run(ctx context.Context) error {
	var wake cosyne.Signal

	detach := o.Node.AttachSignal(ctx, o.Signal, &wake)
	defer detach()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake.Chan():
		}

		v, err := o.Read(ctx)
		if err != nil {
			return err
		}

		if current, _ := o.value.Load(); !v.IsEqual(current) {
			o.value.Store(v)
		}
	}
}
