package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// notificationChannel is the PostgreSQL notification channel on which
// cluster-wide signals are sent. The payload of each notification is the
// signal.
const notificationChannel = "signal"

// AttachSignal attaches the given cluster-wide signal to the given target, so
// that the target is notified each time the cluster-wide signal is, until the
// returned function is called or ctx is canceled.
//
// A cluster-wide signal is a string, such as "messagequeue". It's notified by
// sending a PostgreSQL notification, usually from a database trigger.
//
// As a convenience, it notifies the target straight away. Callers usually wait
// on the target and load the state they're interested in each time it's
// notified, so this first notification triggers their first load. That load
// happens after the signal is attached, so it can't miss a change made in
// between.
//
// It may be called before [Node.Run].
func (n *Node) AttachSignal(
	ctx context.Context,
	signal string,
	target cosyne.SignalW,
) (detach func()) {
	a := &signalAttachment{
		Signal: signal,
		Target: target,
	}

	n.attach(a)
	target.Notify()

	detachOnce := sync.OnceFunc(
		func() {
			n.detach(a)
		},
	)

	stopAfterFunc := context.AfterFunc(ctx, detachOnce)

	return func() {
		stopAfterFunc()
		detachOnce()
	}
}

// signalAttachment is a cluster-wide signal attached to a target, which is
// notified each time the cluster-wide signal is.
type signalAttachment struct {
	// Signal is the cluster-wide signal.
	Signal string

	// Target is the signal that is notified.
	Target cosyne.SignalW
}

// listen holds a dedicated database connection that listens for cluster-wide
// signals, and notifies the targets of each one it receives.
//
// Signals sent before the connection is listening are never received, so once
// it is, every target is notified. This covers both signals attached before the
// node started and signals missed while the connection was down.
func (n *Node) listen(ctx context.Context) error {
	conn, err := n.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(
		func(raw any) error {
			conn, ok := raw.(*stdlib.Conn)
			if !ok {
				return fmt.Errorf("unexpected connection type: %T", raw)
			}
			pgConn := conn.Conn()

			// Close the underlying pgx connection so that a connection with an
			// active LISTEN command is not returned to the connection pool.
			defer pgConn.Close(context.Background())

			if _, err := pgConn.Exec(
				ctx,
				"LISTEN "+pgx.Identifier{notificationChannel}.Sanitize(),
			); err != nil {
				return fmt.Errorf("unable to listen for signals: %w", err)
			}

			n.EngineLogger.DebugContext(ctx, "listening for signals")

			n.notifyAll()

			for {
				notification, err := pgConn.WaitForNotification(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return fmt.Errorf("unable to wait for signal: %w", err)
				}

				n.notify(notification.Payload)
			}
		},
	)
}

// attach starts notifying the given attachment's target.
func (n *Node) attach(a *signalAttachment) {
	n.signalAttachmentsM.Lock()
	defer n.signalAttachmentsM.Unlock()

	if n.signalAttachments == nil {
		n.signalAttachments = map[string]map[*signalAttachment]struct{}{}
	}

	attached, ok := n.signalAttachments[a.Signal]
	if !ok {
		attached = map[*signalAttachment]struct{}{}
		n.signalAttachments[a.Signal] = attached
	}

	attached[a] = struct{}{}
}

// detach stops notifying the given attachment's target.
func (n *Node) detach(a *signalAttachment) {
	n.signalAttachmentsM.Lock()
	defer n.signalAttachmentsM.Unlock()

	attached := n.signalAttachments[a.Signal]
	delete(attached, a)

	if len(attached) == 0 {
		delete(n.signalAttachments, a.Signal)
	}
}

// notify notifies the targets of the given cluster-wide signal.
func (n *Node) notify(signal string) {
	n.signalAttachmentsM.Lock()
	defer n.signalAttachmentsM.Unlock()

	for a := range n.signalAttachments[signal] {
		a.Target.Notify()
	}
}

// notifyAll notifies every target of every cluster-wide signal.
func (n *Node) notifyAll() {
	n.signalAttachmentsM.Lock()
	defer n.signalAttachmentsM.Unlock()

	for _, attached := range n.signalAttachments {
		for a := range attached {
			a.Target.Notify()
		}
	}
}
