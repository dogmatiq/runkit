package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	// signalChannel is the PostgreSQL notification channel on which
	// cluster-wide signals are sent. The payload of each notification is the
	// signal.
	signalChannel = "signal"

	// heartbeatChannel is the PostgreSQL notification channel on which
	// heartbeats are sent. The payload of each notification is a heartbeat, as
	// [encodeHeartbeat] encodes it.
	heartbeatChannel = "heartbeat"
)

// AttachSignal attaches the given cluster-wide signal to the given target, so
// that the target is notified each time the cluster-wide signal is, until the
// returned function is called or ctx is canceled.
//
// A cluster-wide signal is a string, such as "messages_enqueued". It's notified by
// sending a PostgreSQL notification, usually from a database trigger.
//
// Callers attach the signal before they first load the state they're
// interested in, so that a change made after that load notifies the target.
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
// signals and heartbeats. It notifies the targets of each signal it receives,
// and hands each heartbeat to [Node.exchangeHeartbeats].
//
// A connection never receives a notification that was sent before it started
// listening. Each time the connection starts listening, listen does two things
// to make up for that:
//
//   - It notifies every target. Each target then reloads its state, in case the
//     state changed while the connection wasn't listening.
//   - It tells the node to send a heartbeat. The node then learns which peers
//     are live from their replies.
//
// This covers both signals attached before the node started and notifications
// sent while the connection was down.
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

			for _, channel := range []string{signalChannel, heartbeatChannel} {
				if _, err := pgConn.Exec(
					ctx,
					"LISTEN "+pgx.Identifier{channel}.Sanitize(),
				); err != nil {
					return fmt.Errorf("unable to listen on the %s channel: %w", channel, err)
				}
			}

			n.EngineLogger.DebugContext(ctx, "listening for signals and heartbeats")

			n.notifyAll()

			select {
			case n.listening <- struct{}{}:
			default:
			}

			for {
				notification, err := pgConn.WaitForNotification(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return fmt.Errorf("unable to wait for signal: %w", err)
				}

				switch notification.Channel {
				case signalChannel:
					n.notify(notification.Payload)

				case heartbeatChannel:
					select {
					case n.heartbeatPayloads <- notification.Payload:
					default:
						// The buffer is full. Heartbeats are lossy by design, and
						// a later heartbeat replaces the information in a
						// dropped one, so the listener drops this heartbeat
						// instead of blocking.
					}
				}
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
