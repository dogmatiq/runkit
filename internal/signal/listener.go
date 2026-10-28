package signal

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// channel is the name of the PostgreSQL notification channel on which signals
// are sent. The payload of each notification is the signal's topic.
const channel = "signal"

// Listener receives signals from a PostgreSQL database and delivers them to
// subscribers.
//
// A signal indicates that something about a topic has changed. It carries no
// information other than the topic itself. Delivery is lossy: a subscriber
// whose channel is full does not receive the signal. Subscribers must read the
// state they are interested in from the database, and must not rely on
// receiving every signal.
type Listener struct {
	// DB is the database connection used to listen for signals.
	DB *sql.DB

	// Logger is the target for log messages produced by the listener.
	Logger *slog.Logger

	m           sync.Mutex
	subscribers map[*subscriber]struct{}
}

// subscriber is a target for signals on a specific topic and those beneath it.
type subscriber struct {
	Topic    string
	Subtopic chan<- string
}

// Subscribe registers ch to receive each signal on the given topic, or on any
// topic beneath it.
//
// Topics are hierarchical, with levels separated by dots. A subscriber to
// "a.b" receives signals on "a.b" and "a.b.c", but not on "a.bc".
//
// The value sent on ch is the part of the signal's topic beneath the subscribed
// topic: "c" for a signal on "a.b.c", or "" for a signal on "a.b" itself.
//
// ch should be buffered. If it is full when a signal is received, that signal
// is not delivered to ch.
//
// It returns a function that unsubscribes.
func (l *Listener) Subscribe(topic string, ch chan<- string) func() {
	sub := &subscriber{
		Topic:    topic,
		Subtopic: ch,
	}

	l.m.Lock()
	if l.subscribers == nil {
		l.subscribers = map[*subscriber]struct{}{}
	}
	l.subscribers[sub] = struct{}{}
	l.m.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			l.m.Lock()
			delete(l.subscribers, sub)
			l.m.Unlock()
		})
	}
}

// Run executes the listener until ctx is canceled.
//
// It must be called at most once. Subscribe may be called before Run.
func (l *Listener) Run(ctx context.Context) error {
	return supervisor.Run(ctx, l.Logger, l.listen)
}

// listen holds a dedicated database connection with LISTEN active on the
// signal channel, and dispatches each notification it receives.
//
// Signals sent before LISTEN is active are never received, so once it is, every
// subscriber is signaled as though a signal had arrived on its own topic. This
// covers both subscribers that registered before the listener started and
// signals missed while the connection was down.
func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(raw any) error {
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
			"LISTEN "+pgx.Identifier{channel}.Sanitize(),
		); err != nil {
			return fmt.Errorf("unable to listen for signals: %w", err)
		}

		l.Logger.DebugContext(ctx, "listening for signals")

		l.wake()

		for {
			n, err := pgConn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("unable to wait for signal: %w", err)
			}

			l.dispatch(n.Payload)
		}
	})
}

// wake delivers a signal to every subscriber, as though one had been received
// on the subscriber's own topic.
func (l *Listener) wake() {
	l.m.Lock()
	defer l.m.Unlock()

	for sub := range l.subscribers {
		select {
		case sub.Subtopic <- "":
		default:
		}
	}
}

// dispatch delivers a signal to every subscriber of its topic or of a topic
// above it.
func (l *Listener) dispatch(topic string) {
	l.m.Lock()
	defer l.m.Unlock()

	for sub := range l.subscribers {
		if subtopic, ok := subtopicOf(sub.Topic, topic); ok {
			select {
			case sub.Subtopic <- subtopic:
			default:
				// The subscriber's channel is full. Delivery is lossy by design,
				// so the signal is dropped rather than blocking the listener.
			}
		}
	}
}

// subtopicOf returns the part of topic beneath parent, and true, if topic is
// parent or is beneath it in the topic hierarchy. Otherwise, it returns false.
func subtopicOf(parent, topic string) (string, bool) {
	if topic == parent {
		return "", true
	}

	return strings.CutPrefix(topic, parent+".")
}
