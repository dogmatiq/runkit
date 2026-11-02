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

// Bus sends and receives signals using a PostgreSQL database, and delivers the
// signals it receives to subscribers.
//
// A signal indicates that something about a topic has changed. It carries no
// information other than the topic itself. Delivery is lossy: a subscriber
// whose channel is full does not receive the signal. Subscribers must read the
// state they are interested in from the database, and must not rely on
// receiving every signal.
type Bus struct {
	// DB is the database used to send and receive signals.
	DB *sql.DB

	// Logger is the target for log messages produced by the bus.
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
func (b *Bus) Subscribe(topic string, ch chan<- string) func() {
	sub := &subscriber{
		Topic:    topic,
		Subtopic: ch,
	}

	b.m.Lock()
	if b.subscribers == nil {
		b.subscribers = map[*subscriber]struct{}{}
	}
	b.subscribers[sub] = struct{}{}
	b.m.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			b.m.Lock()
			delete(b.subscribers, sub)
			b.m.Unlock()
		})
	}
}

// Send sends a signal on the given topic.
func (b *Bus) Send(ctx context.Context, topic string) error {
	if _, err := b.DB.ExecContext(
		ctx,
		`SELECT pg_notify($1, $2)`,
		channel,
		topic,
	); err != nil {
		return fmt.Errorf("unable to send signal: %w", err)
	}

	return nil
}

// Run executes the bus until ctx is canceled.
//
// It must be called at most once. Subscribe may be called before Run.
func (b *Bus) Run(ctx context.Context) error {
	return supervisor.Run(ctx, b.Logger, b.run)
}

// run holds a dedicated database connection with LISTEN active on the signal
// channel, and dispatches each notification it receives.
//
// Signals sent before LISTEN is active are never received, so once it is, every
// subscriber is signaled as though a signal had arrived on its own topic. This
// covers both subscribers that registered before the bus started and signals
// missed while the connection was down.
func (b *Bus) run(ctx context.Context) error {
	conn, err := b.DB.Conn(ctx)
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

		b.Logger.DebugContext(ctx, "listening for signals")

		b.wake()

		for {
			n, err := pgConn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("unable to wait for signal: %w", err)
			}

			b.dispatch(n.Payload)
		}
	})
}

// wake delivers a signal to every subscriber, as though one had been received
// on the subscriber's own topic.
func (b *Bus) wake() {
	b.m.Lock()
	defer b.m.Unlock()

	for sub := range b.subscribers {
		select {
		case sub.Subtopic <- "":
		default:
		}
	}
}

// dispatch delivers a signal to every subscriber of its topic or of a topic
// above it.
func (b *Bus) dispatch(topic string) {
	b.m.Lock()
	defer b.m.Unlock()

	for sub := range b.subscribers {
		if subtopic, ok := subtopicOf(sub.Topic, topic); ok {
			select {
			case sub.Subtopic <- subtopic:
			default:
				// The subscriber's channel is full. Delivery is lossy by design,
				// so the signal is dropped rather than blocking the bus.
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
