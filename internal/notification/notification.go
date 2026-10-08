package notification

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// Listener listens for PostgreSQL notifications using a single database
// connection, and dispatches them to multiple subscriber channels.
type Listener struct {
	// DB is the database connection used to listen for notifications.
	DB *sql.DB

	// Logger is the target for log messages produced by the listener.
	Logger *slog.Logger

	conn *pgx.Conn
	done xsync.Latch

	once        sync.Once
	sub, unsub  chan *subscriber
	subscribers map[string]map[*subscriber]struct{}
}

// subscriber is a target for notifications for a specific topic.
type subscriber struct {
	Topic        string
	Payloads     chan<- string
	Unsubscribed xsync.Latch
}

// waitForNotificationResult encapsulates the result of a call to
// [pgx.Conn.WaitForNotification].
type waitForNotificationResult struct {
	Notification *pgconn.Notification
	Error        error
}

// Run listens for PostgreSQL notifications and dispatches them to subscriber
// channels until ctx is canceled.
func (l *Listener) Run(ctx context.Context) {
	l.init()
	defer l.done.Set()

	for {
		if err := l.run(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}

			l.Logger.ErrorContext(
				ctx,
				"unable to listen for notifications",
				xslog.Error(err),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
			// We use a relatively long delay here, as we should only reach this
			// path when there's been a database connection issue.
		}
	}
}

// Subscribe registers a channel to receive notifications for the specified
// topic.
//
// It returns a function that unsubscribes.
func (l *Listener) Subscribe(topic string, ch chan<- string) func() {
	l.init()

	sub := &subscriber{
		Topic:    topic,
		Payloads: ch,
	}

	select {
	case <-l.done.Chan():
	case l.sub <- sub:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			sub.Unsubscribed.Set()

			select {
			case <-l.done.Chan():
			case l.unsub <- sub:
			}
		})
	}
}

func (l *Listener) init() {
	l.once.Do(func() {
		l.sub = make(chan *subscriber)
		l.unsub = make(chan *subscriber)
		l.subscribers = map[string]map[*subscriber]struct{}{}
	})
}

// run establishes a connection to the database, listens for notifications on
// all subscribed topics, and dispatches them to the appropriate subscriber
// channels.
func (l *Listener) run(ctx context.Context) error {
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
		l.conn = conn.Conn()

		// Close the underlying pgx connection so that a connection with active
		// LISTEN commands is not returned to the connection pool.
		defer l.conn.Close(context.Background())

		// Listen on all previously subscribed topics, as we may have restarted
		// after losing the database connection.
		for topic := range l.subscribers {
			if err := l.listen(ctx, topic); err != nil {
				return err
			}
		}

		for {
			if err := l.tick(ctx); err != nil {
				return err
			}
		}
	})
}

// tick waits for the next notification from the database, or for a subscription
// or unsubscription request. It dispatches notifications to the appropriate
// subscriber channels.
func (l *Listener) tick(ctx context.Context) error {
	// Setup a context that can be used to cancel an in-progress call to
	// [pgx.Conn.WaitForNotification]. This is necessary because we can not
	// issue new LISTEN/UNLISTEN commands while this function is using the
	// connection.
	waitCtx, stopWaiting := context.WithCancel(ctx)
	waitResult := make(chan waitForNotificationResult, 1)

	// Start a goroutine that waits for the next notification, and dispatch it
	// to the [waitResult] channel.
	go func() {
		n, err := l.conn.WaitForNotification(waitCtx)
		waitResult <- waitForNotificationResult{n, err}
	}()

	select {
	case <-ctx.Done():
		<-waitResult
		return ctx.Err()

	case sub := <-l.sub:
		stopWaiting()
		result := <-waitResult

		// We've already unblocked the subscriber's send to l.sub. From its
		// perspective the subscription has been acknowledged, so we action its
		// request _before_ dispatching the result of the previous
		// [pgx.Conn.WaitForNotification] call.
		if err := l.subscribe(ctx, sub); err != nil {
			return err
		}

		return l.dispatch(ctx, result)

	case sub := <-l.unsub:
		stopWaiting()
		result := <-waitResult

		// We've already unblocked the (un)subscriber, so we action its request
		// _before_ dispatching the result of the previous WaitForNotification
		// call.
		if err := l.unsubscribe(ctx, sub); err != nil {
			return err
		}

		return l.dispatch(ctx, result)

	case result := <-waitResult:
		return l.dispatch(ctx, result)
	}
}

// dispatch sends a notification to all subscribers of the relevant channel.
func (l *Listener) dispatch(ctx context.Context, result waitForNotificationResult) error {
	// Treat context cancellation as a non-error / no-op result.
	if errors.Is(result.Error, context.Canceled) {
		return nil
	}

	// Otherwise, there was likely a database connection problem.
	if result.Error != nil {
		return result.Error
	}

	var group sync.WaitGroup

	for sub := range l.subscribers[result.Notification.Channel] {
		group.Go(func() {
			select {
			case <-ctx.Done():
			case <-sub.Unsubscribed.Chan():
			case sub.Payloads <- result.Notification.Payload:
			}
		})
	}

	group.Wait()

	return nil
}

// subscribe registers a new subscriber. It issues a LISTEN command to the
// database for the relevant topic if this is the first subscriber for that
// topic.
func (l *Listener) subscribe(ctx context.Context, sub *subscriber) error {
	if subs, ok := l.subscribers[sub.Topic]; ok {
		subs[sub] = struct{}{}
		return nil
	}

	l.subscribers[sub.Topic] = map[*subscriber]struct{}{sub: {}}

	return l.listen(ctx, sub.Topic)
}

// unsubscribe removes a subscriber. It issues an UNLISTEN command to the
// database for the relevant topic if this was the last subscriber for that
// topic.
func (l *Listener) unsubscribe(ctx context.Context, sub *subscriber) error {
	subs := l.subscribers[sub.Topic]
	delete(subs, sub)

	if len(subs) != 0 {
		return nil
	}

	delete(l.subscribers, sub.Topic)

	return l.unlisten(ctx, sub.Topic)
}

// listen issues a LISTEN command to the database for the specified topic.
func (l *Listener) listen(ctx context.Context, topic string) error {
	sanitized := pgx.Identifier{topic}.Sanitize()

	if _, err := l.conn.Exec(ctx, "LISTEN "+sanitized); err != nil {
		return fmt.Errorf("unable to listen to topic: %s: %w", topic, err)
	}

	l.Logger.DebugContext(
		ctx,
		`started listening to notification topic`,
		slog.String("topic", topic),
	)

	return nil
}

// unlisten issues an UNLISTEN command to the database for the specified topic.
func (l *Listener) unlisten(ctx context.Context, topic string) error {
	sanitized := pgx.Identifier{topic}.Sanitize()

	if _, err := l.conn.Exec(ctx, "UNLISTEN "+sanitized); err != nil {
		return fmt.Errorf("unable to unlisten to topic: %s: %w", topic, err)
	}

	l.Logger.DebugContext(
		ctx,
		`stopped listening to notification topic`,
		slog.String("topic", topic),
	)

	return nil
}
