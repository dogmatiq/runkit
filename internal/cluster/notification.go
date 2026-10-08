package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// NotificationListener listens for inter-node notifications.
type NotificationListener struct {
	DB     *sql.DB
	Logger *slog.Logger

	once          sync.Once
	done          chan struct{}
	sub, unsub    chan subscription
	conn          *pgx.Conn
	subscriptions map[string]map[chan<- string]int
}

type subscription struct {
	Topic string
	Chan  chan<- string
}

type waitResult struct {
	Notification *pgconn.Notification
	Error        error
}

// Run starts the observer, which listens for notifications and manages
// subscriptions. It blocks until the context is canceled or an error occurs.
func (l *NotificationListener) Run(ctx context.Context) {
	l.init()
	defer close(l.done)

	for {
		if err := l.run(ctx); err != nil {
			if err == ctx.Err() {
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

// Subscribe registers ch to receive notifications.
func (l *NotificationListener) Subscribe(topic string, ch chan<- string) func() {
	l.init()

	sub := subscription{
		Topic: topic,
		Chan:  ch,
	}

	select {
	case l.sub <- sub:
	case <-l.done:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			select {
			case <-l.done:
			case l.unsub <- sub:
			}
		})
	}
}

func (l *NotificationListener) init() {
	l.once.Do(func() {
		l.done = make(chan struct{})
		l.sub = make(chan subscription)
		l.unsub = make(chan subscription)
		l.subscriptions = map[string]map[chan<- string]int{}
	})
}

func (l *NotificationListener) run(ctx context.Context) error {
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

		// Truly close the underlying pgx connection so that a connection with
		// active LISTEN commands is not returned to the connection pool.
		defer l.conn.Close(context.Background())

		for topic := range l.subscriptions {
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

func (l *NotificationListener) tick(ctx context.Context) error {
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()

	waitDone := make(chan waitResult, 1)

	go func() {
		n, err := l.conn.WaitForNotification(waitCtx)

		// Do not propagate context cancellation as an error - it's simply a
		// no-op result.
		if errors.Is(err, context.Canceled) {
			err = nil
		}

		waitDone <- waitResult{n, err}
	}()

	select {
	case <-ctx.Done():
		<-waitDone
		return ctx.Err()

	case sub := <-l.sub:
		cancelWait()
		if err := l.dispatch(ctx, <-waitDone); err != nil {
			return err
		}
		return l.subscribe(ctx, sub)

	case sub := <-l.unsub:
		cancelWait()
		if err := l.dispatch(ctx, <-waitDone); err != nil {
			return err
		}
		return l.unsubscribe(ctx, sub)

	case result := <-waitDone:
		return l.dispatch(ctx, result)
	}
}

func (l *NotificationListener) dispatch(ctx context.Context, result waitResult) error {
	if result.Error != nil {
		return result.Error
	}

	if result.Notification == nil {
		return nil
	}

	var group sync.WaitGroup

	for ch := range l.subscriptions[result.Notification.Channel] {
		group.Go(func() {
			select {
			case ch <- result.Notification.Payload:
			case <-ctx.Done():
			}
		})
	}

	group.Wait()

	return nil
}

func (l *NotificationListener) subscribe(ctx context.Context, sub subscription) error {
	l.Logger.DebugContext(
		ctx,
		`notification subscription added`,
		slog.String("topic", sub.Topic),
		slog.Any("channel", sub.Chan),
	)

	if subs, ok := l.subscriptions[sub.Topic]; ok {
		subs[sub.Chan]++
		return nil
	}

	l.subscriptions[sub.Topic] = map[chan<- string]int{sub.Chan: 1}

	return l.listen(ctx, sub.Topic)
}

func (l *NotificationListener) unsubscribe(ctx context.Context, sub subscription) error {
	l.Logger.DebugContext(
		ctx,
		`notification subscription removed`,
		slog.String("topic", sub.Topic),
		slog.Any("channel", sub.Chan),
	)

	l.subscriptions[sub.Topic][sub.Chan]--

	if l.subscriptions[sub.Topic][sub.Chan] != 0 {
		return nil
	}

	delete(l.subscriptions[sub.Topic], sub.Chan)

	if len(l.subscriptions[sub.Topic]) != 0 {
		return nil
	}

	delete(l.subscriptions, sub.Topic)
	return l.unlisten(ctx, sub.Topic)
}

func (l *NotificationListener) listen(ctx context.Context, topic string) error {
	sanitized := pgx.Identifier{topic}.Sanitize()

	if _, err := l.conn.Exec(ctx, "LISTEN "+sanitized); err != nil {
		return fmt.Errorf("unable to listen to topic %s: %w", topic, err)
	}

	l.Logger.DebugContext(
		ctx,
		`started listening to notification topic`,
		slog.String("topic", topic),
	)

	return nil
}

func (l *NotificationListener) unlisten(ctx context.Context, topic string) error {
	sanitized := pgx.Identifier{topic}.Sanitize()

	if _, err := l.conn.Exec(ctx, "UNLISTEN "+sanitized); err != nil {
		return fmt.Errorf("unable to unlisten to topic %s: %w", topic, err)
	}

	l.Logger.DebugContext(
		ctx,
		`stopped listening to notification topic`,
		slog.String("topic", topic),
	)

	return nil
}
