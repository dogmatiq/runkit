package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// DeadlineConsumer discovers and delivers one process handler's deadlines on
// its rendezvous owner. Membership assigns work; instance locks protect state
// when membership views overlap. Run must be called at most once.
type DeadlineConsumer struct {
	Node            *cluster.Node
	Signals         *signal.Bus
	Sink            *Sink
	DeadlineTypeIDs *uuidcollection.Set

	// Capacity is the engine-wide semaphore, shared by all deadline consumers.
	Capacity                chan struct{}
	LockTimeout             time.Duration
	ContentionDelay         time.Duration
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

type deadlineCandidate struct {
	messageID  *uuidpb.UUID
	instanceID string
	deliverAt  time.Time
}

// deadlineRuntime survives supervised restarts and ownership changes. Workers
// remove their instance before releasing capacity, independently of backoff.
type deadlineRuntime struct {
	mu      sync.Mutex
	active  map[string]struct{}
	changed chan struct{}
	errors  chan error
	workers sync.WaitGroup
}

func (s *deadlineRuntime) activeIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.active))
	for id := range s.active {
		ids = append(ids, id)
	}
	return ids
}

// pendingErrors retains every failure, including fatal classifications, while
// coalescing failures from concurrent attempts into one supervisor restart.
func (s *deadlineRuntime) pendingErrors(first error) error {
	errs := []error{first}
	for {
		select {
		case err := <-s.errors:
			errs = append(errs, err)
		default:
			return errors.Join(errs...)
		}
	}
}

func (c *DeadlineConsumer) Run(ctx context.Context) error {
	if cap(c.Capacity) == 0 || c.LockTimeout <= 0 || c.ContentionDelay <= 0 {
		panic("deadline capacity and queue timings must be positive")
	}
	ctx, cancel := context.WithCancel(ctx)
	state := &deadlineRuntime{
		active:  make(map[string]struct{}),
		changed: make(chan struct{}, 1),
		errors:  make(chan error, cap(c.Capacity)),
	}
	defer func() { cancel(); state.workers.Wait() }()
	signals, stop := c.Signals.Watch(ctx, "process.deadlines."+c.Sink.Identity.GetKey().AsString(), 1)
	defer stop()
	return supervisor.Run(ctx, c.Logger, func(ctx context.Context) error {
		return c.run(ctx, state, signals)
	})
}

func (c *DeadlineConsumer) responsible() bool {
	membership, _ := c.Node.Membership().Load()
	return membership.LocalNodeIsResponsibleFor(c.Sink.Identity.GetKey())
}

func (c *DeadlineConsumer) run(ctx context.Context, state *deadlineRuntime, signals <-chan string) error {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	dirty := true
	var candidate *deadlineCandidate
	for {
		if err := state.pendingErrors(nil); err != nil {
			return err
		}
		membership, membershipChanged := c.Node.Membership().Load()
		owned := membership.LocalNodeIsResponsibleFor(c.Sink.Identity.GetKey())
		var capacity chan struct{}
		var due <-chan time.Time
		timer.Stop()
		if owned {
			if dirty {
				capacity = c.Capacity
			} else if candidate != nil {
				if delay := time.Until(candidate.deliverAt); delay > 0 {
					timer.Reset(delay)
					due = timer.C
				} else {
					capacity = c.Capacity
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-membershipChanged:
			dirty = true
			candidate = nil
		case _, ok := <-signals:
			if !ok {
				return ctx.Err()
			}
			dirty = true
		case <-state.changed:
			dirty = true
		case err := <-state.errors:
			return state.pendingErrors(err)
		case <-due:
		case capacity <- struct{}{}:
			// Reserving before a refresh also applies backpressure to dirty
			// discovery while all delivery slots are occupied. Future or empty
			// results release the reservation immediately, without a transaction.
			if ctx.Err() != nil || !c.responsible() {
				<-c.Capacity
				dirty = true
				continue
			}
			if dirty {
				var err error
				candidate, err = c.discover(ctx, state.activeIDs())
				if err != nil {
					<-c.Capacity
					return err
				}
				dirty = false
			}
			if candidate == nil || time.Now().Before(candidate.deliverAt) {
				<-c.Capacity
				continue
			}
			if ctx.Err() != nil || !c.responsible() {
				<-c.Capacity
				dirty = true
				continue
			}
			state.mu.Lock()
			_, active := state.active[candidate.instanceID]
			if !active {
				state.active[candidate.instanceID] = struct{}{}
			}
			state.mu.Unlock()
			if active {
				<-c.Capacity
				dirty = true
				continue
			}
			state.workers.Add(1)
			go c.attempt(ctx, state, *candidate)
			candidate = nil
			dirty = true
		}
	}
}

func (c *DeadlineConsumer) attempt(ctx context.Context, state *deadlineRuntime, candidate deadlineCandidate) {
	defer state.workers.Done()
	defer func() {
		state.mu.Lock()
		delete(state.active, candidate.instanceID)
		<-c.Capacity
		state.mu.Unlock()
		select {
		case state.changed <- struct{}{}:
		default:
		}
	}()
	if err := c.deliver(ctx, candidate); err != nil && ctx.Err() == nil {
		select {
		case state.errors <- err:
		case <-ctx.Done():
		}
	}
}

func (c *DeadlineConsumer) discover(ctx context.Context, active []string) (*deadlineCandidate, error) {
	row := c.Sink.DB.QueryRowContext(ctx, `SELECT d.message_id, d.instance_id, d.deliver_at
	FROM unnest($2::uuid[]) AS r(type_id)
	CROSS JOIN LATERAL (
		SELECT q.message_id, q.instance_id, q.deliver_at
		FROM process.deadlines AS q
		WHERE q.handler_key = $1 AND q.message_type_id = r.type_id
			AND q.instance_id <> ALL($3::text[])
		ORDER BY q.deliver_at, q.message_id LIMIT 1
	) AS d
	ORDER BY d.deliver_at, d.message_id LIMIT 1`,
		xsql.UUID(c.Sink.Identity.GetKey()), xsql.UUIDSeq(c.DeadlineTypeIDs.All()), active)
	candidate := &deadlineCandidate{messageID: &uuidpb.UUID{}}
	if err := row.Scan(xsql.UUID(candidate.messageID), &candidate.instanceID, &candidate.deliverAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("unable to discover process deadline: %w", err)
	}
	return candidate, nil
}
