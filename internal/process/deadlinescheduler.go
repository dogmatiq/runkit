package process

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// DeadlineScheduler dispatches pending deadlines to process message handlers.
//
// It uses the cluster membership to assign each process instance's deadlines
// to a node, using the handler's identity and instance ID as the workload.
//
// Deliveries are handled concurrently, up to each sink's concurrency limit.
type DeadlineScheduler struct {
	// Node is the cluster node used to determine which process instances
	// have their deadlines delivered by this scheduler.
	Node *cluster.Node

	// Signals is the bus used to receive notifications of changes to pending
	// deadlines.
	//
	// Any deadline change wakes discovery. A single shared topic allows
	// buffered notifications to coalesce without losing a wake-up for work
	// assigned to this node.
	Signals *signal.Bus

	// Sinks is the set of sinks to which deadlines are dispatched.
	Sinks []*DeadlineSink

	// Logger is the target for log messages produced by the scheduler and its
	// supervisor.
	Logger *slog.Logger
}

// deadlineRunner is the state of one supervised attempt to run a scheduler.
//
// Its maps are accessed only by the discovery goroutine. Delivery goroutines
// report completion through done, which has room for every active delivery.
type deadlineRunner struct {
	scheduler *DeadlineScheduler
	group     sync.WaitGroup
	done      chan deadlineCompletion
	active    map[string]deadlineDelivery
	running   map[*DeadlineSink]int
}

// deadlineDelivery identifies an active delivery within a handler.
//
// It is used only by deadlineRunner to exclude active instances from discovery
// and release the handler's concurrency slot when delivery completes.
type deadlineDelivery struct {
	Sink       *DeadlineSink
	InstanceID string
}

// deadlineCompletion is the result of a supervised deadline delivery.
type deadlineCompletion struct {
	WorkloadID string
	Err        error
}

// Run runs the deadline scheduler until ctx is canceled.
//
// It must be called at most once. Each sink's Concurrency must be positive.
func (s *DeadlineScheduler) Run(ctx context.Context) error {
	for _, sink := range s.Sinks {
		if sink.Concurrency <= 0 {
			panic("deadline concurrency must be positive")
		}
	}

	return supervisor.Run(ctx, s.Logger, s.run)
}

// run discovers and delivers deadlines until ctx is canceled or discovery fails.
//
// All active deliveries are canceled and awaited before run returns, so a
// restarted scheduler does not overlap with deliveries from its previous run.
func (s *DeadlineScheduler) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)

	capacity := 0
	for _, sink := range s.Sinks {
		capacity += sink.Concurrency
	}

	r := &deadlineRunner{
		scheduler: s,
		done:      make(chan deadlineCompletion, capacity),
		active:    map[string]deadlineDelivery{},
		running:   map[*DeadlineSink]int{},
	}
	defer func() {
		cancel()
		r.group.Wait()
	}()

	changes, stop := s.Signals.Watch(ctx, "process.deadlines", 1)
	defer stop()

	for {
		membership, membershipChanged := s.Node.Membership().Load()

		// Membership changes affect new deliveries only. A delivery already
		// in progress can finish; the instance lock prevents another node
		// from handling the same instance concurrently during the handoff.
		next, err := r.discover(ctx, membership)
		if err != nil {
			return err
		}

		if err := r.wait(ctx, changes, membershipChanged, next); err != nil {
			return err
		}
	}
}

// discover starts due deliveries assigned to this node and returns the time
// of its next pending deadline, or the zero time if none is known.
func (r *deadlineRunner) discover(
	ctx context.Context,
	membership *cluster.Membership,
) (time.Time, error) {
	var next time.Time

	for _, sink := range r.scheduler.Sinks {
		if r.running[sink] >= sink.Concurrency {
			continue
		}

		at, err := r.discoverSink(ctx, membership, sink)
		if err != nil {
			return time.Time{}, err
		}

		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}

	return next, nil
}

// discoverSink starts due deliveries for sink and returns its next deadline time.
//
// The query returns one candidate per instance, excluding active deliveries.
// Future deadlines are included so discovery can set a timer for the next one.
// Rendezvous assignment is evaluated in Go using the same membership algorithm
// as other engine workloads.
func (r *deadlineRunner) discoverSink(
	ctx context.Context,
	membership *cluster.Membership,
	sink *DeadlineSink,
) (time.Time, error) {
	// Active instances already have a delivery that will wake discovery when
	// it finishes. Exclude them before grouping the pending deadlines.
	active := []string{}
	for _, delivery := range r.active {
		if delivery.Sink == sink {
			active = append(active, delivery.InstanceID)
		}
	}

	rows, err := sink.DB.QueryContext(
		ctx,
		`SELECT
			d.instance_id,
			MIN(d.deliver_at)
		FROM process.deadlines AS d
		INNER JOIN process.instances AS i
			ON i.handler_key = d.handler_key
			AND i.instance_id = d.instance_id
		WHERE d.handler_key = $1
		AND d.message_type_id = ANY($2)
		AND NOT i.ended
		AND NOT (d.instance_id = ANY($3))
		GROUP BY d.instance_id
		ORDER BY MIN(d.deliver_at), d.instance_id`,
		xsql.UUID(sink.Identity.GetKey()),
		xsql.UUIDSeq(sink.DeadlineTypeIDs.All()),
		active,
	)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to discover deadline workloads: %w", err)
	}
	defer rows.Close()

	var next time.Time

	for rows.Next() {
		var (
			instanceID string
			at         time.Time
		)
		if err := rows.Scan(&instanceID, &at); err != nil {
			return time.Time{}, fmt.Errorf("unable to scan deadline workload: %w", err)
		}

		workload := uuidpb.Derive(sink.Identity.GetKey(), instanceID)
		if !membership.LocalNodeIsResponsibleFor(workload) {
			continue
		}

		if r.running[sink] >= sink.Concurrency {
			break
		}

		// Only owned deadlines contribute to the timer. An earlier deadline
		// assigned to another node must not cause this node to poll it.
		if at.After(time.Now()) {
			next = at
			break
		}

		r.start(ctx, sink, instanceID)
	}

	if err := rows.Err(); err != nil {
		return time.Time{}, fmt.Errorf("unable to read deadline workloads: %w", err)
	}

	return next, nil
}

// start starts one delivery and records it until wait receives its completion.
func (r *deadlineRunner) start(
	ctx context.Context,
	sink *DeadlineSink,
	instanceID string,
) {
	// Include the handler identity because instance IDs are only unique
	// within a handler.
	id := uuidpb.Derive(sink.Identity.GetKey(), instanceID).AsString()
	r.active[id] = deadlineDelivery{sink, instanceID}
	r.running[sink]++

	r.group.Go(func() {
		// Database failures are retried without restarting other deliveries.
		// A terminal error is passed back to the scheduler with completion.
		err := supervisor.Run(ctx, sink.Logger, func(ctx context.Context) error {
			return sink.Deliver(ctx, instanceID)
		})

		r.done <- deadlineCompletion{id, err}
	})
}

// wait waits for a delivery to finish, a deadline to become due, or a change
// to pending deadlines or cluster membership.
func (r *deadlineRunner) wait(
	ctx context.Context,
	changes <-chan string,
	membershipChanged <-chan struct{},
	next time.Time,
) error {
	var (
		timer *time.Timer
		ready <-chan time.Time
	)
	if !next.IsZero() {
		timer = time.NewTimer(time.Until(next))
		defer timer.Stop()
		ready = timer.C
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-r.done:
		delivery := r.active[result.WorkloadID]
		r.running[delivery.Sink]--
		delete(r.active, result.WorkloadID)

		return result.Err
	case <-changes:
	case <-membershipChanged:
	case <-ready:
	}

	return nil
}
