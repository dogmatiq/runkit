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
	Signals *signal.Bus

	// Sinks is the set of sinks to which deadlines are dispatched.
	Sinks []*DeadlineSink

	// Logger is the target for log messages produced by the scheduler and its
	// supervisor.
	Logger *slog.Logger
}

// deadlineTask is an active delivery for a process instance.
type deadlineTask struct {
	sink       *DeadlineSink
	instanceID string
	cancel     context.CancelFunc
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

// run discovers pending deadlines and starts deliveries for process instances
// assigned to this node until ctx is canceled or discovery fails.
//
// Active deliveries are canceled when the cluster membership assigns their
// process instance to another node. All deliveries are canceled and awaited
// before run returns.
func (s *DeadlineScheduler) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	defer func() {
		cancel()
		group.Wait()
	}()

	changes, stop := s.Signals.Watch(ctx, "process.deadlines", 1)
	defer stop()

	capacity := 0
	for _, sink := range s.Sinks {
		capacity += sink.Concurrency
	}
	done := make(chan string, capacity)
	active := map[string]deadlineTask{}
	running := map[*DeadlineSink]int{}

	for {
		membership, membershipChanged := s.Node.Membership().Load()
		for _, task := range active {
			workload := uuidpb.Derive(task.sink.Identity.GetKey(), task.instanceID)
			if !membership.LocalNodeIsResponsibleFor(workload) {
				task.cancel()
			}
		}

		var next time.Time
		for _, sink := range s.Sinks {
			if running[sink] >= sink.Concurrency {
				continue
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
				GROUP BY d.instance_id
				ORDER BY MIN(d.deliver_at), d.instance_id`,
				xsql.UUID(sink.Identity.GetKey()),
				xsql.UUIDSeq(sink.DeadlineTypeIDs.All()),
			)
			if err != nil {
				return fmt.Errorf("unable to discover deadline workloads: %w", err)
			}

			for rows.Next() {
				var (
					instanceID string
					at         time.Time
				)
				if err := rows.Scan(&instanceID, &at); err != nil {
					rows.Close()
					return fmt.Errorf("unable to scan deadline workload: %w", err)
				}
				workload := uuidpb.Derive(sink.Identity.GetKey(), instanceID)
				if !membership.LocalNodeIsResponsibleFor(workload) {
					continue
				}
				id := workload.AsString()
				if _, ok := active[id]; ok || running[sink] >= sink.Concurrency {
					continue
				}
				if at.After(time.Now()) {
					if next.IsZero() || at.Before(next) {
						next = at
					}
					continue
				}

				taskCtx, taskCancel := context.WithCancel(ctx)
				active[id] = deadlineTask{sink, instanceID, taskCancel}
				running[sink]++
				group.Go(func() {
					defer taskCancel()
					_ = supervisor.Run(taskCtx, sink.Logger, func(ctx context.Context) error {
						return sink.Deliver(ctx, instanceID)
					})
					done <- id
				})
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return fmt.Errorf("unable to read deadline workloads: %w", err)
			}
		}

		var (
			timer *time.Timer
			ready <-chan time.Time
		)
		if !next.IsZero() {
			timer = time.NewTimer(time.Until(next))
			ready = timer.C
		}

		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return ctx.Err()
		case id := <-done:
			task := active[id]
			task.cancel()
			running[task.sink]--
			delete(active, id)
		case <-changes:
		case <-membershipChanged:
		case <-ready:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}
