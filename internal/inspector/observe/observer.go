package observe

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// listLimit is the largest number of queues, event streams or
	// checkpoints that one observation reads.
	listLimit = 1000

	// historyLength is the number of samples that [Observer.History] keeps.
	historyLength = 600
)

// Observation is what the inspector read from the database at one time.
//
// Each part is read separately, so one part can fail while the others
// succeed. A part that failed has a non-nil error.
type Observation struct {
	// At is the database's clock at the time of the observation. Times in the
	// observation come from the same clock, so they are compared with At,
	// not with the inspector's clock.
	At time.Time

	// ReadAt is the inspector's clock at the time of the observation. Adding
	// the time since ReadAt to At estimates the database's clock now.
	ReadAt time.Time

	// Err is non-nil if the observation could not read the database at all.
	Err error

	// BoundApplication is the identity of the application that the database
	// is bound to, or nil if it is not bound to one.
	BoundApplication *identitypb.Identity

	// LiveNodes is the live nodes, according to the listener.
	LiveNodes LiveNodes

	// ActiveWindow is how recently an aggregate instance must have recorded
	// an event to count as active.
	ActiveWindow time.Duration

	// Workloads is the number of workloads that each live node owns, as the
	// inspector computes ownership, in the order of [LiveNodes.Nodes]. It is
	// nil if the live nodes are not known.
	Workloads []NodeCount

	// Queues is the queues that hold at least one message, with the most
	// messages first.
	Queues []Queue

	// QueuesErr explains why Queues could not be read.
	QueuesErr error

	// IsQueuesTruncated is true if there are more queues than the
	// observation reads, so totals over Queues are too low.
	IsQueuesTruncated bool

	// RemovedMessages is the number of messages that have been removed from
	// the queues since PostgreSQL's statistics were last reset. See
	// [Database.removedMessages].
	RemovedMessages uint64

	// RemovedMessagesErr explains why RemovedMessages could not be read.
	RemovedMessagesErr error

	// EventStreams is the engine's event streams, ordered by ID.
	EventStreams []EventStream

	// EventStreamsErr explains why EventStreams could not be read.
	EventStreamsErr error

	// IsEventStreamsTruncated is true if there are more event streams than
	// the observation reads, so totals over EventStreams are too low.
	IsEventStreamsTruncated bool

	// Handlers is a summary of each of the application's handlers, in the
	// order of [Application.Handlers].
	Handlers []HandlerSummary
}

// QueuedMessages returns the total number of messages in the observed queues.
func (o *Observation) QueuedMessages() uint64 {
	var n uint64
	for _, q := range o.Queues {
		n += uint64(q.Messages)
	}
	return n
}

// TotalWorkloads returns the number of workloads that the live nodes own.
func (o *Observation) TotalWorkloads() int {
	n := 0
	for _, w := range o.Workloads {
		n += w.Count
	}
	return n
}

// Events returns the total number of events on the observed event streams.
func (o *Observation) Events() uint64 {
	var n uint64
	for _, s := range o.EventStreams {
		n += s.Length
	}
	return n
}

// Queue returns the queue with the given ID. It returns false if the queue
// holds no messages.
func (o *Observation) Queue(id *uuidpb.UUID) (Queue, bool) {
	for _, q := range o.Queues {
		if q.ID.Equal(id) {
			return q, true
		}
	}
	return Queue{}, false
}

// EventStream returns the event stream with the given ID. It returns false if
// the stream was not observed.
func (o *Observation) EventStream(id *uuidpb.UUID) (EventStream, bool) {
	for _, s := range o.EventStreams {
		if s.ID.Equal(id) {
			return s, true
		}
	}
	return EventStream{}, false
}

// Handler returns the summary of the handler with the given key. It returns
// false if the application has no such handler.
func (o *Observation) Handler(key *uuidpb.UUID) (HandlerSummary, bool) {
	for _, s := range o.Handlers {
		if s.Handler.Key.Equal(key) {
			return s, true
		}
	}
	return HandlerSummary{}, false
}

// HandlerSummary is what an observation read about one handler.
type HandlerSummary struct {
	// Handler is the handler.
	Handler *Handler

	// Instances is the number of instances, for an aggregate or process
	// handler.
	Instances InstanceCount

	// InstancesErr explains why Instances could not be counted.
	InstancesErr error

	// ActiveInstances is the number of instances that recorded an event within
	// [Observation.ActiveWindow] of the observation, for an aggregate handler.
	ActiveInstances InstanceCount

	// ActiveInstancesErr explains why ActiveInstances could not be counted.
	ActiveInstancesErr error

	// Bindings is how the handler's instances are spread across the things
	// that they are bound to. For an aggregate handler it covers every
	// instance, by event stream. For a process handler it covers the
	// instances that are in progress, which are those that have not ended, by
	// the queue for their deadlines.
	Bindings Bindings

	// BindingsErr explains why Bindings could not be read.
	BindingsErr error

	// Progress is how far the handler has got through each event stream, for
	// a process or projection handler.
	Progress []StreamProgress

	// ProgressErr explains why Progress could not be read.
	ProgressErr error

	// QueuedMessages is the number of queued messages for the handler. They
	// are commands for an aggregate or integration handler, and deadlines for
	// a process handler.
	QueuedMessages int

	// FailingMessages is the number of QueuedMessages whose most recent
	// attempt failed.
	FailingMessages int

	// QueuedMessagesErr explains why QueuedMessages could not be counted.
	QueuedMessagesErr error

	// LastCompactedAt is the time that the engine stores as the projection's
	// most recent compaction, for a projection handler. Before the first
	// compaction, it is the time that an engine first ran the handler. It is
	// zero if no engine has run the handler.
	LastCompactedAt time.Time

	// CompactionErr explains why LastCompactedAt could not be read.
	CompactionErr error

	// Nodes is how the handler's work is spread across the live nodes, as the
	// inspector computes ownership, in the order of [LiveNodes.Nodes].
	//
	// For an integration handler, it counts the commands in the handler's
	// backlog, by the node that owns the workload of the queue that holds
	// each one. For a projection handler, it counts the observed event
	// streams, by the node that owns the workload of consuming each one for
	// the handler. It is nil for other handlers, and if it could not be read.
	Nodes []NodeCount

	// backlog is the number of the handler's queued messages in each queue.
	backlog []queuedGroup
}

// UnhandledIsAtLeast returns true if the count of unhandled events stopped at
// its limit on any event stream, so there are at least [HandlerSummary.Unhandled]
// of them.
func (h HandlerSummary) UnhandledIsAtLeast() bool {
	for _, p := range h.Progress {
		if p.UnhandledIsAtLeast() {
			return true
		}
	}
	return false
}

// Unhandled returns the total number of events that the handler has not yet
// handled, across every local event stream.
func (h HandlerSummary) Unhandled() int {
	n := 0
	for _, p := range h.Progress {
		n += p.Unhandled
	}
	return n
}

// Sample is a few totals from one observation, kept to show how they change
// over time.
type Sample struct {
	// At is the time of the observation, on the database's clock.
	At time.Time

	// Events is the total number of events on the observed event streams.
	Events Total

	// QueuedMessages is the total number of messages in the observed queues.
	QueuedMessages Total

	// RemovedMessages is the number of messages removed from the queues since
	// PostgreSQL's statistics were last reset.
	RemovedMessages Total
}

// NodeCount is a number of things, such as workloads, that belong to one live
// node, as the inspector computes ownership.
type NodeCount struct {
	// NodeID is the ID of the node.
	NodeID *uuidpb.UUID

	// Count is the number of things.
	Count int
}

// countByNode returns the total of the given numbers for each live node, in
// the order of [LiveNodes.Nodes]. The add function passes each number to its
// count function, with the rendezvous key of the workload that it belongs
// to. It returns nil if the live nodes are not known.
func countByNode(nodes LiveNodes, add func(count func(key *uuidpb.UUID, n int))) []NodeCount {
	if !nodes.Known {
		return nil
	}

	counts := map[string]int{}

	add(func(key *uuidpb.UUID, n int) {
		if id, ok := nodes.FindWorkloadOwner(key); ok {
			counts[id.AsString()] += n
		}
	})

	result := make([]NodeCount, 0, len(nodes.Nodes))
	for _, n := range nodes.Nodes {
		result = append(
			result,
			NodeCount{
				NodeID: n.ID,
				Count:  counts[n.ID.AsString()],
			},
		)
	}

	return result
}

// Total is one total, such as the number of events recorded, as one
// observation read it.
type Total struct {
	// Value is the total.
	Value uint64

	// IsKnown is false if the observation could not read the total, in which
	// case Value is zero.
	IsKnown bool
}

// Observer reads the database repeatedly, and keeps the most recent
// observation.
//
// It reads again soon after the engine sends a signal that something has
// changed, but never more often than [Observer.MinInterval]. Deleting or
// postponing a queued message sends no signal, so it also reads every
// [Observer.MaxInterval].
type Observer struct {
	// Database is the engine's database.
	Database *Database

	// Listener is the listener that provides the live nodes.
	Listener *Listener

	// Application is the application that the inspector is linked with.
	Application *Application

	// MinInterval is the shortest time between two observations.
	MinInterval time.Duration

	// MaxInterval is the longest time between two observations.
	MaxInterval time.Duration

	// SlowInterval is the shortest time between two readings of the parts of
	// an observation that cost the most to read. Those are the number of
	// instances of each handler, and how far each process and projection
	// handler has got through each event stream.
	SlowInterval time.Duration

	// ActiveWindow is how recently an aggregate instance must have recorded an
	// event to count as active.
	ActiveWindow time.Duration

	// CheckpointTimeout is the time allowed for all of the calls to one
	// projection handler's CheckpointOffset method in one reading.
	CheckpointTimeout time.Duration

	// OnObserve is called after each observation.
	OnObserve func(*Observation)

	// m guards latest and history.
	m sync.Mutex

	// latest is the most recent observation, or nil.
	latest *Observation

	// history is the samples from the recent observations, oldest first.
	history []Sample

	// changedOnce guards the creation of changed.
	changedOnce sync.Once

	// changed is notified when the engine signals that something has changed.
	changed chan struct{}

	// slowAt is the time that the slow parts of an observation were last
	// read. Only the Run goroutine uses it.
	slowAt time.Time

	// slow is what was read at slowAt, keyed by handler key. Only the Run
	// goroutine uses it.
	slow map[string]HandlerSummary

	// lengths is the length of each event stream at recent observations,
	// oldest first. The first entry is the newest that is at least
	// ActiveWindow old, once there is one. Only the Run goroutine uses it.
	lengths []streamLengths
}

// streamLengths is the length of each event stream at one observation.
type streamLengths struct {
	// At is the time of the observation, on the database's clock.
	At time.Time

	// Lengths is the length of each event stream, keyed by stream ID.
	Lengths map[string]uint64
}

// recordLengths keeps the lengths of the event streams in obs, and discards
// those that are no longer needed.
func (o *Observer) recordLengths(obs *Observation) {
	if obs.EventStreamsErr != nil {
		return
	}

	lengths := streamLengths{
		At:      obs.At,
		Lengths: map[string]uint64{},
	}

	for _, s := range obs.EventStreams {
		lengths.Lengths[s.ID.AsString()] = s.Length
	}

	o.lengths = append(o.lengths, lengths)

	since := obs.At.Add(-o.ActiveWindow)
	for len(o.lengths) > 1 && !o.lengths[1].At.After(since) {
		o.lengths = o.lengths[1:]
	}
}

// activityScans returns, for each event stream in obs, the range of offsets
// that holds the events recorded within ActiveWindow of the observation. A
// range starts at the stream's length when the window began, if the observer
// knows it, and holds at most [countLimit] events.
func (o *Observer) activityScans(obs *Observation) []activityScan {
	var before map[string]uint64
	if len(o.lengths) > 0 && !o.lengths[0].At.After(obs.At.Add(-o.ActiveWindow)) {
		before = o.lengths[0].Lengths
	}

	var scans []activityScan

	for _, s := range obs.EventStreams {
		scan := activityScan{
			StreamID: s.ID,
			End:      s.Length,
		}

		if s.Length > countLimit {
			scan.Begin = s.Length - countLimit
			scan.IsCapped = true
		}

		if n, ok := before[s.ID.AsString()]; ok && n >= scan.Begin {
			scan.Begin = n
			scan.IsCapped = false
		}

		if scan.Begin < scan.End {
			scans = append(scans, scan)
		}
	}

	return scans
}

// Changed tells the observer that something in the database has changed.
func (o *Observer) Changed() {
	select {
	case o.changedSignal() <- struct{}{}:
	default:
	}
}

// Latest returns the most recent observation, or nil if there has not been
// one yet.
func (o *Observer) Latest() *Observation {
	o.m.Lock()
	defer o.m.Unlock()

	return o.latest
}

// History returns the samples from the recent observations, oldest first.
func (o *Observer) History() []Sample {
	o.m.Lock()
	defer o.m.Unlock()

	return append([]Sample(nil), o.history...)
}

// Run observes the database until ctx is canceled.
func (o *Observer) Run(ctx context.Context) error {
	if o.MinInterval <= 0 {
		panic("minimum interval must be positive")
	}

	if o.MaxInterval < o.MinInterval {
		panic("maximum interval must not be less than the minimum interval")
	}

	if o.SlowInterval <= 0 {
		panic("slow interval must be positive")
	}

	if o.ActiveWindow <= 0 {
		panic("active window must be positive")
	}

	if o.CheckpointTimeout <= 0 {
		panic("checkpoint timeout must be positive")
	}

	if o.OnObserve == nil {
		panic("observe callback must not be nil")
	}

	changed := o.changedSignal()

	for {
		obs := o.observe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		o.publish(obs)
		o.OnObserve(obs)

		// Wait at least the minimum interval, so that a busy engine's signals
		// don't set the rate of reads.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.MinInterval):
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-time.After(o.MaxInterval - o.MinInterval):
		}
	}
}

// changedSignal returns the channel that [Observer.Changed] notifies.
func (o *Observer) changedSignal() chan struct{} {
	o.changedOnce.Do(func() {
		o.changed = make(chan struct{}, 1)
	})

	return o.changed
}

func (o *Observer) publish(obs *Observation) {
	o.m.Lock()
	defer o.m.Unlock()

	o.latest = obs

	if obs.Err != nil {
		return
	}

	o.history = append(
		o.history,
		Sample{
			At: obs.At,
			Events: Total{
				Value:   obs.Events(),
				IsKnown: obs.EventStreamsErr == nil,
			},
			QueuedMessages: Total{
				Value:   obs.QueuedMessages(),
				IsKnown: obs.QueuesErr == nil,
			},
			RemovedMessages: Total{
				Value:   obs.RemovedMessages,
				IsKnown: obs.RemovedMessagesErr == nil,
			},
		},
	)

	if len(o.history) > historyLength {
		o.history = o.history[len(o.history)-historyLength:]
	}
}

// observe reads one observation.
func (o *Observer) observe(ctx context.Context) *Observation {
	obs := &Observation{
		LiveNodes:    o.Listener.LiveNodes(time.Now()),
		ActiveWindow: o.ActiveWindow,
	}

	at, bound, err := o.Database.boundApplication(ctx)
	if err != nil {
		obs.Err = err
		return obs
	}

	obs.At = at
	obs.ReadAt = time.Now()
	obs.BoundApplication = bound

	obs.Queues, obs.QueuesErr = o.Database.queues(ctx, listLimit+1)
	if len(obs.Queues) > listLimit {
		obs.Queues = obs.Queues[:listLimit]
		obs.IsQueuesTruncated = true
	}

	obs.RemovedMessages, obs.RemovedMessagesErr = o.Database.removedMessages(ctx)

	obs.EventStreams, obs.EventStreamsErr = o.Database.eventStreams(ctx, listLimit+1)
	if len(obs.EventStreams) > listLimit {
		obs.EventStreams = obs.EventStreams[:listLimit]
		obs.IsEventStreamsTruncated = true
	}

	obs.Workloads = o.workloads(obs)
	o.recordLengths(obs)

	if time.Since(o.slowAt) >= o.SlowInterval {
		o.slow = o.observeSlowParts(ctx, obs)
		o.slowAt = time.Now()
	}

	compactions, compactionsErr := o.Database.compactions(ctx)

	for _, h := range o.Application.Handlers {
		s := o.slow[h.Key.AsString()]
		s.Handler = h

		if h.Type == config.ProjectionHandlerType {
			s.LastCompactedAt = compactions[h.Key.AsString()]
			s.CompactionErr = compactionsErr
		}

		s.Nodes = o.handlerNodes(obs, s)

		obs.Handlers = append(obs.Handlers, s)
	}

	return obs
}

// workloads returns the number of workloads that each live node owns: delivering
// the messages in each queue, and consuming each observed event stream for each
// handler that handles events. It returns nil if the live nodes are not known.
func (o *Observer) workloads(obs *Observation) []NodeCount {
	// A projection handler that minimizes concurrency has one workload for
	// all of the event streams, so each key is counted once.
	keys := map[string]*uuidpb.UUID{}

	for _, q := range obs.Queues {
		keys[q.ID.AsString()] = q.ID
	}

	for _, h := range o.Application.Handlers {
		if !h.HandlesEvents() {
			continue
		}

		for _, s := range obs.EventStreams {
			k := h.StreamWorkloadKey(s.ID)
			keys[k.AsString()] = k
		}
	}

	return countByNode(
		obs.LiveNodes,
		func(count func(*uuidpb.UUID, int)) {
			for _, k := range keys {
				count(k, 1)
			}
		},
	)
}

// handlerNodes returns how the work of the handler that s summarizes is spread
// across the live nodes. See [HandlerSummary.Nodes].
func (o *Observer) handlerNodes(obs *Observation, s HandlerSummary) []NodeCount {
	switch s.Handler.Type {
	case config.IntegrationHandlerType:
		if s.QueuedMessagesErr != nil {
			return nil
		}

		return countByNode(
			obs.LiveNodes,
			func(count func(*uuidpb.UUID, int)) {
				for _, q := range s.backlog {
					count(q.QueueID, q.Messages)
				}
			},
		)

	case config.ProjectionHandlerType:
		if obs.EventStreamsErr != nil {
			return nil
		}

		return countByNode(
			obs.LiveNodes,
			func(count func(*uuidpb.UUID, int)) {
				for _, es := range obs.EventStreams {
					count(s.Handler.StreamWorkloadKey(es.ID), 1)
				}
			},
		)
	}

	return nil
}

// observeSlowParts reads the parts of an observation that cost the most to
// read, keyed by handler key.
func (o *Observer) observeSlowParts(ctx context.Context, obs *Observation) map[string]HandlerSummary {
	slow := map[string]HandlerSummary{}

	queued, queuedErr := o.queuedByHandler(ctx)

	var (
		active    map[string]InstanceCount
		activeErr = obs.EventStreamsErr
	)
	if activeErr == nil {
		active, activeErr = o.Database.activeAggregateInstances(
			ctx,
			o.activityScans(obs),
			obs.At.Add(-o.ActiveWindow),
		)
	}

	for _, h := range o.Application.Handlers {
		s := queued[h.Key.AsString()]
		s.QueuedMessagesErr = queuedErr

		switch h.Type {
		case config.AggregateHandlerType:
			s.ActiveInstances = active[h.Key.AsString()]
			s.ActiveInstancesErr = activeErr
			s.Bindings, s.BindingsErr = o.Database.AggregateBindings(ctx, h.Key)
			s.InstancesErr = s.BindingsErr
			s.Instances = InstanceCount{
				Count:     s.Bindings.Instances,
				IsAtLeast: s.Bindings.IsAtLeast,
			}

		case config.ProcessHandlerType:
			s.Instances, s.InstancesErr = o.Database.CountProcessInstances(ctx, h.Key)
			s.Bindings, s.BindingsErr = o.Database.ProcessBindings(ctx, h.Key)
			s.Progress, s.ProgressErr = o.Database.processProgress(ctx, h, listLimit)

		case config.ProjectionHandlerType:
			if obs.EventStreamsErr != nil {
				s.ProgressErr = obs.EventStreamsErr
				break
			}

			s.Progress, s.ProgressErr = o.Database.projectionProgress(
				ctx,
				h,
				obs.EventStreams,
				o.CheckpointTimeout,
			)
		}

		slow[h.Key.AsString()] = s
	}

	return slow
}

// queuedByHandler returns a summary of each handler that holds only the
// number of its queued messages, keyed by handler key.
func (o *Observer) queuedByHandler(ctx context.Context) (map[string]HandlerSummary, error) {
	groups, err := o.Database.queuedGroups(ctx)
	if err != nil {
		return nil, err
	}

	summaries := map[string]HandlerSummary{}

	for _, g := range groups {
		key := g.HandlerKey

		// A command has no handler key of its own, so it belongs to the
		// handler that handles its type.
		if key == nil {
			h, ok := o.Application.CommandHandler(g.TypeID)
			if !ok {
				continue
			}
			key = h.Key
		}

		s := summaries[key.AsString()]
		s.QueuedMessages += g.Messages
		s.FailingMessages += g.Failing
		s.backlog = append(s.backlog, g)
		summaries[key.AsString()] = s
	}

	return summaries, nil
}

// boundApplication returns the database's clock, and the identity of the
// application that the database is bound to, or nil if it is not bound.
func (d *Database) boundApplication(ctx context.Context) (time.Time, *identitypb.Identity, error) {
	var (
		at   time.Time
		key  sql.Null[string]
		name sql.Null[string]
	)

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			return tx.QueryRowContext(
				ctx,
				`SELECT now(), i.key, i.name
				FROM (SELECT 1) AS one
				LEFT JOIN application.identity AS i ON true`,
			).Scan(&at, &key, &name)
		},
	)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("unable to read the application identity: %w", err)
	}

	if !key.Valid {
		return at, nil, nil
	}

	k, err := uuidpb.Parse(key.V)
	if err != nil {
		return time.Time{}, nil, err
	}

	return at, identitypb.New(name.V, k), nil
}

// compactions returns the time that the engine stores as each projection's
// most recent compaction, keyed by handler key.
func (d *Database) compactions(ctx context.Context) (map[string]time.Time, error) {
	compactions := map[string]time.Time{}

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT h.handler_key, h.last_compacted_at
				FROM projection.handlers AS h`,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var (
					key = &uuidpb.UUID{}
					at  time.Time
				)

				if err := rows.Scan(xsql.UUID(key), &at); err != nil {
					return err
				}

				compactions[key.AsString()] = at
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read projection compactions: %w", err)
	}

	return compactions, nil
}
