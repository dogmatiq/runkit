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

	// Queues is the queues that hold at least one message, with the most
	// messages first.
	Queues []Queue

	// QueuesErr explains why Queues could not be read.
	QueuesErr error

	// IsQueuesTruncated is true if there are more queues than the
	// observation reads, so totals over Queues are too low.
	IsQueuesTruncated bool

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

// Events returns the total number of events on the observed event streams.
func (o *Observation) Events() uint64 {
	var n uint64
	for _, s := range o.EventStreams {
		n += s.Length
	}
	return n
}

// Totals returns the total number of queued messages, and the number of those
// whose most recent attempt failed.
func (o *Observation) Totals() (messages, failing int) {
	for _, q := range o.Queues {
		messages += q.Messages
		failing += q.Failing
	}
	return messages, failing
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

	// Bindings is how the handler's instances are spread across the things
	// that they are bound to. For an aggregate handler it covers every
	// instance, by event stream. For a process handler it covers the
	// instances that have not ended, by the queue for their deadlines.
	Bindings Bindings

	// BindingsErr explains why Bindings could not be read.
	BindingsErr error

	// ConsumeRate is the number of events per second by which the handler's
	// checkpoint offsets advanced across every local event stream, between
	// the two most recent readings, for a process or projection handler. A
	// checkpoint also advances past events of types that the handler does not
	// handle.
	ConsumeRate float64

	// HasConsumeRate is true if ConsumeRate is known, which needs two
	// readings.
	HasConsumeRate bool

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
	Events uint64

	// Messages is the total number of queued messages.
	Messages int

	// Failing is the number of queued messages whose most recent attempt
	// failed.
	Failing int
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

	// consumed is the total of each handler's checkpoint offsets at slowAt,
	// keyed by handler key, used to compute the rate at which they advance.
	// Only the Run goroutine uses it.
	consumed map[string]consumedTotal
}

// consumedTotal is the total of a handler's checkpoint offsets at one time.
type consumedTotal struct {
	// At is the time of the reading.
	At time.Time

	// Total is the sum of the handler's checkpoint offsets across every local
	// event stream.
	Total uint64
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

	messages, failing := obs.Totals()

	o.history = append(
		o.history,
		Sample{
			At:       obs.At,
			Events:   obs.Events(),
			Messages: messages,
			Failing:  failing,
		},
	)

	if len(o.history) > historyLength {
		o.history = o.history[len(o.history)-historyLength:]
	}
}

// observe reads one observation.
func (o *Observer) observe(ctx context.Context) *Observation {
	obs := &Observation{
		LiveNodes: o.Listener.LiveNodes(time.Now()),
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

	obs.EventStreams, obs.EventStreamsErr = o.Database.eventStreams(ctx, listLimit+1)
	if len(obs.EventStreams) > listLimit {
		obs.EventStreams = obs.EventStreams[:listLimit]
		obs.IsEventStreamsTruncated = true
	}

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

		obs.Handlers = append(obs.Handlers, s)
	}

	return obs
}

// observeSlowParts reads the parts of an observation that cost the most to
// read, keyed by handler key.
func (o *Observer) observeSlowParts(ctx context.Context, obs *Observation) map[string]HandlerSummary {
	slow := map[string]HandlerSummary{}

	queued, queuedErr := o.queuedByHandler(ctx)

	for _, h := range o.Application.Handlers {
		s := queued[h.Key.AsString()]
		s.QueuedMessagesErr = queuedErr

		switch h.Type {
		case config.AggregateHandlerType:
			s.Bindings, s.BindingsErr = o.Database.AggregateBindings(ctx, h.Key)
			s.InstancesErr = s.BindingsErr
			s.Instances = InstanceCount{
				Count:     s.Bindings.Instances,
				IsAtLeast: s.Bindings.IsAtLeast,
			}

		case config.ProcessHandlerType:
			s.Instances, s.InstancesErr = o.Database.CountProcessInstances(ctx, h.Key)
			s.Bindings, s.BindingsErr = o.Database.ActiveProcessBindings(ctx, h.Key)
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

		if h.HandlesEvents() && s.ProgressErr == nil {
			s.ConsumeRate, s.HasConsumeRate = o.consumeRate(h, s.Progress)
		}

		slow[h.Key.AsString()] = s
	}

	return slow
}

// consumeRate returns the rate at which the handler's checkpoint offsets have
// advanced since the previous reading, and records the current total for the
// next reading. It returns false if there is no previous reading, or if the
// total went down, such as when a projection was reset.
func (o *Observer) consumeRate(h *Handler, progress []StreamProgress) (float64, bool) {
	var total uint64
	for _, p := range progress {
		if !p.IsRemote {
			total += p.CheckpointOffset
		}
	}

	if o.consumed == nil {
		o.consumed = map[string]consumedTotal{}
	}

	now := time.Now()
	key := h.Key.AsString()
	prev, ok := o.consumed[key]

	o.consumed[key] = consumedTotal{
		At:    now,
		Total: total,
	}

	if !ok || total < prev.Total {
		return 0, false
	}

	seconds := now.Sub(prev.At).Seconds()
	if seconds <= 0 {
		return 0, false
	}

	return float64(total-prev.Total) / seconds, true
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
