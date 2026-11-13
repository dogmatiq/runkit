package pages

import (
	"fmt"
	"html/template"
	"time"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

// frame is what every page needs to render its layout.
type frame struct {
	// Title is the page's title.
	Title string

	// Section is the navigation link that the page belongs to.
	Section string

	// Root is the relative URL of the inspector's root, ending in a slash.
	// Every link on the page starts with it, so the inspector works wherever
	// it is mounted.
	Root string

	// Self is the relative URL of the page itself, which the page fetches
	// again when an update arrives.
	Self string

	// App is the application that the inspector is linked with.
	App *observe.Application

	// Obs is the most recent observation, or nil if there has not been one
	// yet.
	Obs *observe.Observation

	// Now is the database's clock, estimated from the most recent
	// observation.
	Now time.Time

	// LocalNow is the inspector's clock. Heartbeat times come from it.
	LocalNow time.Time
}

// Owner returns the node that owns the workload with the given key.
func (f frame) Owner(key *uuidpb.UUID) ownerView {
	v := ownerView{
		Root: f.Root,
	}

	if f.Obs == nil || !f.Obs.LiveNodes.Known {
		return v
	}

	v.Known = true
	v.ID, _ = f.Obs.LiveNodes.FindWorkloadOwner(key)

	return v
}

// QueueView returns a view of the given queue.
func (f frame) QueueView(q observe.Queue) queueView {
	return queueView{
		Queue:       q,
		DeliveredBy: f.Owner(q.ID),
	}
}

// HandlerName returns the name of the handler with the given key, or its key
// if the application has no such handler.
func (f frame) HandlerName(key *uuidpb.UUID) string {
	if h, ok := f.App.Handler(key); ok {
		return h.Name
	}
	return key.AsString()
}

// HandlersOf returns the handlers of the given kind, such as "aggregate", in
// the order of the application's handlers.
func (f frame) HandlersOf(kind string) []*observe.Handler {
	var handlers []*observe.Handler

	for _, h := range f.App.Handlers {
		if h.Type.String() == kind {
			handlers = append(handlers, h)
		}
	}

	return handlers
}

// HandlerSummary returns the summary of the handler with the given key from
// the most recent observation, or nil if there is none.
func (f frame) HandlerSummary(key *uuidpb.UUID) *observe.HandlerSummary {
	if f.Obs == nil {
		return nil
	}

	if s, ok := f.Obs.Handler(key); ok {
		return &s
	}

	return nil
}

// nodeWorkloadCount is the number of workloads that one live node owns.
type nodeWorkloadCount struct {
	NodeID    *uuidpb.UUID
	Workloads int
}

// WorkloadBar returns a bar of the number of workloads that each live node
// owns: delivering each queue's messages, and consuming each local event stream
// for each process and projection handler. It returns an empty bar if the live
// nodes are not known.
func (f frame) WorkloadBar() template.HTML {
	if f.Obs == nil || !f.Obs.LiveNodes.Known {
		return workloadBar(nil)
	}

	counts := map[string]int{}

	add := func(key *uuidpb.UUID) {
		if id, ok := f.Obs.LiveNodes.FindWorkloadOwner(key); ok {
			counts[id.AsString()]++
		}
	}

	for _, q := range f.Obs.Queues {
		add(q.ID)
	}

	for _, h := range f.App.Handlers {
		if !h.HandlesEvents() {
			continue
		}

		for _, s := range f.Obs.EventStreams {
			add(h.StreamWorkloadKey(s.ID))
		}
	}

	// The nodes stay in the order of their IDs, so that a node's segment keeps
	// its place as workloads move.
	var result []nodeWorkloadCount
	for _, n := range f.Obs.LiveNodes.Nodes {
		result = append(
			result,
			nodeWorkloadCount{
				NodeID:    n.ID,
				Workloads: counts[n.ID.AsString()],
			},
		)
	}

	return workloadBar(result)
}

// Envelope returns a view of the given envelope.
func (f frame) Envelope(env *envelopepb.Envelope) envelopeView {
	body := env.GetBody()
	header := env.GetHeader()

	v := envelopeView{
		MessageID:         body.GetMessageId(),
		CausationID:       header.GetCausationId(),
		CorrelationID:     header.GetCorrelationId(),
		Description:       body.GetMessage().GetDescription(),
		Type:              f.App.MessageType(body.GetMessage().GetTypeId()),
		IdempotencyKey:    body.GetIdempotencyKey(),
		SourceApplication: header.GetSource().GetApplication(),
		SourceHandler:     header.GetSource().GetHandler(),
		SourceInstanceID:  header.GetSource().GetInstanceId(),
	}

	if t := body.GetCreatedAt(); t != nil {
		v.CreatedAt = t.AsTime()
	}

	if t := body.GetScheduledFor(); t != nil {
		v.ScheduledFor = t.AsTime()
	}

	return v
}

// Payload returns the decoded content of the message in the given envelope.
func (f frame) Payload(env *envelopepb.Envelope) payloadView {
	m, err := f.App.DecodeMessage(
		env.GetBody().GetMessage().GetTypeId(),
		env.GetBody().GetMessage().GetData(),
	)
	if err != nil {
		return payloadView{
			Err: err,
		}
	}

	return format(m)
}

// ownerView is the node that owns a workload, as a page shows it.
type ownerView struct {
	// Root is the relative URL of the inspector's root.
	Root string

	// Known is false if the live nodes are not known.
	Known bool

	// ID is the ID of the node that owns the workload, or nil if there are no
	// live nodes.
	ID *uuidpb.UUID
}

// queueView is a queue, as a page shows it.
type queueView struct {
	observe.Queue

	// DeliveredBy is the node that owns the workload of delivering the
	// queue's messages.
	DeliveredBy ownerView
}

// envelopeView is a message envelope, as a page shows it.
type envelopeView struct {
	MessageID         *uuidpb.UUID
	CausationID       *uuidpb.UUID
	CorrelationID     *uuidpb.UUID
	Description       string
	Type              *observe.MessageType
	CreatedAt         time.Time
	ScheduledFor      time.Time
	IdempotencyKey    string
	SourceApplication *identitypb.Identity
	SourceHandler     *identitypb.Identity
	SourceInstanceID  string
}

// IsCausedBySelf returns true if the message is its own cause, which is true
// of a command that application code outside any handler executed.
func (v envelopeView) IsCausedBySelf() bool {
	return v.CausationID.Equal(v.MessageID)
}

// payloadView is the content of a message or root, decoded and formatted.
type payloadView struct {
	// Text is the formatted content.
	Text string

	// Err explains why the content could not be decoded.
	Err error
}

// format returns the formatted form of v. It returns an error if formatting
// panics.
func format(v any) (p payloadView) {
	defer func() {
		if r := recover(); r != nil {
			p = payloadView{
				Err: fmt.Errorf("unable to format value: %v", r),
			}
		}
	}()

	return payloadView{
		Text: dapper.Format(v),
	}
}

// rate returns the change per second in a counter between each pair of
// consecutive samples, over the given window. It skips each pair in which
// either sample could not read the counter.
func rate(samples []observe.Sample, window time.Duration, counter func(observe.Sample) observe.Counter) []float64 {
	if len(samples) == 0 {
		return nil
	}

	since := samples[len(samples)-1].At.Add(-window)

	var rates []float64

	for i := 1; i < len(samples); i++ {
		prev, cur := samples[i-1], samples[i]

		if cur.At.Before(since) {
			continue
		}

		seconds := cur.At.Sub(prev.At).Seconds()
		if seconds <= 0 {
			continue
		}

		before, after := counter(prev), counter(cur)
		if !before.IsKnown || !after.IsKnown {
			continue
		}

		// A total can go down, such as when an event stream disappears from
		// the observed streams, or PostgreSQL's statistics are reset. That is
		// not a negative rate.
		change := 0.0
		if after.Value > before.Value {
			change = float64(after.Value - before.Value)
		}

		rates = append(rates, change/seconds)
	}

	return rates
}

// perMinute returns the average of the most recent few of the given rates per
// second, as a rate per minute. The rate between two observations is noisy, so
// one alone is not shown.
func perMinute(rates []float64) float64 {
	recent := rates[max(0, len(rates)-rateSmoothing):]
	if len(recent) == 0 {
		return 0
	}

	sum := 0.0
	for _, r := range recent {
		sum += r
	}

	return sum / float64(len(recent)) * 60
}
