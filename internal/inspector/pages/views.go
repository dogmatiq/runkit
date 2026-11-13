package pages

import (
	"fmt"
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

	// Issues is the things that need attention, from the most recent
	// observation.
	Issues []issue
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

// SummariesOf returns the summaries of the handlers of the given kind, from the
// most recent observation. It returns nil if there is no observation yet.
func (f frame) SummariesOf(kind string) []observe.HandlerSummary {
	if f.Obs == nil {
		return nil
	}

	var summaries []observe.HandlerSummary

	for _, s := range f.Obs.Handlers {
		if s.Handler.Type.String() == kind {
			summaries = append(summaries, s)
		}
	}

	return summaries
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

// rate returns the number of events recorded per second between each pair of
// consecutive samples, over the given window.
func rate(samples []observe.Sample, window time.Duration) []float64 {
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

		// A stream can disappear from the observed streams, so the total can
		// go down. That is not a negative rate.
		events := 0.0
		if cur.Events > prev.Events {
			events = float64(cur.Events - prev.Events)
		}

		rates = append(rates, events/seconds)
	}

	return rates
}

// series returns one value from each sample over the given window.
func series(samples []observe.Sample, window time.Duration, value func(observe.Sample) float64) []float64 {
	if len(samples) == 0 {
		return nil
	}

	since := samples[len(samples)-1].At.Add(-window)

	var values []float64

	for _, s := range samples {
		if !s.At.Before(since) {
			values = append(values, value(s))
		}
	}

	return values
}
