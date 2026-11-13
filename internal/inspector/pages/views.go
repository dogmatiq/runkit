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

// Owner returns the node that owns the workload with the given key.
func (f frame) Owner(key *uuidpb.UUID) ownerView {
	v := ownerView{
		Root: f.Root,
	}

	if f.Obs == nil || !f.Obs.Membership.Known {
		return v
	}

	v.Known = true
	v.ID, _ = f.Obs.Membership.FindWorkloadOwner(key)

	return v
}

// Node returns a view of the node with the given ID, as the owner of
// something.
func (f frame) Node(id *uuidpb.UUID) ownerView {
	return ownerView{
		Root:  f.Root,
		Known: true,
		ID:    id,
	}
}

// queueView is a queue, as a page shows it.
type queueView struct {
	observe.Queue

	// Label says what the queue's messages are.
	Label string

	// Owner is the node that owns the workload of delivering the queue's
	// messages.
	Owner ownerView
}

// QueueView returns a view of the given queue.
func (f frame) QueueView(q observe.Queue) queueView {
	return queueView{
		Queue: q,
		Label: f.QueueLabel(q.ID),
		Owner: f.Owner(q.ID),
	}
}

// QueueLabel returns a short description of the messages in the queue with
// the given ID.
//
// The engine chooses each message's queue. A command for an integration
// handler that minimizes concurrency goes to the queue whose ID is the
// handler's key. A command for an aggregate instance goes to the queue whose
// ID is the instance's event stream ID. Everything else goes to the queue
// whose ID is the ID of the node that added it.
func (f frame) QueueLabel(id *uuidpb.UUID) string {
	if h, ok := f.App.Handler(id); ok {
		return "commands for " + h.Name
	}

	if f.Obs != nil {
		if _, ok := f.Obs.EventStream(id); ok {
			return "commands for aggregate instances on event stream " + shortID(id)
		}
	}

	return "messages added by the node with this ID"
}

// HandlerName returns the name of the handler with the given key, or its key
// if the application has no such handler.
func (f frame) HandlerName(key *uuidpb.UUID) string {
	if h, ok := f.App.Handler(key); ok {
		return h.Name
	}
	return key.AsString()
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

// MessageType returns the message type with the given ID.
func (f frame) MessageType(id *uuidpb.UUID) *observe.MessageType {
	return f.App.MessageType(id)
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
