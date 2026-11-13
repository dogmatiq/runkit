package pages

import (
	"fmt"
	"html/template"
	"time"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/enginekit/config"
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

	// History is the samples from the recent observations, oldest first.
	History []observe.Sample

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

// WorkloadBar returns a bar of the number of workloads that each live node
// owns. It returns an empty bar if the live nodes are not known.
func (f frame) WorkloadBar() template.HTML {
	var counts []observe.NodeCount
	if f.Obs != nil {
		counts = f.Obs.Workloads
	}

	return nodeBar(
		counts,
		func(c observe.NodeCount) string {
			return fmt.Sprintf("%s owned by node %s", countOf(c.Count, "workload", "workloads"), shortID(c.NodeID))
		},
	)
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

// point is a value at a time, on the database's clock.
type point struct {
	At    time.Time
	Value float64
}

// chart is the points of a sparkline, and the time at its right-hand edge.
type chart struct {
	Points []point
	End    time.Time
}

// chartMargin is how far before the start of a chart's window its points
// begin, so that the page can scroll the chart left until the next update
// without showing a gap at the left-hand edge.
const chartMargin = chartWindow / 10

// chartLead is how far behind the most recent observation a chart's
// right-hand edge is. It is longer than the time between two observations
// that count handled events, so each new point is placed beyond the edge and
// scrolls in, instead of appearing at the edge.
const chartLead = 10 * time.Second

// Chart returns a chart of the given points, whose right-hand edge is
// [chartLead] before the time of the most recent observation.
func (f frame) Chart(points []point) chart {
	c := chart{
		Points: points,
	}

	if len(f.History) > 0 {
		c.End = f.History[len(f.History)-1].At.Add(-chartLead)
	}

	return c
}

// rate returns the change per second in a counter between each pair of
// consecutive samples that read it, over the given window and its margin. Each
// point has the time of the later sample of its pair.
func rate(samples []observe.Sample, window time.Duration, counter func(observe.Sample) observe.Total) []point {
	if len(samples) == 0 {
		return nil
	}

	since := samples[len(samples)-1].At.Add(-chartLead - window - chartMargin)

	var (
		points []point
		prev   *observe.Sample
	)

	for i := range samples {
		cur := &samples[i]
		if !counter(*cur).IsKnown {
			continue
		}

		if prev != nil && !cur.At.Before(since) {
			if seconds := cur.At.Sub(prev.At).Seconds(); seconds > 0 {
				before, after := counter(*prev).Value, counter(*cur).Value

				// A total can go down, such as when an event stream
				// disappears from the observed streams, or PostgreSQL's
				// statistics are reset. That is not a negative rate.
				change := 0.0
				if after > before {
					change = float64(after - before)
				}

				points = append(points, point{cur.At, change / seconds})
			}
		}

		prev = cur
	}

	return points
}

// smooth returns the given rates with each one replaced by the average of it
// and the [chartSmoothing] - 1 rates before it. A rate between two close
// observations is noisy, and the average follows the trend instead.
func smooth(rates []point) []point {
	smoothed := make([]point, len(rates))

	for i, r := range rates {
		recent := rates[max(0, i-chartSmoothing+1) : i+1]

		sum := 0.0
		for _, p := range recent {
			sum += p.Value
		}

		smoothed[i] = point{r.At, sum / float64(len(recent))}
	}

	return smoothed
}

// series returns the value of a total in each sample over the given window and
// its margin. It skips each sample that could not read the total.
func series(samples []observe.Sample, window time.Duration, total func(observe.Sample) observe.Total) []point {
	if len(samples) == 0 {
		return nil
	}

	since := samples[len(samples)-1].At.Add(-chartLead - window - chartMargin)

	var points []point

	for _, s := range samples {
		if t := total(s); t.IsKnown && !s.At.Before(since) {
			points = append(points, point{s.At, float64(t.Value)})
		}
	}

	return points
}

// handlerRate is how fast a handler records or handles events.
type handlerRate struct {
	// Chart is the number of events per second between each pair of
	// consecutive samples that counted them.
	Chart chart
}

// HandlerRate returns how fast the given handler records events, for an
// aggregate or integration handler, or handles them, for a process or
// projection handler. It returns nil for an integration handler that records
// no events.
func (f frame) HandlerRate(h *observe.Handler) *handlerRate {
	key := h.Key.AsString()

	if h.Type == config.IntegrationHandlerType && !h.RecordsEvents() {
		return nil
	}

	return &handlerRate{
		Chart: f.Chart(
			smooth(
				rate(
					f.History,
					chartWindow,
					func(s observe.Sample) observe.Total { return s.HandlerEvents[key] },
				),
			),
		),
	}
}

// perMinute returns the average of the most recent few of the given rates per
// second, as a rate per minute. The rate between two observations is noisy, so
// one alone is not shown.
func perMinute(rates []point) float64 {
	recent := rates[max(0, len(rates)-rateSmoothing):]
	if len(recent) == 0 {
		return 0
	}

	sum := 0.0
	for _, r := range recent {
		sum += r.Value
	}

	return sum / float64(len(recent)) * 60
}
