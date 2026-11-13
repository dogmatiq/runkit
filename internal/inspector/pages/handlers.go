package pages

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

const (
	// chartWindow is the period of time that the charts show.
	chartWindow = 5 * time.Minute

	// rateSmoothing is the number of recent rates that the current rate of
	// recorded events averages.
	rateSmoothing = 8

	// pageSize is the number of messages, events or instances in one page
	// of a list.
	pageSize = 50

	// maxReplayedEvents is the largest number of events that the inspector
	// applies to an aggregate root to show its current state.
	maxReplayedEvents = 10_000
)

func (s *Server) notFound(w http.ResponseWriter, r *http.Request, f frame) {
	f.Title = "Not found"
	render(w, http.StatusNotFound, "notfound.html", f)
}

func (s *Server) overview(w http.ResponseWriter, r *http.Request, f frame) {
	f.Title = "Overview"
	f.Section = "overview"

	history := s.Observer.History()

	eventRates := rate(
		history,
		chartWindow,
		func(s observe.Sample) observe.Counter { return s.Events },
	)

	removalRates := rate(
		history,
		chartWindow,
		func(s observe.Sample) observe.Counter { return s.RemovedMessages },
	)

	data := struct {
		frame
		EventRates               []float64
		EventsPerMinute          float64
		RemovalRates             []float64
		RemovedMessagesPerMinute float64
	}{
		frame:                    f,
		EventRates:               eventRates,
		EventsPerMinute:          perMinute(eventRates),
		RemovalRates:             removalRates,
		RemovedMessagesPerMinute: perMinute(removalRates),
	}

	render(w, http.StatusOK, "overview.html", data)
}

func (s *Server) nodes(w http.ResponseWriter, r *http.Request, f frame) {
	f.Title = "Nodes"
	f.Section = "nodes"

	// nodeWorkloads is the workloads that one node owns.
	type nodeWorkloads struct {
		Node            observe.LiveNode
		Queues          []queueView
		ConsumedStreams []consumedStreams
	}

	var nodes []*nodeWorkloads

	if f.Obs != nil {
		byID := map[string]*nodeWorkloads{}

		for _, n := range f.Obs.LiveNodes.Nodes {
			workloads := &nodeWorkloads{
				Node: n,
			}
			nodes = append(nodes, workloads)
			byID[n.ID.AsString()] = workloads
		}

		for _, q := range f.Obs.Queues {
			v := f.QueueView(q)
			if v.DeliveredBy.ID == nil {
				continue
			}
			if workloads, ok := byID[v.DeliveredBy.ID.AsString()]; ok {
				workloads.Queues = append(workloads.Queues, v)
			}
		}

		for _, h := range s.Application.Handlers {
			if !h.HandlesEvents() {
				continue
			}

			counts := map[string]int{}
			for _, es := range f.Obs.EventStreams {
				if id, ok := f.Obs.LiveNodes.FindWorkloadOwner(h.StreamWorkloadKey(es.ID)); ok {
					counts[id.AsString()]++
				}
			}

			for id, n := range counts {
				if workloads, ok := byID[id]; ok {
					workloads.ConsumedStreams = append(
						workloads.ConsumedStreams,
						consumedStreams{
							Handler:      h,
							EventStreams: n,
						},
					)
				}
			}
		}
	}

	render(
		w,
		http.StatusOK,
		"nodes.html",
		struct {
			frame
			Nodes []*nodeWorkloads
		}{
			frame: f,
			Nodes: nodes,
		},
	)
}

// consumedStreams is the number of event streams that one node consumes for
// one handler.
type consumedStreams struct {
	Handler      *observe.Handler
	EventStreams int
}

func (s *Server) queues(w http.ResponseWriter, r *http.Request, f frame) {
	f.Title = "Queues"
	f.Section = "queues"

	var queues []queueView
	if f.Obs != nil {
		for _, q := range f.Obs.Queues {
			queues = append(queues, f.QueueView(q))
		}
	}

	render(
		w,
		http.StatusOK,
		"queues.html",
		struct {
			frame
			Queues []queueView
		}{
			frame:  f,
			Queues: queues,
		},
	)
}

func (s *Server) queue(w http.ResponseWriter, r *http.Request, f frame) {
	id, ok := parseUUID(r.PathValue("id"))
	if !ok {
		s.notFound(w, r, f)
		return
	}

	f.Title = "Queue " + shortID(id)
	f.Section = "queues"

	data := struct {
		frame
		ID          *uuidpb.UUID
		DeliveredBy ownerView
		Queue       observe.Queue
		HasMessages bool
		Messages    []observe.QueuedMessage
		Err         error
		Limit       int
	}{
		frame:       f,
		ID:          id,
		DeliveredBy: f.Owner(id),
		Limit:       pageSize * 2,
	}

	if f.Obs != nil {
		data.Queue, data.HasMessages = f.Obs.Queue(id)
	}

	data.Messages, data.Err = s.Database.QueuedMessages(r.Context(), id, data.Limit)

	render(w, http.StatusOK, "queue.html", data)
}

func (s *Server) message(w http.ResponseWriter, r *http.Request, f frame) {
	id, ok := parseUUID(r.PathValue("id"))
	if !ok {
		s.notFound(w, r, f)
		return
	}

	f.Title = "Message " + shortID(id)

	data := struct {
		frame
		ID       *uuidpb.UUID
		Queued   observe.QueuedMessage
		IsQueued bool
		Event    observe.Event
		IsEvent  bool
		Err      error
	}{
		frame: f,
		ID:    id,
	}

	data.Queued, data.IsQueued, data.Err = s.Database.QueuedMessage(r.Context(), id)

	if data.Err == nil && !data.IsQueued {
		data.Event, data.IsEvent, data.Err = s.Database.Event(r.Context(), id)
	}

	status := http.StatusOK
	isMissing := !data.IsQueued && !data.IsEvent
	if data.Err == nil && isMissing {
		status = http.StatusNotFound
	}

	render(w, status, "message.html", data)
}

func (s *Server) eventStreams(w http.ResponseWriter, r *http.Request, f frame) {
	f.Title = "Event streams"
	f.Section = "streams"

	render(w, http.StatusOK, "streams.html", f)
}

func (s *Server) eventStream(w http.ResponseWriter, r *http.Request, f frame) {
	id, ok := parseUUID(r.PathValue("id"))
	if !ok {
		s.notFound(w, r, f)
		return
	}

	f.Title = "Event stream " + shortID(id)
	f.Section = "streams"

	data := struct {
		frame
		ID        *uuidpb.UUID
		Stream    observe.EventStream
		IsKnown   bool
		Before    uint64
		IsNewest  bool
		Events    []observe.Event
		Err       error
		Progress  []streamProgressView
		OlderLink string
	}{
		frame: f,
		ID:    id,
	}

	if f.Obs != nil {
		data.Stream, data.IsKnown = f.Obs.EventStream(id)
	}

	// The newest page reads from the end of the stream, which may be past
	// its length in the most recent observation. Offsets are stored as
	// bigint, so no offset is larger than math.MaxInt64.
	data.Before = math.MaxInt64
	data.IsNewest = true

	if b, err := strconv.ParseUint(r.URL.Query().Get("before"), 10, 64); err == nil {
		data.Before = min(b, math.MaxInt64)
		data.IsNewest = false
	}

	data.Events, data.Err = s.Database.EventStreamEvents(r.Context(), id, data.Before, pageSize)

	if n := len(data.Events); n == pageSize {
		oldest := data.Events[n-1].Offset
		data.OlderLink = "streams/" + id.AsString() + "?before=" + strconv.FormatUint(oldest, 10)
	}

	if f.Obs != nil {
		for _, summary := range f.Obs.Handlers {
			for _, p := range summary.Progress {
				if p.StreamID.Equal(id) {
					data.Progress = append(
						data.Progress,
						streamProgressView{
							Handler:    summary.Handler,
							Progress:   p,
							ConsumedBy: f.Owner(summary.Handler.StreamWorkloadKey(id)),
						},
					)
				}
			}
		}
	}

	render(w, http.StatusOK, "stream.html", data)
}

// streamProgressView is how far a handler has got through an event stream,
// as a page shows it.
type streamProgressView struct {
	Handler    *observe.Handler
	Progress   observe.StreamProgress
	ConsumedBy ownerView
}

func (s *Server) aggregates(w http.ResponseWriter, r *http.Request, f frame) {
	s.handlerList(w, f, "aggregate", "Aggregates")
}

func (s *Server) processes(w http.ResponseWriter, r *http.Request, f frame) {
	s.handlerList(w, f, "process", "Processes")
}

func (s *Server) integrations(w http.ResponseWriter, r *http.Request, f frame) {
	s.handlerList(w, f, "integration", "Integrations")
}

func (s *Server) projections(w http.ResponseWriter, r *http.Request, f frame) {
	s.handlerList(w, f, "projection", "Projections")
}

// handlerList renders the page that lists the handlers of one kind.
//
// The kind is the name of the handler type, such as "aggregate".
func (s *Server) handlerList(w http.ResponseWriter, f frame, kind, title string) {
	f.Title = title
	f.Section = kind + "s"

	render(
		w,
		http.StatusOK,
		"handlers.html",
		struct {
			frame
			Kind string
		}{
			frame: f,
			Kind:  kind,
		},
	)
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request, f frame) {
	key, ok := parseUUID(r.PathValue("key"))
	if !ok {
		s.notFound(w, r, f)
		return
	}

	h, ok := s.Application.Handler(key)
	if !ok {
		s.notFound(w, r, f)
		return
	}

	f.Title = h.Name
	f.Section = kindSection(h)

	data := struct {
		frame
		Handler           *observe.Handler
		Summary           observe.HandlerSummary
		Progress          []streamProgressView
		After             string
		NextLink          string
		Aggregates        []observe.AggregateInstance
		Processes         []processRow
		InstancesErr      error
		Commands          []observe.QueuedMessage
		CommandsErr       error
		Deadlines         []observe.QueuedMessage
		DeadlinesErr      error
		HandlerQueueOwner ownerView
	}{
		frame:             f,
		Handler:           h,
		After:             r.URL.Query().Get("after"),
		HandlerQueueOwner: f.Owner(h.Key),
	}

	if f.Obs != nil {
		data.Summary, _ = f.Obs.Handler(key)

		for _, p := range data.Summary.Progress {
			data.Progress = append(
				data.Progress,
				streamProgressView{
					Handler:    h,
					Progress:   p,
					ConsumedBy: f.Owner(h.StreamWorkloadKey(p.StreamID)),
				},
			)
		}
	}

	ctx := r.Context()

	// lastID is the ID of the last instance on the page, if the page is
	// full, so there may be more.
	lastID := ""

	switch h.Type {
	case config.AggregateHandlerType:
		data.Aggregates, data.InstancesErr = s.Database.AggregateInstances(ctx, key, data.After, pageSize)

		if len(data.Aggregates) == pageSize {
			lastID = data.Aggregates[pageSize-1].ID
		}

	case config.ProcessHandlerType:
		var instances []observe.ProcessInstance
		instances, data.InstancesErr = s.Database.ProcessInstances(ctx, key, data.After, pageSize)

		if len(instances) == pageSize {
			lastID = instances[pageSize-1].ID
		}

		for _, inst := range instances {
			row := processRow{
				Instance: inst,
			}

			// The engine discards the state of an instance that has ended, so
			// there is nothing to describe.
			if inst.HasEnded {
				data.Processes = append(data.Processes, row)
				continue
			}

			if root, err := observe.ProcessRoot(h, inst); err == nil {
				row.Description, row.DescriptionErr = observe.ProcessRootDescription(root, inst.HasEnded)
			} else {
				row.DescriptionErr = err
			}

			data.Processes = append(data.Processes, row)
		}

		data.Deadlines, data.DeadlinesErr = s.Database.QueuedDeadlines(ctx, key, "", pageSize)
	}

	if commandTypes := h.MessageTypeIDs(config.HandlesCommandRouteType); len(commandTypes) > 0 {
		data.Commands, data.CommandsErr = s.queuedCommands(ctx, f, commandTypes)
	}

	if lastID != "" {
		data.NextLink = "handlers/" + key.AsString() + "?after=" + url.QueryEscape(lastID)
	}

	render(w, http.StatusOK, "handler.html", data)
}

// processRow is one process instance in a list.
type processRow struct {
	Instance       observe.ProcessInstance
	Description    string
	DescriptionErr error
}

func (s *Server) instance(w http.ResponseWriter, r *http.Request, f frame) {
	key, ok := parseUUID(r.PathValue("key"))
	if !ok {
		s.notFound(w, r, f)
		return
	}

	h, ok := s.Application.Handler(key)
	if !ok {
		s.notFound(w, r, f)
		return
	}

	id := r.PathValue("id")
	ctx := r.Context()

	f.Title = h.Name + " " + id
	f.Section = kindSection(h)

	data := struct {
		frame
		Handler        *observe.Handler
		ID             string
		Found          bool
		Err            error
		Aggregate      observe.AggregateInstance
		Process        observe.ProcessInstance
		Description    string
		DescriptionErr error
		RestoredRoot   payloadView
		Events         []observe.Event
		EventsErr      error
		Deadlines      []observe.QueuedMessage
		DeadlinesErr   error
	}{
		frame:   f,
		Handler: h,
		ID:      id,
	}

	switch h.Type {
	case config.AggregateHandlerType:
		data.Aggregate, data.Found, data.Err = s.Database.AggregateInstance(ctx, key, id)
		if data.Found {
			root, err := s.Database.AggregateRoot(ctx, s.Application, h, data.Aggregate, maxReplayedEvents)
			if err == nil {
				data.RestoredRoot = format(root)
				data.Description, data.DescriptionErr = observe.AggregateRootDescription(root)
			} else {
				data.RestoredRoot.Err = err
			}

			data.Events, data.EventsErr = s.Database.AggregateInstanceEvents(ctx, key, id, pageSize)
		}

	case config.ProcessHandlerType:
		data.Process, data.Found, data.Err = s.Database.ProcessInstance(ctx, key, id)

		// The engine discards the state of an instance that has ended, so
		// there is no root to restore.
		if data.Found && !data.Process.HasEnded {
			root, err := observe.ProcessRoot(h, data.Process)
			if err == nil {
				data.RestoredRoot = format(root)
				data.Description, data.DescriptionErr = observe.ProcessRootDescription(root, data.Process.HasEnded)
			} else {
				data.RestoredRoot.Err = err
			}
		}

		if data.Found {
			data.Deadlines, data.DeadlinesErr = s.Database.QueuedDeadlines(ctx, key, id, pageSize)
		}

	default:
		s.notFound(w, r, f)
		return
	}

	status := http.StatusOK
	if data.Err == nil && !data.Found {
		status = http.StatusNotFound
	}

	render(w, status, "instance.html", data)
}

// kindSection returns the name of the navigation section for the handler's
// kind, such as "aggregates".
func kindSection(h *observe.Handler) string {
	return h.Type.String() + "s"
}

// parseUUID parses a UUID from a URL. It returns false if s is not a UUID.
func parseUUID(s string) (*uuidpb.UUID, bool) {
	id, err := uuidpb.Parse(s)
	return id, err == nil
}

// queuedCommands returns the queued commands of the given types, in the order
// that they become due, up to [pageSize].
//
// It looks only in the queues of the given frame's observation, so that the
// query can use the index on queue IDs.
func (s *Server) queuedCommands(
	ctx context.Context,
	f frame,
	typeIDs []*uuidpb.UUID,
) ([]observe.QueuedMessage, error) {
	if f.Obs == nil {
		return nil, nil
	}

	var queueIDs []*uuidpb.UUID
	for _, q := range f.Obs.Queues {
		queueIDs = append(queueIDs, q.ID)
	}

	return s.Database.QueuedCommands(ctx, queueIDs, typeIDs, pageSize)
}
