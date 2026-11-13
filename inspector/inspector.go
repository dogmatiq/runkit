// Package inspector serves a website that shows what a runkit engine cluster
// is doing, for the people who develop and operate a Dogma application.
//
// It shows the engine's nodes and the work that each one owns, the commands
// and deadlines waiting in each queue and whether their delivery is failing,
// the event streams and their newest events, and each of the application's
// handlers with their instances, their checkpoints and their progress through
// each event stream. Pages update live while they are open.
//
// # Reading, not writing
//
// The inspector never changes the engine's data and stores nothing of its
// own. Every query runs in a read-only transaction. Apart from those queries,
// it runs one LISTEN statement to hear the heartbeats of the engine's nodes.
// It never sends a notification and never calls one of the engine's SQL
// functions.
//
// The database user needs only these privileges:
//
//   - CONNECT on the database
//   - USAGE on the schemas application, eventstream, messagequeue, aggregate,
//     process and projection
//   - SELECT on every table in those schemas
//
// It can read from a read-only replica of the engine's database, but a
// replica can not deliver notifications, so the inspector can not tell which
// nodes are live, and the pages say so.
//
// # Application code
//
// The inspector is linked with the application, because the database holds
// only the keys of handlers and the IDs of message types. It calls these
// methods of application code:
//
//   - Configure, on the application and on each handler
//   - New, on aggregate and process handlers, to make a root
//   - UnmarshalBinary, on messages and roots, to show their content
//   - ApplyEvent, on an aggregate root, to show the root's current state
//   - AggregateInstanceDescription and ProcessInstanceDescription, on roots
//   - CheckpointOffset, on projection handlers
//
// The inspector relies on these methods having no effect outside the values
// that they return, which is how Dogma applications usually implement them.
// The guarantees above cover only the inspector's own queries. In particular,
// CheckpointOffset reads the projection's own storage, with whatever access
// the application was built with.
//
// # Access
//
// The inspector has no authentication of its own, and it shows the content of
// messages and the state of instances. Mount it behind whatever authentication
// already protects your operational tools, and serve it only to people who
// may read the engine's database.
package inspector

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/runkit/internal/inspector/observe"
	"github.com/dogmatiq/runkit/internal/inspector/pages"
	"golang.org/x/sync/errgroup"
)

const (
	// statementTimeout is the longest time that one of the inspector's SQL
	// statements may run.
	statementTimeout = 5 * time.Second

	// heartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before the inspector no longer considers it live. It must
	// equal the engine's heartbeat timeout, which is not configurable.
	heartbeatTimeout = 5 * time.Second

	// listenRetryDelay is the time to wait before listening again after the
	// listening connection fails.
	listenRetryDelay = 3 * time.Second

	// minObserveInterval is the shortest time between two readings of the
	// database.
	minObserveInterval = 1 * time.Second

	// maxObserveInterval is the longest time between two readings of the
	// database.
	maxObserveInterval = 3 * time.Second

	// slowObserveInterval is the shortest time between two readings of the
	// parts that cost the most to read.
	slowObserveInterval = 5 * time.Second

	// checkpointTimeout is the time allowed for all of the calls to one
	// projection handler's CheckpointOffset method in one reading.
	checkpointTimeout = 10 * time.Second

	// pageTTL is how long a rendered page is reused before the queries behind
	// it run again.
	pageTTL = 1 * time.Second

	// renderTimeout is the time allowed to render one page, including the
	// queries behind it.
	renderTimeout = 30 * time.Second
)

// Inspector is a website that shows what a runkit engine cluster is doing.
//
// It implements [http.Handler]. Its pages use relative links, so it works
// wherever it is mounted, for example under a prefix removed by
// [http.StripPrefix].
//
// Use [New] to create one. The pages update only while [Inspector.Run] is
// running.
type Inspector struct {
	listener *observe.Listener
	observer *observe.Observer
	server   *pages.Server
}

// New returns an inspector for the given application, which reads the
// engine's database through db.
//
// db must use the pgx driver, as the engine's database does. New returns an
// error if the application's configuration is invalid.
func New(app dogma.Application, db *sql.DB) (*Inspector, error) {
	application, err := observe.NewApplication(app)
	if err != nil {
		return nil, err
	}

	database := &observe.Database{
		DB:               db,
		StatementTimeout: statementTimeout,
	}

	broadcaster := &pages.Broadcaster{}

	listener := &observe.Listener{
		DB:               db,
		HeartbeatTimeout: heartbeatTimeout,
		RetryDelay:       listenRetryDelay,
		OnHeartbeat:      broadcaster.Heartbeat,
	}

	observer := &observe.Observer{
		Database:          database,
		Listener:          listener,
		Application:       application,
		MinInterval:       minObserveInterval,
		MaxInterval:       maxObserveInterval,
		SlowInterval:      slowObserveInterval,
		CheckpointTimeout: checkpointTimeout,
		OnObserve: func(*observe.Observation) {
			broadcaster.Observed()
		},
	}

	listener.OnChange = observer.Changed

	return &Inspector{
		listener: listener,
		observer: observer,
		server: &pages.Server{
			Application:   application,
			Database:      database,
			Observer:      observer,
			Broadcaster:   broadcaster,
			PageTTL:       pageTTL,
			RenderTimeout: renderTimeout,
		},
	}, nil
}

// Run reads the database and listens for the nodes' heartbeats until ctx is
// canceled.
//
// It must be called at most once.
func (i *Inspector) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return i.listener.Run(ctx)
	})

	g.Go(func() error {
		return i.observer.Run(ctx)
	})

	return g.Wait()
}

// ServeHTTP serves the inspector's pages. It serves only GET and HEAD
// requests.
func (i *Inspector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	i.server.ServeHTTP(w, r)
}
