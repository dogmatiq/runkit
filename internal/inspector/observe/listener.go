package observe

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	// notificationChannel is the PostgreSQL notification channel on which the
	// engine sends its signals. The payload of each notification is a signal,
	// such as "messagequeue" or "heartbeat.<node-id>".
	notificationChannel = "signal"

	// heartbeatPrefix is the start of the signal that a node sends as its
	// heartbeat. The rest of the signal is the node's ID.
	heartbeatPrefix = "heartbeat."
)

// Listener listens for the signals that the engine's nodes send, and keeps
// track of which nodes are live.
//
// It only listens. It never sends a signal.
type Listener struct {
	// DB is the engine's database.
	DB *sql.DB

	// HeartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before the listener no longer considers it live. It must match
	// the timeout that the engine uses.
	HeartbeatTimeout time.Duration

	// RetryDelay is the time to wait before listening again after the
	// connection fails.
	RetryDelay time.Duration

	// OnHeartbeat is called with the ID of the node that sent each
	// heartbeat.
	OnHeartbeat func(nodeID *uuidpb.UUID)

	// OnChange is called for each signal other than a heartbeat. Such a signal means that the engine has added messages to a
	// queue, created an event stream or appended to one.
	OnChange func()

	// m guards the fields below it.
	m sync.Mutex

	// listeningSince is the time that the listener began listening, or zero
	// if it is not listening.
	listeningSince time.Time

	// listenErr explains why the listener is not listening, or is nil.
	listenErr error

	// heartbeats maps the ID of each node to the time its most recent
	// heartbeat arrived.
	heartbeats uuidcollection.Map[time.Time]
}

// LiveNodes returns the nodes that the listener considers live at the given
// time.
func (l *Listener) LiveNodes(now time.Time) LiveNodes {
	l.m.Lock()
	defer l.m.Unlock()

	if l.listeningSince.IsZero() {
		reason := l.listenErr
		if reason == nil {
			reason = errors.New("not listening for heartbeats yet")
		}

		return LiveNodes{
			Reason: reason,
		}
	}

	// A node that is live may not have sent a heartbeat since the listener
	// began listening, so the set is not complete until a whole timeout has
	// passed.
	if now.Sub(l.listeningSince) < l.HeartbeatTimeout {
		return LiveNodes{
			Reason: fmt.Errorf("listening for heartbeats for less than %s", l.HeartbeatTimeout),
		}
	}

	live := LiveNodes{
		Known: true,
		set:   &uuidcollection.Set{},
	}

	for id, at := range l.heartbeats.All() {
		if now.Sub(at) > l.HeartbeatTimeout {
			l.heartbeats.Delete(id)
			continue
		}

		live.Nodes = append(
			live.Nodes,
			LiveNode{
				ID:              id,
				LastHeartbeatAt: at,
			},
		)
		live.set.Add(id)
	}

	slices.SortFunc(
		live.Nodes,
		func(a, b LiveNode) int {
			return cmp.Compare(a.ID.AsString(), b.ID.AsString())
		},
	)

	return live
}

// Run listens for signals until ctx is canceled. If the connection fails, it
// listens again after [Listener.RetryDelay].
func (l *Listener) Run(ctx context.Context) error {
	if l.HeartbeatTimeout <= 0 {
		panic("heartbeat timeout must be positive")
	}

	if l.RetryDelay <= 0 {
		panic("retry delay must be positive")
	}

	if l.OnHeartbeat == nil || l.OnChange == nil {
		panic("heartbeat and change callbacks must not be nil")
	}

	for {
		err := l.listen(ctx)
		l.stopListening(err)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.RetryDelay):
		}
	}
}

// listen holds a dedicated connection that listens for signals until the
// connection fails or ctx is canceled.
func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(
		func(raw any) error {
			conn, ok := raw.(*stdlib.Conn)
			if !ok {
				return fmt.Errorf("unexpected connection type: %T", raw)
			}
			pgConn := conn.Conn()

			// Close the underlying connection so that a connection that is
			// still listening is not returned to the pool.
			defer pgConn.Close(context.Background())

			if _, err := pgConn.Exec(
				ctx,
				"LISTEN "+pgx.Identifier{notificationChannel}.Sanitize(),
			); err != nil {
				return fmt.Errorf("unable to listen for signals: %w", err)
			}

			l.startListening()

			for {
				n, err := pgConn.WaitForNotification(ctx)
				if err != nil {
					return fmt.Errorf("unable to wait for signal: %w", err)
				}

				l.receive(n.Payload)
			}
		},
	)
}

// receive handles one signal.
func (l *Listener) receive(signal string) {
	idText, ok := strings.CutPrefix(signal, heartbeatPrefix)
	if !ok {
		l.OnChange()
		return
	}

	nodeID, err := uuidpb.Parse(idText)
	if err != nil {
		return
	}

	l.recordHeartbeat(nodeID)

	l.OnHeartbeat(nodeID)
}

func (l *Listener) recordHeartbeat(nodeID *uuidpb.UUID) {
	l.m.Lock()
	defer l.m.Unlock()

	l.heartbeats.Set(nodeID, time.Now())
}

func (l *Listener) startListening() {
	l.m.Lock()
	defer l.m.Unlock()

	l.listeningSince = time.Now()
	l.listenErr = nil
}

func (l *Listener) stopListening(err error) {
	l.m.Lock()
	defer l.m.Unlock()

	l.listeningSince = time.Time{}
	l.listenErr = err
	l.heartbeats.Clear()
}

// LiveNodes is the set of nodes that the listener considers live at one
// time.
type LiveNodes struct {
	// Known is false if the listener can not tell which nodes are live, either
	// because it is not listening, or because it has not listened for long
	// enough.
	Known bool

	// Reason explains why the live nodes are not known, if Known is false.
	Reason error

	// Nodes is the live nodes, ordered by ID.
	Nodes []LiveNode

	// set is the set of the IDs in Nodes.
	set *uuidcollection.Set
}

// FindWorkloadOwner returns the ID of the node that owns the workload with the
// given key. It returns false if the live nodes are not known, or if there are
// no live nodes.
//
// It chooses the owner the same way that every node does, so it agrees with
// the nodes while they all see the same live nodes.
func (l LiveNodes) FindWorkloadOwner(key *uuidpb.UUID) (*uuidpb.UUID, bool) {
	if !l.Known {
		return nil, false
	}

	return cluster.FindWorkloadOwner(key, l.set)
}

// LiveNode is a node that has sent a heartbeat within the heartbeat timeout.
type LiveNode struct {
	// ID is the node's ID.
	ID *uuidpb.UUID

	// LastHeartbeatAt is the time that the node's most recent heartbeat
	// arrived.
	LastHeartbeatAt time.Time
}
