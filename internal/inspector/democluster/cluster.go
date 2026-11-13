// Package democluster runs a throwaway engine cluster in one process, so that
// the inspector can be seen working against a running application.
//
// It creates a new database with a unique name on a PostgreSQL server, and
// never touches any other database. The inspector reads that database as a new
// role that may only read. Both are dropped when the cluster stops.
package democluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/inspector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
)

const (
	// nodeCount is the number of engine nodes that run at once.
	nodeCount = 3

	// nodeLifetime is the time between stopping one node and starting
	// another, so that the inspector shows workloads moving between nodes.
	nodeLifetime = time.Minute

	// dropTimeout is the time allowed to drop the database and the role.
	dropTimeout = 10 * time.Second
)

// engineSchemas is the PostgreSQL schemas that hold the engine's tables.
var engineSchemas = []string{
	"application",
	"eventstream",
	"messagequeue",
	"aggregate",
	"process",
	"projection",
}

// Cluster is a throwaway engine cluster that runs in this process.
//
// It implements [dogma.CommandExecutor] by executing each command on one of
// its running nodes, chosen at random.
type Cluster struct {
	// Name is the name of the cluster's database, and of the role that the
	// inspector uses to read it.
	Name string

	// Inspector is the inspector for the cluster.
	Inspector *inspector.Inspector

	// server is a connection pool for the PostgreSQL server's default
	// database, used to create and drop the cluster's database and role.
	server *sql.DB

	// engineDB is a connection pool for the cluster's database, used by the
	// engine.
	engineDB *sql.DB

	// readerDB is a connection pool for the cluster's database, used by the
	// inspector as a role that may only read.
	readerDB *sql.DB

	// group runs the nodes and the inspector.
	group *errgroup.Group

	// m guards nodes.
	m sync.Mutex

	// nodes is the running nodes, oldest first.
	nodes []node
}

// node is one of the cluster's running engine nodes.
type node struct {
	Engine *runkit.Engine
	Stop   context.CancelFunc
}

// Start creates a database on the PostgreSQL server that serverDSN connects
// to, and runs app against it on [nodeCount] nodes until ctx is canceled.
// Every [nodeLifetime], it stops the oldest node and starts another.
//
// Call [Cluster.Wait] to wait for the cluster to stop and drop its database.
func Start(ctx context.Context, serverDSN string, app dogma.Application) (_ *Cluster, err error) {
	serverConfig, err := pgx.ParseConfig(serverDSN)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	c := &Cluster{
		Name:   "inspector-demo-" + uuidpb.Generate().AsString()[:8],
		server: stdlib.OpenDB(*serverConfig),
	}

	defer func() {
		if err != nil {
			c.drop()
		}
	}()

	password := uuidpb.Generate().AsString()

	if err := c.createDatabaseAndRole(ctx, password); err != nil {
		return nil, err
	}

	engineConfig := serverConfig.Copy()
	engineConfig.Database = c.Name
	c.engineDB = stdlib.OpenDB(*engineConfig)

	if err := runkit.CreateSchema(ctx, c.engineDB); err != nil {
		return nil, fmt.Errorf("unable to create the engine's schema: %w", err)
	}

	if err := c.grantRead(ctx); err != nil {
		return nil, err
	}

	readerConfig := engineConfig.Copy()
	readerConfig.User = c.Name
	readerConfig.Password = password
	c.readerDB = stdlib.OpenDB(*readerConfig)

	c.Inspector, err = inspector.New(app, c.readerDB)
	if err != nil {
		return nil, err
	}

	c.group, ctx = errgroup.WithContext(ctx)

	c.group.Go(func() error {
		return c.Inspector.Run(ctx)
	})

	c.group.Go(func() error {
		c.run(ctx, app)
		return ctx.Err()
	})

	return c, nil
}

// ExecuteCommand executes a command on one of the cluster's running nodes,
// chosen at random. It returns an error if no node is running.
func (c *Cluster) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	e := c.randomNode()
	if e == nil {
		return errors.New("no engine node is running")
	}

	return e.ExecuteCommand(ctx, command, options...)
}

// Wait waits for the cluster to stop, then drops its database and role.
func (c *Cluster) Wait() error {
	err := c.group.Wait()
	c.drop()
	return err
}

// run keeps [nodeCount] nodes running, and replaces one every
// [nodeLifetime], until ctx is canceled.
func (c *Cluster) run(ctx context.Context, app dogma.Application) {
	for range nodeCount {
		c.start(ctx, app)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(nodeLifetime):
		}

		c.stopOldest()
		c.start(ctx, app)
	}
}

// start starts a node, and adds it to the running nodes once it is ready.
func (c *Cluster) start(ctx context.Context, app dogma.Application) {
	nodeCtx, stop := context.WithCancel(ctx)

	e := runkit.New(
		app,
		c.engineDB,
		runkit.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	)

	go e.Run(nodeCtx)

	select {
	case <-e.Ready():
	case <-ctx.Done():
		stop()
		return
	}

	c.m.Lock()
	defer c.m.Unlock()

	c.nodes = append(
		c.nodes,
		node{
			Engine: e,
			Stop:   stop,
		},
	)
}

// stopOldest stops the node that has run for the longest time.
func (c *Cluster) stopOldest() {
	c.m.Lock()
	defer c.m.Unlock()

	if len(c.nodes) == 0 {
		return
	}

	c.nodes[0].Stop()
	c.nodes = c.nodes[1:]
}

// randomNode returns a running node's engine, chosen at random, or nil if
// there is none.
func (c *Cluster) randomNode() *runkit.Engine {
	c.m.Lock()
	defer c.m.Unlock()

	if len(c.nodes) == 0 {
		return nil
	}

	return c.nodes[rand.IntN(len(c.nodes))].Engine
}

// createDatabaseAndRole creates the cluster's database, and a role with the
// same name that may log in with the given password.
func (c *Cluster) createDatabaseAndRole(ctx context.Context, password string) error {
	ident := pgx.Identifier{c.Name}.Sanitize()

	if _, err := c.server.ExecContext(ctx, `CREATE DATABASE `+ident); err != nil {
		return fmt.Errorf("unable to create database: %w", err)
	}

	quoted := "'" + strings.ReplaceAll(password, "'", "''") + "'"

	if _, err := c.server.ExecContext(ctx, `CREATE ROLE `+ident+` LOGIN PASSWORD `+quoted); err != nil {
		return fmt.Errorf("unable to create role: %w", err)
	}

	return nil
}

// grantRead gives the cluster's role the privileges that the inspector needs,
// which are the privileges to read the engine's tables.
func (c *Cluster) grantRead(ctx context.Context) error {
	ident := pgx.Identifier{c.Name}.Sanitize()

	statements := []string{
		`GRANT CONNECT ON DATABASE ` + ident + ` TO ` + ident,
	}

	for _, schema := range engineSchemas {
		statements = append(
			statements,
			`GRANT USAGE ON SCHEMA `+schema+` TO `+ident,
			`GRANT SELECT ON ALL TABLES IN SCHEMA `+schema+` TO `+ident,
		)
	}

	for _, s := range statements {
		if _, err := c.engineDB.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("unable to grant read privileges: %w", err)
		}
	}

	return nil
}

// drop closes the cluster's connection pools, and drops its database and
// role.
func (c *Cluster) drop() {
	if c.readerDB != nil {
		c.readerDB.Close()
	}

	if c.engineDB != nil {
		c.engineDB.Close()
	}

	defer c.server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), dropTimeout)
	defer cancel()

	ident := pgx.Identifier{c.Name}.Sanitize()

	if _, err := c.server.ExecContext(ctx, `DROP DATABASE IF EXISTS `+ident+` WITH (FORCE)`); err != nil {
		fmt.Fprintln(os.Stderr, "unable to drop database:", err)
	}

	if _, err := c.server.ExecContext(ctx, `DROP ROLE IF EXISTS `+ident); err != nil {
		fmt.Fprintln(os.Stderr, "unable to drop role:", err)
	}
}

// Serve serves h on a port of the loopback interface that the operating
// system chooses, until ctx is canceled. It prints the URL of each of the
// given paths once it is listening.
func Serve(ctx context.Context, h http.Handler, paths ...string) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	for _, p := range paths {
		fmt.Printf("http://%s%s\n", listener.Addr(), p)
	}

	server := &http.Server{
		Handler: h,
	}

	go func() {
		<-ctx.Done()
		server.Close()
	}()

	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return ctx.Err()
}
