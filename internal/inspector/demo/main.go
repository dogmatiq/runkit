// Command demo runs a small engine cluster and serves the inspector for it,
// so that the inspector can be seen working.
//
// It takes the connection string of a PostgreSQL server as its only argument:
//
//	go run ./internal/inspector/demo postgres://user:password@localhost:5432/postgres
//
// It creates a new database with a unique name on that server, and never
// touches any other database. It runs three engine nodes in this process with
// a small banking application, keeps executing commands, and every minute
// stops one node and starts another. The inspector connects as a new role
// that may only read. When the command stops, it drops the database and the
// role.
package main

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
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/inspector"
	"github.com/dogmatiq/runkit/internal/inspector/demoapp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
)

const (
	// nodeCount is the number of engine nodes that run at once.
	nodeCount = 3

	// nodeLifetime is the time between stopping one node and starting
	// another.
	nodeLifetime = time.Minute

	// commandInterval is the average time between the commands that the demo
	// executes.
	commandInterval = 250 * time.Millisecond
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: demo <postgres-connection-string>")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1]); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, serverDSN string) error {
	serverConfig, err := pgx.ParseConfig(serverDSN)
	if err != nil {
		return fmt.Errorf("unable to parse connection string: %w", err)
	}

	server := stdlib.OpenDB(*serverConfig)
	defer server.Close()

	name := "inspector-demo-" + uuidpb.Generate().AsString()[:8]
	password := uuidpb.Generate().AsString()

	if err := createDatabaseAndRole(ctx, server, name, password); err != nil {
		return err
	}
	defer dropDatabaseAndRole(server, name)

	engineConfig := serverConfig.Copy()
	engineConfig.Database = name
	engineDB := stdlib.OpenDB(*engineConfig)
	defer engineDB.Close()

	if err := runkit.CreateSchema(ctx, engineDB); err != nil {
		return fmt.Errorf("unable to create the engine's schema: %w", err)
	}

	if err := grantRead(ctx, engineDB, name); err != nil {
		return err
	}

	readerConfig := engineConfig.Copy()
	readerConfig.User = name
	readerConfig.Password = password
	readerDB := stdlib.OpenDB(*readerConfig)
	defer readerDB.Close()

	app := &demoapp.App{
		StatementInterval: 20 * time.Second,
		MailerFailureRate: 0.3,
	}

	insp, err := inspector.New(app, readerDB)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("database %q, read by role %q\n", name, name)
	fmt.Printf("inspector: http://%s/\n", listener.Addr())

	httpServer := &http.Server{
		Handler: insp,
	}

	cluster := &cluster{
		app: app,
		db:  engineDB,
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return insp.Run(ctx)
	})

	g.Go(func() error {
		if err := httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return ctx.Err()
	})

	g.Go(func() error {
		<-ctx.Done()
		return httpServer.Close()
	})

	g.Go(func() error {
		cluster.Run(ctx)
		return ctx.Err()
	})

	g.Go(func() error {
		executeCommands(ctx, cluster)
		return ctx.Err()
	})

	return g.Wait()
}

// cluster runs the demo's engine nodes in this process.
type cluster struct {
	// app is the application that every node runs.
	app dogma.Application

	// db is the engine's database.
	db *sql.DB

	// m guards nodes.
	m sync.Mutex

	// nodes is the running nodes, oldest first.
	nodes []node
}

// node is one of the demo's running engine nodes.
type node struct {
	Engine *runkit.Engine
	Stop   context.CancelFunc
}

// Run keeps [nodeCount] nodes running, and replaces one every
// [nodeLifetime], until ctx is canceled.
func (c *cluster) Run(ctx context.Context) {
	for range nodeCount {
		c.start(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(nodeLifetime):
		}

		c.stopOldest()
		c.start(ctx)
	}
}

// Executor returns a running engine to execute a command on, or nil if there
// is none.
func (c *cluster) Executor() *runkit.Engine {
	c.m.Lock()
	defer c.m.Unlock()

	if len(c.nodes) == 0 {
		return nil
	}

	return c.nodes[rand.IntN(len(c.nodes))].Engine
}

// start starts a node, and adds it to the running nodes once it is ready.
func (c *cluster) start(ctx context.Context) {
	nodeCtx, stop := context.WithCancel(ctx)

	e := runkit.New(
		c.app,
		c.db,
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
func (c *cluster) stopOldest() {
	c.m.Lock()
	defer c.m.Unlock()

	if len(c.nodes) == 0 {
		return
	}

	c.nodes[0].Stop()
	c.nodes = c.nodes[1:]
}

// executeCommands executes random commands until ctx is canceled.
func executeCommands(ctx context.Context, c *cluster) {
	var accounts []string

	for {
		delay := time.Duration(rand.Int64N(int64(2 * commandInterval)))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		e := c.Executor()
		if e == nil {
			continue
		}

		var command dogma.Command

		if len(accounts) == 0 || rand.IntN(10) == 0 {
			id := fmt.Sprintf("acct-%04d", len(accounts)+1)
			accounts = append(accounts, id)
			command = &demoapp.OpenAccount{
				AccountID: id,
				Name:      holders[rand.IntN(len(holders))],
			}
		} else {
			id := accounts[rand.IntN(len(accounts))]
			amount := 100 * (1 + rand.Int64N(500))

			if rand.IntN(3) == 0 {
				command = &demoapp.Withdraw{
					AccountID: id,
					Amount:    amount,
				}
			} else {
				command = &demoapp.Deposit{
					AccountID: id,
					Amount:    amount,
				}
			}
		}

		// A node that is stopping can not execute commands, so failures are
		// expected and ignored.
		e.ExecuteCommand(ctx, command)
	}
}

// holders is the names of the demo's account holders.
var holders = []string{
	"Alex", "Bea", "Chidi", "Dana", "Eli", "Fatima", "Goran", "Hana",
	"Ines", "Jun", "Kofi", "Lena", "Mateo", "Noor", "Oskar", "Priya",
}

// createDatabaseAndRole creates the demo's database, and a role with the same
// name that may log in with the given password.
func createDatabaseAndRole(ctx context.Context, server *sql.DB, name, password string) error {
	ident := pgx.Identifier{name}.Sanitize()

	if _, err := server.ExecContext(ctx, `CREATE DATABASE `+ident); err != nil {
		return fmt.Errorf("unable to create database: %w", err)
	}

	quoted := "'" + strings.ReplaceAll(password, "'", "''") + "'"

	if _, err := server.ExecContext(ctx, `CREATE ROLE `+ident+` LOGIN PASSWORD `+quoted); err != nil {
		return fmt.Errorf("unable to create role: %w", err)
	}

	return nil
}

// grantRead gives the role with the given name the privileges that the
// inspector needs, which are the privileges to read the engine's tables.
func grantRead(ctx context.Context, db *sql.DB, name string) error {
	ident := pgx.Identifier{name}.Sanitize()

	statements := []string{
		`GRANT CONNECT ON DATABASE ` + ident + ` TO ` + ident,
	}

	for _, schema := range []string{
		"application",
		"eventstream",
		"messagequeue",
		"aggregate",
		"process",
		"projection",
	} {
		statements = append(
			statements,
			`GRANT USAGE ON SCHEMA `+schema+` TO `+ident,
			`GRANT SELECT ON ALL TABLES IN SCHEMA `+schema+` TO `+ident,
		)
	}

	for _, s := range statements {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("unable to grant read privileges: %w", err)
		}
	}

	return nil
}

// dropDatabaseAndRole drops the demo's database and role.
func dropDatabaseAndRole(server *sql.DB, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ident := pgx.Identifier{name}.Sanitize()

	if _, err := server.ExecContext(ctx, `DROP DATABASE IF EXISTS `+ident+` WITH (FORCE)`); err != nil {
		fmt.Fprintln(os.Stderr, "unable to drop database:", err)
	}

	if _, err := server.ExecContext(ctx, `DROP ROLE IF EXISTS `+ident); err != nil {
		fmt.Fprintln(os.Stderr, "unable to drop role:", err)
	}
}
