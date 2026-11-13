// Command exampledemo runs the Dogma example application on a small engine
// cluster, and serves the application's own user interface beside the
// inspector.
//
// The example application, github.com/dogmatiq/example, implements a simple
// bank. Its user interface lets a person sign up, open accounts, and deposit,
// withdraw and transfer money. Each action executes a command on one of the
// cluster's nodes, so the inspector shows the engine handling it.
//
// It takes the connection string of a PostgreSQL server as its only argument:
//
//	cd internal/inspector/exampledemo
//	go run . postgres://user:password@localhost:5432/postgres
//
// It serves the bank at http://127.0.0.1:8071/ and the inspector at
// http://127.0.0.1:8071/inspector/.
//
// It runs the cluster with [democluster.Start], which creates a new database
// for it and drops the database when the command stops. The application's
// projections are kept in an in-memory SQLite database.
//
// This command is a separate Go module, so that the example application and
// its dependencies are not dependencies of the engine. Its go.mod pins the
// example application to the most recent version that uses the same version
// of Dogma as the engine.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/dogmatiq/example"
	"github.com/dogmatiq/example/ui"
	"github.com/dogmatiq/example/ui/projections"
	"github.com/dogmatiq/runkit/internal/inspector/democluster"
)

// listenAddress is the address of the bank and the inspector.
const listenAddress = "127.0.0.1:8071"

// eventWaitTimeout is the longest time that the bank's user interface waits
// for the events that a command causes. A transfer scheduled for later causes
// its events only when it runs, so the page returns after this time without
// them.
const eventWaitTimeout = 5 * time.Second

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: exampledemo <postgres-connection-string>")
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
	readDB, err := projections.NewDB()
	if err != nil {
		return fmt.Errorf("unable to create the projection database: %w", err)
	}
	defer readDB.Close()

	// Every node in this process writes to the same in-memory database, and
	// SQLite allows one writer at a time, so one connection serializes them.
	readDB.SetMaxOpenConns(1)

	app := &example.App{
		ReadDB: readDB,
	}

	cluster, err := democluster.Start(ctx, serverDSN, app)
	if err != nil {
		return err
	}

	fmt.Printf("database %q, read by role %q\n", cluster.Name, cluster.Name)

	bank := &ui.Handler{
		DB:              readDB,
		CommandExecutor: cluster,
	}

	mux := http.NewServeMux()
	mux.Handle("/inspector/", http.StripPrefix("/inspector", cluster.Inspector))

	// The bank's user interface waits for the events that its commands cause.
	// The engine waits for events only within a context that has a deadline,
	// and stops waiting at the deadline, so each request's context gets one.
	mux.HandleFunc(
		"/",
		func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), eventWaitTimeout)
			defer cancel()

			bank.ServeHTTP(w, r.WithContext(ctx))
		},
	)

	err = democluster.Serve(ctx, listenAddress, mux, "/", "/inspector/")

	if waitErr := cluster.Wait(); !errors.Is(waitErr, context.Canceled) {
		err = errors.Join(err, waitErr)
	}

	return err
}
