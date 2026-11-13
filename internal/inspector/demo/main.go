// Command demo runs a small engine cluster and serves the inspector for it,
// so that the inspector can be seen working.
//
// It takes the connection string of a PostgreSQL server as its only argument:
//
//	go run ./internal/inspector/demo postgres://user:password@localhost:5432/postgres
//
// It runs a small banking application on a [democluster.Cluster], and keeps
// executing commands against it.
package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/runkit/internal/inspector/demoapp"
	"github.com/dogmatiq/runkit/internal/inspector/democluster"
	"golang.org/x/sync/errgroup"
)

// commandInterval is the average time between the commands that the demo
// executes.
const commandInterval = 250 * time.Millisecond

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
	app := &demoapp.App{
		StatementInterval: 20 * time.Second,
		MailerFailureRate: 0.3,
	}

	cluster, err := democluster.Start(ctx, serverDSN, app)
	if err != nil {
		return err
	}

	fmt.Printf("database %q, read by role %q\n", cluster.Name, cluster.Name)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return democluster.Serve(ctx, cluster.Inspector, "/")
	})

	g.Go(func() error {
		executeCommands(ctx, cluster)
		return ctx.Err()
	})

	err = g.Wait()

	if waitErr := cluster.Wait(); !errors.Is(waitErr, context.Canceled) {
		err = errors.Join(err, waitErr)
	}

	return err
}

// executeCommands executes random commands until ctx is canceled.
func executeCommands(ctx context.Context, executor dogma.CommandExecutor) {
	var accounts []string

	for {
		delay := time.Duration(rand.Int64N(int64(2 * commandInterval)))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
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
		executor.ExecuteCommand(ctx, command)
	}
}

// holders is the names of the demo's account holders.
var holders = []string{
	"Alex", "Bea", "Chidi", "Dana", "Eli", "Fatima", "Goran", "Hana",
	"Ines", "Jun", "Kofi", "Lena", "Mateo", "Noor", "Oskar", "Priya",
}
