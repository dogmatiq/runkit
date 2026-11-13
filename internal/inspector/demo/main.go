// Command demo runs a small engine cluster and serves the inspector for it,
// so that the inspector can be seen working.
//
// It takes the connection string of a PostgreSQL server as its only argument:
//
//	go run ./internal/inspector/demo postgres://user:password@localhost:5432/postgres
//
// It runs a small banking application on a [democluster.Cluster], keeps
// executing commands against it, and serves the inspector at
// http://127.0.0.1:8070/.
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

const (
	// listenAddress is the address of the inspector.
	listenAddress = "127.0.0.1:8070"

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
	app := &demoapp.App{
		StatementInterval: 20 * time.Second,
		MailServer: &cyclingMailServer{
			start: time.Now(),
		},
	}

	cluster, err := democluster.Start(ctx, serverDSN, app)
	if err != nil {
		return err
	}

	fmt.Printf("database %q, read by role %q\n", cluster.Name, cluster.Name)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return democluster.Serve(ctx, listenAddress, cluster.Inspector, "/")
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

// mailCycle is the length of the repeating cycle of [cyclingMailServer].
const mailCycle = 3 * time.Minute

// cyclingMailServer is a [demoapp.MailServer] whose behavior follows a
// repeating cycle, so that the inspector shows the mailer's queue backing up
// and draining, and messages that fail repeatedly and then succeed.
//
// For the first 90 seconds of each cycle it sends each statement quickly, with
// an occasional slow one. For the next 50 seconds it is slow. The mailer
// integration handles one command at a time, so its queue backs up without
// any failures. For the next 25 seconds every attempt fails. For the rest of
// the cycle it is quick again.
type cyclingMailServer struct {
	start time.Time
}

func (s *cyclingMailServer) Send(ctx context.Context, _ string, _ int) error {
	var (
		delay   time.Duration
		refused bool
	)

	switch at := time.Since(s.start) % mailCycle; {
	case at >= 90*time.Second && at < 140*time.Second:
		delay = 1500*time.Millisecond + randomDuration(time.Second)
	case at >= 140*time.Second && at < 165*time.Second:
		delay = 50 * time.Millisecond
		refused = true
	case rand.IntN(10) == 0:
		delay = 400*time.Millisecond + randomDuration(500*time.Millisecond)
	default:
		delay = 20*time.Millisecond + randomDuration(60*time.Millisecond)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
	}

	if refused {
		return errors.New("mail server refused the connection")
	}

	return nil
}

// randomDuration returns a random duration from zero up to, but not including,
// d.
func randomDuration(d time.Duration) time.Duration {
	return time.Duration(rand.Int64N(int64(d)))
}

// holders is the names of the demo's account holders.
var holders = []string{
	"Alex", "Bea", "Chidi", "Dana", "Eli", "Fatima", "Goran", "Hana",
	"Ines", "Jun", "Kofi", "Lena", "Mateo", "Noor", "Oskar", "Priya",
}
