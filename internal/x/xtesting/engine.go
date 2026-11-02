package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

const (
	// appKey is the unique key used for Dogma applications within tests.
	appKey = "f1d3a3b4-5b6c-4d7e-8f9a-0b1c2d3e4f5a"

	// concurrentEngines is the number of concurrent engines to run in tests.
	concurrentEngines = 3
)

// ProjectionCompactInterval is the projection compaction interval configured
// on engines started by [RunEngines] and related functions.
const ProjectionCompactInterval = 10 * time.Millisecond

// RunEngines runs the given Dogma application in a test engine and executes the
// given function while the engine is running.
//
// Multiple engines are run concurrently to ensure that behavior is consistent
// when multiple engines running against the same database.
//
// If any of the engines stop before fn returns, the context passed to fn is
// canceled, and the test fails.
func RunEngines(
	t *testing.T,
	fn func(
		testing.TB,
		*runkit.Engine,
		*sql.DB,
	),
	routes ...dogma.HandlerRoute,
) {
	t.Helper()

	SetupThenRunEngines(
		t,
		func(testing.TB, *sql.DB) {},
		fn,
		routes...,
	)
}

// SetupThenRunEngines is like [RunEngines] but accepts a setup function that
// receives the test database before any engines start.
func SetupThenRunEngines(
	t *testing.T,
	setup func(testing.TB, *sql.DB),
	fn func(testing.TB, *runkit.Engine, *sql.DB),
	routes ...dogma.HandlerRoute,
) {
	t.Helper()
	t.Parallel()

	db := NewDatabase(t)
	setup(t, db)

	RunEnginesWithDB(t, db, fn, routes...)
}

// RunEnginesWithDB is like [RunEngines] but uses the given database instead of
// creating one.
//
// Unlike [RunEngines] and [SetupThenRunEngines], it does not call t.Parallel().
func RunEnginesWithDB(
	t *testing.T,
	db *sql.DB,
	fn func(testing.TB, *runkit.Engine, *sql.DB),
	routes ...dogma.HandlerRoute,
) {
	t.Helper()

	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity(t.Name(), appKey)
			c.Routes(routes...)
		},
	}

	engineContext, stopEngines := context.WithCancel(t.Context())
	engineGroup, engineContext := errgroup.WithContext(engineContext)

	defer func() {
		stopEngines()
		engineGroup.Wait()
	}()

	testTimeout := 10 * time.Second
	if os.Getenv("CI") != "" {
		testTimeout = 1 * time.Minute
	}

	// runContext is canceled if any engine stops. The test's own timeout
	// starts once the engines have converged.
	runContext, stopRun := context.WithCancel(t.Context())
	defer stopRun()

	logger := spruce.NewTestLogger(t)

	var engine *runkit.Engine
	nodes := make(chan contexthook.Node, concurrentEngines)

	for idx := range concurrentEngines {
		e := runkit.New(
			app,
			db,
			runkit.WithProjectionCompactInterval(ProjectionCompactInterval),
			runkit.WithListenAddress("127.0.0.1:0"),
			runkit.WithLogger(logger.With("engine", idx)),
		)

		if engine == nil {
			engine = e
		}

		engineGroup.Go(func() error {
			defer stopRun()

			ctx := contexthook.With(
				engineContext,
				func(n contexthook.Node) {
					nodes <- n
				},
			)

			err := e.Run(ctx)

			if !errors.Is(err, context.Canceled) {
				t.Errorf("engine %d stopped unexpectedly: %v", idx, err)
			}

			return err
		})

	}

	select {
	case <-engineContext.Done():
	case <-engine.Ready():
	}

	// Wait for every engine to see all of the others, so that the test starts
	// from a cluster that has converged.
	convergeContext, cancelConverge := context.WithTimeout(runContext, WaitTimeout)
	defer cancelConverge()

	for range concurrentEngines {
		var n contexthook.Node

		select {
		case <-engineContext.Done():
		case <-convergeContext.Done():
			t.Fatal("timeout waiting for engines to join the cluster")
		case n = <-nodes:
		}

		for n.Node != nil {
			peers, changed := n.Node.Peers()
			if peers.Len() == concurrentEngines-1 {
				break
			}

			select {
			case <-engineContext.Done():
				n.Node = nil
			case <-convergeContext.Done():
				t.Fatalf("timeout waiting for engines to see each other: %d of %d peers", peers.Len(), concurrentEngines-1)
			case <-changed:
			}
		}
	}

	testContext, stopTest := context.WithTimeout(runContext, testTimeout)
	defer stopTest()

	fn(
		testingTBWithContext{t, testContext},
		engine,
		db,
	)
}

// ExecuteCommand executes the given command on the engine, and fails the test
// if it returns an error.
func ExecuteCommand(
	t testing.TB,
	engine *runkit.Engine,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) *envelopepb.Envelope {
	t.Helper()

	return ExecuteCommandWithHook(
		t,
		engine,
		command,
		func(contexthook.ExecuteCommand) {},
		options...,
	)
}

// ExecuteCommandAndWait executes the given command on the engine and waits for
// it to be removed from the command queue.
func ExecuteCommandAndWait(
	t testing.TB,
	engine *runkit.Engine,
	db *sql.DB,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) *envelopepb.Envelope {
	t.Helper()

	commandEnvelope := ExecuteCommand(t, engine, command, options...)

	WaitForCommandToBeRemovedFromQueue(
		t,
		db,
		commandEnvelope.GetBody().GetMessageId(),
	)

	return commandEnvelope
}

// ExecuteCommandsSequentially executes the given commands on the engine
// sequentially, and fails the test if any of them returns an error.
func ExecuteCommandsSequentially(
	t testing.TB,
	engine *runkit.Engine,
	db *sql.DB,
	commands ...dogma.Command,
) []*envelopepb.Envelope {
	t.Helper()

	var commandEnvelopes []*envelopepb.Envelope

	for _, command := range commands {
		commandEnvelope := ExecuteCommandAndWait(t, engine, db, command)
		commandEnvelopes = append(commandEnvelopes, commandEnvelope)
	}

	return commandEnvelopes
}

// ExecuteCommandWithHook executes the given command on the engine, and fails
// the test if it returns an error.
func ExecuteCommandWithHook(
	t testing.TB,
	engine *runkit.Engine,
	command dogma.Command,
	hook func(contexthook.ExecuteCommand),
	options ...dogma.ExecuteCommandOption,
) *envelopepb.Envelope {
	t.Helper()

	var commandEnvelope *envelopepb.Envelope

	ctx := contexthook.With(
		t.Context(),
		func(x contexthook.ExecuteCommand) {
			hook(x)
			commandEnvelope = x.CommandEnvelope
		},
	)

	if err := engine.ExecuteCommand(
		ctx,
		command,
		options...,
	); err != nil {
		t.Fatalf("unable to execute command: %v", err)
	}

	return commandEnvelope
}

type testingTBWithContext struct {
	testing.TB
	ctx context.Context
}

func (t testingTBWithContext) Context() context.Context {
	return t.ctx
}
