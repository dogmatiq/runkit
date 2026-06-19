package xtesting

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// appKey is the unique key used for Dogma applications within tests.
const appKey = "f1d3a3b4-5b6c-4d7e-8f9a-0b1c2d3e4f5a"

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
		*dogmaengine.Engine,
	),
	routes ...dogma.HandlerRoute,
) {
	t.Helper()
	t.Parallel()

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

	testTimeout := 3 * time.Second
	if os.Getenv("CI") != "" {
		testTimeout = 20 * time.Second
	}

	testContext, stopTest := context.WithTimeout(t.Context(), testTimeout)
	defer stopTest()

	db := NewDatabase(t)
	logger := spruce.NewTestLogger(t)

	const numEngines = 3
	var engine *dogmaengine.Engine

	for idx := range numEngines {
		e := &dogmaengine.Engine{
			DB:     db,
			App:    app,
			Logger: logger.With("engine", idx),
		}

		if engine == nil {
			engine = e
		}

		engineGroup.Go(func() error {
			defer stopTest()

			err := e.Run(engineContext)

			if !errors.Is(err, context.Canceled) {
				t.Errorf("engine %d stopped unexpectedly: %v", idx, err)
			}

			return err
		})
	}

	fn(
		testingTBWithContext{t, testContext},
		engine,
	)
}

// ExecuteCommand executes the given command on the engine, and fails the test
// if it returns an error.
func ExecuteCommand(
	t testing.TB,
	engine *dogmaengine.Engine,
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

// ExecuteCommandAndWait executes the given command on the engine and waits for it to be removed from the command queue.
func ExecuteCommandAndWait(
	t testing.TB,
	engine *dogmaengine.Engine,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) *envelopepb.Envelope {
	t.Helper()

	commandEnvelope := ExecuteCommand(t, engine, command, options...)

	WaitForCommandToBeRemovedFromQueue(
		t,
		engine.DB,
		commandEnvelope.GetBody().GetMessageId(),
	)

	return commandEnvelope
}

// ExecuteCommandsSequentially executes the given commands on the engine
// sequentially, and fails the test if any of them returns an error.
func ExecuteCommandsSequentially(
	t testing.TB,
	engine *dogmaengine.Engine,
	commands ...dogma.Command,
) []*envelopepb.Envelope {
	t.Helper()

	var commandEnvelopes []*envelopepb.Envelope

	for _, command := range commands {
		commandEnvelope := ExecuteCommandAndWait(t, engine, command)
		commandEnvelopes = append(commandEnvelopes, commandEnvelope)
	}

	return commandEnvelopes
}

// ExecuteCommandWithHook executes the given command on the engine, and fails
// the test if it returns an error.
func ExecuteCommandWithHook(
	t testing.TB,
	engine *dogmaengine.Engine,
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
