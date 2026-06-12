package xtesting

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// RunEngines runs the given Dogma application in a test engine and executes the given
// function while the engine is running.
//
// Multiple engines are run concurrently to ensure that behavior is consistent
// when multiple engines running against the same database.
//
// If any of the engines stops before fn returns, the context passed to fn is
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
			c.Identity(t.Name(), uuidpb.Generate().AsString())
			c.Routes(routes...)
		},
	}

	engineContext, stopEngines := context.WithCancel(t.Context())
	engineGroup, engineContext := errgroup.WithContext(engineContext)

	defer func() {
		stopEngines()
		engineGroup.Wait()
	}()

	testContext, stopTest := context.WithTimeout(t.Context(), 3*time.Second)
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
