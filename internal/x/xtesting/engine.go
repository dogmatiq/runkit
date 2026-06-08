package xtesting

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/spruce"
)

// Run runs the given Dogma application in a test engine and executes the
// given function while the engine is running.
//
// If the engine stops before the function returns the context passed to fn
// is canceled, and the test fails.
func Run(
	t testing.TB,
	app dogma.Application,
	fn func(context.Context, *dogmaengine.Engine),
) {
	t.Helper()

	db := NewDatabase(t)

	engine := &dogmaengine.Engine{
		DB:     db,
		App:    app,
		Logger: spruce.NewTestLogger(t),
	}

	var engineStopped xsync.Latch
	engineContext, stopEngine := context.WithCancel(t.Context())
	defer func() {
		stopEngine()
		engineStopped.Wait()
	}()

	testContext, stopTest := context.WithTimeout(t.Context(), 3*time.Second)
	defer stopTest()

	go func() {
		defer stopTest()
		defer engineStopped.Set()

		if err := engine.Run(engineContext); err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("engine stopped unexpectedly: %v", err)
				return
			}
		}
	}()

	fn(testContext, engine)
}

// ExecuteCommand executes the given command on the engine, and fails the test
// if it returns an error.
func ExecuteCommand(
	t testing.TB,
	engine *dogmaengine.Engine,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) {
	t.Helper()

	if err := engine.ExecuteCommand(
		t.Context(),
		command,
		options...,
	); err != nil {
		t.Fatalf("unable to execute command: %v", err)
	}
}
