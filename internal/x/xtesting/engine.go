package xtesting

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	"github.com/dogmatiq/spruce"
)

// RunApp runs the given Dogma application in a test engine and executes the
// given function while the engine is running.
//
// If the engine stops before the function returns the context passed to fn
// is canceled, and the test fails.
func RunApp(
	t testing.TB,
	app dogma.Application,
	fn func(context.Context, *dogmaengine.Engine),
) {
	t.Helper()

	db := databasetest.NewWithSchema(t)

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
			}
		}
	}()

	fn(testContext, engine)
}
