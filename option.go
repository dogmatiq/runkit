package runkit

import (
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
)

// Option is a function that configures an [Engine].
type Option func(*Engine)

// WithApplication returns an [Option] that registers app with the engine.
//
// It panics if app is nil or if an application with the same identity key has
// already been registered.
func WithApplication(app dogma.Application) Option {
	if app == nil {
		panic("runkit: application must not be nil")
	}

	id := runtimeconfig.FromApplication(app).Identity()
	key := id.Key.AsString()

	return func(e *Engine) {
		if _, exists := e.appsByKey[key]; exists {
			panic(fmt.Sprintf(
				"runkit: application is already registered: %s (%s)",
				id.Name,
				key,
			))
		}
		e.appsByKey[key] = struct{}{}

		ex := &executor{}

		e.apps = append(e.apps, app)
		e.executors[app] = ex
	}
}

// FromEnvironment returns an [Option] that configures the engine using
// environment variables.
//
// If it is placed after other options, it may override their configuration if
// the relevant environment variables are set. If it is placed before other
// options, the configuration from the environment may be overridden by them.
func FromEnvironment() Option {
	return func(e *Engine) {
		e.fromEnv = true
	}
}
