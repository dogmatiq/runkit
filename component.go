package dogmaengine

import "context"

// component is an interface for subsystems that run within the engine and
// manage their own lifecycle.
type component interface {
	// Run executes the component until ctx is canceled.
	Run(context.Context)
}
