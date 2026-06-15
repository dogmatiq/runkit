package dogmaengine

import "context"

// controller is an interface for a subsystem controller.
type controller interface {
	// Run executes the controller until ctx is canceled.
	Run(context.Context)
}
