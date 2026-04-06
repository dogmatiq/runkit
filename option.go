package runkit

import (
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Option is a function that configures an [Engine].
type Option func(*Engine)

// WithSiteID returns an [Option] that sets the site identity for the engine.
//
// A site represents a distinct installation of the same set of applications.
// Separate sites are used when running independent deployments, for example:
//
//   - geographical regions (US, EU, APAC)
//   - environment tiers (development, staging, production)
//   - isolated tenants or customers
//
// Each site has its own persisted state; two sites never share data. The site
// identity is included in every message envelope the engine produces.
//
// id is a canonical RFC 9562 UUID string. If you're unsure, generate a new
// random (v4) UUID and hardcode it.
//
// If [FromEnvironment] is also used, this option takes precedence over the
// value of the DOGMA_SITE_ID environment variable.
//
// It panics if id is not a valid UUID.
func WithSiteID(id string) Option {
	parsed, err := uuidpb.Parse(id)
	if err != nil {
		panic(fmt.Sprintf("runkit: invalid site ID: %s", err))
	}

	return func(e *Engine) {
		e.siteID = parsed
	}
}

// WithNodeID returns an [Option] that sets the node identity for the engine.
//
// A node represents a single running instance of the engine within a site. In a
// clustered deployment each host is a separate node.
//
// id is a canonical RFC 9562 UUID string. If neither this option nor
// [FromEnvironment] supplies a node ID, the engine generates a random UUID at
// startup.
//
// If [FromEnvironment] is also used, this option takes precedence over the
// value of the DOGMA_NODE_ID environment variable.
//
// It panics if id is not a valid UUID.
func WithNodeID(id string) Option {
	parsed, err := uuidpb.Parse(id)
	if err != nil {
		panic(fmt.Sprintf("runkit: invalid node ID: %s", err))
	}

	return func(e *Engine) {
		e.nodeID = parsed
	}
}

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
// It reads the following environment variables:
//
//   - DOGMA_SITE_ID (see [WithSiteID])
//   - DOGMA_NODE_ID (see [WithNodeID])
//
// Explicit options always take precedence over environment variables, regardless
// of the order in which options are specified.
func FromEnvironment() Option {
	return func(e *Engine) {
		if e.siteID == nil {
			if v, ok := siteIDVar.Value(); ok {
				id, err := uuidpb.Parse(v)
				if err != nil {
					panic(fmt.Sprintf("runkit: invalid DOGMA_SITE_ID: %s", err))
				}
				e.siteID = id
			}
		}
		if e.nodeID == nil {
			if v, ok := nodeIDVar.Value(); ok {
				id, err := uuidpb.Parse(v)
				if err != nil {
					panic(fmt.Sprintf("runkit: invalid DOGMA_NODE_ID: %s", err))
				}
				e.nodeID = id
			}
		}
	}
}
