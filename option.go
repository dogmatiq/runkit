package runkit

import (
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Option is a function that configures an [Engine].
type Option func(*Engine)

// WithSite returns an [Option] that sets the site identity for the engine.
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
// name is a human-readable label for the site. key is a canonical RFC 9562
// UUID string that uniquely identifies the site. If you're unsure, generate a
// new random (v4) UUID and hardcode it.
//
// If [FromEnvironment] is also used, this option takes precedence over the
// values of the DOGMA_SITE_NAME and DOGMA_SITE_KEY environment variables.
//
// It panics if name is empty or if key is not a valid UUID.
func WithSite(name, key string) Option {
	site, err := identitypb.Parse(name, key)
	if err != nil {
		panic(fmt.Sprintf("runkit: invalid site identity: %s", err))
	}

	return func(e *Engine) {
		e.site = site
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

// WithPersistence returns an [Option] that configures the persistence provider
// for the engine.
//
// A persistence provider is required. [Engine.Run] panics if none is configured.
func WithPersistence(p PersistenceProvider) Option {
	if p == nil {
		panic("runkit: persistence provider must not be nil")
	}
	return func(e *Engine) {
		e.persistence = p
	}
}

// WithBindAddress returns an [Option] that sets the TCP address the engine
// listens on, in "host:port" format (e.g. "0.0.0.0:7831").
//
// If [FromEnvironment] is also used, this option takes precedence over
// DOGMA_BIND_ADDRESS.
//
// It panics if addr is empty.
func WithBindAddress(addr string) Option {
	if addr == "" {
		panic("runkit: bind address must not be empty")
	}
	return func(e *Engine) {
		e.bindAddr = addr
	}
}

// WithAdvertiseAddress returns an [Option] that sets the address the engine
// advertises to peers, in "host:port" format.
//
// If unset, the advertise address is derived from the bind address and network
// interface introspection at startup.
//
// If [FromEnvironment] is also used, this option takes precedence over
// DOGMA_ADVERTISE_ADDRESS.
//
// It panics if addr is empty.
func WithAdvertiseAddress(addr string) Option {
	if addr == "" {
		panic("runkit: advertise address must not be empty")
	}
	return func(e *Engine) {
		e.advertiseAddr = addr
	}
}

// FromEnvironment returns an [Option] that configures the engine using
// environment variables.
//
// It reads the following environment variables:
//
//   - DOGMA_SITE_NAME (see [WithSite])
//   - DOGMA_SITE_KEY (see [WithSite])
//   - DOGMA_NODE_ID (see [WithNodeID])
//   - DOGMA_BIND_ADDRESS (see [WithBindAddress])
//   - DOGMA_ADVERTISE_ADDRESS (see [WithAdvertiseAddress])
//
// Explicit options always take precedence over environment variables, regardless
// of the order in which options are specified.
func FromEnvironment() Option {
	return func(e *Engine) {
		if e.site == nil {
			if key, ok := envSiteKey.Value(); ok {
				site, err := identitypb.Parse(envSiteName.Value(), key)
				if err != nil {
					panic(fmt.Sprintf("runkit: invalid site identity from environment: %s", err))
				}
				e.site = site
			}
		}
		if e.nodeID == nil {
			if v, ok := envNodeID.Value(); ok {
				id, err := uuidpb.Parse(v)
				if err != nil {
					panic(fmt.Sprintf("runkit: invalid DOGMA_NODE_ID: %s", err))
				}
				e.nodeID = id
			}
		}
		if e.bindAddr == "" {
			if addr, ok := envBindAddress.Value(); ok {
				e.bindAddr = addr
			}
		}
		if e.advertiseAddr == "" {
			if addr, ok := envAdvertiseAddress.Value(); ok {
				e.advertiseAddr = addr
			}
		}
	}
}
