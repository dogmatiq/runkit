package runkit

import (
	"fmt"
	"net"

	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/persistence"
)

// Option is a function that configures an [Engine].
type Option func(*config)

// config holds the immutable configuration of an [Engine].
type config struct {
	// useEnvironment enables reading configuration from environment variables.
	useEnvironment bool

	// site is the identity of the deployment site.
	site *identitypb.Identity

	// nodeID is the unique identifier for this engine instance within the
	// cluster.
	nodeID *uuidpb.UUID

	// persistence is the provider of the engine's persistent stores.
	persistence PersistenceProvider

	// listenAddr is the TCP address the engine listens on, in "host:port" format.
	listenAddr string

	// advertiseAddr is the address the engine advertises to other nodes, in
	// "host:port" format.
	advertiseAddr string
}

// WithSiteIdentity returns an [Option] that sets the site identity for the engine.
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
// This option takes precedence over the DOGMA_SITE_NAME and DOGMA_SITE_KEY
// environment variables.
//
// It panics if name is empty or if key is not a valid UUID.
func WithSiteIdentity(name, key string) Option {
	site, err := identitypb.Parse(name, key)
	if err != nil {
		panic(fmt.Sprintf("runkit: invalid site identity: %s", err))
	}

	return func(c *config) {
		c.site = site
	}
}

// WithNodeID returns an [Option] that sets the node identity for the engine.
//
// A node represents a single running instance of the engine within a site. In a
// clustered deployment each host is a separate node.
//
// This option takes precedence over the DOGMA_NODE_ID environment variable.
//
// If neither this option nor the DOGMA_NODE_ID environment variable supplies a
// node ID, the engine generates a random UUID at startup.
//
// It panics if id is not a canonical RFC 9562 UUID string.
func WithNodeID(id string) Option {
	parsed, err := uuidpb.Parse(id)
	if err != nil {
		panic(fmt.Sprintf("runkit: invalid node ID: %s", err))
	}

	return func(c *config) {
		c.nodeID = parsed
	}
}

// WithPersistence returns an [Option] that configures the persistence
// provider for the engine using a URL string.
//
// This option takes precedence over the DOGMA_PERSISTENCE_URL environment
// variable.
//
// It panics if the URL is malformed or if the scheme is unrecognized.
func WithPersistence(url string) Option {
	p, err := persistence.NewProvider(url)
	if err != nil {
		panic(fmt.Sprintf("runkit: %s", err))
	}

	return func(c *config) {
		c.persistence = p
	}
}

// WithPersistenceProvider returns an [Option] that configures the persistence
// provider for the engine.
//
// This option takes precedence over the DOGMA_PERSISTENCE_URL environment
// variable.
//
// It panics if p is nil.
func WithPersistenceProvider(p PersistenceProvider) Option {
	if p == nil {
		panic("runkit: persistence provider must not be nil")
	}

	return func(c *config) {
		c.persistence = p
	}
}

// WithListenAddress returns an [Option] that sets the TCP address the engine
// listens on, in "host:port" format (e.g. "0.0.0.0:7831").
//
// This option takes precedence over the DOGMA_LISTEN_ADDRESS environment
// variable.
//
// It panics if addr is not a valid host:port address.
func WithListenAddress(addr string) Option {
	if !isHostPort(addr) {
		panic("runkit: listen address must be a valid host:port address")
	}

	return func(c *config) {
		c.listenAddr = addr
	}
}

// WithAdvertiseAddress returns an [Option] that sets the address the engine
// advertises to other nodes, in "host:port" format.
//
// Most deployments only need [WithListenAddress]; the advertise address is
// derived from the listen address automatically. Use this option when the
// address visible to other nodes differs from the listen address, for example
// when behind a NAT or load balancer.
//
// If no listen address is configured the engine listens on the advertise
// address directly.
//
// This option takes precedence over the DOGMA_ADVERTISE_ADDRESS environment
// variable.
//
// It panics if addr is not a routable host:port address. Wildcard hosts
// (such as 0.0.0.0 or ::) and port 0 are not permitted.
func WithAdvertiseAddress(addr string) Option {
	if !isRoutableHostPort(addr) {
		panic("runkit: advertise address must be a routable host:port address")
	}

	return func(c *config) {
		c.advertiseAddr = addr
	}
}

// isHostPort returns true if v is a valid host:port address.
func isHostPort(v string) bool {
	_, _, err := net.SplitHostPort(v)
	return err == nil
}

// isRoutableHostPort returns true if v is a routable host:port address. It
// rejects wildcard/unspecified hosts and port 0.
func isRoutableHostPort(v string) bool {
	host, port, err := net.SplitHostPort(v)
	if err != nil {
		return false
	}

	if port == "0" {
		return false
	}

	if host == "" {
		return false
	}

	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return false
	}

	return true
}

// WithoutEnvironment returns an [Option] that prevents the engine from reading
// configuration from environment variables.
func WithoutEnvironment() Option {
	return func(c *config) {
		c.useEnvironment = false
	}
}

// applyEnvironment reads configuration from environment variables for any
// fields that have not already been set by explicit options.
func applyEnvironment(c *config) {
	// TODO: use a typed Ferrite variable to avoid re-parsing, see https://github.com/dogmatiq/ferrite/issues/188
	if c.site == nil {
		if key, ok := envSiteKey.Value(); ok {
			WithSiteIdentity(envSiteName.Value(), key)(c)
		}
	}

	// TODO: use a UUID Ferrite variable to avoid re-parsing, see https://github.com/dogmatiq/ferrite/issues/187
	if c.nodeID == nil {
		if v, ok := envNodeID.Value(); ok {
			WithNodeID(v)(c)
		}
	}

	// TODO: use a typed Ferrite variable to avoid re-parsing, see https://github.com/dogmatiq/ferrite/issues/188
	if c.persistence == nil {
		if u, ok := envPersistenceURL.Value(); ok {
			WithPersistence(u)(c)
		}
	}

	if c.listenAddr == "" {
		if addr, ok := envListenAddress.Value(); ok {
			WithListenAddress(addr)(c)
		}
	}

	if c.advertiseAddr == "" {
		if addr, ok := envAdvertiseAddress.Value(); ok {
			WithAdvertiseAddress(addr)(c)
		}
	}
}
