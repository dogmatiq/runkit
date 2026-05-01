package runkit

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit"
)

// Option is a function that configures an [Engine].
type Option func(*engineConfig)

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

	return func(c *engineConfig) {
		c.Site = site
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

	return func(c *engineConfig) {
		c.NodeID = parsed
	}
}

// WithPersistence returns an [Option] that configures the persistence driver
// for the engine using a URL string.
//
// This option takes precedence over the DOGMA_PERSISTENCE_URL environment
// variable.
//
// It panics if the URL is malformed or if the scheme is unrecognized.
func WithPersistence(url string) Option {
	cfg, err := persistencekit.ParseURL(context.Background(), url)
	if err != nil {
		panic(fmt.Sprintf("runkit: %s", err))
	}

	return func(c *engineConfig) {
		c.Persistence = cfg
	}
}

// WithPersistenceConfig returns an [Option] that configures persistence using
// a [persistencekit.Config].
//
// This option takes precedence over the DOGMA_PERSISTENCE_URL environment
// variable.
//
// It panics if cfg is nil.
func WithPersistenceConfig(cfg persistencekit.Config) Option {
	if cfg == nil {
		panic("runkit: persistence config must not be nil")
	}

	return func(c *engineConfig) {
		c.Persistence = cfg
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

	return func(c *engineConfig) {
		c.ListenAddr = addr
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

	return func(c *engineConfig) {
		c.AdvertiseAddr = addr
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
	return func(c *engineConfig) {
		c.ApplyEnvironment = false
	}
}

// engineConfig holds the immutable configuration of an [Engine].
type engineConfig struct {
	// ApplyEnvironment enables reading configuration from environment variables.
	ApplyEnvironment bool

	// App is the configuration of the application to run.
	App *config.Application

	// Site is the identity of the deployment Site.
	Site *identitypb.Identity

	// NodeID is the unique identifier for this engine instance within the
	// cluster.
	NodeID *uuidpb.UUID

	// Persistence is the configuration for the engine's persistent stores.
	Persistence persistencekit.Config

	// ListenAddr is the TCP address the engine listens on, in "host:port" format.
	ListenAddr string

	// AdvertiseAddr is the address the engine advertises to other nodes, in
	// "host:port" format.
	AdvertiseAddr string
}

// newEngineConfig creates a new [engineConfig] from the given application and
// options. It validates the configuration and returns an error if it is
// invalid.
func newEngineConfig(app dogma.Application, opts ...Option) (engineConfig, error) {
	if app == nil {
		panic("runkit: application must not be nil")
	}

	cfg := engineConfig{
		ApplyEnvironment: true,
		App:              runtimeconfig.FromApplication(app),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ApplyEnvironment {
		applyEnvironment(&cfg)
	}

	if cfg.Site == nil {
		return cfg, errors.New("runkit: a site identity is required, use WithSiteIdentity() or set DOGMA_SITE_NAME and DOGMA_SITE_KEY")
	}

	if cfg.NodeID == nil {
		cfg.NodeID = uuidpb.Generate()
	}

	if cfg.Persistence == nil {
		return cfg, errors.New("runkit: a persistence driver is required, use WithPersistence(), WithPersistenceConfig() or set DOGMA_PERSISTENCE_URL")
	}

	if cfg.ListenAddr == "" && cfg.AdvertiseAddr != "" {
		cfg.ListenAddr = cfg.AdvertiseAddr
	}

	return cfg, nil
}

// applyEnvironment reads configuration from environment variables for any
// fields that have not already been set by explicit options.
func applyEnvironment(c *engineConfig) {
	if c.Site == nil {
		if key, ok := envSiteKey.Value(); ok {
			c.Site = identitypb.New(envSiteName.Value(), key)
		}
	}

	if c.NodeID == nil {
		if id, ok := envNodeID.Value(); ok {
			c.NodeID = id
		}
	}

	if c.Persistence == nil {
		if o, ok := envPersistenceURL.Value(); ok {
			c.Persistence = o.Config
		}
	}

	if c.ListenAddr == "" {
		if addr, ok := envListenAddress.Value(); ok {
			WithListenAddress(addr)(c)
		}
	}

	if c.AdvertiseAddr == "" {
		if addr, ok := envAdvertiseAddress.Value(); ok {
			WithAdvertiseAddress(addr)(c)
		}
	}
}
