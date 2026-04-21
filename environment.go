package runkit

import (
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/ferrite"
	"github.com/dogmatiq/runkit/internal/persistence"
)

// FerriteRegistry is the ferrite environment variable registry for the Runkit
// engine. It can be passed to ferrite.Init() to validate Runkit's environment
// variables alongside the application's own variables.
var FerriteRegistry = ferrite.NewRegistry(
	"dogmatiq.runkit",
	"Runkit",
)

var envSiteKey = ferrite.
	String("DOGMA_SITE_KEY", "the UUID that uniquely identifies this site").
	WithConstraint("must be a UUID", isUUID).
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envSiteName = ferrite.
	String("DOGMA_SITE_NAME", "the human-readable name of this site").
	Required(
		ferrite.RelevantIf(envSiteKey),
		ferrite.WithRegistry(FerriteRegistry),
	)

var envNodeID = ferrite.
	String("DOGMA_NODE_ID", "the UUID that uniquely identifies this node").
	WithConstraint("must be a UUID", isUUID).
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envListenAddress = ferrite.
	String("DOGMA_LISTEN_ADDRESS", "the TCP address the engine listens on").
	WithConstraint("must be a host:port address", isHostPort).
	WithExample("0.0.0.0:7831", "listen on all IPv4 interfaces").
	WithExample("192.168.0.10:7831", "listen on a specific interface").
	WithExample("[2001:db8::1]:7831", "listen on a specific IPv6 address").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envAdvertiseAddress = ferrite.
	String("DOGMA_ADVERTISE_ADDRESS", "the address other nodes use to connect to this node").
	WithConstraint("must be a host:port address", isHostPort).
	WithExample("192.168.0.10:7831", "advertise a specific IPv4 address").
	WithExample("[2001:db8::1]:7831", "advertise a specific IPv6 address").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envPersistenceURL = ferrite.
	String("DOGMA_PERSISTENCE_URL", "the URL of the persistence provider").
	WithConstraint("must be a valid persistence URL", isPersistenceURL).
	WithExample("memory:///silo", "in-process memory storage (testing only)").
	WithExample("postgres://user:pass@host/dbname", "PostgreSQL storage").
	WithExample("postgresql://user:pass@host/dbname", "PostgreSQL storage (alternate scheme)").
	WithExample("dynamodb:///table-prefix", "DynamoDB storage").
	WithExample("s3:///bucket", "S3 storage").
	Optional(ferrite.WithRegistry(FerriteRegistry))

// isUUID returns true if v is a valid UUID string.
func isUUID(v string) bool {
	_, err := uuidpb.Parse(v)
	return err == nil
}

// isPersistenceURL returns true if v is a valid persistence URL.
func isPersistenceURL(v string) bool {
	_, err := persistence.NewProvider(v)
	return err == nil
}
