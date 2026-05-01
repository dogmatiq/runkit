package runkit

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/ferrite"
	"github.com/dogmatiq/persistencekit"
)

// FerriteRegistry is the [ferrite] environment variable registry for the Runkit
// engine.
//
// If the application uses [ferrite] to manage its environment variables, it
// should use [ferrite.WithRegistry] to include this registry in its call to
// [ferrite.Init] to ensure the application's generated documentation includes
// the environment variables used by the engine.
var FerriteRegistry = ferrite.NewRegistry(
	"dogmatiq.runkit",
	"Dogma",
)

var envSite = ferrite.
	TextEncodedP[*identitypb.Identity]("DOGMA_SITE", "the identity of this site, formatted as '<uuid> <name>'").
	WithExample(identitypb.MustParse("us-east-prod", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"), "a geographical deployment").
	WithExample(identitypb.MustParse("acme-corp", "c3d4e5f6-a7b8-4c9d-8e1f-2a3b4c5d6e7f"), "an isolated tenant").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envNodeID = ferrite.
	TextEncodedP[*uuidpb.UUID]("DOGMA_NODE_ID", "the UUID that uniquely identifies this node").
	WithExample(uuidpb.MustParse("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"), "a specific node ID").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envListenAddress = ferrite.
	String("DOGMA_LISTEN_ADDRESS", "the TCP address the engine listens on").
	WithConstraint("must be a host:port address", isHostPort).
	WithExample(":7831", "listen on all interfaces (IPv4 and IPv6)").
	WithExample("0.0.0.0:7831", "listen on all IPv4 interfaces").
	WithExample("[::]:7831", "listen on all IPv6 interfaces").
	WithExample(":0", "listen on all interfaces with a random port").
	WithExample("192.168.0.10:7831", "listen on a specific IPv4 address").
	WithExample("[2001:db8::1]:7831", "listen on a specific IPv6 address").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envAdvertiseAddress = ferrite.
	String("DOGMA_ADVERTISE_ADDRESS", "the address other nodes use to connect to this node").
	WithConstraint("must be a routable host:port address", isRoutableHostPort).
	WithExample("192.168.0.10:7831", "advertise a specific IPv4 address").
	WithExample("[2001:db8::1]:7831", "advertise a specific IPv6 address").
	WithExample("node1.example.com:7831", "advertise a hostname").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envPersistenceURL = ferrite.
	TextEncoded[persistenceDriver]("DOGMA_PERSISTENCE_URL", "the URL of the persistence driver").
	WithExample(persistenceDriver{URL: "postgres://user:pass@host/dbname"}, "PostgreSQL storage").
	WithExample(persistenceDriver{URL: "dynamodb:///table-prefix"}, "DynamoDB storage").
	WithExample(persistenceDriver{URL: "s3:///bucket"}, "S3 storage").
	WithExample(persistenceDriver{URL: "memory:///silo"}, "in-process memory storage (testing only)").
	Optional(ferrite.WithRegistry(FerriteRegistry))

// persistenceDriver holds a parsed persistence URL and the deferred opener it
// produces.
type persistenceDriver struct {
	URL    string
	Config persistencekit.Config
}

func (d persistenceDriver) MarshalText() ([]byte, error) {
	return []byte(d.URL), nil
}

func (d *persistenceDriver) UnmarshalText(text []byte) error {
	cfg, err := persistencekit.ParseURL(context.Background(), string(text))
	if err != nil {
		return err
	}

	d.URL = string(text)
	d.Config = cfg

	return nil
}
