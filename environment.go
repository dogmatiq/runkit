package runkit

import (
	"context"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/ferrite"
	"github.com/dogmatiq/persistencekit"
)

// FerriteRegistry is the ferrite environment variable registry for the Runkit
// engine. It can be passed to ferrite.Init() to validate Runkit's environment
// variables alongside the application's own variables.
var FerriteRegistry = ferrite.NewRegistry(
	"dogmatiq.runkit",
	"Runkit",
)

var envSiteKey = ferrite.
	Custom("DOGMA_SITE_KEY", "the UUID that uniquely identifies this site", uuidpb.Parse, marshalUUID).
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envSiteName = ferrite.
	String("DOGMA_SITE_NAME", "the human-readable name of this site").
	Required(
		ferrite.RelevantIf(envSiteKey),
		ferrite.WithRegistry(FerriteRegistry),
	)

var envNodeID = ferrite.
	Custom("DOGMA_NODE_ID", "the UUID that uniquely identifies this node", uuidpb.Parse, marshalUUID).
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
	WithConstraint("must be a routable host:port address", isRoutableHostPort).
	WithExample("192.168.0.10:7831", "advertise a specific IPv4 address").
	WithExample("[2001:db8::1]:7831", "advertise a specific IPv6 address").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envPersistenceURL = ferrite.
	Custom(
		"DOGMA_PERSISTENCE_URL",
		"the URL of the persistence driver",
		func(v string) (persistenceDriver, error) {
			var d persistenceDriver
			return d, d.UnmarshalText([]byte(v))
		},
		func(d persistenceDriver) (string, error) {
			b, err := d.MarshalText()
			return string(b), err
		},
	).
	WithExample(persistenceDriver{URL: "memory:///silo"}, "in-process memory storage (testing only)").
	WithExample(persistenceDriver{URL: "postgres://user:pass@host/dbname"}, "PostgreSQL storage").
	WithExample(persistenceDriver{URL: "dynamodb:///table-prefix"}, "DynamoDB storage").
	WithExample(persistenceDriver{URL: "s3:///bucket"}, "S3 storage").
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

func marshalUUID(id *uuidpb.UUID) (string, error) {
	return id.AsString(), nil
}
