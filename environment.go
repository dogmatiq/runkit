package runkit

import (
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/ferrite"
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

var envBindAddress = ferrite.
	String("DOGMA_BIND_ADDRESS", "the TCP address the engine listens on (host:port)").
	Optional(ferrite.WithRegistry(FerriteRegistry))

var envAdvertiseAddress = ferrite.
	String("DOGMA_ADVERTISE_ADDRESS", "the address peers use to connect to this node (host:port)").
	Optional(ferrite.WithRegistry(FerriteRegistry))

func isUUID(v string) bool {
	_, err := uuidpb.Parse(v)
	return err == nil
}
