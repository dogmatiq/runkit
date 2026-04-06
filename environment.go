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

func isUUID(v string) bool {
	_, err := uuidpb.Parse(v)
	return err == nil
}

var siteIDVar = ferrite.
	String("DOGMA_SITE_ID", "the UUID that uniquely identifies this site").
	WithConstraint("must be a UUID", isUUID).
	Optional(ferrite.WithRegistry(FerriteRegistry))

var nodeIDVar = ferrite.
	String("DOGMA_NODE_ID", "the UUID that uniquely identifies this node").
	WithConstraint("must be a UUID", isUUID).
	Optional(ferrite.WithRegistry(FerriteRegistry))
