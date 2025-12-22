package uuidpersistence

import (
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/marshaler"
)

// Marshaler is a [marshaler.Marshaler] for [*uuidpb.UUID] values.
var Marshaler = marshaler.New(
	func(uuid *uuidpb.UUID) ([]byte, error) {
		return uuid.AsBytes(), nil
	},
	func(data []byte) (*uuidpb.UUID, error) {
		return uuidpb.FromBytes(data)
	},
)
