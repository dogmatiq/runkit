package persistence

import (
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/marshaler"
)

// UUIDMarshaler is a [marshaler.Marshaler] for [*uuidpb.UUID] values.
var UUIDMarshaler = marshaler.New(
	func(uuid *uuidpb.UUID) ([]byte, error) {
		return uuid.AsBytes(), nil
	},
	func(data []byte) (*uuidpb.UUID, error) {
		return uuidpb.FromBytes(data)
	},
)
