package persistence

import (
	"github.com/dogmatiq/persistencekit/marshaler"
)

// transactionMarshaler is a [marshaler.Marshaler] for [Transaction] values.
var transactionMarshaler = marshaler.NewProto[*Transaction]()
