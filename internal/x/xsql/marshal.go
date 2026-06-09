package xsql

import (
	"database/sql/driver"
	"encoding"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// UUID returns a value that marshals and unmarshals a UUID to/from its binary
// representation as used in the database.
func UUID(id *uuidpb.UUID) Value {
	if id == nil {
		panic("UUID must not be nil")
	}
	return text{id}
}

// UUIDs marshals a slice of UUIDs as a slice of UUID strings, suitable for use
// with the PostgreSQL ANY operator.
func UUIDs(ids []*uuidpb.UUID) []string {
	stringIDs := make([]string, len(ids))
	for i, id := range ids {
		stringIDs[i] = id.AsString()
	}

	return stringIDs
}

// Envelope returns a value that marshals and unmarshals an envelope to/from its
// binary representation as used in the database.
func Envelope(envelope *envelopepb.Envelope) Value {
	if envelope == nil {
		panic("envelope must not be nil")
	}
	return binary{envelope}
}

type text struct {
	V interface {
		encoding.TextMarshaler
		encoding.TextUnmarshaler
	}
}

func (x text) Scan(v any) error {
	data, ok := v.(string)
	if !ok {
		return fmt.Errorf("cannot scan %T into %T", v, x.V)
	}

	return x.V.UnmarshalText([]byte(data))
}

func (x text) Value() (driver.Value, error) {
	data, err := x.V.MarshalText()
	return string(data), err
}

type binary struct {
	V interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
	}
}

func (x binary) Scan(v any) error {
	data, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into %T", v, x.V)
	}

	return x.V.UnmarshalBinary(data)
}

func (x binary) Value() (driver.Value, error) {
	return x.V.MarshalBinary()
}
