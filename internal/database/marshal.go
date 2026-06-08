package database

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// Value is a type that can be stored in and retrieved from the database.
type Value interface {
	driver.Value
	sql.Scanner
}

// UUID returns a value that marshals and unmarshals a UUID to/from its binary
// representation as used in the database.
func UUID(id *uuidpb.UUID) Value {
	return text{id}
}

// Envelope returns a value that marshals and unmarshals an envelope to/from its
// binary representation as used in the database.
func Envelope(envelope *envelopepb.Envelope) Value {
	return binary{envelope}
}

type text struct {
	V interface {
		encoding.TextMarshaler
		encoding.TextUnmarshaler
	}
}

func (e text) Scan(v any) error {
	data, ok := v.(string)
	if !ok {
		return fmt.Errorf("cannot scan %T into %T", v, e.V)
	}

	return e.V.UnmarshalText([]byte(data))
}

func (e text) Value() (driver.Value, error) {
	data, err := e.V.MarshalText()
	return string(data), err
}

type binary struct {
	V interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
	}
}

func (e binary) Scan(v any) error {
	data, ok := v.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into %T", v, e.V)
	}

	return e.V.UnmarshalBinary(data)
}

func (e binary) Value() (driver.Value, error) {
	return e.V.MarshalBinary()
}
