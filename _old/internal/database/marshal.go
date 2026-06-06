package database

import (
	"database/sql"
	"database/sql/driver"
	"encoding"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// UnmarshalEnvelope returns an [sql.Scanner] that decodes a database value
// into env.
//
// The source must be a []byte; NULL columns are reported as an error.
func UnmarshalEnvelope(env *envelopepb.Envelope) sql.Scanner {
	return binaryScanner{env}
}

// MarshalEnvelope returns a [driver.Valuer] that encodes env to a database
// value.
func MarshalEnvelope(env *envelopepb.Envelope) driver.Valuer {
	return binaryValuer{env}
}

// UnmarshalUUID returns an [sql.Scanner] that decodes a Postgres uuid
// column into x.
//
// The source must be a string; NULL columns are reported as an error.
func UnmarshalUUID(x *uuidpb.UUID) sql.Scanner {
	return textScanner{x}
}

// MarshalUUID returns a [driver.Valuer] that encodes x as its canonical
// string representation for binding to a Postgres uuid column.
func MarshalUUID(x *uuidpb.UUID) driver.Valuer {
	return textValuer{x}
}

// binaryScanner is an [sql.Scanner] that decodes a database value into
// an [encoding.BinaryUnmarshaler].
type binaryScanner struct {
	Target encoding.BinaryUnmarshaler
}

func (s binaryScanner) Scan(src any) error {
	data, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into binary value", src)
	}
	return s.Target.UnmarshalBinary(data)
}

// binaryValuer is a [driver.Valuer] that encodes an
// [encoding.BinaryMarshaler] value into a database value.
type binaryValuer struct {
	Source encoding.BinaryMarshaler
}

func (v binaryValuer) Value() (driver.Value, error) {
	return v.Source.MarshalBinary()
}

// textScanner is an [sql.Scanner] that decodes a database value into an
// [encoding.TextUnmarshaler].
type textScanner struct {
	Target encoding.TextUnmarshaler
}

func (s textScanner) Scan(src any) error {
	str, ok := src.(string)
	if !ok {
		return fmt.Errorf("cannot scan %T into text value", src)
	}
	return s.Target.UnmarshalText([]byte(str))
}

// textValuer is a [driver.Valuer] that encodes an [encoding.TextMarshaler]
// value into a database value.
type textValuer struct {
	Source encoding.TextMarshaler
}

func (v textValuer) Value() (driver.Value, error) {
	data, err := v.Source.MarshalText()
	if err != nil {
		return nil, err
	}
	return string(data), nil
}
