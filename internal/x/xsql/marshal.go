package xsql

import (
	_ "database/sql" // imported for documentation linking
	"database/sql/driver"
	"fmt"
	"iter"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// UUID returns a value that marshals and unmarshals a UUID to/from its binary
// representation as used in the database.
func UUID(id *uuidpb.UUID) ScannerValuer {
	return scannerValuerFuncs{
		func(v any) error {
			if id == nil {
				return fmt.Errorf("cannot scan into nil UUID pointer")
			}

			switch v := v.(type) {
			case nil:
				id.Reset()
				return nil
			case string:
				return id.UnmarshalText([]byte(v))
			default:
				return fmt.Errorf("cannot scan %T into %T", v, id)
			}
		},
		func() (driver.Value, error) {
			if id == nil {
				return nil, nil
			}

			data, err := id.MarshalText()
			return string(data), err
		},
	}
}

// UUIDs returns a []string containing the text representation of each UUID,
// suitable for use with PostgreSQL's ANY($n) operator.
func UUIDs(ids ...*uuidpb.UUID) any {
	values := make([]string, len(ids))
	for idx, id := range ids {
		values[idx] = id.AsString()
	}
	return values
}

// UUIDSeq collects UUIDs from an iterator into a []string containing the text
// representation of each UUID, suitable for use with PostgreSQL's ANY($n)
// operator.
func UUIDSeq(seq iter.Seq[*uuidpb.UUID]) any {
	// Use a non-nil empty slice so that pgx encodes it as an empty PostgreSQL
	// array ('{}') rather than NULL.
	values := []string{}
	for id := range seq {
		values = append(values, id.AsString())
	}

	return values
}

// Envelope returns a value that marshals and unmarshals an envelope to/from its
// binary representation as used in the database.
func Envelope(envelope *envelopepb.Envelope) ScannerValuer {
	if envelope == nil {
		panic("envelope must not be nil")
	}

	return scannerValuerFuncs{
		func(v any) error {
			if envelope == nil {
				return fmt.Errorf("cannot scan into nil envelope pointer")
			}

			switch v := v.(type) {
			case nil:
				envelope.Reset()
				return nil
			case []byte:
				return envelope.UnmarshalBinary(v)
			default:
				return fmt.Errorf("cannot scan %T into %T", v, envelope)
			}
		},
		func() (driver.Value, error) {
			if envelope == nil {
				return nil, nil
			}

			return envelope.MarshalBinary()
		},
	}
}

// scannerFunc is a function that implements [sql.Scanner] by calling itself.
type scannerFunc func(any) error

// valuerFunc is a function that implements [driver.Valuer] by calling itself.
type valuerFunc func() (driver.Value, error)

// scannerValuerFuncs is a pair of functions that implement both [sql.Scanner]
// and [driver.Valuer].
type scannerValuerFuncs struct {
	scannerFunc
	valuerFunc
}

func (f scannerFunc) Scan(s any) error            { return f(s) }
func (f valuerFunc) Value() (driver.Value, error) { return f() }
