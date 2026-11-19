package xsql

import (
	_ "database/sql" // imported for documentation linking
	"database/sql/driver"
	"fmt"
	"iter"
	"uuid"

	"github.com/dogmatiq/protobuf/envelopepb"
)

// UUID returns a value that marshals and unmarshals a UUID to/from its binary
// representation as used in the database.
//
// A [uuid.UUID] value can only be marshaled. A *[uuid.UUID] can be marshaled
// and unmarshaled. A NULL is unmarshaled as the nil UUID, and the nil UUID is
// marshaled as NULL.
func UUID[T uuid.UUID | *uuid.UUID](id T) ScannerValuer {
	switch id := any(id).(type) {
	case uuid.UUID:
		return scannerValuerFuncs{
			func(any) error {
				return fmt.Errorf("cannot scan into a %T value", id)
			},
			func() (driver.Value, error) {
				return uuidValue(id)
			},
		}
	default:
		ptr := id.(*uuid.UUID)

		return scannerValuerFuncs{
			func(v any) error {
				if ptr == nil {
					return fmt.Errorf("cannot scan into nil UUID pointer")
				}

				switch v := v.(type) {
				case nil:
					*ptr = uuid.Nil()
					return nil
				case string:
					return ptr.UnmarshalText([]byte(v))
				default:
					return fmt.Errorf("cannot scan %T into %T", v, ptr)
				}
			},
			func() (driver.Value, error) {
				if ptr == nil {
					return nil, nil
				}

				return uuidValue(*ptr)
			},
		}
	}
}

// uuidValue returns the database value of the given UUID.
func uuidValue(id uuid.UUID) (driver.Value, error) {
	if id == uuid.Nil() {
		return nil, nil
	}

	return id.String(), nil
}

// UUIDSeq collects UUIDs from an iterator into a []string containing the text
// representation of each UUID, suitable for use with PostgreSQL's ANY($n)
// operator.
func UUIDSeq(seq iter.Seq[uuid.UUID]) any {
	// Use a non-nil empty slice so that pgx encodes it as an empty PostgreSQL
	// array ('{}') rather than NULL.
	values := []string{}

	for id := range seq {
		values = append(values, id.String())
	}

	return values
}

// Envelope returns a value that marshals and unmarshals an envelope to/from its
// binary representation as used in the database.
func Envelope(env *envelopepb.Envelope) ScannerValuer {
	if env == nil {
		panic("envelope must not be nil")
	}

	return scannerValuerFuncs{
		func(v any) error {
			if env == nil {
				return fmt.Errorf("cannot scan into nil envelope pointer")
			}

			switch v := v.(type) {
			case nil:
				env.Reset()
				return nil
			case []byte:
				return env.UnmarshalBinary(v)
			default:
				return fmt.Errorf("cannot scan %T into %T", v, env)
			}
		},
		func() (driver.Value, error) {
			if env == nil {
				return nil, nil
			}

			return env.MarshalBinary()
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
