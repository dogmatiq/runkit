package xsql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// UUID returns a value that marshals and unmarshals a UUID to/from its binary
// representation as used in the database.
func UUID(id *uuidpb.UUID) ScannerValuer {
	if id == nil {
		panic("UUID must not be nil")
	}

	return scannerValuerFuncs{
		func(v any) error {
			data, ok := v.(string)
			if !ok {
				return fmt.Errorf("cannot scan %T into %T", v, id)
			}
			return id.UnmarshalText([]byte(data))
		},
		func() (driver.Value, error) {
			data, err := id.MarshalText()
			return string(data), err
		},
	}
}

// Envelope returns a value that marshals and unmarshals an envelope to/from its
// binary representation as used in the database.
func Envelope(envelope *envelopepb.Envelope) ScannerValuer {
	if envelope == nil {
		panic("envelope must not be nil")
	}

	return scannerValuerFuncs{
		func(v any) error {
			data, ok := v.([]byte)
			if !ok {
				return fmt.Errorf("cannot scan %T into %T", v, envelope)
			}
			return envelope.UnmarshalBinary(data)
		},
		func() (driver.Value, error) {
			return envelope.MarshalBinary()
		},
	}
}

// UnpackEnvelopeError is an [error] returned by [UnpackEnvelope] when it cannot
// unmarshal data to a valid envelope, or unpack the messages within it.
type UnpackEnvelopeError struct {
	cause error
}

func (e UnpackEnvelopeError) Error() string {
	return e.cause.Error()
}

func (e UnpackEnvelopeError) Unwrap() error {
	return e.cause
}

// UnpackEnvelope returns a value that unmarshals an envelope and unpacks the
// message within it (possibly multiple times).
func UnpackEnvelope[
	T interface {
		dogma.Message
		Validate(S) error
	},
	S dogma.MessageValidationScope,
](
	envelope *envelopepb.Envelope,
	messages ...*T,
) sql.Scanner {
	if envelope == nil {
		panic("envelope must not be nil")
	}

	return scannerFunc(func(v any) error {
		data, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("cannot scan %T into %T", v, envelope)
		}

		if err := envelope.UnmarshalBinary(data); err != nil {
			return UnpackEnvelopeError{cause: err}
		}

		if err := envelope.Validate(); err != nil {
			return UnpackEnvelopeError{cause: err}
		}

		for idx := range messages {
			m, err := envelopepb.Unpack[T](envelope)
			if err != nil {
				return UnpackEnvelopeError{cause: err}
			}

			if idx == 0 {
				// Validate the command once; assume that subsequent unpacking
				// of the same data _must_ be valid too.
				s := messageValidationScope{envelope}
				if err := m.Validate(any(s).(S)); err != nil {
					return UnpackEnvelopeError{cause: err}
				}
			}

			*messages[idx] = m
		}

		return nil
	})
}

type messageValidationScope struct {
	envelope *envelopepb.Envelope
}

func (s messageValidationScope) IsNew() bool {
	return false
}

func (s messageValidationScope) ExecutedAt() time.Time {
	return s.envelope.GetBody().GetCreatedAt().AsTime()
}

func (s messageValidationScope) RecordedAt() time.Time {
	return s.envelope.GetBody().GetCreatedAt().AsTime()
}

func (s messageValidationScope) ScheduledAt() time.Time {
	return s.envelope.GetBody().GetCreatedAt().AsTime()
}

func (s messageValidationScope) ScheduledFor() time.Time {
	return s.envelope.GetBody().GetScheduledFor().AsTime()
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

func (f scannerFunc) Scan(v any) error            { return f(v) }
func (f valuerFunc) Value() (driver.Value, error) { return f() }
