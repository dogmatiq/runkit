package xmessage_test

import (
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	. "github.com/dogmatiq/runkit/internal/x/xmessage"
)

func TestUnpackMessage(t *testing.T) {
	packer := &envelope.Packer{
		Application: identitypb.New("<app>", uuidpb.Generate()),
	}

	t.Run("it returns an error if decoding the message panics", func(t *testing.T) {
		env := packer.PackCommand(&stubs.CommandStub[panicsWhenDecoded]{})

		var command dogma.Command
		err := UnpackMessage(env, &command)

		if _, ok := errors.AsType[xerrors.PanicError](err); !ok {
			t.Fatalf("expected a panic error, got %v", err)
		}
	})

	t.Run("it returns a deterministic failure if the message can't be unmarshaled", func(t *testing.T) {
		env := packer.PackCommand(&stubs.CommandStub[failsWhenDecoded]{})

		var command dogma.Command
		err := UnpackMessage(env, &command)

		if !xerrors.IsDeterministic(err) {
			t.Fatalf("expected a deterministic failure, got %v", err)
		}
	})
}

func TestUnpack(t *testing.T) {
	t.Run("it returns a deterministic failure if the envelope can't be unmarshaled", func(t *testing.T) {
		var command dogma.Command
		err := Unpack([]byte{0}, &envelopepb.Envelope{}, &command)

		if !xerrors.IsDeterministic(err) {
			t.Fatalf("expected a deterministic failure, got %v", err)
		}
	})
}

// panicsWhenDecoded is the content of a command that panics when it is
// decoded.
type panicsWhenDecoded struct{}

func init() {
	dogma.RegisterCommand[*stubs.CommandStub[panicsWhenDecoded]]("9d5f1d0e-6b0a-4c0e-8a39-1f3c2b7e4d61")
}

// UnmarshalJSON panics.
func (*panicsWhenDecoded) UnmarshalJSON([]byte) error {
	panic("<decode panic>")
}

// failsWhenDecoded is the content of a command that returns an error when it
// is decoded.
type failsWhenDecoded struct{}

func init() {
	dogma.RegisterCommand[*stubs.CommandStub[failsWhenDecoded]]("4b8e2f6a-3c1d-4e7f-9a2b-6d5c8e1f0a37")
}

// UnmarshalJSON returns an error.
func (*failsWhenDecoded) UnmarshalJSON([]byte) error {
	return errors.New("<decode error>")
}
