package xmessage

import (
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// Unpack unmarshals an envelope from its binary representation and unpacks the
// message within it (possibly multiple times).
func Unpack[
	T interface {
		dogma.Message
		Validate(S) error
	},
	S dogma.MessageValidationScope,
](
	data []byte,
	envelope *envelopepb.Envelope,
	messages ...*T,
) error {
	if envelope == nil {
		panic("envelope must not be nil")
	}

	if err := envelope.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("unable to unmarshal envelope: %w", err)
	}

	if err := envelope.Validate(); err != nil {
		return err
	}

	return UnpackMessage(envelope, messages...)
}

// UnpackMessage unpacks the message(s) from an already-parsed envelope.
//
// Unlike [Unpack], it does not unmarshal the envelope from binary; the caller
// is responsible for ensuring the envelope is already populated and valid.
func UnpackMessage[
	T interface {
		dogma.Message
		Validate(S) error
	},
	S dogma.MessageValidationScope,
](
	envelope *envelopepb.Envelope,
	messages ...*T,
) error {
	for idx := range messages {
		m, err := envelopepb.Unpack[T](envelope)
		if err != nil {
			return err
		}

		if idx == 0 {
			// Validate the message once; assume that subsequent unpacking
			// of the same data _must_ be valid too.
			s := ValidationScope{
				IsNewMessage: false,
				Envelope:     envelope,
			}

			if err := m.Validate(any(s).(S)); err != nil {
				return err
			}
		}

		*messages[idx] = m
	}

	return nil
}
