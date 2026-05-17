package eventstream

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// Offset is a zero-based position of an event in the event stream.
type Offset uint64

// OffsetOf returns the event stream offset stored on env's body extensions.
//
// It panics if the envelope was not obtained from the event stream.
func OffsetOf(env *envelopepb.Envelope) Offset {
	pos, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](env.GetBody())
	if err != nil {
		panic("unexpected error reading event stream position: " + err.Error())
	}
	if !ok {
		panic("envelope does not have an event stream position")
	}

	return Offset(pos.GetOffset())
}

// setOffset stores offset on env's body extensions, using the envelope's source
// application identity as the stream ID.
func setOffset(env *envelopepb.Envelope, offset Offset) {
	envelopepb.SetExtension(
		env.GetBody(),
		envelopepb.
			NewEventStreamPositionBuilder().
			WithStreamId(env.GetHeader().GetSource().GetApplication().GetKey()).
			WithOffset(uint64(offset)).
			Build(),
	)
}
