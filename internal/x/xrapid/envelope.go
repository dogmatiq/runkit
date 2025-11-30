package xrapid

import (
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"pgregory.net/rapid"
)

// Envelope returns a generator of random [*envelopepb.Envelope] values.
//
// By design, the message type and data encoded within the envelope is not
// necessarily valid.
func Envelope() *rapid.Generator[*envelopepb.Envelope] {
	return rapid.Custom(
		func(t *rapid.T) *envelopepb.Envelope {
			return &envelopepb.Envelope{
				MessageId:         UUID().Draw(t, "message id"),
				CausationId:       UUID().Draw(t, "causation id"),
				CorrelationId:     UUID().Draw(t, "correlation id"),
				SourceSite:        Nillable(Identity()).Draw(t, "source site"),
				SourceApplication: Identity().Draw(t, "source application"),
				SourceHandler:     Nillable(Identity()).Draw(t, "source handler"),
				SourceInstanceId:  rapid.String().Draw(t, "source instance id"),
				CreatedAt:         Timestamp().Draw(t, "created at"),
				ScheduledFor:      Nillable(Timestamp()).Draw(t, "scheduled for"),
				Description:       rapid.String().Draw(t, "description"),
				TypeId:            UUID().Draw(t, "type id"),
				Data:              rapid.SliceOf(rapid.Byte()).Draw(t, "data"),
				Attributes:        rapid.MapOf(rapid.String(), rapid.String()).Draw(t, "attributes"),
			}
		},
	)
}
