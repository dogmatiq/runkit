package xslog

import (
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

// UUID returns an [slog.Attr] for a UUID value.
func UUID(k string, v *uuidpb.UUID) slog.Attr {
	return slog.String(k, v.AsString())
}

// Identity returns an [slog.Attr] for an application or handler identity.
func Identity(k string, v *identitypb.Identity, extra ...slog.Attr) slog.Attr {
	attrs := []any{
		UUID("key", v.GetKey()),
		slog.String("name", v.GetName()),
	}

	for _, a := range extra {
		attrs = append(attrs, a)
	}

	return slog.Group(k, attrs...)
}

// Envelope returns an [slog.Attr] for a message envelope.
func Envelope(v *envelopepb.Envelope, extra ...slog.Attr) slog.Attr {
	attrs := []any{
		UUID("message_id", v.GetBody().GetMessageId()),
		UUID("causation_id", v.GetHeader().GetCausationId()),
		UUID("correlation_id", v.GetHeader().GetCorrelationId()),
		slog.String("description", v.GetBody().GetMessage().GetDescription()),
		messageType(v.GetBody().GetMessage().GetTypeId()),
	}

	for _, a := range extra {
		attrs = append(attrs, a)
	}

	return slog.Group("message", attrs...)
}

func messageType(v *uuidpb.UUID) slog.Attr {
	attrs := []any{
		UUID("id", v),
	}

	if t, ok := dogma.RegisteredMessageTypeByID(v.AsString()); ok {
		attrs = append(
			attrs,
			slog.String("name", t.GoType().Name()),
		)
	}

	return slog.Group("type", attrs...)
}

// Error returns an [slog.Attr] for an error value.
func Error(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}

	return slog.String("error", err.Error())
}
