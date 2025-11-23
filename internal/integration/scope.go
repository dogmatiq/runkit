package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/runkit/internal/telemetry"
)

// scope is an implementation of [dogma.IntegrationCommandScope].
type scope struct {
	Context   context.Context
	Identity  *identitypb.Identity
	Packer    *envelopepb.Packer
	Command   *envelopepb.Envelope
	Events    []*envelopepb.Envelope
	Telemetry *telemetry.Recorder
}

func (s *scope) RecordEvent(e dogma.Event) {
	env := s.Packer.Pack(
		e,
		envelopepb.WithHandler(s.Identity),
		envelopepb.WithCause(s.Command),
	)

	s.Events = append(s.Events, env)

	s.Telemetry.Info(
		s.Context,
		"integration.event_recorded",
		"event recorded",
		telemetry.UUID("event.message_id", env.GetMessageId()),
		telemetry.UUID("event.causation_id", env.GetMessageId()),
		telemetry.UUID("event.correlation_id", env.GetCorrelationId()),
		telemetry.UUID("event.type.id", env.GetTypeId()),
		telemetry.MessageTypeName("event.type.name", env.GetTypeId()),
		telemetry.String("event.description", env.GetDescription()),
	)
}

func (s *scope) Now() time.Time {
	return time.Now()
}

func (s *scope) Log(format string, args ...any) {
	s.Telemetry.Info(
		s.Context,
		"integration.message_logged",
		fmt.Sprintf(format, args...),
	)
}
