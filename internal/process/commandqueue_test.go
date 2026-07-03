package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestCommandQueue_handlersCanExecuteCommands(t *testing.T) {
	t.Run("via HandleEvent", func(t *testing.T) {
		const handlerKey = "b7ddb31c-8bee-40bb-b8e4-1218c8233cac"

		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				xtesting.PopulateEventStreams(
					t,
					engine.DB,
					func(_ *uuidpb.UUID, offset uint64) dogma.Event {
						return stubs.EventA1
					},
					1, // one stream with one event
				)

				xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
				xtesting.ExpectCommandToBeQueued(
					t,
					engine.DB,
					stubs.CommandA1,
				)
			},
			dogma.ViaProcess(
				&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
					ConfigureFunc: func(c dogma.ProcessConfigurer) {
						c.Identity("<handler>", handlerKey)
						c.Routes(
							dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeA]](),
						)
					},
					RouteEventToInstanceFunc: func(
						context.Context,
						dogma.Event,
					) (string, bool, error) {
						return "<instance>", true, nil
					},
					HandleEventFunc: func(
						_ context.Context,
						_ *stubs.ProcessRootStub,
						s dogma.ProcessEventScope[*stubs.ProcessRootStub],
						_ dogma.Event,
					) error {
						s.ExecuteCommand(stubs.CommandA1)
						return nil
					},
				},
			),
		)
	})

	t.Run("via HandleDeadline", func(t *testing.T) {
		const handlerKey = "4f90ce19-a899-4e1b-a5d3-722b785f5d98"

		xtesting.RunEngines(
			t,
			func(t testing.TB, engine *dogmaengine.Engine) {
				xtesting.PopulateEventStreams(
					t,
					engine.DB,
					func(_ *uuidpb.UUID, offset uint64) dogma.Event {
						return stubs.EventA1
					},
					1, // one stream with one event
				)

				xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
				xtesting.WaitForNoPendingDeadlines(t, engine.DB)
				xtesting.ExpectCommandToBeQueued(
					t,
					engine.DB,
					stubs.CommandA1,
				)
			},
			dogma.ViaProcess(
				&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
					ConfigureFunc: func(c dogma.ProcessConfigurer) {
						c.Identity("<handler>", handlerKey)
						c.Routes(
							dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeA]](),
							dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
						)
					},
					RouteEventToInstanceFunc: func(
						context.Context,
						dogma.Event,
					) (string, bool, error) {
						return "<instance>", true, nil
					},
					HandleEventFunc: func(
						_ context.Context,
						_ *stubs.ProcessRootStub,
						s dogma.ProcessEventScope[*stubs.ProcessRootStub],
						_ dogma.Event,
					) error {
						s.ScheduleDeadline(stubs.DeadlineA1, time.Now())
						return nil
					},
					HandleDeadlineFunc: func(
						_ context.Context,
						_ *stubs.ProcessRootStub,
						s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
						d dogma.Deadline,
					) error {
						s.ExecuteCommand(stubs.CommandA1)
						return nil
					},
				},
			),
		)
	})
}
