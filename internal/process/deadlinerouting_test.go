package process_test

import (
	"context"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestDeadlineRouting_deadlinesAreRoutedBackToTheirSourceInstance verifies that
// a deadline scheduled by a process instance is delivered back to that same
// instance.
func TestDeadlineRouting_deadlinesAreRoutedBackToTheirSourceInstance(t *testing.T) {
	var done xsync.Latch

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

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					_ dogma.Deadline,
				) error {
					defer done.Set()

					if got, want := s.InstanceID(), "<instance>"; got != want {
						t.Errorf("deadline routed to wrong instance: got %q, want %q", got, want)
					}

					return nil
				},
			},
		),
	)
}

// TestDeadlineRouting_deadlinesAreNotDeliveredUntilTheirScheduledTime verifies
// that a deadline scheduled in the future is not delivered early.
func TestDeadlineRouting_deadlinesAreNotDeliveredUntilTheirScheduledTime(t *testing.T) {
	var done xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					return stubs.EventA1
				},
				1,
			)

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "ef0660b4-a68e-4383-b156-5857ac294dce")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
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
					s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(500*time.Millisecond))
					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					s dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					_ dogma.Deadline,
				) error {
					defer done.Set()

					if now := s.Now(); now.Before(s.ScheduledFor()) {
						t.Errorf("deadline delivered at %s, before its scheduled time of %s", now, s.ScheduledFor())
					}

					return nil
				},
			},
		),
	)
}
