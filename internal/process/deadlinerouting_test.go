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
					c.Identity("<handler>", "ec809a3b-ebc8-4c36-b0da-a8118c3022e7")
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
					c.Identity("<handler>", "f088972b-4c15-4c68-ba92-fa2f96d93b30")
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

func TestDeadlineRouting_deadlinesAreNotDeliveredToEndedInstances(t *testing.T) {
	var ended xsync.Latch

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(_ *uuidpb.UUID, offset uint64) dogma.Event {
					switch offset {
					case 0:
						return stubs.EventA1 // creates instance, schedules deadline
					default:
						return stubs.EventB1 // ends the instance
					}
				},
				2,
			)

			xtesting.ExpectLatchesSetEventually(t, &ended)
			xtesting.WaitForNoPendingDeadlines(t, engine.DB)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", "bc3e3035-3473-49cc-8f36-598bc7ec218c")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeB]](),
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
					m dogma.Event,
				) error {
					switch m.(type) {
					case *stubs.EventStub[stubs.TypeA]:
						s.ScheduleDeadline(
							stubs.DeadlineA1,
							time.Now().Add(1*time.Second),
						)
					case *stubs.EventStub[stubs.TypeB]:
						s.End()
						ended.Set()
					default:
						panic(dogma.UnexpectedMessage)
					}

					return nil
				},
				HandleDeadlineFunc: func(
					context.Context,
					*stubs.ProcessRootStub,
					dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					dogma.Deadline,
				) error {
					t.Errorf("deadline was delivered to an ended instance")
					return nil
				},
			},
		),
	)
}

func TestDeadlineRouting_deadlinesScheduledInTheSameScopeAsEndAreNotDelivered(t *testing.T) {
	t.Run("via HandleEvent", func(t *testing.T) {
		const handlerKey = "ef0660b4-a68e-4383-b156-5857ac294dce"

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

				xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
				xtesting.WaitForNoPendingDeadlines(t, engine.DB)
			},
			dogma.ViaProcess(
				&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
					ConfigureFunc: func(c dogma.ProcessConfigurer) {
						c.Identity("<handler>", handlerKey)
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
						s.End()
						return nil
					},
					HandleDeadlineFunc: func(
						context.Context,
						*stubs.ProcessRootStub,
						dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
						dogma.Deadline,
					) error {
						t.Errorf("deadline was delivered to an ended instance")
						return nil
					},
				},
			),
		)
	})

	t.Run("via HandleDeadline", func(t *testing.T) {
		var ended xsync.Latch

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

				xtesting.ExpectLatchesSetEventually(t, &ended)
				xtesting.WaitForNoPendingDeadlines(t, engine.DB)
			},
			dogma.ViaProcess(
				&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
					ConfigureFunc: func(c dogma.ProcessConfigurer) {
						c.Identity("<handler>", "77cb6923-ab95-4074-91b6-3dab39344f74")
						c.Routes(
							dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
							dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
							dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeB]](),
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
						switch d.(type) {
						case *stubs.DeadlineStub[stubs.TypeA]:
							s.ScheduleDeadline(stubs.DeadlineB1, time.Now())
							s.End()
							ended.Set()
						default:
							t.Errorf("deadline was delivered to an ended instance")
						}
						return nil
					},
				},
			),
		)
	})
}

// TestDeadlineRouting_deadlinesForRemovedRoutesAreNotDelivered verifies that
// when a deadline route is removed from a handler, previously-scheduled
// deadlines of that type are silently skipped rather than delivered. Deadlines
// of other still-routed types continue to be delivered.
//
// This models the scenario where a handler is redeployed with a deadline type
// removed while deadlines of that type are already pending in the database.
func TestDeadlineRouting_deadlinesForRemovedRoutesAreNotDelivered(t *testing.T) {
	t.Parallel()

	const handlerKey = "cbd8b78d-b8b6-40e5-8ad4-b3aa1e26f5ef"

	db := xtesting.NewDatabase(t)

	// Run the handler with both TypeA and TypeB deadline routes and schedule
	// one of each far enough in the future that they do not fire during this
	// phase.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.PopulateEventStreams(
				t,
				engine.DB,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1,
			)

			xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeA]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeB]](),
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
					s.ScheduleDeadline(stubs.DeadlineA1, time.Now().Add(100*time.Millisecond))
					s.ScheduleDeadline(stubs.DeadlineB1, time.Now().Add(150*time.Millisecond))
					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					_ dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					d dogma.Deadline,
				) error {
					t.Errorf("unexpected deadline delivered: %T", d)
					return nil
				},
			},
		),
	)

	var delivered xsync.Latch

	// Restart with the TypeA deadline route removed. The TypeB deadline is
	// delivered; the TypeA deadline is silently skipped and remains pending in
	// the database.
	xtesting.RunEnginesWithDB(
		t,
		db,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExpectLatchesSetEventually(t, &delivered)

			// The TypeA deadline was skipped rather than delivered, so it
			// should still be pending in the database.
			xtesting.WaitForQueryResult(
				t,
				"TypeA deadline is still pending",
				1,
				engine.DB,
				`SELECT COUNT(*)
				FROM process.deadlines`,
			)
		},
		dogma.ViaProcess(
			&stubs.ProcessMessageHandlerStub[*stubs.ProcessRootStub]{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
						dogma.ExecutesCommand[*stubs.CommandStub[stubs.TypeX]](),
						dogma.SchedulesDeadline[*stubs.DeadlineStub[stubs.TypeB]](),
					)
				},
				RouteEventToInstanceFunc: func(
					context.Context,
					dogma.Event,
				) (string, bool, error) {
					return "<instance>", true, nil
				},
				HandleEventFunc: func(
					context.Context,
					*stubs.ProcessRootStub,
					dogma.ProcessEventScope[*stubs.ProcessRootStub],
					dogma.Event,
				) error {
					return nil
				},
				HandleDeadlineFunc: func(
					_ context.Context,
					_ *stubs.ProcessRootStub,
					_ dogma.ProcessDeadlineScope[*stubs.ProcessRootStub],
					d dogma.Deadline,
				) error {
					switch d.(type) {
					case *stubs.DeadlineStub[stubs.TypeB]:
						delivered.Set()
					default:
						t.Errorf("unexpected deadline delivered: %T", d)
					}

					return nil
				},
			},
		),
	)
}
