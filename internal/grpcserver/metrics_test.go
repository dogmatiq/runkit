package grpcserver_test

import (
	"bytes"
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/protobuf/configpb"
	"github.com/dogmatiq/protobuf/metricsgrpc"
	. "github.com/dogmatiq/runkit/internal/grpcserver"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMetricsService_preamble(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, ctx := newMetricsClient(t, cluster)
			responses := watchClusterMetrics(t, ctx, client)

			preamble := responses.receive().GetPreamble()
			if preamble == nil {
				t.Fatal("expected the first response to be a preamble")
			}

			if err := preamble.GetId().Validate(); err != nil {
				t.Fatal(err)
			}

			xtesting.ExpectQueryResult(
				t,
				"the cluster ID is the one that the database stores",
				preamble.GetClusterId().AsStd().String(),
				db,
				`SELECT id
				FROM runkit.cluster`,
			)

			app := preamble.GetApplication()

			if got, want := app.GetIdentity().GetName(), t.Name(); got != want {
				t.Fatalf("unexpected application name: got %q, want %q", got, want)
			}

			if len(app.GetHandlers()) != 1 {
				t.Fatalf("unexpected number of handlers: got %d, want 1", len(app.GetHandlers()))
			}

			handler := app.GetHandlers()[0]

			if got, want := handler.GetType(), configpb.HandlerType_HANDLER_TYPE_INTEGRATION; got != want {
				t.Fatalf("unexpected handler type: got %s, want %s", got, want)
			}

			if got, want := handler.GetIdentity().GetName(), "<handler>"; got != want {
				t.Fatalf("unexpected handler name: got %q, want %q", got, want)
			}

			if got, want := handler.GetConcurrencyPreference(), configpb.ConcurrencyPreference_CONCURRENCY_PREFERENCE_MAXIMIZE; got != want {
				t.Fatalf("unexpected concurrency preference: got %s, want %s", got, want)
			}

			commandType := string(message.NameFor[*stubs.CommandStub[stubs.TypeA]]())
			eventType := string(message.NameFor[*stubs.EventStub[stubs.TypeA]]())

			expectMessageKind(t, app, commandType, configpb.MessageKind_MESSAGE_KIND_COMMAND)
			expectMessageKind(t, app, eventType, configpb.MessageKind_MESSAGE_KIND_EVENT)

			if usage := handler.GetMessages()[commandType]; !usage.GetIsConsumed() || usage.GetIsProduced() {
				t.Fatalf("a command should be consumed and not produced: got %v", usage)
			}

			if usage := handler.GetMessages()[eventType]; usage.GetIsConsumed() || !usage.GetIsProduced() {
				t.Fatalf("an event should be produced and not consumed: got %v", usage)
			}

			want := []string{
				"dogma.metrics.v1alpha1.EventStreamLength",
				"dogma.metrics.v1alpha1.AggregateInstanceEventStreamBindings",
			}
			if got := preamble.GetSupportedMetrics(); !slices.Equal(got, want) {
				t.Fatalf("unexpected supported metrics: got %v, want %v", got, want)
			}
		},
		consumerAPIApp(),
	)
}

func TestMetricsService_nodeMetricsAreUnimplemented(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, ctx := newMetricsClient(t, cluster)

			responses, err := client.WatchNodeMetrics(
				ctx,
				metricsgrpc.NewWatchNodeMetricsRequestBuilder().Build(),
			)
			if err != nil {
				t.Fatal(err)
			}

			_, err = responses.Recv()
			if got, want := status.Code(err), codes.Unimplemented; got != want {
				t.Fatalf("unexpected status code: got %s, want %s: %v", got, want, err)
			}
		},
		consumerAPIApp(),
	)
}

func TestMetricsService_eventStreamLength(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, ctx := newMetricsClient(t, cluster)
			responses := watchClusterMetrics(t, ctx, client)

			// The preamble shows that the call has begun.
			responses.receive()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1)
			first := responses.expectTotalLength(1)

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandB1, stubs.CommandC1)
			second := responses.expectTotalLength(3)

			if bytes.Compare(second, first) <= 0 {
				t.Fatalf("the revision of a later measurement is not greater: got %x after %x", second, first)
			}
		},
		consumerAPIApp(),
	)
}

func TestMetricsService_newCallsStartWithTheLatestMeasurements(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, ctx := newMetricsClient(t, cluster)

			// While another call is in progress, the sampler is running.
			first := watchClusterMetrics(t, ctx, client)
			first.receive()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1, stubs.CommandB1)
			first.expectTotalLength(2)

			second := watchClusterMetrics(t, ctx, client)
			second.receive()
			second.expectTotalLength(2)
		},
		consumerAPIApp(),
	)
}

func TestMetricsService_measuresAgainAfterTheLastCallEnds(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, _ := newMetricsClient(t, cluster)

			firstCtx, endFirst := context.WithCancel(t.Context())
			defer endFirst()

			first := watchClusterMetrics(t, firstCtx, client)
			first.receive()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1)
			first.expectTotalLength(1)

			// Ending the only call stops the measurements.
			endFirst()
			first.expectEnd()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandB1)

			// A new call starts them again, with the current lengths.
			second := watchClusterMetrics(t, t.Context(), client)
			second.receive()
			second.expectTotalLength(2)
		},
		consumerAPIApp(),
	)
}

func TestMetricsService_endsTheCallWhenMeasuringFails(t *testing.T) {
	t.Parallel()

	// Measuring queries the database, so it fails when the database is closed.
	db := xtesting.NewDatabase(t)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	server := &Server{
		ListenAddress: ":0",
		MetricsAPI: &MetricsService{
			NodeID:       uuid.New(),
			ClusterID:    uuid.New(),
			Application:  configpb.NewApplicationBuilder().Build(),
			DB:           db,
			MinInterval:  time.Hour,
			MaxDutyCycle: 0.1,
			EngineLogger: spruce.NewTestLogger(t),
		},
		EngineFailurePolicy: xtesting.FailurePolicy(),
		EngineLogger:        spruce.NewTestLogger(t),
	}

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	defer cancel()

	go server.Run(ctx)

	addr, err := server.ListenAddr(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client := metricsgrpc.NewMetricsAPIClient(
		xtesting.DialGRPCAddr(t, addr.String()),
	)

	responses := watchClusterMetrics(t, ctx, client)

	// The preamble comes first, and then the call ends.
	responses.receive()

	_, err = responses.Responses.Recv()
	if got, want := status.Code(err), codes.Unavailable; got != want {
		t.Fatalf("unexpected status code: got %s, want %s: %v", got, want, err)
	}
}

func TestMetricsService_aggregateInstanceEventStreamBindings(t *testing.T) {
	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			client, ctx := newMetricsClient(t, cluster)
			responses := watchClusterMetrics(t, ctx, client)

			// The preamble shows that the call has begun.
			responses.receive()

			// Each command goes to the instance that its content names. A1 and A2
			// create two instances, a second A1 uses the first again, and A3
			// creates a third.
			xtesting.ExecuteCommandsSequentially(
				t,
				cluster,
				db,
				stubs.CommandA1,
				stubs.CommandA2,
			)
			responses.expectTotalBindings(2)

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1)
			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA3)
			responses.expectTotalBindings(3)
		},
		aggregateApp(),
	)
}

// aggregateApp returns a handler route for an aggregate that records an event
// for each command, in the instance that the command's content names.
func aggregateApp() dogma.HandlerRoute {
	return dogma.ViaAggregate(
		&stubs.AggregateMessageHandlerStub[*stubs.AggregateRootStub]{
			ConfigureFunc: func(c dogma.AggregateConfigurer) {
				c.Identity("<aggregate>", "9b1d7f3e-2c4a-4e58-8a6d-0f3b5c7e9a11")
				c.Routes(
					dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeA]](),
					dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
				)
			},
			RouteCommandToInstanceFunc: func(m dogma.Command) string {
				return string(m.(*stubs.CommandStub[stubs.TypeA]).Content)
			},
			HandleCommandFunc: func(
				_ *stubs.AggregateRootStub,
				s dogma.AggregateCommandScope[*stubs.AggregateRootStub],
				_ dogma.Command,
			) {
				s.RecordEvent(stubs.EventA1)
			},
		},
	)
}

// expectMessageKind fails the test unless the application has the given kind
// of message.
func expectMessageKind(
	t testing.TB,
	app *configpb.Application,
	typeName string,
	want configpb.MessageKind,
) {
	t.Helper()

	got, ok := app.GetMessages()[typeName]
	if !ok {
		t.Fatalf("the application has no message type %q, it has %v", typeName, app.GetMessages())
	}

	if got != want {
		t.Fatalf("unexpected kind of %q: got %s, want %s", typeName, got, want)
	}
}

// newMetricsClient returns a client for the cluster's metrics API, and a
// context that is canceled when the test's wait timeout elapses.
func newMetricsClient(
	t testing.TB,
	cluster *xtesting.Cluster,
) (metricsgrpc.MetricsAPIClient, context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	t.Cleanup(cancel)

	conn := xtesting.DialGRPC(t, cluster.Nodes()[0])

	return metricsgrpc.NewMetricsAPIClient(conn), ctx
}

// watchClusterMetrics begins watching the cluster's metrics.
func watchClusterMetrics(
	t testing.TB,
	ctx context.Context,
	client metricsgrpc.MetricsAPIClient,
) *metricsReceiver {
	t.Helper()

	responses, err := client.WatchClusterMetrics(
		ctx,
		metricsgrpc.NewWatchClusterMetricsRequestBuilder().Build(),
	)
	if err != nil {
		t.Fatal(err)
	}

	return &metricsReceiver{
		T:         t,
		Responses: responses,
		Lengths:   map[uuid.UUID]uint64{},
		Bindings:  map[bindingKey]uint64{},
	}
}

// bindingKey identifies the instances that a sample of
// AggregateInstanceEventStreamBindings counts.
type bindingKey struct {
	HandlerKey uuid.UUID
	StreamID   uuid.UUID
}

// metricsReceiver receives the responses of a call to WatchClusterMetrics.
type metricsReceiver struct {
	T         testing.TB
	Responses metricsgrpc.MetricsAPI_WatchClusterMetricsClient

	// Lengths maps each stream to the length in the last sample of it that the
	// call received.
	Lengths map[uuid.UUID]uint64

	// Bindings maps each handler and stream to the number of instances in the
	// last sample of them that the call received.
	Bindings map[bindingKey]uint64
}

// receive returns the next response, and fails the test if the call ends first.
func (r *metricsReceiver) receive() *metricsgrpc.WatchMetricsResponse {
	r.T.Helper()

	res, err := r.Responses.Recv()
	if err != nil {
		r.T.Fatal(err)
	}

	return res
}

// receiveSample receives the next response, which must be a sample, and records
// its value. It returns the sample's revision.
//
// It fails the test if the sample's subject is not the one that its value
// implies, or if its value is lower than that of an earlier sample of the same
// subject.
func (r *metricsReceiver) receiveSample() []byte {
	r.T.Helper()

	sample := r.receive().GetSample()
	if sample == nil {
		r.T.Fatal("expected a sample")
	}

	switch sample.GetMetricIndex() {
	case 0:
		length := &metricsgrpc.EventStreamLength{}
		if err := proto.Unmarshal(sample.GetValue(), length); err != nil {
			r.T.Fatal(err)
		}

		id := length.GetEventStreamId().AsStd()

		if !bytes.Equal(sample.GetSubject(), id[:]) {
			r.T.Fatalf("the subject %x is not the stream's ID %s", sample.GetSubject(), id)
		}

		if length.GetValue() < r.Lengths[id] {
			r.T.Fatalf("the length of stream %s fell from %d to %d", id, r.Lengths[id], length.GetValue())
		}

		r.Lengths[id] = length.GetValue()

	case 1:
		bindings := &metricsgrpc.AggregateInstanceEventStreamBindings{}
		if err := proto.Unmarshal(sample.GetValue(), bindings); err != nil {
			r.T.Fatal(err)
		}

		key := bindingKey{
			bindings.GetHandlerKey().AsStd(),
			bindings.GetEventStreamId().AsStd(),
		}

		subject := slices.Concat(key.HandlerKey[:], key.StreamID[:])

		if !bytes.Equal(sample.GetSubject(), subject) {
			r.T.Fatalf("the subject %x is not the handler's key and the stream's ID %x", sample.GetSubject(), subject)
		}

		if bindings.GetValue() < r.Bindings[key] {
			r.T.Fatalf("the instances bound fell from %d to %d", r.Bindings[key], bindings.GetValue())
		}

		r.Bindings[key] = bindings.GetValue()

	default:
		r.T.Fatalf("unexpected metric index: %d", sample.GetMetricIndex())
	}

	return sample.GetRevision()
}

// expectTotalLength receives samples until the lengths of all of the streams
// that it has received add up to the given total, and returns the revision of
// the last sample.
func (r *metricsReceiver) expectTotalLength(total uint64) []byte {
	r.T.Helper()

	var revision []byte

	for {
		var sum uint64
		for _, length := range r.Lengths {
			sum += length
		}

		if sum == total && revision != nil {
			return revision
		}

		if sum > total {
			r.T.Fatalf("the lengths add up to %d, which is more than %d", sum, total)
		}

		revision = r.receiveSample()
	}
}

// expectTotalBindings receives samples until the number of instances that are
// bound to event streams equals the given total. It adds up the latest sample of
// every handler and stream that it has received.
func (r *metricsReceiver) expectTotalBindings(total uint64) {
	r.T.Helper()

	for {
		var sum uint64
		for _, instances := range r.Bindings {
			sum += instances
		}

		if sum == total {
			return
		}

		if sum > total {
			r.T.Fatalf("the bindings add up to %d, which is more than %d", sum, total)
		}

		r.receiveSample()
	}
}

// expectEnd fails the test unless the call ends.
func (r *metricsReceiver) expectEnd() {
	r.T.Helper()

	for {
		if _, err := r.Responses.Recv(); err != nil {
			return
		}
	}
}
