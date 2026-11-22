package grpcserver_test

import (
	"bytes"
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"
	"uuid"

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
			responses := watchMetrics(t, ctx, client)

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

			var supported []string
			for _, m := range preamble.GetSupportedMetrics() {
				supported = append(supported, m.GetMessageName())

				if m.GetReach() != metricsgrpc.Reach_REACH_CLUSTER {
					t.Fatalf("unexpected reach of %s: %s", m.GetMessageName(), m.GetReach())
				}
			}

			want := []string{"dogma.metrics.v1alpha1.EventStreamLength"}
			if !slices.Equal(supported, want) {
				t.Fatalf("unexpected supported metrics: got %v, want %v", supported, want)
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
			responses := watchMetrics(t, ctx, client)

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
			first := watchMetrics(t, ctx, client)
			first.receive()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1, stubs.CommandB1)
			first.expectTotalLength(2)

			second := watchMetrics(t, ctx, client)
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

			first := watchMetrics(t, firstCtx, client)
			first.receive()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandA1)
			first.expectTotalLength(1)

			// Ending the only call stops the measurements.
			endFirst()
			first.expectEnd()

			xtesting.ExecuteCommandsSequentially(t, cluster, db, stubs.CommandB1)

			// A new call starts them again, with the current lengths.
			second := watchMetrics(t, t.Context(), client)
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
			Interval:     time.Hour,
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

	responses := watchMetrics(t, ctx, client)

	// The preamble comes first, and then the call ends.
	responses.receive()

	_, err = responses.Responses.Recv()
	if got, want := status.Code(err), codes.Unavailable; got != want {
		t.Fatalf("unexpected status code: got %s, want %s: %v", got, want, err)
	}
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

// watchMetrics begins watching the metrics.
func watchMetrics(
	t testing.TB,
	ctx context.Context,
	client metricsgrpc.MetricsAPIClient,
) *metricsReceiver {
	t.Helper()

	responses, err := client.WatchMetrics(
		ctx,
		metricsgrpc.NewWatchMetricsRequestBuilder().Build(),
	)
	if err != nil {
		t.Fatal(err)
	}

	return &metricsReceiver{
		T:         t,
		Responses: responses,
		Lengths:   map[uuid.UUID]uint64{},
	}
}

// metricsReceiver receives the responses of a call to WatchMetrics.
type metricsReceiver struct {
	T         testing.TB
	Responses metricsgrpc.MetricsAPI_WatchMetricsClient

	// Lengths maps each stream to the length in the last sample of it that the
	// call received.
	Lengths map[uuid.UUID]uint64
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

// expectTotalLength receives samples until the lengths of all of the streams
// that it has received add up to the given total, and returns the revision of
// the last sample.
//
// It fails the test if a sample is not of an event stream's length, or reports
// a length that is lower than an earlier one.
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

		sample := r.receive().GetSample()
		if sample == nil {
			r.T.Fatal("expected a sample")
		}

		if sample.GetMetricIndex() != 0 {
			r.T.Fatalf("unexpected metric index: %d", sample.GetMetricIndex())
		}

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
		revision = sample.GetRevision()
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
