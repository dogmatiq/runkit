package source_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream"
	. "github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRemote(t *testing.T) {
	var (
		streamA = uuidpb.Generate()
		streamB = uuidpb.Generate()
		typeA   = uuidpb.Generate()
		typeB   = uuidpb.Generate()
	)

	// event returns a well-formed envelope for an event of the given type at
	// the given offset of the given stream.
	event := func(streamID *uuidpb.UUID, offset uint64, typeID *uuidpb.UUID) *envelopepb.Envelope {
		id := uuidpb.Generate()

		env := envelopepb.NewEnvelopeBuilder().
			WithHeader(
				envelopepb.NewHeaderBuilder().
					WithCausationId(id).
					WithCorrelationId(id).
					WithSource(
						envelopepb.NewSourceBuilder().
							WithApplication(identitypb.New("<app>", uuidpb.Generate())).
							Build(),
					).
					Build(),
			).
			WithBody(
				envelopepb.NewBodyBuilder().
					WithMessageId(id).
					WithCreatedAt(timestamppb.Now()).
					WithMessage(
						envelopepb.NewMessageBuilder().
							WithTypeId(typeID).
							WithDescription("<event>").
							Build(),
					).
					Build(),
			).
			Build()

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(streamID).
				WithOffset(offset).
				Build(),
		)

		return env
	}

	// segment returns a segment of the given stream as the remote engine sends
	// it, with each of the given events in its own multi-envelope.
	segment := func(
		streamID *uuidpb.UUID,
		begin, end uint64,
		envelopes ...*envelopepb.Envelope,
	) *messaginggrpc.EventStreamSegment {
		var multi []*envelopepb.MultiEnvelope

		for _, env := range envelopes {
			multi = append(multi, env.AsMultiEnvelope())
		}

		return messaginggrpc.NewEventStreamSegmentBuilder().
			WithEventStreamId(streamID).
			WithBeginOffset(begin).
			WithEndOffset(end).
			WithEnvelopes(multi).
			Build()
	}

	// offsets returns the offsets of the events in seg.
	offsets := func(seg Segment) []uint64 {
		var offsets []uint64

		for _, env := range seg.Envelopes {
			offsets = append(
				offsets,
				envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset(),
			)
		}

		return offsets
	}

	// expectSegment asserts that seg is the given segment of the given stream
	// and holds events at the given offsets.
	expectSegment := func(
		t *testing.T,
		seg Segment,
		streamID *uuidpb.UUID,
		begin, end uint64,
		want ...uint64,
	) {
		t.Helper()

		if !seg.StreamID.Equal(streamID) {
			t.Fatalf("unexpected stream: got %s, want %s", seg.StreamID, streamID)
		}

		if seg.Begin != begin || seg.End != end {
			t.Fatalf("unexpected segment: got [%d, %d), want [%d, %d)", seg.Begin, seg.End, begin, end)
		}

		if got := offsets(seg); !slices.Equal(got, want) {
			t.Fatalf("unexpected offsets: got %v, want %v", got, want)
		}
	}

	// expectError asserts that err has the given message.
	expectError := func(t *testing.T, err error, want string) {
		t.Helper()

		if err == nil {
			t.Fatalf("expected an error: want %q", want)
		}

		if got := err.Error(); got != want {
			t.Fatalf("unexpected error:\ngot  %q\nwant %q", got, want)
		}
	}

	// expectEnded asserts that the call made with the given context has ended.
	expectEnded := func(t *testing.T, ctx context.Context) {
		t.Helper()

		if ctx.Err() == nil {
			t.Fatal("expected the call to have ended")
		}
	}

	// receive waits for a value on ch and returns it.
	receive := func(t *testing.T, description string, ch <-chan *messaginggrpc.WatchEventsRequest) *messaginggrpc.WatchEventsRequest {
		t.Helper()

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for %s", description)
			return nil
		case v := <-ch:
			return v
		}
	}

	t.Run("Watch", func(t *testing.T) {
		// watch runs src.Watch on another goroutine until the test ends,
		// delivering each segment to the returned channel.
		watch := func(t *testing.T, src *Remote, types *uuidcollection.Set) <-chan Segment {
			t.Helper()

			segments := make(chan Segment)

			xtesting.GoE(
				t,
				func(ctx context.Context) error {
					return src.Watch(
						ctx,
						types,
						func(seg Segment) error {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case segments <- seg:
								return nil
							}
						},
					)
				},
			)

			return segments
		}

		// expect waits for a segment to be delivered and returns it.
		expect := func(t *testing.T, segments <-chan Segment) Segment {
			t.Helper()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for a segment")
				return Segment{}
			case seg := <-segments:
				return seg
			}
		}

		// watchUntilError calls src.Watch and returns the segments it
		// delivers and the error it returns.
		watchUntilError := func(t *testing.T, src *Remote, types *uuidcollection.Set) ([]Segment, error) {
			t.Helper()

			var segments []Segment

			err := src.Watch(
				t.Context(),
				types,
				func(seg Segment) error {
					segments = append(segments, seg)
					return nil
				},
			)

			return segments, err
		}

		t.Run("it watches for events of the given types", func(t *testing.T) {
			requests := make(chan *messaginggrpc.WatchEventsRequest, 1)

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						req *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						requests <- req
						return openCall(ctx), nil
					},
				},
			}

			want := uuidcollection.NewSet(typeA, typeB)
			watch(t, src, want)

			req := receive(t, "request", requests)

			if got := uuidcollection.NewSet(req.GetMessageTypeIds()...); !got.IsEqual(want) {
				t.Fatalf("unexpected message types: got %s, want %s", got, want)
			}
		})

		t.Run("it watches for events of every type if none are given", func(t *testing.T) {
			requests := make(chan *messaginggrpc.WatchEventsRequest, 1)

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						req *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						requests <- req
						return openCall(ctx), nil
					},
				},
			}

			watch(t, src, nil)

			req := receive(t, "request", requests)

			if got := req.GetMessageTypeIds(); len(got) != 0 {
				t.Fatalf("unexpected message types: got %s, want none", got)
			}
		})

		t.Run("it delivers each segment as it arrives", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return openCall(
							ctx,
							segment(streamA, 3, 5, event(streamA, 3, typeA)),
							segment(streamB, 0, 1, event(streamB, 0, typeA)),
							segment(streamA, 5, 6),
						), nil
					},
				},
			}

			segments := watch(t, src, nil)

			expectSegment(t, expect(t, segments), streamA, 3, 5, 3)
			expectSegment(t, expect(t, segments), streamB, 0, 1, 0)
			expectSegment(t, expect(t, segments), streamA, 5, 6)
		})

		t.Run("it delivers each event in a multi-envelope", func(t *testing.T) {
			first := event(streamA, 1, typeA)
			second := event(streamA, 2, typeA)

			multi := first.AsMultiEnvelope()
			multi.AppendBodies(second.GetBody())

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return openCall(
							ctx,
							messaginggrpc.NewEventStreamSegmentBuilder().
								WithEventStreamId(streamA).
								WithBeginOffset(0).
								WithEndOffset(3).
								WithEnvelopes([]*envelopepb.MultiEnvelope{multi}).
								Build(),
						), nil
					},
				},
			}

			segments := watch(t, src, nil)

			expectSegment(t, expect(t, segments), streamA, 0, 3, 1, 2)
		})

		t.Run("it returns fn's error unchanged, and ends the call", func(t *testing.T) {
			var call context.Context

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						call = ctx
						return openCall(ctx, segment(streamA, 0, 1)), nil
					},
				},
			}

			errRejected := errors.New("<rejected>")

			err := src.Watch(
				t.Context(),
				nil,
				func(Segment) error {
					return errRejected
				},
			)

			if err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}

			expectEnded(t, call)
		})

		t.Run("it returns the context's error once the context is canceled", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return openCall(ctx), nil
					},
				},
			}

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := src.Watch(
				ctx,
				nil,
				func(Segment) error {
					return nil
				},
			)

			if err != context.Canceled {
				t.Fatalf("unexpected error: got %v, want %v", err, context.Canceled)
			}
		})

		t.Run("it returns an error when the remote engine ends the call", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return endedCall(ctx, io.EOF, segment(streamA, 0, 1)), nil
					},
				},
			}

			segments, err := watchUntilError(t, src, nil)

			expectError(t, err, "remote engine stopped watching events")

			if len(segments) != 1 {
				t.Fatalf("unexpected number of segments: got %d, want 1", len(segments))
			}
		})

		t.Run("it returns an error when the call fails", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return endedCall(ctx, status.Error(codes.Unavailable, "<unavailable>")), nil
					},
				},
			}

			_, err := watchUntilError(t, src, nil)

			expectError(t, err, "unable to watch events: rpc error: code = Unavailable desc = <unavailable>")
		})

		t.Run("it returns an error when the remote engine does not offer event streams", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{},
			}

			_, err := watchUntilError(t, src, nil)

			expectError(t, err, "unable to watch events: rpc error: code = Unimplemented desc = method WatchEvents not implemented")
		})

		t.Run("it returns an error when a segment does not begin where the previous segment of its stream ended", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return openCall(
							ctx,
							segment(streamA, 0, 1),
							segment(streamB, 5, 5),
							segment(streamA, 2, 3),
						), nil
					},
				},
			}

			segments, err := watchUntilError(t, src, nil)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"invalid segment [2, 3) of event stream %s: it does not begin at offset 1, where the previous segment ended",
					streamA,
				),
			)

			if len(segments) != 2 {
				t.Fatalf("unexpected number of segments: got %d, want 2", len(segments))
			}
		})

		t.Run("it returns an error when a segment is invalid", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.WatchEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
						return openCall(ctx, segment(streamA, 2, 1)), nil
					},
				},
			}

			_, err := watchUntilError(t, src, nil)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"invalid segment [2, 1) of event stream %s: it ends before it begins",
					streamA,
				),
			)
		})
	})

	t.Run("Read", func(t *testing.T) {
		// read calls src.Read and returns the segments it delivers and the
		// error it returns.
		read := func(
			t *testing.T,
			src *Remote,
			streamID *uuidpb.UUID,
			types *uuidcollection.Set,
			begin uint64,
		) ([]Segment, error) {
			t.Helper()

			var segments []Segment

			err := src.Read(
				t.Context(),
				streamID,
				types,
				begin,
				func(seg Segment) error {
					segments = append(segments, seg)
					return nil
				},
			)

			return segments, err
		}

		// reading returns a source whose remote engine responds to each read
		// with the given segments, then ends the call.
		reading := func(segments ...*messaginggrpc.EventStreamSegment) *Remote {
			return &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					ReadEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.ReadEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						return endedCall(ctx, io.EOF, segments...), nil
					},
				},
			}
		}

		t.Run("it reads the given stream from the given offset, for the given types", func(t *testing.T) {
			var req *messaginggrpc.ReadEventsRequest

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					ReadEventsFunc: func(
						ctx context.Context,
						r *messaginggrpc.ReadEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						req = r
						return endedCall(ctx, io.EOF), nil
					},
				},
			}

			types := uuidcollection.NewSet(typeA, typeB)

			if _, err := read(t, src, streamA, types, 3); err != nil {
				t.Fatal(err)
			}

			if got := req.GetEventStreamId(); !got.Equal(streamA) {
				t.Fatalf("unexpected stream: got %s, want %s", got, streamA)
			}

			if got := req.GetBeginOffset(); got != 3 {
				t.Fatalf("unexpected begin offset: got %d, want 3", got)
			}

			if got := uuidcollection.NewSet(req.GetMessageTypeIds()...); !got.IsEqual(types) {
				t.Fatalf("unexpected message types: got %s, want %s", got, types)
			}
		})

		t.Run("it reads every type if none are given", func(t *testing.T) {
			var req *messaginggrpc.ReadEventsRequest

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					ReadEventsFunc: func(
						ctx context.Context,
						r *messaginggrpc.ReadEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						req = r
						return endedCall(ctx, io.EOF), nil
					},
				},
			}

			if _, err := read(t, src, streamA, nil, 0); err != nil {
				t.Fatal(err)
			}

			if got := req.GetMessageTypeIds(); len(got) != 0 {
				t.Fatalf("unexpected message types: got %s, want none", got)
			}
		})

		t.Run("it delivers each segment until the remote engine ends the call", func(t *testing.T) {
			src := reading(
				segment(streamA, 1, 3, event(streamA, 1, typeA)),
				segment(streamA, 3, 5, event(streamA, 4, typeA)),
			)

			segments, err := read(t, src, streamA, nil, 1)
			if err != nil {
				t.Fatal(err)
			}

			if len(segments) != 2 {
				t.Fatalf("unexpected number of segments: got %d, want 2", len(segments))
			}

			expectSegment(t, segments[0], streamA, 1, 3, 1)
			expectSegment(t, segments[1], streamA, 3, 5, 4)
		})

		t.Run("it returns fn's error unchanged, and ends the call", func(t *testing.T) {
			var call context.Context

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					ReadEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.ReadEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						call = ctx
						return openCall(ctx, segment(streamA, 0, 1)), nil
					},
				},
			}

			errRejected := errors.New("<rejected>")

			err := src.Read(
				t.Context(),
				streamA,
				nil,
				0,
				func(Segment) error {
					return errRejected
				},
			)

			if err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}

			expectEnded(t, call)
		})

		t.Run("it returns an error when the call fails", func(t *testing.T) {
			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					ReadEventsFunc: func(
						ctx context.Context,
						_ *messaginggrpc.ReadEventsRequest,
						_ ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						return endedCall(ctx, messaginggrpc.UnrecognizedEventStreamError(streamA)), nil
					},
				},
			}

			_, err := read(t, src, streamA, nil, 0)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"unable to read event stream %s: rpc error: code = NotFound desc = unrecognized event stream",
					streamA,
				),
			)
		})

		t.Run("it returns an error when a segment is of another stream", func(t *testing.T) {
			src := reading(segment(streamB, 0, 1))

			_, err := read(t, src, streamA, nil, 0)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"invalid segment [0, 1) of event stream %s: it is not a segment of event stream %s, which was requested",
					streamB,
					streamA,
				),
			)
		})

		t.Run("it returns an error when the first segment does not begin at the given offset", func(t *testing.T) {
			src := reading(segment(streamA, 0, 3))

			_, err := read(t, src, streamA, nil, 1)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"invalid segment [0, 3) of event stream %s: it does not begin at offset 1, which was requested",
					streamA,
				),
			)
		})

		t.Run("it returns an error when a segment does not begin where the previous one ended", func(t *testing.T) {
			src := reading(
				segment(streamA, 0, 2),
				segment(streamA, 3, 4),
			)

			segments, err := read(t, src, streamA, nil, 0)

			expectError(
				t,
				err,
				fmt.Sprintf(
					"invalid segment [3, 4) of event stream %s: it does not begin at offset 2, where the previous segment ended",
					streamA,
				),
			)

			if len(segments) != 1 {
				t.Fatalf("unexpected number of segments: got %d, want 1", len(segments))
			}
		})

		t.Run("it returns an error when a segment is invalid", func(t *testing.T) {
			cases := []struct {
				Name string

				// Setup returns the invalid segment and the error that it is
				// expected to produce.
				Setup func() (*messaginggrpc.EventStreamSegment, string)
			}{
				{
					"the stream ID is invalid",
					func() (*messaginggrpc.EventStreamSegment, string) {
						invalid := &uuidpb.UUID{}

						return segment(invalid, 0, 1), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: invalid event stream ID: %s",
							invalid,
							invalid.Validate(),
						)
					},
				},
				{
					"it ends before it begins",
					func() (*messaginggrpc.EventStreamSegment, string) {
						return segment(streamA, 2, 1), fmt.Sprintf(
							"invalid segment [2, 1) of event stream %s: it ends before it begins",
							streamA,
						)
					},
				},
				{
					"an envelope is malformed",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 0, typeA)
						env.GetBody().GetMessage().SetDescription("")

						return segment(streamA, 0, 1, env), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: invalid envelope at index 0: %s",
							streamA,
							env.AsMultiEnvelope().Validate(),
						)
					},
				},
				{
					"an event has a malformed event stream position",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 0, typeA)

						pos, err := anypb.New(&envelopepb.EventStreamPosition{})
						if err != nil {
							t.Fatal(err)
						}
						pos.Value = []byte{0xff}

						env.GetBody().SetExtensions([]*anypb.Any{pos})

						_, _, cause := envelopepb.GetExtension[*envelopepb.EventStreamPosition](env)

						return segment(streamA, 0, 1, env), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: event %s has an invalid event stream position: %s",
							streamA,
							env.GetBody().GetMessageId(),
							cause,
						)
					},
				},
				{
					"an event has no event stream position",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 0, typeA)
						env.GetBody().SetExtensions(nil)

						return segment(streamA, 0, 1, env), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: event %s has no event stream position",
							streamA,
							env.GetBody().GetMessageId(),
						)
					},
				},
				{
					"an event is from another stream",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamB, 0, typeA)

						return segment(streamA, 0, 1, env), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: event %s is from event stream %s",
							streamA,
							env.GetBody().GetMessageId(),
							streamB,
						)
					},
				},
				{
					"an event is before the beginning of the segment",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 0, typeA)

						return segment(streamA, 1, 3, env), fmt.Sprintf(
							"invalid segment [1, 3) of event stream %s: event %s is at offset 0, before the segment begins",
							streamA,
							env.GetBody().GetMessageId(),
						)
					},
				},
				{
					"an event is at the end of the segment",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 3, typeA)

						return segment(streamA, 1, 3, env), fmt.Sprintf(
							"invalid segment [1, 3) of event stream %s: event %s is at offset 3, at or beyond the end of the segment",
							streamA,
							env.GetBody().GetMessageId(),
						)
					},
				},
				{
					"an event is before the event that precedes it",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 1, typeA)

						return segment(streamA, 0, 4, event(streamA, 2, typeA), env), fmt.Sprintf(
							"invalid segment [0, 4) of event stream %s: event %s is at offset 1, which does not follow the previous event's offset, 2",
							streamA,
							env.GetBody().GetMessageId(),
						)
					},
				},
				{
					"an event is at the same offset as the event that precedes it",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 2, typeA)

						return segment(streamA, 0, 4, event(streamA, 2, typeA), env), fmt.Sprintf(
							"invalid segment [0, 4) of event stream %s: event %s is at offset 2, which does not follow the previous event's offset, 2",
							streamA,
							env.GetBody().GetMessageId(),
						)
					},
				},
				{
					"an event is of a type that was not requested",
					func() (*messaginggrpc.EventStreamSegment, string) {
						env := event(streamA, 0, typeB)

						return segment(streamA, 0, 1, env), fmt.Sprintf(
							"invalid segment [0, 1) of event stream %s: event %s is of type %s, which was not requested",
							streamA,
							env.GetBody().GetMessageId(),
							typeB,
						)
					},
				},
			}

			for _, c := range cases {
				t.Run(c.Name, func(t *testing.T) {
					seg, want := c.Setup()
					src := reading(seg)

					segments, err := read(t, src, streamA, uuidcollection.NewSet(typeA), seg.GetBeginOffset())

					expectError(t, err, want)

					if len(segments) != 0 {
						t.Fatalf("unexpected number of segments: got %d, want 0", len(segments))
					}
				})
			}
		})
	})

	t.Run("it delivers the events missed between watches to a catalog's consumers", func(t *testing.T) {
		types := uuidcollection.NewSet(typeA)
		calls := make(chan *callStub, 2)
		reads := make(chan uint64, 1)

		src := &Remote{
			Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
				WatchEventsFunc: func(
					ctx context.Context,
					_ *messaginggrpc.WatchEventsRequest,
					_ ...grpc.CallOption,
				) (messaginggrpc.EventStreamConsumerAPI_WatchEventsClient, error) {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case call := <-calls:
						call.Ctx = ctx
						return call, nil
					}
				},
				ReadEventsFunc: func(
					ctx context.Context,
					req *messaginggrpc.ReadEventsRequest,
					_ ...grpc.CallOption,
				) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
					reads <- req.GetBeginOffset()

					return endedCall(
						ctx,
						io.EOF,
						segment(
							streamA,
							1,
							3,
							event(streamA, 1, typeA),
							event(streamA, 2, typeA),
						),
					), nil
				},
			},
		}

		first := openCall(nil, segment(streamA, 0, 1, event(streamA, 0, typeA)))
		calls <- first
		calls <- openCall(nil, segment(streamA, 3, 3))

		catalog := &eventstream.Catalog{
			CacheSize: 10,
			Logger:    spruce.NewTestLogger(t),
		}

		xtesting.GoE(
			t,
			func(ctx context.Context) error {
				return catalog.Watch(ctx, src, types)
			},
		)

		expectCatalogToHave(t, catalog, streamA)

		handled := make(chan uint64, 10)

		xtesting.GoE(
			t,
			func(ctx context.Context) error {
				return catalog.Consume(
					ctx,
					streamA,
					types,
					0,
					func(seg Segment) error {
						for _, offset := range offsets(seg) {
							handled <- offset
						}
						return nil
					},
				)
			},
		)

		xtesting.ExpectToReceiveN(t, "handled offset", handled, 0)

		first.Err = status.Error(codes.Unavailable, "<unavailable>")
		close(first.Segments)

		xtesting.ExpectToReceiveN(t, "read from offset", reads, 1)
		xtesting.ExpectToReceiveN(t, "handled offset", handled, 1, 2)
		xtesting.ExpectNoReceive(t, "handled offset", handled)
	})

	t.Run("with a running engine", func(t *testing.T) {
		typesA := uuidcollection.NewSet(
			stubs.MessageTypeUUID[*stubs.EventStub[stubs.TypeA]](),
		)

		// eventA returns an event of type A whose content is its offset.
		eventA := func(_ *uuidpb.UUID, offset uint64) dogma.Event {
			return &stubs.EventStub[stubs.TypeA]{
				Content: stubs.TypeA(fmt.Sprint(offset)),
			}
		}

		// watch returns a catalog that watches the engine at the other end of
		// the given connection for events of type A until the test ends, and
		// the number of times the source has called ReadEvents.
		watch := func(t testing.TB, conn grpc.ClientConnInterface) (*eventstream.Catalog, *atomic.Int32) {
			t.Helper()

			client := messaginggrpc.NewEventStreamConsumerAPIClient(conn)
			reads := &atomic.Int32{}

			src := &Remote{
				Client: &messaginggrpc.EventStreamConsumerAPIClientStub{
					WatchEventsFunc: client.WatchEvents,
					ReadEventsFunc: func(
						ctx context.Context,
						req *messaginggrpc.ReadEventsRequest,
						options ...grpc.CallOption,
					) (messaginggrpc.EventStreamConsumerAPI_ReadEventsClient, error) {
						reads.Add(1)
						return client.ReadEvents(ctx, req, options...)
					},
				},
			}

			catalog := &eventstream.Catalog{
				CacheSize: 10,
				Logger:    spruce.NewTestLogger(t),
			}

			xtesting.GoE(
				t,
				func(ctx context.Context) error {
					return catalog.Watch(ctx, src, typesA)
				},
			)

			return catalog, reads
		}

		// consume consumes events of type A from a stream in the catalog,
		// beginning at the given offset, until the test ends. It delivers the
		// offset of each event to the returned channel.
		consume := func(
			t testing.TB,
			catalog *eventstream.Catalog,
			streamID *uuidpb.UUID,
			begin uint64,
		) <-chan uint64 {
			t.Helper()

			handled := make(chan uint64, 1000)

			xtesting.GoE(
				t,
				func(ctx context.Context) error {
					return catalog.Consume(
						ctx,
						streamID,
						typesA,
						begin,
						func(seg Segment) error {
							for _, offset := range offsets(seg) {
								handled <- offset
							}
							return nil
						},
					)
				},
			)

			return handled
		}

		t.Run("it delivers the engine's streams to a catalog", func(t *testing.T) {
			var existing []*uuidpb.UUID

			xtesting.SetupThenRunEngines(
				t,
				func(t testing.TB, db *sql.DB) {
					existing = xtesting.PopulateEventStreams(t, db, eventA, 3, 0)
				},
				func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
					catalog, _ := watch(t, xtesting.Dial(t, cluster.Nodes()[0]))

					expectCatalogToHave(t, catalog, existing...)

					created := xtesting.CreateEventStream(t, db)

					expectCatalogToHave(t, catalog, append(existing, created)...)
				},
			)
		})

		t.Run("it reads a stream's earlier events, then watches for new ones", func(t *testing.T) {
			var streamID *uuidpb.UUID

			xtesting.SetupThenRunEngines(
				t,
				func(t testing.TB, db *sql.DB) {
					streamID = xtesting.PopulateEventStreams(t, db, eventA, 150)[0]
				},
				func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
					catalog, reads := watch(t, xtesting.Dial(t, cluster.Nodes()[0]))

					expectCatalogToHave(t, catalog, streamID)

					handled := consume(t, catalog, streamID, 0)

					var earlier []uint64
					for offset := range uint64(150) {
						earlier = append(earlier, offset)
					}

					xtesting.ExpectToReceiveN(t, "handled offset", handled, earlier...)

					readsBefore := reads.Load()
					if readsBefore == 0 {
						t.Fatal("expected the earlier events to be read")
					}

					xtesting.AppendToEventStream(t, db, streamID, eventA(streamID, 150))

					xtesting.ExpectToReceiveN(t, "handled offset", handled, 150)

					if got := reads.Load(); got != readsBefore {
						t.Fatalf("unexpected reads after the new event: got %d, want none", got-readsBefore)
					}
				},
			)
		})

		t.Run("it continues when the node it is connected to stops", func(t *testing.T) {
			xtesting.RunEngines(
				t,
				func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
					nodes := cluster.Nodes()
					streamID := xtesting.CreateEventStream(t, db)

					catalog, _ := watch(t, xtesting.Dial(t, nodes[0], nodes[1]))

					expectCatalogToHave(t, catalog, streamID)

					handled := consume(t, catalog, streamID, 0)

					xtesting.AppendToEventStream(t, db, streamID, eventA(streamID, 0))
					xtesting.ExpectToReceiveN(t, "handled offset", handled, 0)

					cluster.StopNode(nodes[0])

					xtesting.AppendToEventStream(t, db, streamID, eventA(streamID, 1), eventA(streamID, 2))
					xtesting.ExpectToReceiveN(t, "handled offset", handled, 1, 2)
					xtesting.ExpectNoReceive(t, "handled offset", handled)
				},
			)
		})
	})
}

// callStub is a stub of a call to a remote engine that returns segments.
//
// Recv returns each segment sent on Segments, then Err once Segments is
// closed. If Ctx is canceled first, it fails as a gRPC call does.
type callStub struct {
	grpc.ClientStream

	Ctx      context.Context
	Segments chan *messaginggrpc.EventStreamSegment
	Err      error
}

func (c *callStub) Recv() (*messaginggrpc.EventStreamSegment, error) {
	select {
	case <-c.Ctx.Done():
		return nil, status.FromContextError(c.Ctx.Err()).Err()
	case seg, ok := <-c.Segments:
		if ok {
			return seg, nil
		}
		return nil, c.Err
	}
}

// openCall returns a call that receives the given segments, then waits for
// ctx to be canceled.
func openCall(
	ctx context.Context,
	segments ...*messaginggrpc.EventStreamSegment,
) *callStub {
	ch := make(chan *messaginggrpc.EventStreamSegment, len(segments))

	for _, seg := range segments {
		ch <- seg
	}

	return &callStub{
		Ctx:      ctx,
		Segments: ch,
	}
}

// endedCall returns a call that receives the given segments, then fails with
// the given error.
func endedCall(
	ctx context.Context,
	err error,
	segments ...*messaginggrpc.EventStreamSegment,
) *callStub {
	call := openCall(ctx, segments...)
	call.Err = err
	close(call.Segments)
	return call
}

// expectCatalogToHave waits for the catalog to have exactly the given streams.
func expectCatalogToHave(
	t testing.TB,
	c *eventstream.Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := c.StreamIDs().Load()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected stream IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
