package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Remote is a [Source] of the event streams of an application hosted by a
// remote engine, consumed via its [messaginggrpc.EventStreamConsumerAPI]
// service.
//
// Each segment that the remote engine sends is checked before it is
// delivered. If a segment is malformed, holds events of types that were not
// requested, or does not begin where the previous segment of its stream ended,
// [Remote.Watch] or [Remote.Read] returns an error that wraps
// [ErrInvalidSegment] instead of delivering it.
type Remote struct {
	// Client is the client used to call the remote engine's event stream
	// consumer API.
	Client messaginggrpc.EventStreamConsumerAPIClient
}

// Watch delivers events to fn as they are appended to the remote
// application's streams.
//
// Each stream's opening segment begins wherever the remote engine begins it.
// The remote engine never ends the watch of its own accord, so it ending is a
// failure.
func (s *Remote) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(Segment) error,
) error {
	panicIfEmpty(types)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	segments, err := s.Client.WatchEvents(
		ctx,
		messaginggrpc.NewWatchEventsRequestBuilder().
			WithFilter(messageFilter(types)).
			Build(),
	)
	if err != nil {
		return watchError(ctx, err)
	}

	var ends uuidcollection.Map[uint64]

	for {
		res, err := segments.Recv()
		if err != nil {
			return watchError(ctx, err)
		}

		seg, err := parseSegment(res, types)
		if err != nil {
			return err
		}

		if end, ok := ends.Get(seg.StreamID); ok && seg.Begin != end {
			return invalidSegment(
				res,
				"it does not begin at offset %d, where the previous segment ended",
				end,
			)
		}

		if err := fn(seg); err != nil {
			return err
		}

		ends.Set(seg.StreamID, seg.End)
	}
}

// Read delivers events from a stream to fn, beginning at the given offset.
func (s *Remote) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) error {
	panicIfEmpty(types)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	segments, err := s.Client.ReadEvents(
		ctx,
		messaginggrpc.NewReadEventsRequestBuilder().
			WithEventStreamId(streamID).
			WithBeginOffset(begin).
			WithFilter(messageFilter(types)).
			Build(),
	)
	if err != nil {
		return readError(ctx, streamID, err)
	}

	first := true

	for {
		res, err := segments.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return readError(ctx, streamID, err)
		}

		seg, err := parseSegment(res, types)
		if err != nil {
			return err
		}

		if !seg.StreamID.Equal(streamID) {
			return invalidSegment(
				res,
				"it is not a segment of event stream %s, which was requested",
				streamID,
			)
		}

		if seg.Begin != begin {
			reason := "where the previous segment ended"
			if first {
				reason = "which was requested"
			}

			return invalidSegment(
				res,
				"it does not begin at offset %d, %s",
				begin,
				reason,
			)
		}

		if err := fn(seg); err != nil {
			return err
		}

		begin = seg.End
		first = false
	}
}

// watchError returns the error that [Remote.Watch] returns when its call to
// the remote engine fails with the given error.
func watchError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err == io.EOF {
		return errors.New("remote engine stopped watching events")
	}

	switch status.Code(err) {
	case codes.Aborted:
		return fmt.Errorf("unable to watch events: %w (%w)", err, ErrConsumerTooSlow)
	case codes.Unimplemented:
		return fmt.Errorf("unable to watch events: %w (%w)", err, errors.ErrUnsupported)
	}

	return fmt.Errorf("unable to watch events: %w", err)
}

// messageFilter returns the filter that matches events of the given types, or
// of every type if types is nil.
func messageFilter(types *uuidcollection.Set) *messaginggrpc.MessageFilter {
	if types == nil {
		return messaginggrpc.NewMessageFilterBuilder().
			WithAll(&emptypb.Empty{}).
			Build()
	}

	return messaginggrpc.NewMessageFilterBuilder().
		WithSpecific(
			messaginggrpc.NewMessageFilter_MessageTypesBuilder().
				WithIds(slices.Collect(types.All())).
				Build(),
		).
		Build()
}

// readError returns the error that [Remote.Read] returns when its call to the
// remote engine fails with the given error.
func readError(ctx context.Context, streamID *uuidpb.UUID, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("unable to read event stream %s: %w (%w)", streamID, err, ErrUnrecognizedStream)
	case codes.OutOfRange:
		return fmt.Errorf("unable to read event stream %s: %w (%w)", streamID, err, ErrOffsetOutOfRange)
	}

	return fmt.Errorf("unable to read event stream %s: %w", streamID, err)
}

// parseSegment returns the segment that the remote engine sent in res.
//
// It returns an error if the segment is malformed, or holds events of types
// other than those given. A nil set of types means events of every type.
func parseSegment(
	res *messaginggrpc.EventStreamSegment,
	types *uuidcollection.Set,
) (Segment, error) {
	seg := Segment{
		StreamID: res.GetEventStreamId(),
		Begin:    res.GetBeginOffset(),
		End:      res.GetEndOffset(),
	}

	if err := seg.StreamID.Validate(); err != nil {
		return Segment{}, invalidSegment(res, "invalid event stream ID: %w", err)
	}

	if seg.End < seg.Begin {
		return Segment{}, invalidSegment(res, "it ends before it begins")
	}

	// next is the lowest offset that the next event may have.
	next := seg.Begin

	for i, multi := range res.GetEnvelopes() {
		if err := multi.Validate(); err != nil {
			return Segment{}, invalidSegment(res, "invalid envelope at index %d: %w", i, err)
		}

		for env := range multi.All() {
			id := env.GetBody().GetMessageId()

			pos, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](env)
			if err != nil {
				return Segment{}, invalidSegment(res, "event %s has an invalid event stream position: %w", id, err)
			}

			if !ok {
				return Segment{}, invalidSegment(res, "event %s has no event stream position", id)
			}

			if !pos.GetStreamId().Equal(seg.StreamID) {
				return Segment{}, invalidSegment(res, "event %s is from event stream %s", id, pos.GetStreamId())
			}

			switch offset := pos.GetOffset(); {
			case offset < seg.Begin:
				return Segment{}, invalidSegment(res, "event %s is at offset %d, before the segment begins", id, offset)
			case offset >= seg.End:
				return Segment{}, invalidSegment(res, "event %s is at offset %d, at or beyond the end of the segment", id, offset)
			case offset < next:
				return Segment{}, invalidSegment(res, "event %s is at offset %d, which does not follow the previous event's offset, %d", id, offset, next-1)
			}

			if typeID := env.GetBody().GetMessage().GetTypeId(); types != nil && !types.Has(typeID) {
				return Segment{}, invalidSegment(res, "event %s is of type %s, which was not requested", id, typeID)
			}

			seg.Envelopes = append(seg.Envelopes, env)
			next = pos.GetOffset() + 1
		}
	}

	return seg, nil
}

// invalidSegment returns an [ErrInvalidSegment] describing why the segment that
// the remote engine sent in res is invalid.
func invalidSegment(
	res *messaginggrpc.EventStreamSegment,
	format string,
	args ...any,
) error {
	return fmt.Errorf(
		"%w [%d, %d) of event stream %s: %w",
		ErrInvalidSegment,
		res.GetBeginOffset(),
		res.GetEndOffset(),
		res.GetEventStreamId(),
		fmt.Errorf(format, args...),
	)
}
