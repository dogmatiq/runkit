package eventstream

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/internal/cache"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// feed serves consumers of one event stream from one [source.Source], whether
// they are keeping up with the stream or catching up on it.
//
// A consumer that is keeping up is served from a cache of the stream's most
// recent events, which is filled as the source delivers them. One that has
// fallen behind the cache is served by reading from the source until it
// catches up.
//
// The same event stream may be delivered by several sources, in which case
// the [Catalog] has a feed from each.
type feed struct {
	// ID is the ID of the event stream.
	ID *uuidpb.UUID

	// Source is the source that the feed comes from.
	Source source.Source

	// Types is the set of message types that the feed carries, or nil if it
	// carries every type.
	Types *uuidcollection.Set

	// cache holds the stream's most recent events.
	cache *cache.Cache

	// ctx is canceled when the feed is closed, which cancels any read from
	// the source that is in progress.
	ctx context.Context

	// cancel cancels ctx.
	cancel context.CancelFunc

	// m guards closed and reads, so that a read from the source can not begin
	// while the feed is being closed, nor once it is closed.
	m sync.Mutex

	// closed is set when the feed is closed.
	closed cosyne.Latch

	// reads counts the reads from the source that are in progress.
	reads sync.WaitGroup
}

var (
	// errFeedClosed is returned by [feed.consume] when the feed is closed.
	errFeedClosed = errors.New("feed closed")

	// errCaughtUp stops a read from the source once the consumer's checkpoint
	// reaches the cached segment.
	errCaughtUp = errors.New("caught up")
)

// Append appends a segment of the stream.
//
// The segment is expected to begin where the previous one ended. One that does
// not is the opening segment of a new call to [source.Source.Watch], so the
// feed restarts from the segment's begin offset, discarding the events it has
// cached.
//
// It has no effect once the feed is closed.
func (f *feed) Append(seg Segment) {
	if f.closed.IsSet() {
		return
	}

	if c, _ := f.cache.Segment().Load(); seg.Begin != c.End {
		f.cache.Reset(seg.Begin)
	}

	f.cache.Append(seg.Begin, seg.End, seg.Envelopes)
}

// Close closes the feed.
//
// Consumers stop being served from the feed, and any read from the source
// that is in progress on their behalf is canceled.
//
// Closing a feed more than once has no effect.
func (f *feed) Close() {
	f.m.Lock()
	defer f.m.Unlock()

	if f.closed.Set() {
		f.cancel()
	}
}

// Wait blocks until every read from the source has returned.
//
// If the feed is closed, no other read begins, so no call to the source is in
// progress once it returns.
func (f *feed) Wait() {
	f.reads.Wait()
}

// consume delivers events to fn, beginning at the given offset.
//
// Only events of the given types are delivered, in contiguous segments. It
// blocks until ctx is canceled, fn returns an error, a read from the source
// fails, or the feed is closed, in which case it returns [errFeedClosed].
//
// It returns the offset at which delivery stopped, which is the end of the
// last segment that fn accepted.
//
// If the given offset is beyond the end of the cached segment, it reads from
// the source before waiting, in case the cache is behind the source. It returns
// an error that wraps [source.ErrOffsetOutOfRange] if the offset is beyond the
// end of the source's stream.
func (f *feed) consume(
	ctx context.Context,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) (uint64, error) {
	cp := begin

	// confirmed is true once cp is known to be within the source's stream. It
	// becomes true when anything is delivered, or when a read from cp
	// succeeds. A stream never gets shorter, so it remains true even if the
	// cached segment later ends before cp.
	confirmed := false

	for {
		seg, changed := f.cache.Segment().Load()
		envelopes, rel := seg.Read(cp, types)

		switch rel {
		case cache.Within:
			err := fn(Segment{
				StreamID:  f.ID,
				Begin:     cp,
				End:       seg.End,
				Envelopes: envelopes,
			})
			if err != nil {
				return cp, err
			}

			cp = seg.End
			confirmed = true

		case cache.Before:
			from := cp
			var err error

			cp, err = f.read(ctx, types, cp, fn)
			if err != nil {
				return cp, err
			}

			// The source delivered nothing, so the stream is shorter than the
			// cached segment claims, which can not be served.
			if cp == from {
				return cp, fmt.Errorf(
					"event stream %s ends at offset %d, before the cached segment",
					f.ID,
					cp,
				)
			}

			confirmed = true

		case cache.After:
			if cp > seg.End && !confirmed {
				var err error

				cp, err = f.read(ctx, types, cp, fn)
				if err != nil {
					return cp, err
				}

				confirmed = true
				continue
			}

			select {
			case <-ctx.Done():
				return cp, ctx.Err()
			case <-changed:
			case <-f.closed.Chan():
				return cp, errFeedClosed
			}
		}
	}
}

// read delivers events to fn by reading from the source, beginning at the
// given offset.
//
// Only events of the given types are delivered, in contiguous segments. It
// returns once the consumer's checkpoint reaches the cached segment, the read
// reaches the end of the stream, or the read is stopped.
//
// It returns the offset at which delivery stopped.
func (f *feed) read(
	ctx context.Context,
	types *uuidcollection.Set,
	cp uint64,
	fn func(Segment) error,
) (uint64, error) {
	if !f.startRead() {
		return cp, errFeedClosed
	}
	defer f.reads.Done()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Closing the feed cancels the read, as the source may no longer be
	// able to serve it.
	stop := context.AfterFunc(f.ctx, cancel)
	defer stop()

	var fnErr error

	err := f.Source.Read(
		ctx,
		f.ID,
		types,
		cp,
		func(seg Segment) error {
			if err := fn(seg); err != nil {
				fnErr = err
				return err
			}

			cp = seg.End

			// The cached segment may have moved on during the read, so it is
			// compared against after every segment delivered.
			if c, _ := f.cache.Segment().Load(); cp >= c.Begin {
				return errCaughtUp
			}

			return nil
		},
	)

	switch {
	case fnErr != nil:
		// fn's error is returned even if the feed was closed during the read.
		// Otherwise, the consumer would continue on another feed from the
		// beginning of the segment, delivering again the events that fn had
		// processed before it failed.
		return cp, fnErr

	case errors.Is(err, errCaughtUp):
		return cp, nil

	case f.closed.IsSet():
		return cp, errFeedClosed

	case err != nil:
		// A stream that the source does not recognize or that ends before the
		// offset, or a segment that is invalid, is unlikely to be read
		// successfully soon.
		if errors.Is(err, source.ErrUnrecognizedStream) ||
			errors.Is(err, source.ErrOffsetOutOfRange) ||
			errors.Is(err, source.ErrInvalidSegment) {
			return cp, xerrors.Deterministic(err)
		}

		return cp, err

	default:
		return cp, nil
	}
}

// startRead records that a read from the source is beginning and returns
// true. If the feed is closed, it returns false instead.
func (f *feed) startRead() bool {
	f.m.Lock()
	defer f.m.Unlock()

	if f.closed.IsSet() {
		return false
	}

	f.reads.Add(1)
	return true
}
