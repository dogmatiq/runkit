package commandexecutor

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// readLimit is the maximum number of events read per query.
const readLimit = 100

// errSatisfied stops reading events once an event observer is satisfied.
var errSatisfied = errors.New("event observer satisfied")

// observe passes each event that has the given correlation ID to the given
// observers, first those already recorded and then each one recorded later.
//
// It returns nil once an observer is satisfied, the observer's error if one
// returns an error, and [dogma.ErrEventObserverNotSatisfied] if ctx's deadline
// passes first.
func (e *Executor) observe(
	ctx context.Context,
	correlationID uuid.UUID,
	observers []dogma.EventObserverOption,
) error {
	err := e.observeUntilSatisfied(ctx, correlationID, observers)

	if errors.Is(err, errSatisfied) {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return dogma.ErrEventObserverNotSatisfied
	}

	return err
}

// observeUntilSatisfied passes events to the observers until one is
// satisfied, in which case it returns [errSatisfied], or until it fails or ctx
// is canceled.
func (e *Executor) observeUntilSatisfied(
	ctx context.Context,
	correlationID uuid.UUID,
	observers []dogma.EventObserverOption,
) error {
	types := &uuidset.Set{}
	for _, obs := range observers {
		types.Add(obs.EventType().ID())
	}

	var appended cosyne.Signal

	detach := e.Node.AttachSignal(
		ctx,
		"events_appended.correlation."+correlationID.String(),
		&appended,
	)
	defer detach()

	nextOffsets := map[uuid.UUID]uint64{}

	for {
		for {
			n, err := e.readCorrelatedEvents(ctx, correlationID, types.R(), observers, nextOffsets)
			if err != nil {
				return err
			}

			if n < readLimit {
				break
			}
		}

		if err := appended.WaitContext(ctx); err != nil {
			return err
		}
	}
}

// readCorrelatedEvents passes to the observers the events with the given
// correlation ID and types that come after the offset in nextOffsets on each
// stream, at most [readLimit] of them. It records the offset that follows each
// event it passes in nextOffsets, and returns the number of events read.
func (e *Executor) readCorrelatedEvents(
	ctx context.Context,
	correlationID uuid.UUID,
	types *uuidset.SetR,
	observers []dogma.EventObserverOption,
	nextOffsets map[uuid.UUID]uint64,
) (int, error) {
	var (
		streamIDs []string
		offsets   []uint64
	)

	for streamID, offset := range nextOffsets {
		streamIDs = append(streamIDs, streamID.String())
		offsets = append(offsets, offset)
	}

	rows, err := e.DB.QueryContext(
		ctx,
		`WITH offsets AS (
			SELECT *
			FROM unnest(
				$1::uuid[],
				$2::bigint[]
			) AS o(
				stream_id,
				next_offset
			)
		)
		SELECT
			e.stream_id,
			e.stream_offset,
			e.envelope
		FROM runkit.events AS e
		LEFT JOIN offsets AS o
			ON o.stream_id = e.stream_id
		WHERE e.stream_offset >= COALESCE(o.next_offset, 0)
			AND e.correlation_id = $3
			AND e.message_type_id = ANY($4)
		ORDER BY e.stream_offset
		LIMIT $5`,
		streamIDs,
		offsets,
		xsql.UUID(correlationID),
		xsql.UUIDSeq(types.All()),
		readLimit,
	)
	if err != nil {
		return 0, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	n := 0

	for rows.Next() {
		var (
			streamID uuid.UUID
			offset   uint64
			env      = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			xsql.UUID(&streamID),
			&offset,
			xsql.Envelope(env),
		); err != nil {
			return n, fmt.Errorf("unable to scan event envelope: %w", err)
		}

		event, err := envelope.Unpack[dogma.Event](env)
		if err != nil {
			return n, err
		}

		for _, obs := range observers {
			satisfied, err := obs.Observer()(ctx, event)
			if err != nil {
				return n, err
			}

			if satisfied {
				return n, errSatisfied
			}
		}

		nextOffsets[streamID] = offset + 1
		n++
	}

	if err := rows.Err(); err != nil {
		return n, fmt.Errorf("unable to iterate over event rows: %w", err)
	}

	return n, nil
}
