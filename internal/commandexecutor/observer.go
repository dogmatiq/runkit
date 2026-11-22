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

// errEngineStopped is returned when the engine stops while the executor waits
// for events. The node no longer receives the signal that wakes the executor,
// so waiting longer can't pass any more events to the observers.
var errEngineStopped = errors.New("engine is not running")

// observe passes the events that have the given correlation ID to the
// observers, first the events already recorded and then each event recorded
// later.
//
// It returns nil once an observer is satisfied.
func (e *Executor) observe(
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
			n, satisfied, err := e.readCorrelatedEvents(
				ctx,
				correlationID,
				types.R(),
				observers,
				nextOffsets,
			)
			if err != nil {
				return err
			}

			if satisfied {
				return nil
			}

			if n < readLimit {
				break
			}
		}

		select {
		case <-appended.Chan():
		case <-e.EngineStopped:
			return errEngineStopped
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// readCorrelatedEvents reads up to [readLimit] events that have the given
// correlation ID and types, and passes each event to the observers.
//
// On each stream, it reads only the events at or after the offset in
// nextOffsets, and it updates nextOffsets as it passes each event.
//
// It returns the number of events it passed, and whether an observer is
// satisfied. It stops as soon as an observer is satisfied.
func (e *Executor) readCorrelatedEvents(
	ctx context.Context,
	correlationID uuid.UUID,
	types *uuidset.SetR,
	observers []dogma.EventObserverOption,
	nextOffsets map[uuid.UUID]uint64,
) (int, bool, error) {
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
		return 0, false, fmt.Errorf("unable to query events: %w", err)
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
			return n, false, fmt.Errorf("unable to scan event envelope: %w", err)
		}

		event, err := envelope.Unpack[dogma.Event](env)
		if err != nil {
			return n, false, err
		}

		for _, obs := range observers {
			satisfied, err := obs.Observer()(ctx, event)
			if err != nil {
				return n, false, err
			}

			if satisfied {
				return n, true, nil
			}
		}

		nextOffsets[streamID] = offset + 1
		n++
	}

	if err := rows.Err(); err != nil {
		return n, false, fmt.Errorf("unable to iterate over event rows: %w", err)
	}

	return n, false, nil
}
