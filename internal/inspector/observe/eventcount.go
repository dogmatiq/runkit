package observe

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// eventRange is a range of offsets of one event stream.
type eventRange struct {
	// StreamID is the ID of the event stream.
	StreamID *uuidpb.UUID

	// Begin is the offset of the first event in the range.
	Begin uint64

	// End is the offset after the last event in the range.
	End uint64
}

// countEventsByType returns the number of events of each type in the given
// ranges, keyed by type ID.
//
// It reads each range with the index on each event's stream and offset. It
// reads at most [countLimit] events of each range, the newest, so it
// undercounts a longer range.
func (d *Database) countEventsByType(
	ctx context.Context,
	ranges []eventRange,
) (map[string]int, error) {
	counts := map[string]int{}

	if len(ranges) == 0 {
		return counts, nil
	}

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			for _, r := range ranges {
				begin := r.Begin
				if r.End-begin > countLimit {
					begin = r.End - countLimit
				}

				rows, err := tx.QueryContext(
					ctx,
					`SELECT e.message_type_id, count(*)
					FROM eventstream.events AS e
					WHERE e.stream_id = $1
						AND e.stream_offset >= $2
						AND e.stream_offset < $3
					GROUP BY e.message_type_id`,
					xsql.UUID(r.StreamID),
					int64(begin),
					int64(r.End),
				)
				if err != nil {
					return err
				}

				for rows.Next() {
					var (
						typeID uuidpb.UUID
						n      int
					)

					if err := rows.Scan(xsql.UUID(&typeID), &n); err != nil {
						rows.Close()
						return err
					}

					counts[typeID.AsString()] += n
				}

				if err := rows.Close(); err != nil {
					return err
				}

				if err := rows.Err(); err != nil {
					return err
				}
			}

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to count events by type: %w", err)
	}

	return counts, nil
}
