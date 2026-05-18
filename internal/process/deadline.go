package process

import (
	"context"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// NextDeadlineByCorrelationID returns the time of the earliest deadline with
// the given correlation ID.
func NextDeadlineByCorrelationID(
	ctx context.Context,
	q database.Querier,
	correlationID *uuidpb.UUID,
) (time.Time, bool, error) {
	_ = ctx
	_ = q
	_ = correlationID
	return time.Time{}, false, nil
}
