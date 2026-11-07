package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

type claimedDeadline struct {
	deadlineCandidate
	data     []byte
	failures uint64
}

// claim first tries the discovery hint. If it has been consumed, postponed or
// locked, it finds other currently available work rather than rediscovering the
// same locked ID. Payload bytes are loaded only for the final claim.
func (c *DeadlineConsumer) claim(ctx context.Context, tx *sql.Tx, hint *deadlineCandidate, active []string) (*claimedDeadline, error) {
	if hint != nil {
		delivery, err := c.claimID(ctx, tx, hint.messageID, active)
		if err != nil || delivery != nil {
			return delivery, err
		}
	}
	for ctx.Err() == nil {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT deadline_claim_probe`); err != nil {
			return nil, err
		}
		candidate := &deadlineCandidate{messageID: &uuidpb.UUID{}}
		err := tx.QueryRowContext(ctx, `SELECT d.message_id, d.instance_id, d.deliver_at
		FROM unnest($2::uuid[]) AS r(type_id)
		CROSS JOIN LATERAL (
			SELECT q.message_id, q.instance_id, q.deliver_at
			FROM process.deadlines AS q
			WHERE q.handler_key = $1 AND q.message_type_id = r.type_id
				AND q.instance_id <> ALL($3::text[]) AND q.deliver_at <= $4
			ORDER BY q.deliver_at, q.message_id LIMIT 1
			FOR UPDATE SKIP LOCKED
		) AS d
		ORDER BY d.deliver_at, d.message_id LIMIT 1`, xsql.UUID(c.Sink.Identity.GetKey()),
			xsql.UUIDSeq(c.DeadlineTypeIDs.All()), active, time.Now()).Scan(xsql.UUID(candidate.messageID), &candidate.instanceID, &candidate.deliverAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("unable to find available deadline: %w", err)
		}
		// LATERAL can briefly lock one head per routed type. Roll back the
		// probe so no unselected row remains locked while application code runs.
		if _, releaseErr := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT deadline_claim_probe`); releaseErr != nil {
			return nil, releaseErr
		}
		if _, releaseErr := tx.ExecContext(ctx, `RELEASE SAVEPOINT deadline_claim_probe`); releaseErr != nil {
			return nil, releaseErr
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		delivery, err := c.claimID(ctx, tx, candidate.messageID, active)
		if err != nil || delivery != nil {
			return delivery, err
		}
		// Another attempt may win between releasing the probe and claiming
		// its chosen ID. Repeat the bounded-memory probe against current state.
	}
	return nil, ctx.Err()
}

func (c *DeadlineConsumer) claimID(ctx context.Context, tx *sql.Tx, id *uuidpb.UUID, active []string) (*claimedDeadline, error) {
	delivery := &claimedDeadline{deadlineCandidate: deadlineCandidate{messageID: &uuidpb.UUID{}}}
	err := tx.QueryRowContext(ctx, `SELECT message_id, instance_id, deliver_at, envelope, failures
	FROM process.deadlines WHERE message_id = $1 AND handler_key = $2
		AND message_type_id = ANY($3) AND deliver_at <= $4 AND instance_id <> ALL($5::text[])
	FOR UPDATE SKIP LOCKED`, xsql.UUID(id), xsql.UUID(c.Sink.Identity.GetKey()),
		xsql.UUIDSeq(c.DeadlineTypeIDs.All()), time.Now(), active).Scan(xsql.UUID(delivery.messageID), &delivery.instanceID, &delivery.deliverAt, &delivery.data, &delivery.failures)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("unable to claim process deadline: %w", err)
	}
	return delivery, nil
}
