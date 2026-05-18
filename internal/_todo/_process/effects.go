package process

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/routes"
)

// persistEffects writes the side-effects accumulated in s to tx.
//
//   - Mutate (s.mutated): UPDATE state and bump mutation_count; refresh
//     the worker's cached lastSeenMutationCount so the next iteration
//     skips the reload.
//   - End (s.ended): UPDATE state=NULL, ended=true, plus DELETE all
//     pending deadlines for this instance.
//   - ExecuteCommand: INSERT commandqueue.commands. Random IDs are correct
//     because the atomic-tx invariant guarantees no prior emissions
//     survive a rolled-back attempt.
//   - ScheduleDeadline: INSERT deadlines with scheduled_for and
//     next_attempt_at both set to the requested time.
func (w *worker) persistEffects(ctx context.Context, tx *sql.Tx, s *scope) error {
	id := w.c.Config.Identity()
	handlerKey := id.GetKey().AsString()

	envelopes, hasEffects := s.packer.Seal()
	if hasEffects {
		executes := routes.MessageTypes(w.c.Config, config.ExecutesCommandRouteType)
		schedules := routes.MessageTypes(w.c.Config, config.SchedulesDeadlineRouteType)

		header := envelopes.GetHeader()
		correlationID := header.GetCorrelationId().AsString()

		for _, body := range envelopes.GetBodies() {
			env := envelopepb.NewEnvelopeBuilder().
				WithHeader(header).
				WithBody(body).
				Build()
			envData, err := env.MarshalBinary()
			if err != nil {
				return err
			}

			typeID := body.GetMessage().GetTypeId().AsString()
			messageID := body.GetMessageId().AsString()

			if scheduled := body.GetScheduledFor(); scheduled != nil {
				if !slices.Contains(schedules, typeID) {
					return fmt.Errorf(
						"handler %q scheduled deadline type %s, which is not in its routes",
						id.GetName(), typeID,
					)
				}
				when := scheduled.AsTime()
				if _, err := tx.ExecContext(
					ctx,
					`INSERT INTO deadlines (
						id, handler_key, instance_id,
						scheduled_for, next_attempt_at, envelope
					 ) VALUES ($1, $2, $3, $4, $5, $6)`,
					messageID,
					handlerKey,
					s.instanceID,
					when,
					when,
					envData,
				); err != nil {
					return err
				}
				continue
			}

			if !slices.Contains(executes, typeID) {
				return fmt.Errorf(
					"handler %q executed command type %s, which is not in its routes",
					id.GetName(), typeID,
				)
			}

			if err := commandqueue.Enqueue(
				ctx, tx,
				messageID, typeID, correlationID, "", envData,
			); err != nil {
				return err
			}
		}
	}

	switch {
	case s.ended:
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE process_instances
			 SET state = NULL, ended = true
			 WHERE handler_key = $1 AND instance_id = $2`,
			handlerKey, s.instanceID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM deadlines WHERE handler_key = $1 AND instance_id = $2`,
			handlerKey, s.instanceID,
		); err != nil {
			return err
		}

	case s.mutated:
		stateBytes, err := w.root.MarshalBinary()
		if err != nil {
			return err
		}
		var newCount int64
		if err := tx.QueryRowContext(
			ctx,
			`UPDATE process_instances
			 SET state = $1, mutation_count = mutation_count + 1
			 WHERE handler_key = $2 AND instance_id = $3
			 RETURNING mutation_count`,
			stateBytes, handlerKey, s.instanceID,
		).Scan(&newCount); err != nil {
			return err
		}
		w.lastSeenMutationCount = newCount
	}

	return nil
}
