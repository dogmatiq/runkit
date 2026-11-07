package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
)

// preparedEnvelope contains the metadata and already serialized bytes of an
// effect. Persistence must not invoke application serialization.
type preparedEnvelope struct {
	envelope *envelopepb.Envelope
	data     []byte
}

// preparedEffects contains all results of an application invocation. Preparation
// completes before any result is written, so a preparation failure can record a
// deadline retry in the same transaction without committing partial effects.
type preparedEffects struct {
	state     []byte
	mutated   bool
	ended     bool
	commands  []preparedEnvelope
	deadlines []preparedEnvelope
}

func prepareEffects(scope *messageScope) (*preparedEffects, error) {
	effects, err := xerrors.RecoverT(func() (*preparedEffects, error) {
		effects := &preparedEffects{mutated: scope.mutated, ended: scope.ended}
		var err error
		if effects.mutated && !effects.ended {
			effects.state, err = scope.root.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("unable to marshal process state: %w", err)
			}
		}
		effects.commands, err = prepareEnvelopes(scope.commandPacker)
		if err != nil {
			return nil, err
		}
		if !effects.ended {
			effects.deadlines, err = prepareEnvelopes(scope.deadlinePacker)
			if err != nil {
				return nil, err
			}
		}
		return effects, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: unable to prepare process effects: %v", messagepump.ErrFailed, err)
	}
	return effects, nil
}

func prepareEnvelopes(packer *envelope.EffectPacker) ([]preparedEnvelope, error) {
	envelopes, ok := packer.Seal()
	if !ok {
		return nil, nil
	}
	var prepared []preparedEnvelope
	for env := range envelopes.All() {
		data, err := env.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("unable to marshal effect envelope: %w", err)
		}
		prepared = append(prepared, preparedEnvelope{env, data})
	}
	return prepared, nil
}

func (e *preparedEffects) persist(ctx context.Context, tx *sql.Tx, handlerKey *uuidpb.UUID, instanceID string) error {
	if err := addCommandsToQueue(ctx, tx, e.commands); err != nil {
		return err
	}
	if e.ended {
		return endInstance(ctx, tx, handlerKey, instanceID)
	}
	if e.mutated {
		if err := saveInstance(ctx, tx, handlerKey, instanceID, e.state); err != nil {
			return err
		}
	}
	return persistDeadlines(ctx, tx, e.deadlines)
}
